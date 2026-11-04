package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// resolveProbeTimeout bounds one extraction. Resolving is on the client's
// critical path (a channel switch, a preload), so it must fail visibly instead
// of hanging the player.
const resolveProbeTimeout = 45 * time.Second

type resolveParams struct {
	Config json.RawMessage `json:"config"`
	Item   struct {
		ExternalID string `json:"external_id"`
		DurationMS int64  `json:"duration_ms"`
	} `json:"item"`
	// StartAtMS is where in the item playback begins: a channel tunes into the
	// middle of a program. It is only honoured by a mode the plugin produces
	// itself (remux); a whole-file or pair URL is seeked by the player.
	StartAtMS int64 `json:"start_at_ms"`
	// Modes is what the host's player can play, best first. Absent means
	// "single or pair" — the two modes that cost the plugin nothing. Remux is
	// only ever offered when the host names it, because it is the only mode
	// where the plugin touches media at all.
	Modes []string `json:"modes"`
}

// Resolve modes. A plugin must never assume the expensive one.
const (
	modeSingle = "single" // one URL, one file or manifest, both tracks inside
	modePair   = "pair"   // one video URL + one audio URL, merged by the player
	modeRemux  = "remux"  // the plugin assembles a stream itself
)

// resolveModes is the host's order of preference, without duplicates or names
// this plugin does not know. Absent means the two modes that need no processing
// here, single first (the smallest number of moving parts).
func resolveModes(requested []string) []string {
	if len(requested) == 0 {
		return []string{modeSingle, modePair}
	}
	seen := map[string]bool{}
	modes := make([]string, 0, len(requested))
	for _, m := range requested {
		switch m {
		case modeSingle, modePair, modeRemux:
			if !seen[m] {
				seen[m] = true
				modes = append(modes, m)
			}
		}
	}
	return modes
}

type resolveResult struct {
	// Mode says which shape this answer is: "single" (URL), "pair" (Sources),
	// or "remux" (URL produced by the plugin as it plays).
	Mode string `json:"mode,omitempty"`
	// URL is a complete playable stream, used by single and remux.
	URL       string            `json:"url,omitempty"`
	Mime      string            `json:"mime,omitempty"`
	ExpiresAt *int64            `json:"expires_at,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
	// Sources is a video track and an audio track for the player to merge. The
	// plugin does no work here: both URLs point at the source's own files.
	Sources []resolveSource `json:"sources,omitempty"`
	// Live reports a URL that is produced as it plays: a player may seek back
	// into what already exists, but forward seeking waits for production.
	Live bool `json:"live,omitempty"`
}

// resolveSource is one track of a pair.
type resolveSource struct {
	Type      string            `json:"type"` // "video" or "audio"
	URL       string            `json:"url"`
	Mime      string            `json:"mime,omitempty"`
	ExpiresAt *int64            `json:"expires_at,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
}

type mediaInfo struct {
	ID          string            `json:"id"`
	Title       string            `json:"title"`
	Duration    float64           `json:"duration"`
	Ext         string            `json:"ext"`
	URL         string            `json:"url"`
	LiveStatus  string            `json:"live_status"`
	HTTPHeaders map[string]string `json:"http_headers"`
	Formats     []mediaFormat     `json:"formats"`
}

type mediaFormat struct {
	FormatID    string            `json:"format_id"`
	Ext         string            `json:"ext"`
	VCodec      string            `json:"vcodec"`
	ACodec      string            `json:"acodec"`
	Width       int               `json:"width"`
	Height      int               `json:"height"`
	FPS         float64           `json:"fps"`
	TBR         float64           `json:"tbr"`
	ABR         float64           `json:"abr"`
	Protocol    string            `json:"protocol"`
	URL         string            `json:"url"`
	MimeType    string            `json:"mime_type"`
	HTTPHeaders map[string]string `json:"http_headers"`
}

func (s *server) resolve(raw json.RawMessage) (any, error) {
	var params resolveParams
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, errf(codeConfig, "media.resolve params: %v", err)
	}
	var cfg sourceConfig
	if err := json.Unmarshal(params.Config, &cfg); err != nil {
		return nil, errf(codeConfig, "media.resolve config: %v", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), resolveProbeTimeout)
	defer cancel()
	started := s.now()
	info, cached, err := s.extract(ctx, params.Item.ExternalID)
	if err != nil {
		return nil, err
	}
	how := "ran"
	if cached {
		how = "cached"
	}
	requestLog(nil, fmt.Sprintf("resolve %s: extraction %s in %s",
		params.Item.ExternalID, how, s.now().Sub(started).Round(time.Millisecond)), 0, 0)
	if info.LiveStatus == "is_live" || info.LiveStatus == "is_upcoming" {
		return nil, errf(codeNotFound, "%s is live or upcoming, which this plugin does not serve", params.Item.ExternalID)
	}

	plan, err := planPlayback(info, cfg)
	if err != nil {
		return nil, err
	}
	startAt := float64(params.StartAtMS) / 1000

	// The host's order is a preference, not a set: it says which shape its player
	// would rather open (docs/PLUGINS.md "Modes"). Honouring it is what lets a
	// host choose between the best picture (`pair`, free, but only for a player
	// that merges) and the smallest number of moving parts (`single`), which is
	// not always the same thing: on this source `single` is a muxed file capped
	// below the separated video track.
	askModes := resolveModes(params.Modes)
	for _, mode := range askModes {
		switch mode {
		case modeSingle:
			if plan.direct == nil {
				continue
			}
			res := directResult(*plan.direct)
			res.Mode = modeSingle
			return res, nil

		case modePair:
			if plan.video == nil || plan.audio == nil {
				continue
			}
			return pairResult(*plan.video, *plan.audio, plan.headers), nil

		case modeRemux:
			// The only mode where this process touches media: remux into HLS, one
			// playlist per track. Never reached unless the host asked for it.
			if len(plan.inputs()) == 0 {
				continue
			}
			openStarted := s.now()
			st, err := s.streams.open(ctx, plan.inputs(), startAt, plan.headers)
			if err != nil {
				return nil, err
			}
			requestLog(nil, fmt.Sprintf("resolve %s: stream ready in %s",
				params.Item.ExternalID, s.now().Sub(openStarted).Round(time.Millisecond)), 0, 0)
			return resolveResult{
				Mode: modeRemux,
				URL:  s.streams.url(st),
				Mime: "application/vnd.apple.mpegurl",
				// A playlist that grows to the end of the item, with one variant per
				// track.
				Live: true,
			}, nil
		}
	}

	// Nothing the host asked for can be produced, but a muxed file plays in any
	// player: answer it rather than leaving the item unplayable. A host that asks
	// for `pair,remux` and means "best picture", falling back only when the source
	// has no separate tracks, is the case this exists for.
	if plan.direct != nil {
		requestLog(nil, fmt.Sprintf("resolve %s: %s not available, answered single",
			params.Item.ExternalID, describeModes(askModes)), 0, 0)
		res := directResult(*plan.direct)
		res.Mode = modeSingle
		return res, nil
	}
	return nil, errf(codeUnsupported,
		"%s cannot be served in any mode this host accepts (%s); it needs a player that merges a video and an audio track (pair) or one that accepts a remuxed stream",
		params.Item.ExternalID, describeModes(askModes))
}

// extractionTTL is how long one item's extraction stays usable. What it carries
// are signed urls, good for hours, and the media info that decided the plan; ten
// minutes keeps a channel switch from extracting the same item again while never
// being the reason an expired url is tried.
const extractionTTL = 10 * time.Minute

// extraction is one item's answer, kept for extractionTTL.
type extraction struct {
	info mediaInfo
	at   time.Time
}

func (s *server) now() time.Time {
	if s.clock != nil {
		return s.clock()
	}
	return time.Now()
}

// extract returns the item's media info, running the extractor only when the
// answer is not already in hand. The second return says which happened, so the
// cost of a resolve can be read in the log instead of guessed at.
func (s *server) extract(ctx context.Context, externalID string) (mediaInfo, bool, error) {
	now := s.now()
	s.extractMu.Lock()
	if cached, ok := s.extracts[externalID]; ok && now.Sub(cached.at) < extractionTTL {
		s.extractMu.Unlock()
		return cached.info, true, nil
	}
	s.extractMu.Unlock()

	watchURL, err := watchURLFor(externalID)
	if err != nil {
		return mediaInfo{}, false, err
	}
	// -4 for the same reason as in catalog.go: an IPv6 address is challenged far
	// more often than an IPv4 one, and a challenge is not retryable.
	out, err := s.catalog.cmd(ctx, nil, "-j", "-4", "--no-warnings", "--no-progress", "--no-playlist", watchURL)
	if err != nil {
		return mediaInfo{}, false, classifyExtractorError(err)
	}
	var info mediaInfo
	if err := decodeJSON(out, &info); err != nil {
		return mediaInfo{}, false, errf(codeUnavailable, "extractor output is not media info: %v", err)
	}

	s.extractMu.Lock()
	if s.extracts == nil {
		s.extracts = map[string]extraction{}
	}
	// Bounded without a limit of its own: a stale answer is dropped on every
	// insert, so the map never holds more than the items resolved inside the ttl.
	for id, cached := range s.extracts {
		if now.Sub(cached.at) >= extractionTTL {
			delete(s.extracts, id)
		}
	}
	s.extracts[externalID] = extraction{info: info, at: now}
	s.extractMu.Unlock()
	return info, false, nil
}

func describeModes(modes []string) string {
	if len(modes) == 0 {
		return "no mode"
	}
	return strings.Join(modes, "+")
}

func pairResult(video, audio mediaFormat, fallback map[string]string) resolveResult {
	source := func(kind string, f mediaFormat) resolveSource {
		src := resolveSource{Type: kind, URL: f.URL, Mime: f.MimeType, Headers: f.HTTPHeaders}
		if src.Mime == "" {
			src.Mime = mimeForExt(f.Ext)
		}
		if len(src.Headers) == 0 {
			src.Headers = fallback
		}
		src.ExpiresAt = expiryOf(f.URL)
		return src
	}
	return resolveResult{
		Mode:    modePair,
		Sources: []resolveSource{source("video", video), source("audio", audio)},
	}
}

// playbackPlan is what a resolve decided to hand the player: one remote file, or
// a pair the plugin remuxes itself.
type playbackPlan struct {
	direct  *mediaFormat
	video   *mediaFormat
	audio   *mediaFormat
	headers map[string]string
}

func (p playbackPlan) inputs() []remuxInput {
	var in []remuxInput
	if p.video != nil {
		in = append(in, remuxInput{url: p.video.URL, selector: "v"})
	}
	if p.audio != nil {
		in = append(in, remuxInput{url: p.audio.URL, selector: "a"})
	}
	return in
}

func directResult(f mediaFormat) resolveResult {
	res := resolveResult{
		URL:     f.URL,
		Mime:    f.MimeType,
		Headers: f.HTTPHeaders,
	}
	if res.Mime == "" {
		res.Mime = mimeForExt(f.Ext)
	}
	if exp := expiryOf(f.URL); exp != nil {
		res.ExpiresAt = exp
	}
	return res
}

// planPlayback picks the streams to play. Preference is driven by what players
// actually decode everywhere: H.264 video and AAC audio, in that order, capped at
// the source's max_height (720 by default — the pair is streamed, so the cap is
// what keeps a channel cheap).
func planPlayback(info mediaInfo, cfg sourceConfig) (playbackPlan, error) {
	headers := info.HTTPHeaders

	if cfg.AudioOnly {
		if best := bestAudio(info.Formats); best != nil {
			return playbackPlan{direct: best, headers: headers}, nil
		}
		return playbackPlan{}, errf(codeNotFound, "no audio stream for %s", info.ID)
	}

	maxHeight := cfg.MaxHeight
	if maxHeight <= 0 {
		maxHeight = 720
	}
	// The tracks beside the muxed file are kept even when a muxed file exists:
	// a host that does not accept `single` (the client cannot merge, so it asks
	// for `remux`) would otherwise be left with a plan it cannot use at all — an
	// item whose only progressive format is 360p muxed became unplayable while
	// the same item's separate tracks were right there.
	video := bestVideo(info.Formats, maxHeight)
	audio := bestAudio(info.Formats)
	plan := playbackPlan{direct: bestMuxed(info.Formats, maxHeight), video: video, audio: audio, headers: headers}
	if video != nil && audio != nil {
		plan.headers = pickHeaders(video, audio, headers)
	}
	if plan.direct != nil {
		return plan, nil
	}
	if plan.video == nil || plan.audio == nil {
		// Audio-only content, or a video with no separate audio track: play
		// whatever single stream exists rather than nothing.
		if only := firstPlayable(info.Formats); only != nil {
			plan.direct = only
			return plan, nil
		}
		return playbackPlan{}, errf(codeNotFound, "no playable stream for %s", info.ID)
	}
	return plan, nil
}

func usable(f mediaFormat) bool {
	if f.URL == "" {
		return false
	}
	switch f.Protocol {
	case "https", "http":
		return true
	}
	return false
}

// bestMuxed is the one file a player that cannot merge can still open, capped
// like the separated tracks: a streamed channel should not pull 1080p because the
// only muxed format happens to be that tall while the cap says 480.
func bestMuxed(formats []mediaFormat, maxHeight int) *mediaFormat {
	if best := tallestMuxed(formats, maxHeight); best != nil {
		return best
	}
	// Nothing under the cap: a file above it still plays, and an item that plays
	// beats an item that honours a soft cap.
	return tallestMuxed(formats, 0)
}

func tallestMuxed(formats []mediaFormat, maxHeight int) *mediaFormat {
	var best *mediaFormat
	for i := range formats {
		f := formats[i]
		if !usable(f) || !hasVideo(f) || !hasAudio(f) {
			continue
		}
		if maxHeight > 0 && f.Height > maxHeight {
			continue
		}
		if best == nil || betterMuxed(f, *best) {
			best = &formats[i]
		}
	}
	return best
}

func betterMuxed(a, b mediaFormat) bool {
	if a.Height != b.Height {
		return a.Height > b.Height
	}
	return a.TBR > b.TBR
}

func firstPlayable(formats []mediaFormat) *mediaFormat {
	var best *mediaFormat
	for i := range formats {
		f := formats[i]
		if !usable(f) {
			continue
		}
		if best == nil || f.TBR > best.TBR {
			best = &formats[i]
		}
	}
	return best
}

func hasVideo(f mediaFormat) bool { return f.VCodec != "" && f.VCodec != "none" }
func hasAudio(f mediaFormat) bool { return f.ACodec != "" && f.ACodec != "none" }

// bestVideo prefers a codec every engine decodes, then the tallest stream within
// the cap, then the highest bitrate.
// videoCandidates lists the formats that carry video alone and fit the height
// cap (no cap when maxHeight is zero or less).
func videoCandidates(formats []mediaFormat, maxHeight int) []mediaFormat {
	var candidates []mediaFormat
	for _, f := range formats {
		if !usable(f) || !hasVideo(f) || hasAudio(f) {
			continue
		}
		if maxHeight > 0 && f.Height > maxHeight {
			continue
		}
		candidates = append(candidates, f)
	}
	return candidates
}

func bestVideo(formats []mediaFormat, maxHeight int) *mediaFormat {
	candidates := videoCandidates(formats, maxHeight)
	if len(candidates) == 0 && maxHeight > 0 {
		// Nothing fits the cap. The cap is a bandwidth preference, not a
		// capability: an item whose lowest format is above it is still better
		// played than left as a hole in the channel, and the smallest of those is
		// the closest to what was asked for.
		candidates = videoCandidates(formats, 0)
		smallest := -1
		for i := range candidates {
			if smallest < 0 || candidates[i].Height < candidates[smallest].Height {
				smallest = i
			}
		}
		if smallest < 0 {
			return nil
		}
		return &candidates[smallest]
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		ci, cj := videoCodecRank(candidates[i].VCodec), videoCodecRank(candidates[j].VCodec)
		if ci != cj {
			return ci < cj
		}
		if candidates[i].Height != candidates[j].Height {
			return candidates[i].Height > candidates[j].Height
		}
		if candidates[i].FPS != candidates[j].FPS {
			return candidates[i].FPS > candidates[j].FPS
		}
		return candidates[i].TBR > candidates[j].TBR
	})
	return &candidates[0]
}

// bestAudio prefers AAC in an mp4 container, which remuxes into HLS without a
// second thought; Opus in mp4 is the fallback, and anything else after that.
func bestAudio(formats []mediaFormat) *mediaFormat {
	var candidates []mediaFormat
	for _, f := range formats {
		if !usable(f) || !hasAudio(f) || hasVideo(f) {
			continue
		}
		candidates = append(candidates, f)
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		ci, cj := audioCodecRank(candidates[i]), audioCodecRank(candidates[j])
		if ci != cj {
			return ci < cj
		}
		return audioBitrate(candidates[i]) > audioBitrate(candidates[j])
	})
	return &candidates[0]
}

func videoCodecRank(codec string) int {
	switch {
	case strings.HasPrefix(codec, "avc1"), strings.HasPrefix(codec, "h264"):
		return 0
	case strings.HasPrefix(codec, "av01"), strings.HasPrefix(codec, "av1"):
		return 1
	case strings.HasPrefix(codec, "vp9"), strings.HasPrefix(codec, "vp09"):
		return 2
	default:
		return 3
	}
}

func audioCodecRank(f mediaFormat) int {
	switch {
	case strings.Contains(f.ACodec, "mp4a"):
		return 0
	case strings.Contains(f.ACodec, "opus"):
		return 1
	default:
		return 2
	}
}

func audioBitrate(f mediaFormat) float64 {
	if f.ABR > 0 {
		return f.ABR
	}
	return f.TBR
}

func pickHeaders(video, audio *mediaFormat, fallback map[string]string) map[string]string {
	if audio != nil && len(audio.HTTPHeaders) > 0 {
		return audio.HTTPHeaders
	}
	if video != nil && len(video.HTTPHeaders) > 0 {
		return video.HTTPHeaders
	}
	return fallback
}

func mimeForExt(ext string) string {
	switch ext {
	case "mp4", "m4v":
		return "video/mp4"
	case "m4a":
		return "audio/mp4"
	case "webm":
		return "video/webm"
	case "m3u8":
		return "application/vnd.apple.mpegurl"
	default:
		return ""
	}
}

// expiryOf reads the expiry the site signs into its media URLs (Unix seconds).
func expiryOf(raw string) *int64 {
	u, err := url.Parse(raw)
	if err != nil {
		return nil
	}
	for _, key := range []string{"expire", "expires"} {
		if v := u.Query().Get(key); v != "" {
			if secs, err := strconv.ParseInt(v, 10, 64); err == nil && secs > 0 {
				ms := secs * 1000
				return &ms
			}
		}
	}
	return nil
}

// watchURLFor turns a catalog item id into a page the extractor understands.
func watchURLFor(externalID string) (string, error) {
	id := strings.TrimSpace(externalID)
	if id == "" {
		return "", errf(codeConfig, "item.external_id is required")
	}
	if strings.HasPrefix(id, "http://") || strings.HasPrefix(id, "https://") {
		return id, nil
	}
	if !validVideoID(id) {
		return "", errf(codeConfig, "item.external_id %q is not a video id", externalID)
	}
	return "https://www.youtube.com/watch?v=" + id, nil
}

// validVideoID accepts the site's id alphabet and length.
func validVideoID(id string) bool {
	if len(id) != 11 {
		return false
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
		default:
			return false
		}
	}
	return true
}
