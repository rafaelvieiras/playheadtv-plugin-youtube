package main

import (
	"context"
	"encoding/json"
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

type modeSet map[string]bool

func resolveModes(requested []string) modeSet {
	set := modeSet{}
	if len(requested) == 0 {
		// No request means the two modes that need no processing here.
		set[modeSingle] = true
		set[modePair] = true
		return set
	}
	for _, m := range requested {
		switch m {
		case modeSingle, modePair, modeRemux:
			set[m] = true
		}
	}
	return set
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
	watchURL, err := watchURLFor(params.Item.ExternalID)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), resolveProbeTimeout)
	defer cancel()
	// -4 for the same reason as in catalog.go: an IPv6 address is challenged far
	// more often than an IPv4 one, and a challenge is not retryable.
	out, err := s.catalog.cmd(ctx, nil, "-j", "-4", "--no-warnings", "--no-progress", "--no-playlist", watchURL)
	if err != nil {
		return nil, classifyExtractorError(err)
	}
	var info mediaInfo
	if err := decodeJSON(out, &info); err != nil {
		return nil, errf(codeUnavailable, "extractor output is not media info: %v", err)
	}
	if info.LiveStatus == "is_live" || info.LiveStatus == "is_upcoming" {
		return nil, errf(codeNotFound, "%s is live or upcoming, which this plugin does not serve", params.Item.ExternalID)
	}

	plan, err := planPlayback(info, cfg)
	if err != nil {
		return nil, err
	}
	modes := resolveModes(params.Modes)
	startAt := float64(params.StartAtMS) / 1000

	// Best case, and free: the source already has a file with both tracks. The
	// player fetches it from the source itself and seeks with a range request.
	if plan.direct != nil && modes[modeSingle] {
		res := directResult(*plan.direct)
		res.Mode = modeSingle
		return res, nil
	}

	// Next best, also free: two files the player merges itself. Nothing is
	// downloaded, remuxed or served here.
	if plan.video != nil && plan.audio != nil && modes[modePair] {
		return pairResult(*plan.video, *plan.audio, plan.headers), nil
	}

	// The fallback, and the only mode where this process touches media: remux
	// into one continuous fragmented MP4 body. Never reached unless the host
	// asked for it.
	if modes[modeRemux] && len(plan.inputs()) > 0 {
		st, err := s.streams.open(ctx, plan.inputs(), startAt, plan.headers)
		if err != nil {
			return nil, err
		}
		return resolveResult{
			Mode: modeRemux,
			URL:  s.streams.url(st),
			Mime: "video/mp4",
			// One continuous body that only ever moves forward: a player must not
			// try to seek or resume it.
			Live: true,
		}, nil
	}

	return nil, errf(codeUnsupported,
		"%s has no single file and this host accepts only %s; it needs a player that merges a video and an audio track (pair) or one that accepts a remuxed stream",
		params.Item.ExternalID, describeModes(modes))
}

func describeModes(modes modeSet) string {
	names := make([]string, 0, len(modes))
	for _, m := range []string{modeSingle, modePair, modeRemux} {
		if modes[m] {
			names = append(names, m)
		}
	}
	if len(names) == 0 {
		return "no mode"
	}
	return strings.Join(names, "+")
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

	if muxed := bestMuxed(info.Formats); muxed != nil {
		return playbackPlan{direct: muxed, headers: headers}, nil
	}

	maxHeight := cfg.MaxHeight
	if maxHeight <= 0 {
		maxHeight = 720
	}
	video := bestVideo(info.Formats, maxHeight)
	audio := bestAudio(info.Formats)
	if video == nil || audio == nil {
		// Audio-only content, or a video with no separate audio track: play
		// whatever single stream exists rather than nothing.
		if only := firstPlayable(info.Formats); only != nil {
			return playbackPlan{direct: only, headers: headers}, nil
		}
		return playbackPlan{}, errf(codeNotFound, "no playable stream for %s", info.ID)
	}
	return playbackPlan{video: video, audio: audio, headers: pickHeaders(video, audio, headers)}, nil
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

func bestMuxed(formats []mediaFormat) *mediaFormat {
	var best *mediaFormat
	for i := range formats {
		f := formats[i]
		if !usable(f) || !hasVideo(f) || !hasAudio(f) {
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
func bestVideo(formats []mediaFormat, maxHeight int) *mediaFormat {
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
