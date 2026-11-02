package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// listingTTL is how long a fetched listing stays usable. A listing is a page of
// the site's own list, and a backend sync asks for one page per call right after
// another; re-running the extraction for every page would be pointless and slow.
const listingTTL = 10 * time.Minute

// maxListingItems bounds one extraction. The host pages through what we return,
// so this is "how much of a channel this plugin will ever look at in one sync".
const maxListingItems = 500

// maxListingItemsLimit bounds the config override.
const maxListingItemsLimit = 5000

// sourceConfig is one source's stored configuration, verbatim from
// media_source.plugin_config_json.
type sourceConfig struct {
	URL           string `json:"url"`
	Kind          string `json:"kind,omitempty"`
	MaxItems      int    `json:"max_items,omitempty"`
	IncludeShorts bool   `json:"include_shorts,omitempty"`
	SeriesTitle   string `json:"series_title,omitempty"`
	MaxHeight     int    `json:"max_height,omitempty"`
	AudioOnly     bool   `json:"audio_only,omitempty"`
}

func (c sourceConfig) validate() error {
	if strings.TrimSpace(c.URL) == "" {
		return errf(codeConfig, "config.url is required: the channel, playlist or video this source lists")
	}
	switch c.Kind {
	case "", "auto", "channel", "playlist", "video":
	default:
		return errf(codeConfig, "config.kind %q is not one of auto, channel, playlist, video", c.Kind)
	}
	if c.MaxItems < 0 {
		return errf(codeConfig, "config.max_items must not be negative")
	}
	return nil
}

func (c sourceConfig) maxItems() int {
	switch {
	case c.MaxItems <= 0:
		return maxListingItems
	case c.MaxItems > maxListingItemsLimit:
		return maxListingItemsLimit
	default:
		return c.MaxItems
	}
}

// catalogItem is one row of a listing, in the shape the host's catalog sync
// accepts (backend/internal/api/media_sources.go, catalogSyncItem).
type catalogItem struct {
	ExternalID       string `json:"external_id"`
	Title            string `json:"title"`
	Kind             string `json:"kind"`
	DurationMS       int64  `json:"duration_ms"`
	SeriesTitle      string `json:"series_title,omitempty"`
	ProductionYear   *int   `json:"production_year,omitempty"`
	SourceModifiedAt *int64 `json:"source_modified_at,omitempty"`
}

// flatPlaylist is the subset of the extractor's flat listing this plugin reads.
type flatPlaylist struct {
	Type         string      `json:"_type"`
	ID           string      `json:"id"`
	Title        string      `json:"title"`
	Channel      string      `json:"channel"`
	Uploader     string      `json:"uploader"`
	LiveStatus   string      `json:"live_status"`
	Availability string      `json:"availability"`
	Duration     float64     `json:"duration"`
	WebpageURL   string      `json:"webpage_url"`
	Entries      []flatEntry `json:"entries"`
}

type flatEntry struct {
	Type          string   `json:"_type"`
	ID            string   `json:"id"`
	Title         string   `json:"title"`
	Duration      *float64 `json:"duration"`
	URL           string   `json:"url"`
	WebpageURL    string   `json:"webpage_url"`
	LiveStatus    string   `json:"live_status"`
	Availability  string   `json:"availability"`
	Channel       string   `json:"channel"`
	PlaylistTitle string   `json:"playlist_title"`
	Timestamp     *float64 `json:"timestamp"`
	ReleaseTime   *float64 `json:"release_timestamp"`
}

type listingCacheEntry struct {
	items   []catalogItem
	fetched time.Time
}

type catalog struct {
	cmd command
	now func() time.Time

	mu    sync.Mutex
	cache map[string]listingCacheEntry
}

func newCatalog() *catalog {
	return &catalog{cache: map[string]listingCacheEntry{}, now: time.Now}
}

// init wires the extractor, which is only known once the artifact is located.
func (c *catalog) init(extractor string) { c.cmd = runHelper(extractor) }

type listParams struct {
	Config json.RawMessage `json:"config"`
	Cursor string          `json:"cursor"`
	Limit  int             `json:"limit"`
}

type listResult struct {
	Items      []catalogItem `json:"items"`
	NextCursor string        `json:"next_cursor"`
}

func (s *server) list(raw json.RawMessage) (any, error) {
	var params listParams
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, errf(codeConfig, "catalog.list params: %v", err)
	}
	var cfg sourceConfig
	if err := json.Unmarshal(params.Config, &cfg); err != nil {
		return nil, errf(codeConfig, "catalog.list config: %v", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	items, err := s.catalog.listing(context.Background(), cfg)
	if err != nil {
		return nil, err
	}

	offset := 0
	if params.Cursor != "" {
		n, err := strconv.Atoi(params.Cursor)
		if err != nil || n < 0 {
			return nil, errf(codeConfig, "cursor %q is not an offset", params.Cursor)
		}
		offset = n
	}
	if offset >= len(items) {
		return listResult{Items: []catalogItem{}}, nil
	}

	limit := params.Limit
	if limit <= 0 {
		limit = 50
	}
	end := offset + limit
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	} else {
		end = len(items)
	}
	return listResult{Items: items[offset:end], NextCursor: next}, nil
}

// listing returns the source's items, fetched at most once per listingTTL.
func (c *catalog) listing(ctx context.Context, cfg sourceConfig) ([]catalogItem, error) {
	key := cfg.URL

	c.mu.Lock()
	if entry, ok := c.cache[key]; ok && c.now().Sub(entry.fetched) < listingTTL {
		c.mu.Unlock()
		return entry.items, nil
	}
	c.mu.Unlock()

	items, err := c.fetch(ctx, cfg)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.cache[key] = listingCacheEntry{items: items, fetched: c.now()}
	c.mu.Unlock()
	return items, nil
}

func (c *catalog) fetch(ctx context.Context, cfg sourceConfig) ([]catalogItem, error) {
	cctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	out, err := c.cmd(cctx, nil,
		"--flat-playlist", "-J",
		// IPv4 on purpose: this plugin usually runs on a server whose name
		// resolves to AAAA first, and the site treats an IPv6 address far more
		// suspiciously than an IPv4 one — extraction from a dual-stack host fails
		// with "Sign in to confirm you're not a bot" while the same machine over
		// IPv4 works. Forcing the family costs nothing here and removes a failure
		// that depends on which address the resolver happens to prefer.
		"-4",
		"--no-warnings", "--ignore-errors", "--no-progress",
		"--playlist-end", strconv.Itoa(cfg.maxItems()),
		cfg.URL,
	)
	if err != nil {
		return nil, classifyExtractorError(err)
	}

	var listing flatPlaylist
	if err := decodeJSON(out, &listing); err != nil {
		return nil, errf(codeUnavailable, "extractor output is not a listing: %v", err)
	}

	// A single video URL answers with a video document, not a playlist: the
	// source lists exactly one item, which is what an ad-hoc video is.
	if listing.Type == "video" {
		entry := flatEntry{
			Type:       "video",
			ID:         listing.ID,
			Title:      listing.Title,
			WebpageURL: listing.WebpageURL,
			Channel:    listing.Channel,
		}
		duration := listing.Duration
		entry.Duration = &duration
		entry.LiveStatus = listing.LiveStatus
		entry.Availability = listing.Availability
		return c.items(cfg, listing.Title, listing.Channel, []flatEntry{entry}), nil
	}

	if len(listing.Entries) == 0 {
		return nil, errf(codeNotFound, "no items listed at %s (private, empty or not a list)", cfg.URL)
	}
	return c.items(cfg, listing.Title, firstNonEmpty(listing.Channel, listing.Uploader), listing.Entries), nil
}

// items maps flat entries onto catalog rows, dropping what the host must not
// receive: an item it cannot schedule (unknown or zero length), a live stream,
// a membership-only video, or a short.
func (c *catalog) items(cfg sourceConfig, playlistTitle, channel string, entries []flatEntry) []catalogItem {
	series := cfg.SeriesTitle
	if series == "" {
		series = firstNonEmpty(playlistTitle, channel)
	}
	kind := "episode"
	switch cfg.Kind {
	case "video":
		kind = "movie"
	case "channel", "playlist":
		kind = "episode"
	}

	items := make([]catalogItem, 0, len(entries))
	seen := map[string]bool{}
	for _, e := range entries {
		id := e.ID
		if id == "" {
			id = videoIDFromURL(e.URL)
		}
		if id == "" || seen[id] {
			continue
		}
		if reason := dropReason(e, cfg); reason != "" {
			fmt.Fprintf(os.Stderr, "plugin-youtube: skipping %s: %s\n", id, reason)
			continue
		}
		seen[id] = true

		item := catalogItem{
			ExternalID:  id,
			Title:       strings.TrimSpace(e.Title),
			Kind:        kind,
			DurationMS:  int64(*e.Duration * 1000),
			SeriesTitle: series,
		}
		if cfg.Kind == "auto" {
			// A single video wrapped in a playlist entry still reads as one
			// program, not as an episode of something.
			if e.Type == "video" {
				item.Kind = "movie"
				item.SeriesTitle = cfg.SeriesTitle
			}
		}
		if item.Title == "" {
			item.Title = id
		}
		if ts := firstPtr(e.Timestamp, e.ReleaseTime); ts != nil {
			ms := int64(*ts * 1000)
			item.SourceModifiedAt = &ms
		}
		items = append(items, item)
	}
	return items
}

// dropReason explains why an entry cannot become a catalog row. An empty string
// means it can.
func dropReason(e flatEntry, cfg sourceConfig) string {
	switch {
	case e.Duration == nil || *e.Duration <= 0:
		// The host refuses these too: a zero-length item makes the client's
		// channel assembly fire transitions back to back.
		return "unknown or zero duration"
	case e.LiveStatus == "is_live" || e.LiveStatus == "is_upcoming" || e.LiveStatus == "post_live":
		return "live or upcoming"
	case e.Availability != "" && e.Availability != "public":
		return "not public (" + e.Availability + ")"
	case !cfg.IncludeShorts && isShort(e):
		return "short"
	case e.Type == "playlist" || e.Type == "multi_video":
		// A nested playlist inside a channel page: listing it here would flatten
		// it into untitled rows the host cannot schedule.
		return "nested playlist"
	}
	return ""
}

// isShort recognises a vertical short by the URL the listing gives back. The
// flat listing carries no other signal (no dimensions, no duration rule that
// holds), and the site keeps them in a separate tab this plugin does not ask
// for unless the source URL points at it.
func isShort(e flatEntry) bool {
	for _, u := range []string{e.URL, e.WebpageURL} {
		if strings.Contains(u, "/shorts/") {
			return true
		}
	}
	return false
}

func videoIDFromURL(raw string) string {
	if raw == "" {
		return ""
	}
	for _, key := range []string{"v=", "/shorts/", "youtu.be/"} {
		if i := strings.Index(raw, key); i >= 0 {
			rest := raw[i+len(key):]
			for j, r := range rest {
				if r == '&' || r == '?' || r == '/' {
					return rest[:j]
				}
			}
			return rest
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func firstPtr(values ...*float64) *float64 {
	for _, v := range values {
		if v != nil {
			return v
		}
	}
	return nil
}

// classifyExtractorError turns the extractor's own failure message into the
// contract's codes, so the host can tell "this source is broken" from "this item
// is gone" from "come back later".
func classifyExtractorError(err error) error {
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "private video"),
		strings.Contains(msg, "video unavailable"),
		strings.Contains(msg, "has been removed"),
		strings.Contains(msg, "does not exist"),
		strings.Contains(msg, "not available in your country"),
		strings.Contains(msg, "this video is not available"):
		return errf(codeNotFound, "%s", err.Error())
	case strings.Contains(msg, "sign in"),
		strings.Contains(msg, "confirm your age"),
		strings.Contains(msg, "members-only"),
		strings.Contains(msg, "http error 429"),
		strings.Contains(msg, "too many requests"):
		return errf(codeRateLimited, "%s", err.Error())
	case strings.Contains(msg, "unsupported url"):
		return errf(codeConfig, "%s", err.Error())
	default:
		return errf(codeUnavailable, "%s", err.Error())
	}
}
