package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
)

// fakeExtractor answers like the real one, from the fixtures in testdata. The
// fixtures are trimmed from real output (URLs sanitised) so the shapes the code
// parses are the shapes the site actually produces.
func fakeExtractor(fixture string, fail error) command {
	return func(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
		if fail != nil {
			return nil, fail
		}
		for _, a := range args {
			if a == "-j" && !contains(args, "--flat-playlist") {
				return os.ReadFile("testdata/media-info.json")
			}
		}
		return os.ReadFile(fixture)
	}
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

func testServer(t *testing.T, cmd command) *server {
	t.Helper()
	c := newCatalog()
	c.cmd = cmd
	srv := &server{catalog: c}
	streams, err := newStreams()
	if err != nil {
		t.Fatalf("streams: %v", err)
	}
	t.Cleanup(streams.close)
	srv.streams = streams
	return srv
}

func call(t *testing.T, srv *server, method string, params any) (json.RawMessage, *rpcError) {
	t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	resp := srv.handle(request{ID: json.RawMessage("1"), Method: method, Params: raw})
	if resp.Error != nil {
		return nil, resp.Error
	}
	body, err := json.Marshal(resp.Result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	return body, nil
}

func TestCatalogListDropsWhatTheHostCannotSchedule(t *testing.T) {
	srv := testServer(t, fakeExtractor("testdata/channel-listing.json", nil))

	body, rpcErr := call(t, srv, "catalog.list", map[string]any{
		"config": map[string]any{"url": "https://www.youtube.com/channel/example/videos"},
		"cursor": "",
		"limit":  50,
	})
	if rpcErr != nil {
		t.Fatalf("catalog.list: %s", rpcErr.Message)
	}

	var result listResult
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// The fixture holds 3 real entries plus a live stream, an upcoming
	// premiere, a short, a members-only video and a duplicate of the first.
	if len(result.Items) != 3 {
		ids := make([]string, 0, len(result.Items))
		for _, it := range result.Items {
			ids = append(ids, it.ExternalID)
		}
		t.Fatalf("items = %v, want the 3 schedulable ones", ids)
	}
	for _, item := range result.Items {
		if item.DurationMS <= 0 {
			t.Errorf("%s: duration_ms = %d, the host refuses these", item.ExternalID, item.DurationMS)
		}
		if item.Kind != "episode" {
			t.Errorf("%s: kind = %q, a channel lists episodes", item.ExternalID, item.Kind)
		}
		if item.SeriesTitle == "" {
			t.Errorf("%s: no series_title, the guide would show it as an orphan", item.ExternalID)
		}
		if item.Title == "" {
			t.Errorf("%s: empty title", item.ExternalID)
		}
	}
	if result.Items[0].DurationMS != 7482000 {
		t.Errorf("duration = %d ms, want 7482 s from the fixture", result.Items[0].DurationMS)
	}
	if result.NextCursor != "" {
		t.Errorf("next_cursor = %q, want empty when the page holds everything", result.NextCursor)
	}
}

func TestCatalogListPages(t *testing.T) {
	srv := testServer(t, fakeExtractor("testdata/channel-listing.json", nil))
	config := map[string]any{"url": "https://www.youtube.com/channel/example/videos"}

	var seen []string
	cursor := ""
	for page := 0; page < 5; page++ {
		body, rpcErr := call(t, srv, "catalog.list", map[string]any{"config": config, "cursor": cursor, "limit": 2})
		if rpcErr != nil {
			t.Fatalf("page %d: %s", page, rpcErr.Message)
		}
		var result listResult
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatalf("decode: %v", err)
		}
		for _, item := range result.Items {
			seen = append(seen, item.ExternalID)
		}
		cursor = result.NextCursor
		if cursor == "" {
			break
		}
	}
	if len(seen) != 3 || seen[2] == "" {
		t.Fatalf("paged ids = %v, want the 3 items once each", seen)
	}
	if cursor != "" {
		t.Errorf("cursor still %q after the last page", cursor)
	}
}

func TestCatalogListRejectsBadConfig(t *testing.T) {
	srv := testServer(t, fakeExtractor("testdata/channel-listing.json", nil))

	for name, config := range map[string]map[string]any{
		"no url":    {},
		"bad kind":  {"url": "https://example.test/x", "kind": "playlist-of-playlists"},
		"negatives": {"url": "https://example.test/x", "max_items": -3},
	} {
		_, rpcErr := call(t, srv, "catalog.list", map[string]any{"config": config})
		if rpcErr == nil {
			t.Fatalf("%s: accepted, want a config error", name)
		}
		if rpcErr.Code != codeConfig {
			t.Errorf("%s: code = %q, want %q", name, rpcErr.Code, codeConfig)
		}
	}
}

func TestResolveAsksForWhatTheHostCanPlay(t *testing.T) {
	srv := testServer(t, fakeExtractor("testdata/channel-listing.json", nil))
	config := map[string]any{"url": "https://www.youtube.com/channel/example/videos"}

	t.Run("pair by default", func(t *testing.T) {
		body, rpcErr := call(t, srv, "media.resolve", map[string]any{
			"config": config,
			"item":   map[string]any{"external_id": "sO_lhW8av3U", "duration_ms": 149000},
		})
		if rpcErr != nil {
			t.Fatalf("resolve: %s", rpcErr.Message)
		}
		var result resolveResult
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if result.Mode != modePair {
			t.Fatalf("mode = %q, want %q: no host asked for a remux", result.Mode, modePair)
		}
		if len(result.Sources) != 2 {
			t.Fatalf("sources = %d, want a video and an audio track", len(result.Sources))
		}
		types := map[string]bool{}
		for _, src := range result.Sources {
			types[src.Type] = true
			if !strings.HasPrefix(src.URL, "https://cdn.example.test/") {
				t.Errorf("%s url = %q, want the source's own url untouched", src.Type, src.URL)
			}
			if src.ExpiresAt == nil {
				t.Errorf("%s: no expires_at, the host cannot know the url goes stale", src.Type)
			}
		}
		if !types["video"] || !types["audio"] {
			t.Fatalf("sources = %v, want one video and one audio", types)
		}
		if result.URL != "" {
			t.Errorf("url = %q, a pair answer has no single url", result.URL)
		}
	})

	t.Run("capable host still prefers a single file", func(t *testing.T) {
		srv := testServer(t, fakeExtractor("testdata/channel-listing.json", nil))
		srv.catalog.cmd = func(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
			// The item does have a muxed file: single wins, nothing is assembled.
			return []byte(`{"id":"x","duration":10,"formats":[
				{"format_id":"18","ext":"mp4","vcodec":"avc1","acodec":"mp4a","height":360,"tbr":500,"protocol":"https","url":"https://cdn.example.test/18?expire=1793602656"}]}`), nil
		}
		body, rpcErr := call(t, srv, "media.resolve", map[string]any{
			"config": config,
			"item":   map[string]any{"external_id": "sO_lhW8av3U"},
			"modes":  []string{"single", "pair"},
		})
		if rpcErr != nil {
			t.Fatalf("resolve: %s", rpcErr.Message)
		}
		var result resolveResult
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if result.Mode != modeSingle || !strings.Contains(result.URL, "cdn.example.test/18") {
			t.Fatalf("result = %+v, want the file's own url", result)
		}
		if result.Mime != "video/mp4" {
			t.Errorf("mime = %q", result.Mime)
		}
	})

	t.Run("pair-only host is refused, not remuxed", func(t *testing.T) {
		srv := testServer(t, fakeExtractor("testdata/channel-listing.json", nil))
		_, rpcErr := call(t, srv, "media.resolve", map[string]any{
			"config": config,
			"item":   map[string]any{"external_id": "sO_lhW8av3U"},
			"modes":  []string{"single"},
		})
		if rpcErr == nil {
			t.Fatal("accepted, want unsupported: the host never offered remux")
		}
		if rpcErr.Code != codeUnsupported {
			t.Errorf("code = %q, want %q", rpcErr.Code, codeUnsupported)
		}
	})

	t.Run("remux when the host asks", func(t *testing.T) {
		srv := testServer(t, fakeExtractor("testdata/channel-listing.json", nil))
		// A remux runs the bundled remuxer; stub it so the test needs no ffmpeg.
		srv.streams.remuxer = writeFakeRemuxer(t)
		body, rpcErr := call(t, srv, "media.resolve", map[string]any{
			"config":      config,
			"item":        map[string]any{"external_id": "sO_lhW8av3U", "duration_ms": 149000},
			"start_at_ms": 5000,
			"modes":       []string{"remux"},
		})
		if rpcErr != nil {
			t.Fatalf("resolve: %s", rpcErr.Message)
		}
		var result resolveResult
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if result.Mode != modeRemux || !result.Live || !strings.HasPrefix(result.URL, "http://127.0.0.1:") {
			t.Fatalf("result = %+v, want a loopback body marked live", result)
		}
		if result.Mime != "video/mp4" {
			t.Errorf("mime = %q, want one continuous mp4 body", result.Mime)
		}
	})
}

func TestResolveRejectsWhatItCannotServe(t *testing.T) {
	config := map[string]any{"url": "https://www.youtube.com/channel/example/videos"}

	t.Run("bad item id", func(t *testing.T) {
		srv := testServer(t, fakeExtractor("testdata/channel-listing.json", nil))
		_, rpcErr := call(t, srv, "media.resolve", map[string]any{"config": config, "item": map[string]any{"external_id": "not an id"}})
		if rpcErr == nil || rpcErr.Code != codeConfig {
			t.Fatalf("error = %+v, want a config error", rpcErr)
		}
	})

	t.Run("gone video", func(t *testing.T) {
		srv := testServer(t, fakeExtractor("", errors.New("ERROR: [youtube] x: Video unavailable")))
		_, rpcErr := call(t, srv, "media.resolve", map[string]any{"config": config, "item": map[string]any{"external_id": "sO_lhW8av3U"}})
		if rpcErr == nil || rpcErr.Code != codeNotFound {
			t.Fatalf("error = %+v, want not_found", rpcErr)
		}
	})

	t.Run("rate limited", func(t *testing.T) {
		srv := testServer(t, fakeExtractor("", errors.New("ERROR: unable to download: HTTP Error 429: Too Many Requests")))
		_, rpcErr := call(t, srv, "media.resolve", map[string]any{"config": config, "item": map[string]any{"external_id": "sO_lhW8av3U"}})
		if rpcErr == nil || rpcErr.Code != codeRateLimited {
			t.Fatalf("error = %+v, want rate_limited", rpcErr)
		}
	})

	t.Run("live", func(t *testing.T) {
		srv := testServer(t, fakeExtractor("", nil))
		srv.catalog.cmd = func(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
			return []byte(`{"id":"x","live_status":"is_live","formats":[]}`), nil
		}
		_, rpcErr := call(t, srv, "media.resolve", map[string]any{"config": config, "item": map[string]any{"external_id": "sO_lhW8av3U"}})
		if rpcErr == nil || rpcErr.Code != codeNotFound {
			t.Fatalf("error = %+v, want not_found for a live stream", rpcErr)
		}
	})
}

func TestUnknownMethod(t *testing.T) {
	srv := testServer(t, fakeExtractor("testdata/channel-listing.json", nil))
	_, rpcErr := call(t, srv, "catalog.delete", nil)
	if rpcErr == nil || rpcErr.Code != codeUnsupported {
		t.Fatalf("error = %+v, want unsupported", rpcErr)
	}
}

// writeFakeRemuxer writes a stand-in for the bundled remuxer: it writes a few
// bytes of "media" to stdout and then stays alive like a real remux would.
func writeFakeRemuxer(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/fake-remuxer"
	script := `#!/bin/sh
printf 'FRAGMENT'
sleep 30
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake remuxer: %v", err)
	}
	return path
}
