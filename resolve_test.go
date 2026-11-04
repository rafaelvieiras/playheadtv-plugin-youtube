package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
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

	t.Run("a host that asks for a pair and a remux still gets the muxed file", func(t *testing.T) {
		srv := testServer(t, fakeExtractor("testdata/channel-listing.json", nil))
		srv.catalog.cmd = func(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
			// The item has one progressive format and no separate tracks: a host
			// that means "best picture" must not be left with nothing.
			return []byte(`{"id":"x","duration":10,"formats":[
				{"format_id":"18","ext":"mp4","vcodec":"avc1","acodec":"mp4a","height":360,"tbr":500,"protocol":"https","url":"https://cdn.example.test/18?expire=1793602656"}]}`), nil
		}
		body, rpcErr := call(t, srv, "media.resolve", map[string]any{
			"config": config,
			"item":   map[string]any{"external_id": "sO_lhW8av3U"},
			"modes":  []string{"pair", "remux"},
		})
		if rpcErr != nil {
			t.Fatalf("resolve: %s", rpcErr.Message)
		}
		var result resolveResult
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if result.Mode != modeSingle || !strings.Contains(result.URL, "cdn.example.test/18") {
			t.Fatalf("result = %+v, want the muxed file as a last resort", result)
		}
	})

	t.Run("the muxed file honours the height cap", func(t *testing.T) {
		formats := []mediaFormat{
			{FormatID: "18", VCodec: "avc1", ACodec: "mp4a", Height: 360, TBR: 500, Protocol: "https", URL: "https://cdn.example.test/18"},
			{FormatID: "22", VCodec: "avc1", ACodec: "mp4a", Height: 720, TBR: 2000, Protocol: "https", URL: "https://cdn.example.test/22"},
		}
		if got := bestMuxed(formats, 480); got == nil || got.FormatID != "18" {
			t.Fatalf("bestMuxed(480) = %+v, want the 360 file", got)
		}
		// Nothing under the cap: the taller file still plays.
		if got := bestMuxed(formats[1:], 480); got == nil || got.FormatID != "22" {
			t.Fatalf("bestMuxed(480) over a 720-only set = %+v, want it anyway", got)
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

	t.Run("the host's order decides, not ours", func(t *testing.T) {
		// A muxed file exists, but the host asked for a pair first: it merges one
		// itself and would rather not take the muxed file this source caps below
		// the separated video track.
		srv := testServer(t, fakeExtractor("testdata/channel-listing.json", nil))
		srv.catalog.cmd = func(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
			return []byte(`{"id":"x","duration":10,"formats":[
				{"format_id":"18","ext":"mp4","vcodec":"avc1","acodec":"mp4a","height":360,"tbr":500,"protocol":"https","url":"https://cdn.example.test/18?expire=1793602656"},
				{"format_id":"137","ext":"mp4","vcodec":"avc1","acodec":"none","height":1080,"tbr":4400,"protocol":"https","url":"https://cdn.example.test/137?expire=1793602656"},
				{"format_id":"140","ext":"m4a","vcodec":"none","acodec":"mp4a","abr":129,"protocol":"https","url":"https://cdn.example.test/140?expire=1793602656"}]}`), nil
		}
		body, rpcErr := call(t, srv, "media.resolve", map[string]any{
			"config": config,
			"item":   map[string]any{"external_id": "sO_lhW8av3U"},
			"modes":  []string{"pair", "single"},
		})
		if rpcErr != nil {
			t.Fatalf("resolve: %s", rpcErr.Message)
		}
		var result resolveResult
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if result.Mode != modePair {
			t.Fatalf("mode = %q, want %q: that is the order the host asked in", result.Mode, modePair)
		}
		if !strings.Contains(result.Sources[0].URL, "/137") {
			t.Errorf("video url = %q, want the separated 1080p track", result.Sources[0].URL)
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

	// A muxed file is the best answer for a host that accepts `single`, but a host
	// that only accepts `remux` needs the tracks beside it. Planning only the
	// muxed file left that host with no plan at all, and the item unplayable
	// while its own separate tracks were right there.
	t.Run("a muxed file does not hide the tracks from a remuxing host", func(t *testing.T) {
		srv := testServer(t, fakeExtractor("testdata/channel-listing.json", nil))
		srv.streams.remuxer = fakeHLSRemuxer(t)
		srv.catalog.cmd = func(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
			return []byte(`{"id":"x","duration":10,"formats":[
				{"format_id":"18","ext":"mp4","vcodec":"avc1","acodec":"mp4a","height":360,"tbr":500,"protocol":"https","url":"https://cdn.example.test/18?expire=1793602656"},
				{"format_id":"136","ext":"mp4","vcodec":"avc1","height":720,"tbr":2000,"protocol":"https","url":"https://cdn.example.test/136?expire=1793602656"},
				{"format_id":"140","ext":"m4a","acodec":"mp4a","abr":128,"protocol":"https","url":"https://cdn.example.test/140?expire=1793602656"}]}`), nil
		}
		body, rpcErr := call(t, srv, "media.resolve", map[string]any{
			"config": config,
			"item":   map[string]any{"external_id": "sO_lhW8av3U", "duration_ms": 149000},
			"modes":  []string{"remux"},
		})
		if rpcErr != nil {
			t.Fatalf("resolve: %s", rpcErr.Message)
		}
		var result resolveResult
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if result.Mode != modeRemux {
			t.Fatalf("mode = %q, want %q", result.Mode, modeRemux)
		}
	})

	// The height cap is a preference. An item whose smallest format is above it
	// still has to play: a hole in the channel is worse than a bigger stream.
	t.Run("an item taller than the cap still plays", func(t *testing.T) {
		srv := testServer(t, fakeExtractor("testdata/channel-listing.json", nil))
		srv.streams.remuxer = fakeHLSRemuxer(t)
		srv.catalog.cmd = func(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
			return []byte(`{"id":"x","duration":10,"formats":[
				{"format_id":"299","ext":"mp4","vcodec":"avc1","height":1080,"tbr":6000,"protocol":"https","url":"https://cdn.example.test/299?expire=1793602656"},
				{"format_id":"140","ext":"m4a","acodec":"mp4a","abr":128,"protocol":"https","url":"https://cdn.example.test/140?expire=1793602656"}]}`), nil
		}
		body, rpcErr := call(t, srv, "media.resolve", map[string]any{
			"config": config,
			"item":   map[string]any{"external_id": "sO_lhW8av3U", "duration_ms": 149000},
			"modes":  []string{"remux"},
		})
		if rpcErr != nil {
			t.Fatalf("resolve: %s", rpcErr.Message)
		}
		var result resolveResult
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if result.Mode != modeRemux {
			t.Fatalf("mode = %q, want %q", result.Mode, modeRemux)
		}
	})

	t.Run("remux when the host asks", func(t *testing.T) {
		srv := testServer(t, fakeExtractor("testdata/channel-listing.json", nil))
		// A remux runs the bundled remuxer; stub it so the test needs no ffmpeg.
		srv.streams.remuxer = fakeHLSRemuxer(t)
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
		if result.Mime != "application/vnd.apple.mpegurl" {
			t.Errorf("mime = %q, want the master playlist", result.Mime)
		}
	})
}

func TestResolveExtractsAnItemOnce(t *testing.T) {
	runs := 0
	cmd := func(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
		runs++
		return os.ReadFile("testdata/media-info.json")
	}
	srv := testServer(t, cmd)
	clockTime := time.Unix(0, 0)
	srv.clock = func() time.Time { return clockTime }
	resolve := func() {
		t.Helper()
		if _, rpcErr := call(t, srv, "media.resolve", map[string]any{
			"config": map[string]any{"url": "https://www.youtube.com/channel/example/videos"},
			"item":   map[string]any{"external_id": "sO_lhW8av3U", "duration_ms": 149000},
		}); rpcErr != nil {
			t.Fatalf("resolve: %s", rpcErr.Message)
		}
	}
	// Extraction is seconds of the critical path of a channel switch, so a second
	// resolve of the same item inside the ttl must not pay for it again.
	resolve()
	resolve()
	if runs != 1 {
		t.Fatalf("extractor ran %d times for two resolves of one item, want 1", runs)
	}
	// Past the ttl the answer is taken again: a url can go stale sooner than it
	// says it will.
	clockTime = clockTime.Add(extractionTTL + time.Second)
	resolve()
	if runs != 2 {
		t.Fatalf("extractor ran %d times, want a fresh extraction after the ttl", runs)
	}
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
