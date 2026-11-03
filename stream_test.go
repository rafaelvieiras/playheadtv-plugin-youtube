package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// runningStream is a stream whose remux process really is alive, which is what
// the pool counts.
func runningStream(t *testing.T, s *streams, token string, lastUse time.Time) *stream {
	t.Helper()
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start the stand-in process: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	st := &stream{token: token, lastUse: lastUse, procs: []*process{{cmd: cmd}}}
	s.byToken[token] = st
	return st
}

func TestMakeRoomEvictsTheStalestStreamWhenThePoolIsFull(t *testing.T) {
	s, err := newStreams()
	if err != nil {
		t.Fatalf("newStreams: %v", err)
	}
	defer s.close()
	now := time.Now()
	// The pool is full, and the oldest stream has been silent long enough that
	// nothing is watching it any more.
	old := runningStream(t, s, "old", now.Add(-2*time.Minute))
	for i, token := range []string{"b", "c", "d"} {
		runningStream(t, s, token, now.Add(-time.Duration(i)*time.Second))
	}

	evicted, err := s.makeRoom()
	if err != nil {
		t.Fatalf("a full pool with a silent stream must make room, got %v", err)
	}
	if evicted == nil || evicted.token != "old" {
		t.Fatalf("evicted %v, want the stalest stream", evicted)
	}
	if _, still := s.byToken["old"]; still {
		t.Fatal("an evicted stream is still in the pool")
	}
	if len(s.byToken) != 3 {
		t.Fatalf("pool holds %d streams, want 3", len(s.byToken))
	}
	_ = old
}

func TestMakeRoomRefusesWhenEveryStreamIsInUse(t *testing.T) {
	s, err := newStreams()
	if err != nil {
		t.Fatalf("newStreams: %v", err)
	}
	defer s.close()
	now := time.Now()
	for _, token := range []string{"a", "b", "c", "d"} {
		runningStream(t, s, token, now)
	}
	if _, err := s.makeRoom(); err == nil {
		t.Fatal("a pool where every stream is fetching now must refuse")
	}
	if len(s.byToken) != 4 {
		t.Fatalf("pool holds %d streams, want 4 kept", len(s.byToken))
	}
}

func TestMakeRoomDropsStreamsWhoseProcessExited(t *testing.T) {
	s, err := newStreams()
	if err != nil {
		t.Fatalf("newStreams: %v", err)
	}
	defer s.close()
	now := time.Now()
	for _, token := range []string{"a", "b", "c"} {
		runningStream(t, s, token, now)
	}
	// A remux that ended on its own (the item finished) must not hold a slot.
	done := exec.Command("true")
	if err := done.Run(); err != nil {
		t.Fatalf("run the stand-in process: %v", err)
	}
	s.byToken["dead"] = &stream{token: "dead", lastUse: now, procs: []*process{{cmd: done}}}

	evicted, err := s.makeRoom()
	if err != nil {
		t.Fatalf("makeRoom: %v", err)
	}
	if evicted != nil {
		t.Fatalf("evicted %q, want nothing: a dead stream frees the slot", evicted.token)
	}
	if _, still := s.byToken["dead"]; still {
		t.Fatal("a stream whose process exited is still in the pool")
	}
}

// The source refuses a freshly resolved url on one of its nodes and serves the
// same url on the next request. One refused read must not cost the item.
func TestASourceRefusalIsTriedAgain(t *testing.T) {
	s, err := newStreams()
	if err != nil {
		t.Fatalf("newStreams: %v", err)
	}
	defer s.close()

	marker := filepath.Join(t.TempDir(), "tried")
	s.remuxer = fakeRefusingRemuxerOnce(t, marker)

	if _, err := s.open(context.Background(), []remuxInput{{url: "https://example.invalid/v", selector: "v"}, {url: "https://example.invalid/a", selector: "a"}}, 0, nil); err != nil {
		t.Fatalf("open: %v", err)
	}
}

// A refusal that keeps happening is the item's problem, and the second try is
// the last one: the caller has a channel to keep running.
func TestASourceRefusalIsNotRetriedForever(t *testing.T) {
	s, err := newStreams()
	if err != nil {
		t.Fatalf("newStreams: %v", err)
	}
	defer s.close()
	s.remuxer = fakeRefusingRemuxer(t)

	if _, err := s.open(context.Background(), []remuxInput{{url: "https://example.invalid/v", selector: "v"}, {url: "https://example.invalid/a", selector: "a"}}, 0, nil); err == nil {
		t.Fatal("open succeeded, want the refusal reported")
	}
}

func TestAStreamIsOnePlaylistPerTrack(t *testing.T) {
	// The player is handed a master playlist that joins two variant playlists, one
	// per track, so the muxer's cut points never have to agree between video and
	// audio.
	s, err := newStreams()
	if err != nil {
		t.Fatalf("newStreams: %v", err)
	}
	defer s.close()
	s.remuxer = fakeHLSRemuxer(t)

	st, err := s.open(context.Background(), []remuxInput{{url: "https://example.invalid/v", selector: "v"}, {url: "https://example.invalid/a", selector: "a"}}, 0, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !strings.HasSuffix(s.url(st), "/"+masterPlaylist) {
		t.Fatalf("url %q, want the master playlist", s.url(st))
	}

	for path, wantType := range map[string]string{
		masterPlaylist:                 "application/vnd.apple.mpegurl",
		videoVariant + "/index.m3u8":   "application/vnd.apple.mpegurl",
		audioVariant + "/index.m3u8":   "application/vnd.apple.mpegurl",
		videoVariant + "/seg00000.m4s": "video/mp4",
	} {
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, httptest.NewRequest("GET", "/s/"+st.token+"/"+path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d, want 200", path, rec.Code)
		}
		if got := rec.Header().Get("Content-Type"); got != wantType {
			t.Errorf("%s: content type %q, want %q", path, got, wantType)
		}
		if rec.Body.Len() == 0 {
			t.Errorf("%s: empty body", path)
		}
	}
}

func TestAStreamCannotBeReadOutsideItsOwnDirectory(t *testing.T) {
	s, err := newStreams()
	if err != nil {
		t.Fatalf("newStreams: %v", err)
	}
	defer s.close()
	s.remuxer = fakeHLSRemuxer(t)
	st, err := s.open(context.Background(), []remuxInput{{url: "https://example.invalid/v", selector: "v"}, {url: "https://example.invalid/a", selector: "a"}}, 0, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest("GET", "/s/"+st.token+"/../../etc/passwd", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", rec.Code)
	}
}

// fakeHLSRemuxer writes a stand-in for the bundled remuxer: it creates the two
// variant playlists and a segment in the directory the plugin told it to use, then
// stays alive like a real remux would.
func fakeHLSRemuxer(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fake-remuxer")
	// One process per track now, so each run is handed its own variant's paths and
	// writes that variant's playlist and one segment.
	script := `#!/bin/sh
playlist=""
for last; do playlist="$last"; done
dir=$(dirname "$playlist")
mkdir -p "$dir"
printf '#EXTM3U\n#EXTINF:6.0,\nseg00000.m4s\n' > "$playlist"
printf 'segment' > "$dir/seg00000.m4s"
sleep 30
`
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatalf("write the stand-in remuxer: %v", err)
	}
	return path
}

func TestParseKeyframePtsReadsTheDecodersFirstFrame(t *testing.T) {
	// The shape ffmpeg's showinfo filter writes, trimmed to what matters. The
	// probe asks for one frame, so the first report is the keyframe the seek
	// landed on and the one both inputs must be cut at.
	report := `[Parsed_showinfo_0 @ 0x7f686c001e00] n:   0 pts:54020967 pts_time:1800.6989 duration:   1001 duration_time:0.0333667 fmt:yuv420p cl:left sar:1/1 s:1280x720 i:P iskey:1 type:I checksum:58AC6354`
	at, ok := parseKeyframePts(report, 1819.283)
	if !ok {
		t.Fatalf("no keyframe found in the probe report")
	}
	if at < 1800.69 || at > 1800.70 {
		t.Fatalf("keyframe = %v, want 1800.6989", at)
	}
}

func TestParseKeyframePtsRefusesAKeyframeAfterTheTarget(t *testing.T) {
	// A keyframe past the requested position is not usable: cutting there would
	// skip media the host asked to play.
	report := `... pts_time:1999.500 ... iskey:1 ...`
	if _, ok := parseKeyframePts(report, 1819.283); ok {
		t.Fatalf("accepted a keyframe after the target")
	}
}

// fakeRefusingRemuxer always refuses the read, the way the source does with 403.
func fakeRefusingRemuxer(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "refusing-remuxer")
	script := `#!/bin/sh
echo "[in#0] Error opening input: Server returned 403 Forbidden (access denied)" >&2
exit 8
`
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatalf("write the refusing remuxer: %v", err)
	}
	return path
}

// fakeRefusingRemuxerOnce refuses the first run and behaves afterwards, which is
// what the source did when measured.
func fakeRefusingRemuxerOnce(t *testing.T, marker string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "refusing-once-remuxer")
	script := `#!/bin/sh
if [ ! -f "` + marker + `" ]; then
  : > "` + marker + `"
  echo "[in#0] Error opening input: Server returned 403 Forbidden (access denied)" >&2
  exit 8
fi
playlist=""
for last; do playlist="$last"; done
dir=$(dirname "$playlist")
mkdir -p "$dir"
printf '#EXTM3U\n#EXTINF:6.0,\nseg00000.m4s\n' > "$playlist"
printf 'segment' > "$dir/seg00000.m4s"
sleep 30
`
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatalf("write the refusing remuxer: %v", err)
	}
	return path
}
