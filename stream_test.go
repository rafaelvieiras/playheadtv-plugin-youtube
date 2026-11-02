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
	st := &stream{token: token, lastUse: lastUse, proc: &process{cmd: cmd}}
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
	s.byToken["dead"] = &stream{token: "dead", lastUse: now, proc: &process{cmd: done}}

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

func TestAStreamIsOneContinuousBody(t *testing.T) {
	// A player gets the media as one body, with no length and nothing to cache:
	// it starts where the resolve asked and only ever moves forward. There is no
	// playlist and no segment to disagree about.
	s, err := newStreams()
	if err != nil {
		t.Fatalf("newStreams: %v", err)
	}
	defer s.close()
	s.remuxer = writeScript(t, `printf 'FRAGMENT-ONE'; printf 'FRAGMENT-TWO'; sleep 1`)

	st, err := s.open(context.Background(), []remuxInput{{url: "https://example.invalid/v", selector: "v"}}, 0, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !strings.HasSuffix(s.url(st), ".mp4") {
		t.Fatalf("url %q, want a single body ending in .mp4", s.url(st))
	}

	path := "/s/" + st.token + "/stream.mp4"
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "video/mp4" {
		t.Fatalf("content type %q, want video/mp4", got)
	}
	if got := rec.Header().Get("Accept-Ranges"); got != "none" {
		t.Fatalf("accept-ranges %q, want none", got)
	}
	if body := rec.Body.String(); body != "FRAGMENT-ONEFRAGMENT-TWO" {
		t.Fatalf("body %q, want the whole media in order", body)
	}
}

func TestAStreamIsGivenToTheFirstPlayerOnly(t *testing.T) {
	// A second player opening the same URL gets a fresh remux from the same
	// parameters rather than a body another response is already reading.
	s, err := newStreams()
	if err != nil {
		t.Fatalf("newStreams: %v", err)
	}
	defer s.close()
	s.remuxer = writeScript(t, `printf 'X'; sleep 1`)

	st, err := s.open(context.Background(), []remuxInput{{url: "https://example.invalid/v", selector: "v"}}, 0, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if st.claim() == nil {
		t.Fatalf("the primed body was not claimable")
	}
	if st.claim() != nil {
		t.Fatalf("the same primed body was handed out twice")
	}
}

// writeScript writes a shell script standing in for the remuxer and returns its
// path.
func writeScript(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "remux.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o700); err != nil {
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
