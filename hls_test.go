package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os/exec"
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
	st := &stream{token: token, cmd: cmd, lastUse: lastUse}
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
	s.byToken["dead"] = &stream{token: "dead", cmd: done, lastUse: now}

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

func TestAPlaylistOfAnEndedRemuxReportsWhy(t *testing.T) {
	s, err := newStreams()
	if err != nil {
		t.Fatalf("newStreams: %v", err)
	}
	defer s.close()
	st := &stream{token: "tok", dir: t.TempDir(), stderr: &tailBuffer{}}
	_, _ = st.stderr.Write([]byte("https://example.invalid/media: HTTP error 403 Forbidden"))
	st.markExited(errors.New("exit status 1"))
	s.byToken["tok"] = st

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest("GET", "/s/tok/index.m3u8", nil))
	if rec.Code != http.StatusGone {
		t.Fatalf("status %d, want 410", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, "403") && !strings.Contains(body, "exit status") {
		t.Fatalf("body %q, want the reason", body)
	}
}
