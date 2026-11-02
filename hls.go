package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The plugin assembles its own stream. This site serves video and audio as
// separate files, and the host contract asks for exactly one playable URL, so
// the pieces are remuxed into HLS on local disk (-c copy: no re-encode, no CPU
// per second of video) and served from a loopback server. A player opens a
// playlist like any other; it never learns there were two upstream files.

// streamIdleTimeout is how long a stream survives without a request. A player
// that stopped fetching is gone (channel switched, app closed), and a live stream
// is not free: it holds a remux process, the bandwidth of the site being read at
// playback speed, and its segments on disk. An HLS player asks for a segment
// every few seconds, so one and a half minutes of silence means nobody is
// watching.
const streamIdleTimeout = 90 * time.Second

// streamEvictIdle is how quiet a stream must be before a new resolve may take its
// place when the pool is full. Long enough to never break playback that is
// merely thinking (a player rebuffering), short enough that a handful of
// abandoned streams cannot make the plugin refuse to start one more.
const streamEvictIdle = 20 * time.Second

// streamReadyTimeout bounds the wait for the first segment. Remuxing starts at
// the requested offset, so this is "however long the site takes to answer".
const streamReadyTimeout = 60 * time.Second

// streamMaxConcurrent caps simultaneously remuxing streams: a crossfade needs
// two, and nothing legitimate needs many more.
const streamMaxConcurrent = 4

const hlsSegmentSeconds = 6

// hlsWindowSegments is the size of the remux window: 20 × 6 s is two minutes of
// media on disk, which is what a player can seek back into. Seeking further back
// is a new resolve, not a longer window.
const hlsWindowSegments = 20

type streams struct {
	mu      sync.Mutex
	byToken map[string]*stream
	baseDir string
	baseURL string
	lf      net.Listener
	srv     *http.Server
	remuxer string
	clock   func() time.Time
}

type stream struct {
	token   string
	dir     string
	cmd     *exec.Cmd
	lastUse time.Time

	mu     sync.Mutex
	stderr *tailBuffer
}

func newStreams() (*streams, error) {
	baseDir, err := os.MkdirTemp("", "playheadtv-yt-")
	if err != nil {
		return nil, fmt.Errorf("create stream directory: %w", err)
	}
	// Loopback by default: a stream the player fetches from the same machine.
	// A host that runs the plugin elsewhere (a server, with the player on a TV)
	// sets PLAYHEADTV_PLUGIN_LISTEN to a reachable address and rewrites the host
	// of the URLs it hands to its client: the port, the token and the paths stay
	// this plugin's own.
	lf, err := net.Listen("tcp", listenAddress())
	if err != nil {
		os.RemoveAll(baseDir)
		return nil, fmt.Errorf("listen on %s: %w", listenAddress(), err)
	}
	// Advertised as loopback always: a listener bound to every interface reports
	// "[::]:port" or "0.0.0.0:port", which no player can open. A host that is not on
	// this machine rewrites the host part of the URL it hands out.
	s := &streams{
		byToken: map[string]*stream{},
		baseDir: baseDir,
		baseURL: "http://127.0.0.1:" + strconv.Itoa(lf.Addr().(*net.TCPAddr).Port) + "/s/",
		lf:      lf,
		clock:   time.Now,
	}
	s.srv = &http.Server{Handler: s, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = s.srv.Serve(lf) }()
	go s.reap()
	fmt.Fprintf(os.Stderr, "plugin-youtube: stream server on %s\n", s.baseURL)
	return s, nil
}

func (s *streams) init(remuxer string) { s.remuxer = remuxer }

// listenAddress is where the stream server binds.
func listenAddress() string {
	if addr := strings.TrimSpace(os.Getenv("PLAYHEADTV_PLUGIN_LISTEN")); addr != "" {
		return addr
	}
	return "127.0.0.1:0"
}

// open starts one remux from the given inputs and returns once the playlist has
// a segment a player can fetch.
func (s *streams) open(inputs []remuxInput, startAt float64, headers map[string]string) (*stream, error) {
	// A full pool is not a reason to refuse a viewer: the stalest stream makes
	// room. Refusing here used to be the whole story, which turned one abandoned
	// playback into minutes of every new tune-in failing (the client retried, the
	// pool never drained).
	if evicted, err := s.makeRoom(); err != nil {
		return nil, err
	} else if evicted != nil {
		evicted.stop()
	}

	token, err := newToken()
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(s.baseDir, token)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create stream directory: %w", err)
	}

	st := &stream{token: token, dir: dir, lastUse: s.clock(), stderr: &tailBuffer{}}

	// -re: produce at playback speed instead of copying as fast as the network
	// allows. A remux exists to feed a player that is watching now, and a copy
	// that races ahead would slide its own window past the segments the player
	// has not fetched yet. It also keeps the disk use of one stream at the window
	// size instead of the whole item.
	args := []string{"-hide_banner", "-nostdin", "-loglevel", "error", "-y", "-re"}
	args = append(args, headerArgs(headers)...)
	for _, in := range inputs {
		// -ss applies to the input that follows it, and input seeking is the
		// cheap kind: both inputs start at the same instant so audio cannot
		// drift against video.
		if startAt > 0 {
			args = append(args, "-ss", strconv.FormatFloat(startAt, 'f', 3, 64))
		}
		args = append(args, "-i", in.url)
	}
	for i, in := range inputs {
		args = append(args, "-map", fmt.Sprintf("%d:%s:0", i, in.selector))
	}
	args = append(args,
		"-c", "copy",
		"-f", "hls",
		"-hls_time", strconv.Itoa(hlsSegmentSeconds),
		// A sliding window, not an ever-growing event playlist: this plugin runs
		// on the same machine as the player, which may be a 2 GB set-top box or
		// a single-board computer. Segments older than the window are deleted as
		// they are produced, so one stream never holds more than
		// hlsWindowSegments × hlsSegmentSeconds of media on disk.
		"-hls_list_size", strconv.Itoa(hlsWindowSegments),
		"-hls_flags", "delete_segments+independent_segments",
		"-hls_segment_filename", filepath.Join(dir, "seg%05d.ts"),
		filepath.Join(dir, "index.m3u8"),
	)

	cmd := exec.Command(s.remuxer, args...)
	cmd.Stdout = st.stderr
	cmd.Stderr = st.stderr
	if err := cmd.Start(); err != nil {
		os.RemoveAll(dir)
		return nil, fmt.Errorf("start remux: %w", err)
	}
	st.cmd = cmd
	go func() { _ = cmd.Wait() }()

	s.mu.Lock()
	s.byToken[token] = st
	s.mu.Unlock()

	if err := st.waitReady(streamReadyTimeout); err != nil {
		st.stop()
		s.forget(token)
		return nil, err
	}
	return st, nil
}

func (s *streams) url(st *stream) string { return s.baseURL + st.token + "/index.m3u8" }

// waitReady blocks until the playlist lists a segment, so the host never hands
// its player a URL that 404s.
func (st *stream) waitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		body, err := os.ReadFile(filepath.Join(st.dir, "index.m3u8"))
		if err == nil && strings.Contains(string(body), "#EXTINF") {
			return nil
		}
		if !st.running() {
			msg := st.stderr.String()
			if msg == "" {
				msg = "remux exited before the first segment"
			}
			return errf(codeUnavailable, "%s", msg)
		}
		time.Sleep(150 * time.Millisecond)
	}
	return errf(codeUnavailable, "no segment after %s", timeout)
}

func (st *stream) running() bool {
	if st.cmd == nil || st.cmd.ProcessState != nil {
		return false
	}
	return true
}

func (st *stream) touch(now time.Time) {
	st.mu.Lock()
	st.lastUse = now
	st.mu.Unlock()
}

func (st *stream) lastUsed() time.Time {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.lastUse
}

// stop kills the remux and removes its segments.
func (st *stream) stop() {
	if st.cmd != nil && st.cmd.Process != nil {
		_ = st.cmd.Process.Kill()
	}
	os.RemoveAll(st.dir)
}

// makeRoom stops and returns the least recently used stream when the pool is
// full and that stream has been quiet for [streamEvictIdle]. It returns a nil
// stream when there is room, and an error only when every slot is in use.
func (s *streams) makeRoom() (*stream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	live := 0
	var stalest *stream
	for token, st := range s.byToken {
		if !st.running() {
			// A process that already exited holds a slot for nothing.
			delete(s.byToken, token)
			continue
		}
		live++
		if stalest == nil || st.lastUsed().Before(stalest.lastUsed()) {
			stalest = st
		}
	}
	if live < streamMaxConcurrent {
		return nil, nil
	}
	if stalest == nil || s.clock().Sub(stalest.lastUsed()) < streamEvictIdle {
		return nil, errf(codeUnavailable, "already remuxing %d streams, all in use", live)
	}
	delete(s.byToken, stalest.token)
	return stalest, nil
}

func (s *streams) forget(token string) {
	s.mu.Lock()
	delete(s.byToken, token)
	s.mu.Unlock()
}

// reap frees streams nothing is fetching any more.
func (s *streams) reap() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		now := s.clock()
		var stale []*stream
		s.mu.Lock()
		for token, st := range s.byToken {
			if now.Sub(st.lastUsed()) > streamIdleTimeout {
				stale = append(stale, st)
				delete(s.byToken, token)
			}
		}
		s.mu.Unlock()
		for _, st := range stale {
			st.stop()
		}
	}
}

func (s *streams) close() {
	s.mu.Lock()
	all := make([]*stream, 0, len(s.byToken))
	for _, st := range s.byToken {
		all = append(all, st)
	}
	s.byToken = map[string]*stream{}
	s.mu.Unlock()
	for _, st := range all {
		st.stop()
	}
	_ = s.srv.Close()
	os.RemoveAll(s.baseDir)
}

// ServeHTTP serves one stream's files. Only loopback clients can reach this and
// the token is unguessable, so a segment is only readable by whoever was handed
// the playlist URL.
func (s *streams) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/s/"), "/", 2)
	if len(parts) != 2 || parts[1] == "" {
		http.NotFound(w, r)
		return
	}
	s.mu.Lock()
	st, ok := s.byToken[parts[0]]
	s.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	st.touch(s.clock())

	name := filepath.Base(parts[1])
	if name != parts[1] {
		http.NotFound(w, r)
		return
	}
	switch {
	case strings.HasSuffix(name, ".m3u8"):
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	case strings.HasSuffix(name, ".ts"):
		w.Header().Set("Content-Type", "video/mp2t")
	}
	w.Header().Set("Cache-Control", "no-store")
	http.ServeFile(w, r, filepath.Join(st.dir, name))
}

type remuxInput struct {
	url      string
	selector string // "v" or "a"
}

// headerArgs turns upstream headers into remuxer flags. User-Agent is set with
// its own flag because that is the only header the remuxer treats specially.
func headerArgs(headers map[string]string) []string {
	if len(headers) == 0 {
		return nil
	}
	var args []string
	var rest []string
	for k, v := range headers {
		if strings.EqualFold(k, "User-Agent") {
			args = append(args, "-user_agent", v)
			continue
		}
		rest = append(rest, k+": "+v+"\r\n")
	}
	if len(rest) > 0 {
		args = append(args, "-headers", strings.Join(rest, ""))
	}
	return args
}

func newToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate stream token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// tailBuffer keeps the last lines a helper wrote to stderr, which is what a
// human needs when a remux fails.
type tailBuffer struct {
	mu   sync.Mutex
	data []byte
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.data = append(t.data, p...)
	if len(t.data) > 8<<10 {
		t.data = t.data[len(t.data)-(8<<10):]
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	lines := strings.Split(strings.TrimSpace(string(t.data)), "\n")
	tail := lines
	if len(tail) > 3 {
		tail = tail[len(tail)-3:]
	}
	return strings.TrimSpace(strings.Join(tail, "; "))
}
