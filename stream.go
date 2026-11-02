package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The plugin assembles its own stream. This site serves video and audio as
// separate files, and the host contract asks for exactly one playable URL, so
// the pieces are remuxed (-c copy: no re-encode, no CPU per second of video) and
// served as one HTTP body. A player opens it like any other progressive file and
// never learns there were two upstream files.
//
// One body, not HLS with segments and a playlist. Segments look like the natural
// shape for a live stream, but with -c copy the muxer cannot cut them where it
// wants: the video can only be cut on a keyframe and the audio anywhere, so the
// two tracks of one stream end up with different segment boundaries and odd
// declared durations (measured: 3.0s, 12.28s and 0.13s segments in one stream,
// with the audio starting seconds after the video). A player that reads that
// through its own extractor stopped a few seconds into every stream with data
// still buffered — no error, no buffering flag, just a frozen picture. A single
// continuous body has no boundaries to disagree about, and it is the shape this
// app already plays best: the media server serves a progressive file the same
// way.

// streamEvictIdle is how quiet a stream must be before a new resolve may take its
// place when the pool is full. Long enough to never break playback that is
// merely thinking (a player rebuffering), short enough that a handful of
// abandoned streams cannot make the plugin refuse to start one more.
const streamEvictIdle = 20 * time.Second

// streamIdleTimeout is how long a stream that nobody ever fetched survives. A
// stream in use is bounded by its own response: when the player disconnects the
// body ends and the process goes with it. This only covers the window between a
// resolve handing out a URL and the player opening it.
const streamIdleTimeout = 90 * time.Second

// streamPrimedBytes is how much of the body is read before the URL is handed
// out, so a source that cannot be read fails the resolve instead of failing the
// player. Small: this is a liveness check, not a buffer.
const streamPrimedBytes = 32 * 1024

// streamReadyTimeout bounds the wait for those first bytes. Remuxing starts at
// the requested offset, so this is "however long the site takes to answer".
const streamReadyTimeout = 60 * time.Second

// streamMaxConcurrent caps simultaneously remuxing streams: a crossfade needs
// two, and nothing legitimate needs many more.
const streamMaxConcurrent = 4

type streams struct {
	mu      sync.Mutex
	byToken map[string]*stream
	baseURL string
	lf      net.Listener
	srv     *http.Server
	remuxer string
	clock   func() time.Time
}

// streamParams is what a token stands for: enough to start the same stream
// again when a player asks for the URL a second time.
type streamParams struct {
	inputs  []remuxInput
	startAt float64
	headers map[string]string
}

// stream is one primed remux, waiting for the player to open its URL.
type stream struct {
	token  string
	params streamParams

	mu      sync.Mutex
	lastUse time.Time
	taken   bool
	proc    *process
}

// process is one running remux and the body it writes.
type process struct {
	cmd    *exec.Cmd
	out    *io.PipeReader
	primed []byte

	mu      sync.Mutex
	stderr  *tailBuffer
	exitErr error
}

func newStreams() (*streams, error) {
	// Loopback by default: a stream the player fetches from the same machine.
	// A host that runs the plugin elsewhere (a server, with the player on a TV)
	// sets PLAYHEADTV_PLUGIN_LISTEN to a reachable address and rewrites the host
	// of the URLs it hands to its client: the port, the token and the paths stay
	// this plugin's own.
	lf, err := net.Listen("tcp", listenAddress())
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", listenAddress(), err)
	}
	// Advertised as loopback always: a listener bound to every interface reports
	// "[::]:port" or "0.0.0.0:port", which no player can open. A host that is not on
	// this machine rewrites the host part of the URL it hands out.
	s := &streams{
		byToken: map[string]*stream{},
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

// open primes one stream from the given inputs and returns once the remuxer has
// produced something, so the host never hands its player a URL that cannot be
// read.
func (s *streams) open(ctx context.Context, inputs []remuxInput, startAt float64, headers map[string]string) (*stream, error) {
	// A full pool is not a reason to refuse a viewer: the stalest stream makes
	// room. Refusing here used to be the whole story, which turned one abandoned
	// playback into minutes of every new tune-in failing (the client retried, the
	// pool never drained).
	if evicted, err := s.makeRoom(); err != nil {
		return nil, err
	} else if evicted != nil {
		evicted.stop()
	}

	// Both inputs must begin at the same instant, and with -c copy the only
	// instant the video can begin on is a keyframe. Asking both for exactly
	// startAt gives the video the keyframe BEFORE it (up to a whole GOP, ~10s on
	// these sources) while the audio loses nothing, so the first seconds of the
	// stream have picture and no sound — and a player that follows the audio
	// clock has nothing to follow. Asking both for the keyframe itself removes
	// the gap: the audio is cut there too.
	if keyframe, ok := s.keyframeAt(ctx, inputs, startAt, headers); ok {
		startAt = keyframe
	}

	token, err := newToken()
	if err != nil {
		return nil, err
	}
	st := &stream{
		token:   token,
		params:  streamParams{inputs: inputs, startAt: startAt, headers: headers},
		lastUse: s.clock(),
	}
	proc, err := s.start(ctx, st.params)
	if err != nil {
		return nil, err
	}
	st.proc = proc

	s.mu.Lock()
	s.byToken[token] = st
	s.mu.Unlock()
	return st, nil
}

func (s *streams) url(st *stream) string { return s.baseURL + st.token + "/stream.mp4" }

// start launches one remux writing a fragmented MP4 to a pipe, and reads enough
// of it to know the source works.
func (s *streams) start(ctx context.Context, params streamParams) (*process, error) {
	args := []string{"-hide_banner", "-nostdin", "-loglevel", "error", "-y"}
	args = append(args, headerArgs(params.headers)...)
	for _, in := range params.inputs {
		// -ss applies to the input that follows it, and input seeking is the
		// cheap kind.
		if params.startAt > 0 {
			args = append(args, "-ss", strconv.FormatFloat(params.startAt, 'f', 3, 64))
		}
		args = append(args, "-i", in.url)
	}
	for i, in := range params.inputs {
		args = append(args, "-map", fmt.Sprintf("%d:%s:0", i, in.selector))
	}
	args = append(args,
		"-c", "copy",
		"-f", "mp4",
		// Fragmented, and with an empty moov, so the first bytes are already
		// playable and the file never has to be finished before it can be read.
		// default_base_moof keeps each fragment self-describing, which is what a
		// reader that joins mid-body needs.
		"-movflags", "+frag_keyframe+empty_moov+default_base_moof",
		"-",
	)

	pr, pw := io.Pipe()
	cmd := exec.Command(s.remuxer, args...)
	cmd.Stdout = pw
	proc := &process{cmd: cmd, out: pr, stderr: &tailBuffer{}}
	cmd.Stderr = proc.stderr
	if err := cmd.Start(); err != nil {
		_ = pw.Close()
		return nil, fmt.Errorf("start remux: %w", err)
	}
	go func() {
		// The exit is kept, not dropped: a body that ends early is a picture that
		// stops without an error on the player's side, and its stderr is the only
		// explanation anyone will get.
		err := cmd.Wait()
		proc.markExited(err)
		_ = pw.CloseWithError(io.EOF)
	}()

	if err := proc.prime(); err != nil {
		proc.stop()
		return nil, err
	}
	return proc, nil
}

// prime reads the first bytes of the body, so a source that cannot be read fails
// here rather than in the player.
func (p *process) prime() error {
	type result struct {
		buf []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		// One read, not a full buffer: the point is to know the body started, and
		// filling the buffer would block until that much media existed.
		buf := make([]byte, streamPrimedBytes)
		n, err := p.out.Read(buf)
		if n > 0 {
			err = nil
		}
		done <- result{buf: buf[:n], err: err}
	}()
	select {
	case res := <-done:
		if res.err != nil {
			msg := strings.TrimSpace(p.stderr.String())
			if msg == "" {
				msg = "the remux wrote nothing"
			}
			return errf(codeUnavailable, "%s", msg)
		}
		p.primed = res.buf
		return nil
	case <-time.After(streamReadyTimeout):
		return errf(codeUnavailable, "no media after %s", streamReadyTimeout)
	}
}

// markExited records that the remux ended, keeping the last of its diagnostic
// output as the reason. The first reason wins: a process only ends once.
func (p *process) markExited(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.exitErr != nil {
		return
	}
	if err == nil {
		err = errors.New("the remux finished")
	}
	if detail := strings.TrimSpace(p.stderr.String()); detail != "" {
		err = fmt.Errorf("%w: %s", err, detail)
	}
	p.exitErr = err
}

func (p *process) running() bool {
	if p.cmd == nil || p.cmd.ProcessState != nil {
		return false
	}
	return true
}

// stop kills the remux.
func (p *process) stop() {
	if p.cmd != nil && p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
	if p.out != nil {
		_ = p.out.Close()
	}
}

// stop kills this stream's remux, if it still has one.
func (st *stream) stop() {
	st.mu.Lock()
	proc := st.proc
	st.proc = nil
	st.mu.Unlock()
	if proc != nil {
		proc.stop()
	}
}

func (st *stream) paramsCopy() streamParams {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.params
}

// claim hands the primed body to the one player that opened this URL.
func (st *stream) claim() *process {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.taken {
		return nil
	}
	st.taken = true
	return st.proc
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

// makeRoom stops and returns the least recently used stream when the pool is
// full and that stream has been quiet for [streamEvictIdle]. It returns a nil
// stream when there is room, and an error only when every slot is in use.
func (s *streams) makeRoom() (*stream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	live := 0
	var stalest *stream
	for token, st := range s.byToken {
		st.mu.Lock()
		alive := st.proc != nil && st.proc.running()
		st.mu.Unlock()
		if !alive {
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

// reap frees streams nothing ever fetched.
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
}

// ServeHTTP serves one stream's body. Only a client that was handed the token can
// read it, and the token is unguessable.
func (s *streams) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.URL.Path, "/s/")
	if i := strings.IndexByte(token, '/'); i >= 0 {
		token = token[:i]
	}
	s.mu.Lock()
	st, ok := s.byToken[token]
	s.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	st.touch(s.clock())

	proc := st.claim()
	if proc == nil {
		// The player opened the URL a second time (it reopened after a stall, or
		// reconnected). The primed body is gone: start the same stream again from
		// the parameters the resolve stored.
		fresh, err := s.start(r.Context(), st.paramsCopy())
		if err != nil {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.Header().Set("Cache-Control", "no-store")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "remux failed: "+err.Error())
			return
		}
		proc = fresh
	}
	defer func() {
		proc.stop()
		if st.claim() == nil && proc == st.proc {
			s.forget(token)
		}
	}()

	w.Header().Set("Content-Type", "video/mp4")
	// The body has no length and must never be cached or resumed from an offset:
	// it starts where the resolve asked and streams forward once.
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Accept-Ranges", "none")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	if len(proc.primed) > 0 {
		if _, err := w.Write(proc.primed); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
	buf := make([]byte, 64*1024)
	for {
		n, err := proc.out.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if err != nil {
			return
		}
	}
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
		return "", fmt.Errorf("random token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// tailBuffer keeps the last of a process's diagnostic output.
type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
}

const tailLimit = 4 * 1024

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if len(t.buf) > tailLimit {
		t.buf = t.buf[len(t.buf)-tailLimit:]
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.buf)
}

// keyframeProbeWindow is how far before the target the keyframe probe starts
// looking, and keyframeProbeTimeout bounds it: the probe reads at most that much
// before the seek target, so it is a few megabytes and a second or two.
var (
	keyframeProbeWindow  = 20 * time.Second
	keyframeProbeTimeout = 20 * time.Second
	keyframePts          = regexp.MustCompile(`iskey:1.*?pts_time:([0-9.]+)|pts_time:([0-9.]+).*?iskey:1`)
)

// keyframeAt reports the last keyframe at or before target, or false when the
// inputs cannot be probed (a single input needs no probing: its own timeline is
// the timeline).
func (s *streams) keyframeAt(ctx context.Context, inputs []remuxInput, target float64, headers map[string]string) (float64, bool) {
	if len(inputs) < 2 || target <= 0 {
		return 0, false
	}
	var video string
	for _, in := range inputs {
		if in.selector == "v" {
			video = in.url
			break
		}
	}
	if video == "" {
		return 0, false
	}
	// The seek point can be a full GOP before the target, so the probe starts a
	// window earlier and asks the decoder for its first frame: the decoder always
	// begins on a keyframe, and -copyts keeps that frame's timestamp in source
	// time (without it the probe would report the seek point as zero).
	from := target - keyframeProbeWindow.Seconds()
	if from < 0 {
		from = 0
	}
	probeCtx, cancel := context.WithTimeout(ctx, keyframeProbeTimeout)
	defer cancel()
	args := []string{"-hide_banner", "-nostdin", "-loglevel", "info", "-copyts",
		"-ss", strconv.FormatFloat(from, 'f', 3, 64), "-noaccurate_seek"}
	args = append(args, headerArgs(headers)...)
	args = append(args, "-i", video, "-map", "0:v:0", "-skip_frame", "nokey",
		"-vf", "showinfo", "-frames:v", "1", "-f", "null", "-")
	// Combined output on purpose: ffmpeg writes the filter report to stderr and
	// exits non-zero when it is killed at the end of the probe, which is the
	// expected outcome here — the report is still what we came for.
	out, _ := exec.CommandContext(probeCtx, s.remuxer, args...).CombinedOutput()
	return parseKeyframePts(string(out), target)
}

// parseKeyframePts reads the timestamp of the first keyframe the probe decoded.
// Split out of keyframeAt so the shape of the remuxer's report is pinned by a
// test rather than by whoever next reads this code.
func parseKeyframePts(report string, target float64) (float64, bool) {
	for _, m := range keyframePts.FindAllStringSubmatch(report, -1) {
		value := m[1]
		if value == "" {
			value = m[2]
		}
		at, err := strconv.ParseFloat(value, 64)
		if err != nil || at <= 0 || at > target {
			continue
		}
		return at, true
	}
	return 0, false
}
