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
	"path/filepath"
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
// HLS with the two tracks in SEPARATE playlists, which is the shape that survives
// -c copy. The muxer cannot cut where it wants: video only on keyframes, audio
// anywhere. Putting both tracks in one playlist therefore gives one stream
// segments of 3.0s, 12.28s and 0.13s with the audio starting seconds after the
// video — measured — and a player reading that stopped a few seconds in with data
// still buffered (no error, no buffering flag, one frozen picture). With one
// playlist per track (an HLS audio group) each track is cut on its own terms and
// the player aligns them by timestamp, which is what it does for DASH.
//
// One continuous body would be simpler and was tried: media3 refuses an MP4 whose
// length it cannot know, because its MP4 reader has to seek back after sniffing
// (reproduced: "Source error", no detail). A playlist per track it can read.

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

// The playlists the remux writes. One variant per track, joined by the master;
// see the file comment for why they are not one playlist.
const (
	masterPlaylist = "master.m3u8"
	videoVariant   = "v0"
	audioVariant   = "audio"
)

// hlsSegmentSeconds is what the muxer aims for between cuts. The tracks are cut
// independently, so the real durations differ per track and that is fine.
const hlsSegmentSeconds = 6

// streamReadyTimeout bounds the wait for the first segment. Remuxing starts at
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
	proc    *process
}

// process is one running remux and the directory it writes into.
type process struct {
	cmd *exec.Cmd
	dir string

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

func (s *streams) url(st *stream) string { return s.baseURL + st.token + "/" + masterPlaylist }

// start launches one remux writing a fragmented MP4 to a pipe, and reads enough
// of it to know the source works.
func (s *streams) start(ctx context.Context, params streamParams) (*process, error) {
	dir, err := os.MkdirTemp("", "playheadtv-yt-")
	if err != nil {
		return nil, fmt.Errorf("create stream directory: %w", err)
	}
	for _, variant := range []string{videoVariant, audioVariant} {
		if err := os.MkdirAll(filepath.Join(dir, variant), 0o700); err != nil {
			os.RemoveAll(dir)
			return nil, fmt.Errorf("create stream directory: %w", err)
		}
	}

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
		"-f", "hls",
		// One playlist per track (a variant stream each) so the muxer's cut points
		// never have to agree between video and audio. This is the whole point of
		// the shape; see the file comment.
		"-var_stream_map", "v:0,agroup:aud,name:"+videoVariant+" a:0,agroup:aud,name:"+audioVariant+",default:yes",
		"-master_pl_name", masterPlaylist,
		"-hls_time", strconv.Itoa(hlsSegmentSeconds),
		// An event playlist: it begins where the host asked and grows to the end of
		// the item, keeping every segment behind it. A sliding live window lists a
		// single segment at the moment a player opens it, and a player that follows
		// a live edge has no cushion.
		"-hls_playlist_type", "event",
		// Fragmented MP4, not MPEG-TS: the sources are already fMP4, and TS would
		// make every player reverse a repackaging it never needed.
		"-hls_segment_type", "fmp4",
		"-hls_fmp4_init_filename", "init.mp4",
		"-hls_flags", "independent_segments",
		"-hls_segment_filename", filepath.Join(dir, "%v", "seg%05d.m4s"),
		filepath.Join(dir, "%v", "index.m3u8"),
	)

	cmd := exec.CommandContext(ctx, s.remuxer, args...)
	proc := &process{cmd: cmd, dir: dir, stderr: &tailBuffer{}}
	cmd.Stdout = proc.stderr
	cmd.Stderr = proc.stderr
	if err := cmd.Start(); err != nil {
		os.RemoveAll(dir)
		return nil, fmt.Errorf("start remux: %w", err)
	}
	go func() {
		// The exit is kept, not dropped: a remux that dies while a player watches
		// leaves a playlist with nothing new in it, which the player shows as a
		// frozen picture with no error.
		err := cmd.Wait()
		proc.markExited(err)
	}()

	if err := proc.waitReady(streamReadyTimeout); err != nil {
		proc.stop()
		return nil, err
	}
	return proc, nil
}

// waitReady blocks until both variant playlists list a segment, so the host never
// hands its player a URL that has nothing behind it.
func (p *process) waitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if p.listed(videoVariant) && p.listed(audioVariant) {
			return nil
		}
		if !p.running() {
			msg := strings.TrimSpace(p.stderr.String())
			if msg == "" {
				msg = "remux exited before the first segment"
			}
			return errf(codeUnavailable, "%s", msg)
		}
		time.Sleep(150 * time.Millisecond)
	}
	return errf(codeUnavailable, "no segment after %s", timeout)
}

func (p *process) listed(variant string) bool {
	body, err := os.ReadFile(filepath.Join(p.dir, variant, "index.m3u8"))
	return err == nil && strings.Contains(string(body), "#EXTINF")
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

// exited reports whether the remux ended, and why.
func (p *process) exited() (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.exitErr == nil {
		return "", false
	}
	return p.exitErr.Error(), true
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
	if p.dir != "" {
		os.RemoveAll(p.dir)
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

// ServeHTTP serves one stream's files: the master playlist, the two variant
// playlists, their init segments and their segments. Only a client holding the
// token can read them, and the token is unguessable.
func (s *streams) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/s/")
	token, name, _ := strings.Cut(rest, "/")
	s.mu.Lock()
	st, ok := s.byToken[token]
	s.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	st.touch(s.clock())

	st.mu.Lock()
	proc := st.proc
	st.mu.Unlock()
	if proc == nil {
		http.NotFound(w, r)
		return
	}

	// Only the files this plugin wrote, under this stream's own directory: the
	// name is resolved and then checked to be inside it.
	full := filepath.Join(proc.dir, filepath.Clean("/"+name))
	if !strings.HasPrefix(full, filepath.Clean(proc.dir)+string(os.PathSeparator)) {
		http.NotFound(w, r)
		return
	}
	switch {
	case strings.HasSuffix(name, ".m3u8"):
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	case strings.HasSuffix(name, ".m4s"), strings.HasSuffix(name, ".mp4"):
		w.Header().Set("Content-Type", "video/mp4")
	}
	w.Header().Set("Cache-Control", "no-store")
	if _, err := os.Stat(full); err != nil {
		if reason, dead := proc.exited(); dead {
			// 410, not 404: the stream existed and is gone, so a player stops
			// instead of retrying a URL that will never work again. The reason is
			// the only explanation anyone will get.
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusGone)
			_, _ = io.WriteString(w, "remux ended: "+reason)
			return
		}
		http.NotFound(w, r)
		return
	}
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	http.ServeFile(rec, r, full)
	fmt.Fprintf(os.Stderr, "stream: %s %s -> %d (%d bytes)\n", r.Method, rest, rec.status, rec.written)
}

// statusRecorder remembers what a response ended up being, so the log says which
// request failed and with what.
type statusRecorder struct {
	http.ResponseWriter
	status  int
	written int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	n, err := r.ResponseWriter.Write(b)
	r.written += n
	return n, err
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
