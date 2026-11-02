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

// An event playlist keeps every segment it produced, so one stream holds as much
// media on disk as the part of the item it has played: about 60 MB per ten
// minutes at 720p, removed when the stream ends (an item change, an idle
// stream, or the plugin stopping). What it buys is a player that can always read
// behind itself instead of racing the producer for the newest segment.

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
	// exitErr is set once the remux process has ended, with its stderr tail as
	// the reason: an ended remux can never produce another segment.
	exitErr error
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

// open starts one remux from the given inputs and returns once the playlist has
// a segment a player can fetch.
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

	token, err := newToken()
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(s.baseDir, token)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create stream directory: %w", err)
	}

	st := &stream{token: token, dir: dir, lastUse: s.clock(), stderr: &tailBuffer{}}

	// Produced as fast as the host can read, not at playback speed (-re). A remux
	// paced to real time hands the player one segment at a time and puts it
	// permanently at the edge of the media, where the smallest hiccup leaves it
	// with nothing to show: the picture stops for a moment every few seconds and
	// never recovers a cushion. Reading ahead gives the player the whole item
	// within seconds, which is what makes it smooth, and it is only safe because
	// the playlist is an event playlist that keeps what it produced (a sliding
	// window would delete the segments the player had not reached yet).
	// Both inputs must begin at the same instant, and with -c copy the only
	// instant the video can begin on is a keyframe. Asking both for exactly
	// startAt gives the video the keyframe BEFORE it (up to a whole GOP, ~10s on
	// these sources) while the audio loses nothing, so the audio starts seconds
	// after the video and the first seconds of the stream have picture and no
	// sound. Players that follow the audio clock then stop at exactly that seam
	// (reproduced: picture froze at 9-10s of every stream, with no error). Asking
	// both for the keyframe itself removes the hole: the audio is cut there too.
	if keyframe, ok := s.keyframeAt(ctx, inputs, startAt, headers); ok {
		startAt = keyframe
	}

	args := []string{"-hide_banner", "-nostdin", "-loglevel", "error", "-y"}
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
		// An event playlist, which is what this stream really is: it begins at the
		// position the host asked for and grows to the end of the item. A sliding
		// live window is the wrong shape here. At the moment a player opens the
		// playlist it holds a single segment, so a player that treats it as a live
		// edge has no cushion to read ahead into and stops as soon as it has
		// played what was listed. An event playlist tells the player to start at
		// the beginning and keep following, and every segment stays readable
		// behind it.
		"-hls_playlist_type", "event",
		// Fragmented MP4 segments, not MPEG-TS. The sources are fragmented MP4
		// already, so this is the same boxes moved into a new file, without the
		// TS repackaging that a player has to reverse: an HLS player reading TS
		// has to reassemble PES packets and Annex-B access units, and a segment
		// cut by the muxer mid-packet makes that reader lose the stream (a
		// picture that stops while the transport keeps working). Every player
		// worth targeting reads fMP4 HLS.
		"-hls_segment_type", "fmp4",
		"-hls_fmp4_init_filename", "init.mp4",
		"-hls_flags", "independent_segments",
		"-hls_segment_filename", filepath.Join(dir, "seg%05d.m4s"),
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
	go func() {
		// The outcome is kept, not dropped: a remux that dies while a player is
		// watching leaves a playlist with nothing new in it, which a player shows
		// as a frozen picture with no error. Whoever asks for the playlist next
		// gets the failure and its reason instead.
		err := cmd.Wait()
		st.markExited(err)
	}()

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

// markExited records that the remux ended, keeping the last of its diagnostic
// output as the reason. The first reason wins: a stream only ends once.
func (st *stream) markExited(err error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.exitErr != nil {
		return
	}
	if err == nil {
		err = errors.New("the remux finished")
	}
	detail := strings.TrimSpace(st.stderr.String())
	if detail != "" {
		err = fmt.Errorf("%w: %s", err, detail)
	}
	st.exitErr = err
}

// exited reports whether the remux ended, and why.
func (st *stream) exited() (string, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.exitErr == nil {
		return "", false
	}
	return st.exitErr.Error(), true
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
	if reason, dead := st.exited(); dead {
		// 410, not 404: the stream existed and is gone, so a player stops instead
		// of retrying a URL that will never work again.
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusGone)
		_, _ = io.WriteString(w, "remux ended: "+reason)
		return
	}
	switch {
	case strings.HasSuffix(name, ".m3u8"):
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	case strings.HasSuffix(name, ".m4s"), strings.HasSuffix(name, ".mp4"):
		w.Header().Set("Content-Type", "video/mp4")
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
