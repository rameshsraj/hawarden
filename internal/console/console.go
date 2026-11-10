// Package console is the optional web gateway for hawarden. It holds no
// cluster logic of its own: every read and every mutation is executed by the
// hawarden CLI (validated argv, no shell), so behaviour, safeguards and the
// audit trail are identical to terminal use.
package console

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"sync"
	"time"
)

//go:embed dist
var embeddedDist embed.FS

// EmbeddedAssets exposes the embedded SPA (dist/).
var EmbeddedAssets fs.FS = embeddedDist

// Options configures Server.
type Options struct {
	Addr     string // listen address, e.g. 127.0.0.1:8080
	Token    string // access token exchanged for a session
	CLIBin   string // path to the hawarden binary
	Config   string // hawarden.yaml path (-c)
	StateDir string // console state: ops journal lives here
	Assets   fs.FS  // embedded SPA (may be nil for API-only use)
	TLSCert  string // optional TLS cert
	TLSKey   string // optional TLS key
}

type session struct {
	ID       string
	Operator string
	CSRF     string
	Created  time.Time
}

// Server is the console HTTP gateway.
type Server struct {
	o        Options
	mon      *monitor
	ops      *ops
	mu       sync.Mutex
	sessions map[string]*session
}

// NewServer builds the gateway; assets may be nil (API-only).
func NewServer(o Options) *Server {
	if o.CLIBin == "" {
		o.CLIBin = "hawarden"
	}
	if o.Config == "" {
		o.Config = "/etc/hawarden.yaml"
	}
	s := &Server{
		o:        o,
		sessions: map[string]*session{},
	}
	s.mon = newMonitor(o.CLIBin, o.Config)
	s.ops = newOps(o.StateDir)
	return s
}

// Run starts the monitor subprocess and serves HTTP until ctx ends.
func (s *Server) Run(ctx context.Context) error {
	s.mon.start(ctx)
	mux := http.NewServeMux()
	s.routes(mux)
	srv := &http.Server{Addr: s.o.Addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(c)
	}()
	var err error
	if s.o.TLSCert != "" {
		err = srv.ListenAndServeTLS(s.o.TLSCert, s.o.TLSKey)
	} else {
		err = srv.ListenAndServe()
	}
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// viewAllowlist maps /api/view/<name> to `hawarden <argv>` read commands.
var viewAllowlist = map[string][]string{
	"status":   {"status"},
	"cluster":  {"cluster"},
	"dcs":      {"dcs"},
	"vip":      {"vip"},
	"topology": {"topology"},
	"events":   {"events"},
	"health":   {"health"},
	"diagnose": {"diagnose"},
}

func (s *Server) routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/login", s.login)
	mux.HandleFunc("POST /api/logout", s.logout)
	mux.HandleFunc("GET /api/me", s.requireAuth(func(w http.ResponseWriter, _ *http.Request, sess *session) {
		writeJSON(w, 200, map[string]any{"operator": sess.Operator, "csrf": sess.CSRF})
	}))
	mux.HandleFunc("GET /api/stream", s.requireAuth(s.stream))
	mux.HandleFunc("GET /api/view/", s.requireAuth(s.view))
	mux.HandleFunc("GET /api/config", s.requireAuth(s.getConfig))
	mux.HandleFunc("POST /api/config/test", s.requireAuth(s.testHost))
	mux.HandleFunc("POST /api/config/add-host", s.requireAuth(s.addHost))
	mux.HandleFunc("POST /api/config/remove-host", s.requireAuth(s.removeHost))
	mux.HandleFunc("POST /api/config/set", s.requireAuth(s.setField))
	mux.HandleFunc("GET /api/ops", s.requireAuth(s.listOps))
	mux.HandleFunc("GET /api/monitor", s.requireAuth(func(w http.ResponseWriter, _ *http.Request, _ *session) {
		writeJSON(w, 200, map[string]any{"running": s.mon.isRunning(), "seq": s.mon.lastSeq()})
	}))
	mux.HandleFunc("GET /api/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]any{"ok": true})
	})
	if s.o.Assets != nil {
		mux.HandleFunc("GET /", s.spa)
	}
}

// --- auth -------------------------------------------------------------------

func rnd(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

type loginReq struct {
	Token    string `json:"token"`
	Operator string `json:"operator"`
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var req loginReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		writeJSON(w, 400, errBody("bad request"))
		return
	}
	if subtle.ConstantTimeCompare([]byte(req.Token), []byte(s.o.Token)) != 1 {
		writeJSON(w, 401, errBody("invalid token"))
		return
	}
	op := strings.TrimSpace(req.Operator)
	if op == "" {
		op = "console"
	}
	sess := &session{ID: rnd(32), Operator: op, CSRF: rnd(32), Created: time.Now()}
	s.mu.Lock()
	s.sessions[sess.ID] = sess
	s.mu.Unlock()
	http.SetCookie(w, &http.Cookie{
		Name: "hwsess", Value: sess.ID, Path: "/", HttpOnly: true,
		SameSite: http.SameSiteStrictMode, MaxAge: 12 * 3600,
	})
	http.SetCookie(w, &http.Cookie{
		Name: "hwcsrf", Value: sess.CSRF, Path: "/",
		SameSite: http.SameSiteStrictMode, MaxAge: 12 * 3600,
	})
	writeJSON(w, 200, map[string]any{"operator": op, "csrf": sess.CSRF})
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie("hwsess"); err == nil {
		s.mu.Lock()
		delete(s.sessions, c.Value)
		s.mu.Unlock()
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

// sessionOf validates the session cookie and, for mutating methods, the
// Origin and the double-submit CSRF token.
func (s *Server) sessionOf(w http.ResponseWriter, r *http.Request) *session {
	c, err := r.Cookie("hwsess")
	if err != nil {
		return nil
	}
	s.mu.Lock()
	sess := s.sessions[c.Value]
	s.mu.Unlock()
	if sess == nil {
		return nil
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		if og := r.Header.Get("Origin"); og != "" {
			if !strings.Contains(og, "://"+r.Host) {
				return nil
			}
		}
		cc, err := r.Cookie("hwcsrf")
		if err != nil || r.Header.Get("X-CSRF") != sess.CSRF || cc.Value != sess.CSRF {
			return nil
		}
	}
	return sess
}

func (s *Server) requireAuth(next func(http.ResponseWriter, *http.Request, *session)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sess := s.sessionOf(w, r)
		if sess == nil {
			writeJSON(w, 401, errBody("unauthenticated"))
			return
		}
		next(w, r, sess)
	}
}

// --- CLI-backed routes --------------------------------------------------------

// view runs `hawarden <view> --json` and proxies stdout. Exit 10/11 (WARN/
// CRIT findings) still return valid JSON — the body is what matters.
func (s *Server) view(w http.ResponseWriter, r *http.Request, _ *session) {
	name := strings.TrimPrefix(r.URL.Path, "/api/view/")
	argv, ok := viewAllowlist[name]
	if !ok {
		writeJSON(w, 404, errBody("unknown view"))
		return
	}
	out, stderr, err := s.runCLI(r.Context(), append(argv, "--json")...)
	if err != nil && len(out) == 0 {
		writeJSON(w, 502, cliErr(stderr, err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(out)
}

func (s *Server) getConfig(w http.ResponseWriter, r *http.Request, _ *session) {
	out, stderr, err := s.runCLI(r.Context(), "config", "show", "--json")
	if err != nil {
		writeJSON(w, 502, cliErr(stderr, err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
}

// testHost runs `hawarden probe --host-json <spec>` for the wizard.
func (s *Server) testHost(w http.ResponseWriter, r *http.Request, sess *session) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeJSON(w, 400, errBody("bad request"))
		return
	}
	// validate it is a host object before putting it on argv
	var host map[string]any
	if json.Unmarshal(body, &host) != nil || host["name"] == nil || host["address"] == nil {
		writeJSON(w, 400, errBody("host spec requires name and address"))
		return
	}
	out, stderr, err := s.runCLI(r.Context(), "probe", "--host-json", string(body))
	s.ops.record(sess.Operator, "probe", summarize(body), err == nil, string(out))
	if err != nil {
		writeJSON(w, 502, cliErr(stderr, err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
}

// addHost appends a host via `hawarden config add-host` then restarts the
// shared monitor so the new machine appears on the live dashboard.
func (s *Server) addHost(w http.ResponseWriter, r *http.Request, sess *session) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeJSON(w, 400, errBody("bad request"))
		return
	}
	var host map[string]any
	if json.Unmarshal(body, &host) != nil || host["name"] == nil || host["address"] == nil {
		writeJSON(w, 400, errBody("host spec requires name and address"))
		return
	}
	out, stderr, err := s.runCLI(r.Context(), "config", "add-host", "--json", string(body))
	ok := err == nil
	s.ops.record(sess.Operator, "config add-host", summarize(body), ok, trimStr(string(out), 400))
	if !ok {
		writeJSON(w, 502, cliErr(stderr, err))
		return
	}
	s.mon.restart()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
}

func (s *Server) removeHost(w http.ResponseWriter, r *http.Request, sess *session) {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil || req.Name == "" {
		writeJSON(w, 400, errBody("name required"))
		return
	}
	out, stderr, err := s.runCLI(r.Context(), "config", "remove-host", req.Name)
	ok := err == nil
	s.ops.record(sess.Operator, "config remove-host", req.Name, ok, trimStr(string(out), 400))
	if !ok {
		writeJSON(w, 502, cliErr(stderr, err))
		return
	}
	s.mon.restart()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
}

func (s *Server) setField(w http.ResponseWriter, r *http.Request, sess *session) {
	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil || req.Key == "" {
		writeJSON(w, 400, errBody("key and value required"))
		return
	}
	out, stderr, err := s.runCLI(r.Context(), "config", "set", req.Key, req.Value)
	ok := err == nil
	s.ops.record(sess.Operator, "config set", req.Key+"="+req.Value, ok, trimStr(string(out), 400))
	if !ok {
		writeJSON(w, 502, cliErr(stderr, err))
		return
	}
	s.mon.restart()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
}

// --- helpers ------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func errBody(msg string) map[string]any { return map[string]any{"error": msg} }

func cliErr(stderr []byte, err error) map[string]any {
	msg := strings.TrimSpace(string(stderr))
	if msg == "" && err != nil {
		msg = err.Error()
	}
	if i := strings.LastIndex(msg, "error: "); i >= 0 {
		msg = msg[i:]
	}
	return map[string]any{"error": msg}
}

func trimStr(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func summarize(body []byte) string {
	var m map[string]any
	if json.Unmarshal(body, &m) == nil {
		if n, ok := m["name"].(string); ok {
			if a, ok := m["address"].(string); ok {
				return n + " (" + a + ")"
			}
			return n
		}
	}
	return trimStr(string(body), 120)
}

const noFrontend = `<!doctype html><title>hawarden console</title>
<body style="background:#0b0f14;color:#8b98a9;font-family:monospace;padding:3em">
<h2>hawarden console</h2>
<p>The API is running but the web frontend is not embedded in this binary.</p>
</body>`

// spa serves the embedded frontend with index.html fallback for client routes.
func (s *Server) spa(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/")
	if p == "" {
		p = "index.html"
	}
	f, err := s.o.Assets.Open(p)
	if err != nil {
		f, err = s.o.Assets.Open("index.html")
		if err != nil {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(noFrontend))
			return
		}
	}
	defer f.Close()
	st, _ := f.Stat()
	name := p
	if strings.HasSuffix(r.URL.Path, "/") || !strings.Contains(name, ".") {
		name = "index.html"
	}
	data, err := io.ReadAll(f)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	http.ServeContent(w, r, name, st.ModTime(), bytes.NewReader(data))
}
