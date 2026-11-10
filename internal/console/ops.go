package console

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

// ops is the bounded operations journal: every console-driven CLI call
// (probe, config mutations) is recorded with actor, input summary and
// outcome, persisted as JSONL under StateDir and fanned to SSE subscribers.
type ops struct {
	dir string

	mu   sync.Mutex
	list []opEntry
	subs map[chan []byte]struct{}
	seq  uint64
}

type opEntry struct {
	ID       uint64    `json:"id"`
	At       time.Time `json:"at"`
	Operator string    `json:"operator"`
	Action   string    `json:"action"`
	Input    string    `json:"input"`
	OK       bool      `json:"ok"`
	Result   string    `json:"result,omitempty"`
}

const opsKeep = 200

func newOps(dir string) *ops {
	o := &ops{dir: dir, subs: map[chan []byte]struct{}{}}
	o.load()
	return o
}

func (o *ops) path() string {
	if o.dir == "" {
		return ""
	}
	return filepath.Join(o.dir, "ops.jsonl")
}

func (o *ops) load() {
	p := o.path()
	if p == "" {
		return
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return
	}
	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var e opEntry
		if json.Unmarshal(line, &e) == nil {
			o.list = append(o.list, e)
			if e.ID > o.seq {
				o.seq = e.ID
			}
		}
	}
	if len(o.list) > opsKeep {
		o.list = o.list[len(o.list)-opsKeep:]
	}
}

func (o *ops) record(operator, action, input string, ok bool, result string) {
	o.mu.Lock()
	o.seq++
	e := opEntry{ID: o.seq, At: time.Now(), Operator: operator, Action: action, Input: input, OK: ok, Result: result}
	o.list = append(o.list, e)
	if len(o.list) > opsKeep {
		o.list = o.list[len(o.list)-opsKeep:]
	}
	o.mu.Unlock()

	if p := o.path(); p != "" {
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err == nil {
			if f, ferr := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600); ferr == nil {
				line, _ := json.Marshal(e)
				_, _ = f.Write(append(line, '\n'))
				_ = f.Close()
			}
		}
	}
	raw, _ := json.Marshal(e)
	sse := []byte(fmt.Sprintf("id: %d\nevent: op\ndata: {\"seq\":%d,\"type\":\"op\",\"data\":%s}\n\n", e.ID, e.ID, raw))
	o.broadcast(sse)
}

func (o *ops) broadcast(sse []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for ch := range o.subs {
		select {
		case ch <- sse:
		default:
		}
	}
}

func (o *ops) entries() []opEntry {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]opEntry, len(o.list))
	copy(out, o.list)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// subscribe is kept for future per-op streaming; ops already ride the
// monitor SSE channel via broadcast→/api/stream.
func (o *ops) subscribe() (chan []byte, func()) {
	ch := make(chan []byte, 32)
	o.mu.Lock()
	o.subs[ch] = struct{}{}
	o.mu.Unlock()
	return ch, func() {
		o.mu.Lock()
		delete(o.subs, ch)
		o.mu.Unlock()
		close(ch)
	}
}

func (s *Server) listOps(w http.ResponseWriter, _ *http.Request, _ *session) {
	writeJSON(w, 200, map[string]any{"ops": s.ops.entries()})
}

// runCLI executes `hawarden -c cfg <argv...>` capturing stdout/stderr.
// argv is a validated allowlist — never free-form browser strings, never a
// shell.
func (s *Server) runCLI(ctx context.Context, argv ...string) (stdout, stderr []byte, err error) {
	args := append([]string{"-c", s.o.Config}, argv...)
	cmd := exec.CommandContext(ctx, s.o.CLIBin, args...)
	var so, se bytes.Buffer
	cmd.Stdout = &so
	cmd.Stderr = &se
	err = cmd.Run()
	return so.Bytes(), se.Bytes(), err
}
