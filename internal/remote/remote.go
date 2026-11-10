// Package remote executes read-only command lines on monitored hosts over
// SSH. Commands are fixed strings built by adapters — never user input —
// and run through "sh -c" so pipelines are available where the adapter
// needs them (e.g. psql via su).
package remote

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// Runner executes a command on a host and returns stdout.
type Runner interface {
	Run(ctx context.Context, host Host, cmd string) ([]byte, error)
}

// Host identifies an SSH target.
type Host struct {
	Name    string
	Address string
	Port    int
	User    string
}

// SSHRunner is a Runner backed by golang.org/x/crypto/ssh with per-host
// connection reuse.
type SSHRunner struct {
	KeyPath       string
	KnownHosts    string
	Timeout       time.Duration
	StrictHostKey bool

	mu      sync.Mutex
	clients map[string]*ssh.Client
}

func NewSSHRunner(key, knownHosts string, timeout time.Duration, strict bool) *SSHRunner {
	return &SSHRunner{
		KeyPath: key, KnownHosts: knownHosts, Timeout: timeout,
		StrictHostKey: strict, clients: map[string]*ssh.Client{},
	}
}

func (r *SSHRunner) clientFor(ctx context.Context, h Host) (*ssh.Client, error) {
	key := fmt.Sprintf("%s@%s:%d", h.User, h.Address, h.Port)
	r.mu.Lock()
	if c, ok := r.clients[key]; ok {
		r.mu.Unlock()
		return c, nil
	}
	r.mu.Unlock()

	keyData, err := os.ReadFile(r.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("read ssh key %s: %w", r.KeyPath, err)
	}
	signer, err := ssh.ParsePrivateKey(keyData)
	if err != nil {
		return nil, fmt.Errorf("parse ssh key %s: %w", r.KeyPath, err)
	}
	var hk ssh.HostKeyCallback
	if r.StrictHostKey && r.KnownHosts != "" {
		hk, err = knownhosts.New(r.KnownHosts)
		if err != nil {
			return nil, fmt.Errorf("load known_hosts %s: %w", r.KnownHosts, err)
		}
	} else {
		hk = ssh.InsecureIgnoreHostKey()
	}
	port := h.Port
	if port == 0 {
		port = 22
	}
	cfg := &ssh.ClientConfig{
		User:            h.User,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: hk,
		Timeout:         r.Timeout,
	}
	addr := net.JoinHostPort(h.Address, fmt.Sprint(port))
	d := net.Dialer{Timeout: r.Timeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	sc, chans, reqs, err := ssh.NewClientConn(conn, addr, cfg)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("ssh handshake %s: %w", addr, err)
	}
	cli := ssh.NewClient(sc, chans, reqs)
	r.mu.Lock()
	r.clients[key] = cli
	r.mu.Unlock()
	return cli, nil
}

// Run executes cmd on h and returns stdout. stderr is folded into the error.
func (r *SSHRunner) Run(ctx context.Context, h Host, cmd string) ([]byte, error) {
	cli, err := r.clientFor(ctx, h)
	if err != nil {
		return nil, err
	}
	sess, err := cli.NewSession()
	if err != nil {
		r.drop(h)
		return nil, fmt.Errorf("ssh session %s: %w", h.Address, err)
	}
	defer sess.Close()
	var stdout, stderr bytes.Buffer
	sess.Stdout = &stdout
	sess.Stderr = &stderr
	type res struct{ err error }
	done := make(chan res, 1)
	go func() { done <- res{sess.Run("sh -c " + shellQuote(cmd))} }()
	select {
	case <-ctx.Done():
		_ = sess.Close()
		return nil, ctx.Err()
	case rr := <-done:
		if rr.err != nil {
			if stderr.Len() > 0 {
				return nil, fmt.Errorf("%s: %s", rr.err, trim(stderr.String(), 300))
			}
			return nil, rr.err
		}
		return stdout.Bytes(), nil
	}
}

// drop discards a cached (probably dead) connection.
func (r *SSHRunner) drop(h Host) {
	port := h.Port
	if port == 0 {
		port = 22
	}
	key := fmt.Sprintf("%s@%s:%d", h.User, h.Address, port)
	r.mu.Lock()
	if c, ok := r.clients[key]; ok {
		delete(r.clients, key)
		_ = c.Close()
	}
	r.mu.Unlock()
}

// Close drops every pooled connection.
func (r *SSHRunner) Close() {
	r.mu.Lock()
	for k, c := range r.clients {
		_ = c.Close()
		delete(r.clients, k)
	}
	r.mu.Unlock()
}

func shellQuote(s string) string {
	return "'" + replaceAll(s, "'", "'\\''") + "'"
}

func replaceAll(s, old, new string) string {
	out := ""
	for {
		i := indexOf(s, old)
		if i < 0 {
			return out + s
		}
		out += s[:i] + new
		s = s[i+len(old):]
	}
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func trim(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
