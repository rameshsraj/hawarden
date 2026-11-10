// Package service holds all command logic. It never prints and never reads
// stdin; callers pass writers and interfaces so everything is fakeable.
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/rameshsraj/hawarden/internal/collector"
	"github.com/rameshsraj/hawarden/internal/config"
	"github.com/rameshsraj/hawarden/internal/diagnose"
	"github.com/rameshsraj/hawarden/internal/etcd"
	"github.com/rameshsraj/hawarden/internal/exitcode"
	"github.com/rameshsraj/hawarden/internal/model"
	"github.com/rameshsraj/hawarden/internal/patroni"
	"github.com/rameshsraj/hawarden/internal/remote"
)

// Service executes hawarden commands against loaded config.
type Service struct {
	Cfg       *config.Config
	CfgPath   string
	Collector *collector.Collector
}

func New(cfg *config.Config, cfgPath string) *Service {
	return &Service{Cfg: cfg, CfgPath: cfgPath, Collector: collector.New(cfg)}
}

// Snapshot collects once and attaches findings.
func (s *Service) Snapshot(ctx context.Context) (*model.Snapshot, error) {
	snap := s.Collector.Collect(ctx)
	snap.Findings = diagnose.Run(snap)
	return snap, nil
}

// Probe checks one host spec end-to-end for the setup wizard. Checks run
// in order and report separately: dns, tcp:22, ssh auth, keepalived,
// patroni api, etcd client port, pg port.
func (s *Service) Probe(ctx context.Context, h config.Host) model.ProbeResult {
	res := model.ProbeResult{Host: h.Name, Address: h.Address, OK: true}
	check := func(name string, fn func() (string, error)) {
		t0 := time.Now()
		detail, err := fn()
		pc := model.ProbeCheck{Name: name, MS: time.Since(t0).Milliseconds()}
		if err != nil {
			pc.OK, pc.Detail = false, err.Error()
			res.OK = false
		} else {
			pc.OK, pc.Detail = true, detail
		}
		res.Checks = append(res.Checks, pc)
	}

	check("dns", func() (string, error) {
		if ip := net.ParseIP(h.Address); ip != nil {
			return "literal IP " + h.Address, nil
		}
		addrs, err := net.DefaultResolver.LookupHost(ctx, h.Address)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v", addrs), nil
	})

	tcpCheck := func(name, addr string) {
		check(name, func() (string, error) {
			d := net.Dialer{Timeout: s.Cfg.Timeout}
			c, err := d.DialContext(ctx, "tcp", addr)
			if err != nil {
				return "", err
			}
			c.Close()
			return "connected " + addr, nil
		})
	}
	tcpCheck("tcp:ssh", net.JoinHostPort(h.Address, "22"))
	if h.Has(config.RolePatroni) {
		port := s.Cfg.Patroni.APIPort
		if port == 0 {
			port = 8008
		}
		tcpCheck("tcp:patroni", net.JoinHostPort(h.Address, fmt.Sprint(port)))
	}
	if h.Has(config.RoleEtcd) {
		tcpCheck("tcp:etcd", net.JoinHostPort(h.Address, "2379"))
	}
	if h.Has(config.RolePG) {
		port := 5432
		if h.PG != nil && h.PG.Port != 0 {
			port = h.PG.Port
		}
		tcpCheck("tcp:pg", net.JoinHostPort(h.Address, fmt.Sprint(port)))
	}

	// SSH auth + remote commands (skipped when SSH telemetry is disabled)
	if h.SSHDisabled {
		return res
	}
	r := remote.NewSSHRunner(s.Cfg.SSH.Key, s.Cfg.SSH.KnownHosts, s.Cfg.SSH.Timeout, s.Cfg.SSH.StrictHostKey)
	defer r.Close()
	sh := remote.Host{Name: h.Name, Address: h.Address, User: h.SSHUser}
	check("ssh:auth", func() (string, error) {
		out, err := r.Run(ctx, sh, "id -un")
		if err != nil {
			return "", err
		}
		return "authenticated as " + trim(string(out)), nil
	})
	if h.Has(config.RoleKeepalived) {
		check("keepalived", func() (string, error) {
			out, err := r.Run(ctx, sh, "pidof keepalived || pgrep -x keepalived")
			if err != nil {
				return "", fmt.Errorf("keepalived process not found: %w", err)
			}
			return "running pid(s) " + trim(string(out)), nil
		})
	}
	if h.Has(config.RolePatroni) {
		check("patroni:api", func() (string, error) {
			cli := patroni.NewClient(s.Cfg.PatroniAPI(h), s.Cfg.Patroni.User, "", s.Cfg.Patroni.Timeout)
			state, role, _, _, err := cli.Member(ctx)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("state=%s role=%s", state, role), nil
		})
	}
	if h.Has(config.RoleEtcd) {
		check("etcd:health", func() (string, error) {
			cli := etcd.NewClient(net.JoinHostPort(h.Address, "2379"), s.Cfg.Etcd.DialTimeout)
			ok, err := cli.Health(ctx)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("healthy=%v", ok), nil
		})
	}
	if h.Has(config.RolePG) && h.PG != nil && h.PG.Adapter == "ssh-psql" {
		check("pg:recovery", func() (string, error) {
			out, err := r.Run(ctx, sh, "su postgres -c 'psql -Atc \"SELECT pg_is_in_recovery()\"'")
			if err != nil {
				return "", err
			}
			return "in_recovery=" + trim(string(out)), nil
		})
	}
	return res
}

// ProbeAll probes every configured host.
func (s *Service) ProbeAll(ctx context.Context) []model.ProbeResult {
	var out []model.ProbeResult
	for _, h := range s.Cfg.Hosts {
		out = append(out, s.Probe(ctx, h))
	}
	return out
}

// Monitor emits NDJSON snapshot events on w until ctx ends or count cycles
// elapse (0 = forever).
func (s *Service) Monitor(ctx context.Context, w *os.File, count int) error {
	enc := json.NewEncoder(w)
	epoch := uint64(time.Now().Unix())
	var seq uint64
	for {
		snap, err := s.Snapshot(ctx)
		seq++
		ev := model.StreamEvent{Seq: seq, Epoch: epoch, At: time.Now()}
		if err != nil {
			ev.Type, ev.Err = "collect_error", err.Error()
		} else {
			ev.Type = "snapshot"
			raw, _ := json.Marshal(snap)
			var m map[string]any
			_ = json.Unmarshal(raw, &m)
			ev.Data = m
		}
		if werr := enc.Encode(ev); werr != nil {
			return werr
		}
		if count > 0 && int(seq) >= count {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.Cfg.Interval):
		}
	}
}

// --- config operations ------------------------------------------------------

// Init writes a starter config to path (refuses to overwrite).
func Init(path string) error {
	if _, err := os.Stat(path); err == nil {
		return exitcode.New(exitcode.Usage, "config already exists").
			WithCause(path).WithHint("hawarden config show")
	}
	cfg := config.Defaults()
	cfg.Cluster = "prod-ha"
	cfg.VIPs = []config.VIP{{Address: "10.0.0.50", Interface: "ens192", Policy: "leader_only"}}
	cfg.Hosts = []config.Host{{
		Name: "db1", Address: "10.0.0.11", SSHUser: "root",
		Roles: []config.Role{config.RolePatroni, config.RolePG, config.RoleKeepalived, config.RoleEtcd},
	}}
	data, err := cfg.Marshal()
	if err != nil {
		return err
	}
	return config.AtomicWrite(path, data, 0o600)
}

// SaveConfig validates and atomically replaces the config file.
func SaveConfig(path string, cfg *config.Config) error {
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return err
	}
	data, err := cfg.Marshal()
	if err != nil {
		return err
	}
	return config.AtomicWrite(path, data, 0o600)
}

// AddHost appends a validated host to the config file and saves atomically.
func AddHost(path string, h config.Host) (*config.Config, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return nil, err
	}
	if _, dup := cfg.HostByName(h.Name); dup {
		return nil, exitcode.New(exitcode.Usage, "host already exists").
			WithCause(h.Name).WithHint("choose a different name or use config remove-host first")
	}
	cfg.Hosts = append(cfg.Hosts, h)
	if err := SaveConfig(path, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// RemoveHost deletes a host by name and saves atomically.
func RemoveHost(path, name string) (*config.Config, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return nil, err
	}
	var kept []config.Host
	found := false
	for _, h := range cfg.Hosts {
		if h.Name == name {
			found = true
			continue
		}
		kept = append(kept, h)
	}
	if !found {
		return nil, exitcode.New(exitcode.Usage, "no such host").WithCause(name)
	}
	cfg.Hosts = kept
	if err := SaveConfig(path, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// SetField applies a dotted config update (cluster, interval, timeout,
// vip.add, vip.remove, etcd.endpoint.add, etcd.endpoint.remove).
func SetField(path, key, value string) (*config.Config, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return nil, err
	}
	switch key {
	case "cluster":
		cfg.Cluster = value
	case "interval":
		d, err := time.ParseDuration(value)
		if err != nil {
			return nil, exitcode.New(exitcode.Usage, "invalid duration").WithCause(value)
		}
		cfg.Interval = d
	case "timeout":
		d, err := time.ParseDuration(value)
		if err != nil {
			return nil, exitcode.New(exitcode.Usage, "invalid duration").WithCause(value)
		}
		cfg.Timeout = d
	case "replication_context":
		cfg.ReplicationContext = value == "true" || value == "on" || value == "1"
	case "vip.add":
		cfg.VIPs = append(cfg.VIPs, config.VIP{Address: value, Policy: "leader_only"})
	case "etcd.endpoint.add":
		cfg.Etcd.Endpoints = append(cfg.Etcd.Endpoints, value)
	case "etcd.endpoint.remove":
		var kept []string
		for _, e := range cfg.Etcd.Endpoints {
			if e != value {
				kept = append(kept, e)
			}
		}
		cfg.Etcd.Endpoints = kept
	default:
		return nil, exitcode.New(exitcode.Usage, "unknown config key").
			WithCause(key).
			WithHint("cluster|interval|timeout|replication_context|vip.add|etcd.endpoint.add|etcd.endpoint.remove")
	}
	if err := SaveConfig(path, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Redacted returns the config with all *_ref values masked for display.
func Redacted(cfg *config.Config) *config.Config {
	cp := *cfg
	if cp.Patroni.PasswordRef != "" {
		cp.Patroni.PasswordRef = "***"
	}
	if cp.Etcd.CACertRef != "" {
		cp.Etcd.CACertRef = "***"
	}
	if cp.Etcd.CertRef != "" {
		cp.Etcd.CertRef = "***"
	}
	if cp.Etcd.KeyRef != "" {
		cp.Etcd.KeyRef = "***"
	}
	return &cp
}

// Healthz is a cheap liveness probe for the console.
func Healthz() map[string]any {
	return map[string]any{"ok": true, "ts": time.Now().UTC()}
}

func trim(s string) string {
	out := ""
	for _, c := range s {
		if c == '\n' || c == '\r' {
			break
		}
		out += string(c)
	}
	if len(out) > 200 {
		return out[:200]
	}
	return out
}
