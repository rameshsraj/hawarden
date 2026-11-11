// Package collector builds one model.Snapshot per cycle. Every source runs
// concurrently with its own timeout; a dead source degrades the snapshot
// without killing it.
package collector

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rameshsraj/hawarden/internal/backendsql"
	"github.com/rameshsraj/hawarden/internal/config"
	"github.com/rameshsraj/hawarden/internal/etcd"
	"github.com/rameshsraj/hawarden/internal/hostcounters"
	"github.com/rameshsraj/hawarden/internal/kalive"
	"github.com/rameshsraj/hawarden/internal/model"
	"github.com/rameshsraj/hawarden/internal/patroni"
	"github.com/rameshsraj/hawarden/internal/remote"
)

// Collector owns the per-cycle fan-out. Create once per monitor process so
// rate state survives across cycles.
type Collector struct {
	cfg    *config.Config
	runner *remote.SSHRunner
	prev   *hostcounters.Prev
}

func New(cfg *config.Config) *Collector {
	return &Collector{
		cfg:    cfg,
		runner: remote.NewSSHRunner(cfg.SSH.Key, cfg.SSH.KnownHosts, cfg.SSH.Timeout, cfg.SSH.StrictHostKey),
		prev:   hostcounters.NewPrev(),
	}
}

// Close releases pooled SSH connections.
func (c *Collector) Close() { c.runner.Close() }

// Runner exposes the SSH runner for service-layer probes (keepalived,
// service control) that need direct remote access.
func (c *Collector) Runner() remote.Runner { return c.runner }

func (c *Collector) sshHost(h config.Host) remote.Host {
	return remote.Host{Name: h.Name, Address: h.Address, User: h.SSHUser}
}

// Collect runs one full cycle and returns the snapshot (always non-nil).
func (c *Collector) Collect(ctx context.Context) *model.Snapshot {
	t0 := time.Now()
	snap := &model.Snapshot{
		Schema:  model.SchemaVersion,
		Cluster: c.cfg.Cluster,
		Taken:   t0,
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	srcOK := func(source, target string, ok bool, dur time.Duration, errStr string) {
		mu.Lock()
		snap.Sources = append(snap.Sources, model.SourceHealth{
			Source: source, Target: target, OK: ok,
			LastOK: func() time.Time {
				if ok {
					return time.Now()
				}
				return time.Time{}
			}(),
			DurationMs: dur.Milliseconds(), Err: errStr,
		})
		if !ok {
			snap.Errors = append(snap.Errors, model.SourceError{Source: source, Target: target, Err: errStr})
		}
		mu.Unlock()
	}
	run := func(f func()) {
		wg.Add(1)
		go func() { defer wg.Done(); f() }()
	}

	timeout := c.cfg.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}

	// --- Patroni -----------------------------------------------------------
	patroniHosts := c.cfg.HostsWith(config.RolePatroni)
	var clusterOnce sync.Once
	var clusterMembers []model.PatroniMember
	var clusterPaused bool
	var clusterSched *model.ScheduledSwitch
	var clusterHistory []model.HistoryEntry
	var clusterErrs []string
	memberProbe := map[string]bool{} // host name -> /patroni reachable
	for _, h := range patroniHosts {
		h := h
		run(func() {
			sctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			start := time.Now()
			cli := patroni.NewClient(c.cfg.PatroniAPI(h), c.cfg.Patroni.User, "", c.cfg.Patroni.Timeout)
			members, paused, sched, err := cli.Cluster(sctx)
			if err != nil {
				mu.Lock()
				clusterErrs = append(clusterErrs, h.Name+": "+err.Error())
				mu.Unlock()
				srcOK("patroni", h.Name, false, time.Since(start), err.Error())
				return
			}
			srcOK("patroni", h.Name, true, time.Since(start), "")
			// direct per-member API observation on this host
			_, _, _, _, merr := cli.Member(sctx)
			mu.Lock()
			memberProbe[h.Name] = merr == nil
			mu.Unlock()
			clusterOnce.Do(func() {
				clusterMembers = members
				clusterPaused = paused
				clusterSched = sched
				if hist, herr := cli.History(sctx); herr == nil {
					clusterHistory = hist
				}
			})
		})
	}

	// --- etcd --------------------------------------------------------------
	for _, ep := range c.cfg.Etcd.Endpoints {
		ep := ep
		run(func() {
			sctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			start := time.Now()
			cli := etcd.NewClient(ep, c.cfg.Etcd.DialTimeout)
			var m model.EtcdMember
			m.Endpoint = ep
			m.LeaderChanges, m.DBSizeBytes, m.RaftIndex, m.RaftTerm = -1, -1, -1, -1
			cli.ProbeEndpoint(sctx, &m)
			mu.Lock()
			snap.Etcd.Members = append(snap.Etcd.Members, m)
			mu.Unlock()
			if !m.Reachable {
				srcOK("etcd", ep, false, time.Since(start), m.Err)
			} else {
				srcOK("etcd", ep, true, time.Since(start), m.Err)
			}
		})
	}

	// --- keepalived + host counters (SSH) ----------------------------------
	vipAddrs := make([]string, 0, len(c.cfg.VIPs))
	for _, v := range c.cfg.VIPs {
		vipAddrs = append(vipAddrs, v.Address)
	}
	kaliveHosts := c.cfg.HostsWith(config.RoleKeepalived)
	for _, h := range kaliveHosts {
		h := h
		run(func() {
			sctx, cancel := context.WithTimeout(ctx, timeout+c.cfg.SSH.Timeout)
			defer cancel()
			start := time.Now()
			if h.SSHDisabled {
				n := model.KeepalivedNode{Host: h.Name, VRRPState: "unknown", Priority: -1,
					Err: "ssh telemetry disabled on this host"}
				mu.Lock()
				snap.Keepalived = append(snap.Keepalived, n)
				mu.Unlock()
				srcOK("keepalived", h.Name, false, time.Since(start), n.Err)
				return
			}
			stateFile := ""
			if h.Keepalived != nil {
				stateFile = h.Keepalived.StateFile
			}
			n := kalive.Probe(sctx, c.runner, c.sshHost(h), vipAddrs, stateFile)
			mu.Lock()
			snap.Keepalived = append(snap.Keepalived, n)
			mu.Unlock()
			srcOK("keepalived", h.Name, n.Reachable, time.Since(start), n.Err)
		})
	}

	// host interface counters for every SSH-observable host
	for _, h := range c.cfg.Hosts {
		h := h
		if h.SSHDisabled {
			continue
		}
		run(func() {
			sctx, cancel := context.WithTimeout(ctx, timeout+c.cfg.SSH.Timeout)
			defer cancel()
			start := time.Now()
			fs, err := c.prev.Probe(sctx, c.runner, c.sshHost(h), c.cfg.Interval.String())
			if err != nil {
				srcOK("host", h.Name, false, time.Since(start), err.Error())
				return
			}
			mu.Lock()
			snap.Flows = append(snap.Flows, fs...)
			mu.Unlock()
			srcOK("host", h.Name, true, time.Since(start), "")
		})
	}

	// --- optional PG replication context -----------------------------------
	if c.cfg.ReplicationContext {
		for _, h := range c.cfg.HostsWith(config.RolePG) {
			h := h
			run(func() {
				if h.SSHDisabled || h.PG == nil || h.PG.Adapter != "ssh-psql" {
					return
				}
				sctx, cancel := context.WithTimeout(ctx, timeout+c.cfg.SSH.Timeout)
				defer cancel()
				start := time.Now()
				b := backendsql.Probe(sctx, c.runner, c.sshHost(h), h.PG.User, h.PG.Port, h.PG.DataDir)
				mu.Lock()
				snap.Backends = append(snap.Backends, b)
				mu.Unlock()
				srcOK("pg", h.Name, b.Reachable, time.Since(start), b.Err)
			})
		}
	}

	wg.Wait()

	// --- assemble Patroni state --------------------------------------------
	if len(clusterMembers) > 0 {
		snap.Patroni.Available = true
		snap.Patroni.Members = clusterMembers
		snap.Patroni.Paused = &clusterPaused
		snap.Patroni.Scheduled = clusterSched
		snap.Patroni.History = clusterHistory
		for i, m := range snap.Patroni.Members {
			// per-member API evidence when the member maps to a
			// configured host we probed directly
			if ok, found := memberProbe[m.Name]; found {
				snap.Patroni.Members[i].APIReachable = ok
			} else {
				for hn, ok := range memberProbe {
					if strings.Contains(m.APIURL, hn) || m.Host == hn {
						snap.Patroni.Members[i].APIReachable = ok
						break
					}
				}
			}
			if m.Role == "leader" || m.Role == "standby_leader" {
				snap.Patroni.Leader = m.Name
			}
		}
	} else {
		snap.Patroni.Available = false
		if len(clusterErrs) > 0 {
			snap.Patroni.Err = clusterErrs[0]
		} else if len(patroniHosts) == 0 {
			snap.Patroni.Err = "no patroni hosts configured"
		}
	}

	// --- assemble etcd state ------------------------------------------------
	var healthy, quorum int
	var version string
	for _, m := range snap.Etcd.Members {
		if m.Reachable && m.Healthy {
			healthy++
		}
		if m.IsLeader && snap.Etcd.LeaderEndpoint == "" {
			snap.Etcd.LeaderEndpoint = m.Endpoint
		}
	}
	total := len(snap.Etcd.Members)
	if total > 0 {
		quorum = total/2 + 1
		snap.Etcd.Available = healthy > 0
		snap.Etcd.QuorumKnown = healthy > 0 // unreachable observers cannot disprove quorum
		snap.Etcd.QuorumLost = healthy < quorum
		if ver, err := etcd.NewClient(snap.Etcd.Members[0].Endpoint, c.cfg.Etcd.DialTimeout).Version(ctx); err == nil {
			version = ver
		}
		snap.Etcd.Version = version
	} else if len(c.cfg.Etcd.Endpoints) == 0 {
		snap.Etcd.Err = "no etcd endpoints configured"
	} else {
		snap.Etcd.Err = "no etcd endpoint reachable"
	}

	// --- VIP observations ----------------------------------------------------
	for _, v := range c.cfg.VIPs {
		chk := model.VIPCheck{Address: v.Address, Interface: v.Interface, Policy: v.Policy}
		var sawAny bool
		for _, n := range snap.Keepalived {
			if !n.Reachable {
				continue
			}
			sawAny = true
			if n.VIPPresent {
				chk.Holders = append(chk.Holders, n.Host)
			}
		}
		switch {
		case len(chk.Holders) > 0:
			chk.Observed = true
		case sawAny:
			chk.Absent = true // verified absent on reachable hosts
		default:
			chk.Unknown = true
		}
		snap.VIP = append(snap.VIP, chk)
	}

	// deterministic ordering — parallel collection appends in completion
	// order, which would make dashboard layout jump between cycles
	sort.Slice(snap.Keepalived, func(i, j int) bool { return snap.Keepalived[i].Host < snap.Keepalived[j].Host })
	sort.Slice(snap.Etcd.Members, func(i, j int) bool { return snap.Etcd.Members[i].Endpoint < snap.Etcd.Members[j].Endpoint })
	sort.Slice(snap.Backends, func(i, j int) bool { return snap.Backends[i].Host < snap.Backends[j].Host })
	sort.Slice(snap.Flows, func(i, j int) bool {
		if snap.Flows[i].Host != snap.Flows[j].Host {
			return snap.Flows[i].Host < snap.Flows[j].Host
		}
		return snap.Flows[i].Iface < snap.Flows[j].Iface
	})
	sort.Slice(snap.Sources, func(i, j int) bool {
		if snap.Sources[i].Source != snap.Sources[j].Source {
			return snap.Sources[i].Source < snap.Sources[j].Source
		}
		return snap.Sources[i].Target < snap.Sources[j].Target
	})
	sort.Slice(snap.Patroni.Members, func(i, j int) bool { return snap.Patroni.Members[i].Name < snap.Patroni.Members[j].Name })

	snap.DurationMs = time.Since(t0).Milliseconds()
	snap.Degraded = len(snap.Errors) > 0 || !snap.Patroni.Available || (snap.Etcd.QuorumKnown && snap.Etcd.QuorumLost)
	if snap.Degraded {
		var parts []string
		for _, e := range snap.Errors {
			parts = append(parts, e.Source+"@"+e.Target)
		}
		if !snap.Patroni.Available {
			parts = append(parts, "patroni-unavailable")
		}
		snap.DegradedReason = join(parts, "; ")
	}
	return snap
}

func join(ss []string, sep string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += sep
		}
		out += s
	}
	return out
}
