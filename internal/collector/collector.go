// Package collector builds one model.Snapshot per cycle. Every source runs
// concurrently with its own timeout; a dead source degrades the snapshot
// without killing it.
package collector

import (
	"context"
	"crypto/tls"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rameshsraj/hawarden/internal/backendsql"
	"github.com/rameshsraj/hawarden/internal/config"
	"github.com/rameshsraj/hawarden/internal/etcd"
	"github.com/rameshsraj/hawarden/internal/hostcounters"
	"github.com/rameshsraj/hawarden/internal/kalive"
	"github.com/rameshsraj/hawarden/internal/model"
	"github.com/rameshsraj/hawarden/internal/netobs"
	"github.com/rameshsraj/hawarden/internal/patroni"
	"github.com/rameshsraj/hawarden/internal/remote"
)

// Collector owns the per-cycle fan-out. Create once per monitor process so
// rate state survives across cycles.
type Collector struct {
	cfg     *config.Config
	runner  *remote.SSHRunner
	prev    *hostcounters.Prev
	etcdTLS *tls.Config
	tlsErr  error // etcd TLS refs could not be loaded; reported per endpoint

	peerMu   sync.Mutex
	peerPrev map[string]peerSample // "from>to" -> last cumulative sent bytes
}

type peerSample struct {
	bytes int64
	at    time.Time
}

func New(cfg *config.Config) *Collector {
	tc, err := etcd.TLSConfig(cfg.Etcd.CACertRef, cfg.Etcd.CertRef, cfg.Etcd.KeyRef)
	return &Collector{
		cfg:      cfg,
		runner:   remote.NewSSHRunner(cfg.SSH.Key, cfg.SSH.KnownHosts, cfg.SSH.Timeout, cfg.SSH.StrictHostKey),
		prev:     hostcounters.NewPrev(),
		etcdTLS:  tc,
		tlsErr:   err,
		peerPrev: map[string]peerSample{},
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
	// srcSkip records a source the config deliberately disabled: visible in
	// source health, but not an error and not a degradation.
	srcSkip := func(source, target, why string) {
		mu.Lock()
		snap.Sources = append(snap.Sources, model.SourceHealth{Source: source, Target: target, Skipped: true, Err: why})
		mu.Unlock()
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
	peerSent := map[string]map[string]int64{} // member ID -> remote ID -> cumulative sent bytes
	for _, ep := range c.cfg.Etcd.Endpoints {
		ep := ep
		run(func() {
			sctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			start := time.Now()
			var m model.EtcdMember
			m.Endpoint = ep
			m.LeaderChanges, m.DBSizeBytes, m.RaftIndex, m.RaftTerm = -1, -1, -1, -1
			var sent map[string]int64
			if c.tlsErr != nil {
				m.Err = c.tlsErr.Error()
			} else {
				sent = etcd.NewClient(ep, c.cfg.Etcd.DialTimeout, c.etcdTLS).ProbeEndpoint(sctx, &m)
			}
			mu.Lock()
			snap.Etcd.Members = append(snap.Etcd.Members, m)
			if m.ID != "" && sent != nil {
				peerSent[m.ID] = sent
			}
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
				srcSkip("keepalived", h.Name, n.Err)
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

	// --- HAProxy tier: routing contract + listener reachability -------------
	// Slices are fully sized before any goroutine indexes into them.
	var proxy *model.ProxyState
	if len(c.cfg.Proxy) > 0 {
		proxy = &model.ProxyState{}
		for _, l := range c.cfg.Proxy {
			pl := model.ProxyListener{Name: l.Name, Port: l.Port, Check: l.Check,
				Write: isWriteCheck(l.Check), Eligible: []string{}}
			if len(c.cfg.VIPs) > 0 {
				pl.VIPOpen = map[string]bool{}
			}
			proxy.Listeners = append(proxy.Listeners, pl)
		}
		for _, h := range c.cfg.HostsWith(config.RoleHAProxy) {
			proxy.Nodes = append(proxy.Nodes, model.ProxyNode{Host: h.Name, Address: h.Address, Open: map[string]bool{}})
		}
		for li, l := range c.cfg.Proxy {
			li, l := li, l
			port := strconv.Itoa(l.Port)
			for _, h := range patroniHosts {
				h := h
				run(func() {
					sctx, cancel := context.WithTimeout(ctx, timeout)
					defer cancel()
					code, err := patroni.NewClient(c.cfg.PatroniAPI(h), "", "", c.cfg.Patroni.Timeout).Check(sctx, l.Check)
					mu.Lock()
					defer mu.Unlock()
					pl := &proxy.Listeners[li]
					switch {
					case err != nil:
						pl.Unreachable = append(pl.Unreachable, h.Name)
					case code == 200:
						pl.Eligible = append(pl.Eligible, h.Name)
					}
				})
			}
			for _, v := range c.cfg.VIPs {
				v := v
				run(func() {
					ok := netobs.TCPOpen(ctx, net.JoinHostPort(v.Address, port), timeout)
					mu.Lock()
					proxy.Listeners[li].VIPOpen[v.Address] = ok
					mu.Unlock()
				})
			}
			for ni := range proxy.Nodes {
				ni := ni
				addr := net.JoinHostPort(proxy.Nodes[ni].Address, port)
				run(func() {
					ok := netobs.TCPOpen(ctx, addr, timeout)
					mu.Lock()
					proxy.Nodes[ni].Open[l.Name] = ok
					mu.Unlock()
				})
			}
		}
	}

	wg.Wait()

	if proxy != nil {
		for i := range proxy.Listeners {
			sort.Strings(proxy.Listeners[i].Eligible)
			sort.Strings(proxy.Listeners[i].Unreachable)
		}
		snap.Proxy = proxy
	}

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
		if ver, err := etcd.NewClient(snap.Etcd.Members[0].Endpoint, c.cfg.Etcd.DialTimeout, c.etcdTLS).Version(ctx); err == nil {
			version = ver
		}
		snap.Etcd.Version = version
		c.etcdTopology(ctx, snap, peerSent)
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
			chk.Evidence = "ssh"
		case sawAny:
			chk.Absent = true // verified absent on reachable hosts
		default:
			chk.Unknown = true
		}
		snap.VIP = append(snap.VIP, chk)
	}
	c.inferVIPByARP(ctx, snap)

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

// etcdTopology fills the roster (from the cluster's own MemberList, so new
// members appear without a config change), the leader ID, and the raft
// links each reachable member reports from its own peer metrics.
func (c *Collector) etcdTopology(ctx context.Context, snap *model.Snapshot, peerSent map[string]map[string]int64) {
	byID := map[string]*model.EtcdMember{}
	for i := range snap.Etcd.Members {
		m := &snap.Etcd.Members[i]
		if m.ID == "" {
			continue
		}
		byID[m.ID] = m
		if m.LeaderID != "" && m.LeaderID == m.ID {
			m.IsLeader = true
		}
		if snap.Etcd.LeaderID == "" && m.LeaderID != "" {
			snap.Etcd.LeaderID = m.LeaderID
		}
	}
	for _, m := range snap.Etcd.Members {
		if !m.Reachable || c.tlsErr != nil {
			continue
		}
		sctx, cancel := context.WithTimeout(ctx, c.cfg.Etcd.DialTimeout)
		roster, err := etcd.NewClient(m.Endpoint, c.cfg.Etcd.DialTimeout, c.etcdTLS).MemberList(sctx)
		cancel()
		if err != nil {
			continue
		}
		for i := range roster {
			if pm, ok := byID[roster[i].ID]; ok {
				roster[i].Endpoint = pm.Endpoint
				pm.Name = roster[i].Name
			}
		}
		sort.Slice(roster, func(i, j int) bool { return roster[i].Name < roster[j].Name })
		snap.Etcd.Roster = roster
		break
	}

	ids := make([]string, 0, len(snap.Etcd.Roster))
	for _, r := range snap.Etcd.Roster {
		ids = append(ids, r.ID)
	}
	if len(ids) == 0 {
		for id := range byID {
			ids = append(ids, id)
		}
		sort.Strings(ids)
	}
	now := time.Now()
	c.peerMu.Lock()
	defer c.peerMu.Unlock()
	for _, from := range ids {
		m, ok := byID[from]
		if !ok || !m.Reachable || !m.PeersKnown {
			continue // no evidence from this sender
		}
		for _, to := range ids {
			if to == from {
				continue
			}
			l := model.EtcdLink{From: from, To: to, Bps: -1}
			for _, p := range m.ActivePeers {
				if p == to {
					l.Active = true
				}
			}
			if b, ok := peerSent[from][to]; ok {
				key := from + ">" + to
				if p, ok := c.peerPrev[key]; ok && b >= p.bytes && now.After(p.at) {
					l.Bps = float64(b-p.bytes) / now.Sub(p.at).Seconds()
				}
				c.peerPrev[key] = peerSample{bytes: b, at: now}
			}
			snap.Etcd.Links = append(snap.Etcd.Links, l)
		}
	}
}

// primary (a read-write listener).
func isWriteCheck(path string) bool {
	switch strings.TrimSuffix(path, "/") {
	case "/primary", "/master", "/leader", "/read-write":
		return true
	}
	return false
}

// inferVIPByARP resolves a VIP that SSH could not verify. When the
// collector shares the L2 segment, the VIP's ARP MAC equals the MAC of the
// keepalived/haproxy host holding it. Only an exact single-host match is
// accepted; routed addresses never appear in the ARP table.
func (c *Collector) inferVIPByARP(ctx context.Context, snap *model.Snapshot) {
	var pending []int
	for i, v := range snap.VIP {
		if v.Unknown {
			pending = append(pending, i)
		}
	}
	if len(pending) == 0 {
		return
	}
	port := "22"
	if len(c.cfg.Proxy) > 0 {
		port = strconv.Itoa(c.cfg.Proxy[0].Port)
	}
	cands := map[string]string{} // host name -> IPv4
	for _, h := range c.cfg.Hosts {
		if h.Has(config.RoleKeepalived) || h.Has(config.RoleHAProxy) {
			if ip := netobs.ResolveIPv4(ctx, h.Address); ip != "" {
				cands[h.Name] = ip
			}
		}
	}
	// any TCP attempt, even a refused one, makes the kernel resolve the neighbour
	var wg sync.WaitGroup
	touch := func(ip string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			netobs.TCPOpen(ctx, net.JoinHostPort(ip, port), c.cfg.Timeout)
		}()
	}
	for _, ip := range cands {
		touch(ip)
	}
	for _, i := range pending {
		touch(snap.VIP[i].Address)
	}
	wg.Wait()
	arp, err := netobs.ARPTable()
	if err != nil {
		return
	}
	for _, i := range pending {
		v := &snap.VIP[i]
		vmac := arp[v.Address]
		if vmac == "" {
			continue
		}
		var match []string
		for name, ip := range cands {
			if arp[ip] == vmac {
				match = append(match, name)
			}
		}
		if len(match) == 1 {
			v.Holders, v.Observed, v.Unknown = match, true, false
			v.Evidence = "arp: " + v.Address + " is-at " + vmac + " = " + match[0]
		}
	}
}
