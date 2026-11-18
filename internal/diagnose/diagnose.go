// Package diagnose implements the section-4 rule set. A rule fires only on
// evidence present in the snapshot; a missing observation produces an INFO
// "insufficient evidence" note, never a guessed severity.
package diagnose

import (
	"fmt"
	"strings"
	"time"

	"github.com/rameshsraj/hawarden/internal/model"
)

// Run evaluates every rule against the snapshot and returns findings.
func Run(snap *model.Snapshot) []model.Finding {
	var out []model.Finding
	now := snap.Taken
	if now.IsZero() {
		now = time.Now()
	}
	add := func(code, sev, subject string, ev, causes, next []string) {
		out = append(out, model.Finding{
			Code: code, Severity: sev, Subject: subject,
			Evidence: ev, Causes: causes, Next: next, FirstSeen: now,
		})
	}

	// --- SPLIT_BRAIN / ROLE_MISMATCH ---------------------------------------
	var leaders []string
	for _, m := range snap.Patroni.Members {
		if m.Role == "leader" || m.Role == "standby_leader" {
			leaders = append(leaders, m.Name)
		}
	}
	if len(leaders) > 1 {
		add("SPLIT_BRAIN", "CRIT",
			fmt.Sprintf("%d members report leader role", len(leaders)),
			leaders,
			[]string{"DCS split or stale member state", "two primaries accepting writes"},
			[]string{"hawarden cluster", "hawarden events"})
	}
	if len(snap.Patroni.Members) > 0 && len(leaders) == 0 && snap.Patroni.Available {
		add("NO_LEADER", "CRIT", "cluster has no observed leader",
			[]string{"patroni /cluster returned members with no leader role"},
			[]string{"leader absent during failover", "DCS outage"},
			[]string{"hawarden events", "hawarden dcs"})
	}

	// --- DCS ---------------------------------------------------------------
	if snap.Etcd.QuorumKnown && snap.Etcd.QuorumLost {
		var ev []string
		for _, m := range snap.Etcd.Members {
			st := "down"
			if m.Reachable {
				st = "up"
			}
			ev = append(ev, fmt.Sprintf("%s %s (healthy=%v)", m.Endpoint, st, m.Healthy))
		}
		add("DCS_QUORUM_LOST", "CRIT", "etcd quorum lost or unproven", ev,
			[]string{"majority of voting members unreachable", "network partition"},
			[]string{"hawarden dcs", "hawarden events"})
	}
	for _, m := range snap.Etcd.Members {
		if !m.Reachable {
			add("ETCD_MEMBER_DOWN", "WARN",
				"etcd endpoint unreachable: "+m.Endpoint,
				[]string{m.Err},
				[]string{"member stopped", "network issue", "endpoint removed"},
				[]string{"hawarden dcs"})
		}
	}
	for _, a := range snap.Etcd.Alarms {
		add("ETCD_ALARM", "CRIT", "etcd alarm: "+a, []string{a},
			[]string{"NOSPACE or CORRUPT alarm raised"},
			[]string{"hawarden dcs"})
	}
	name := map[string]string{}
	for _, r := range snap.Etcd.Roster {
		name[r.ID] = r.Name
		if r.Endpoint == "" {
			add("ETCD_MEMBER_UNCONFIGURED", "INFO",
				"etcd member "+r.Name+" is in the cluster but not monitored",
				[]string{"member list: id " + r.ID + " client " + strings.Join(r.ClientURLs, ",")},
				[]string{"member added after hawarden was configured"},
				[]string{"add its client endpoint to etcd.endpoints in the hawarden config"})
		}
	}
	for _, l := range snap.Etcd.Links {
		if !l.Active {
			add("ETCD_PEER_DOWN", "WARN",
				fmt.Sprintf("no raft stream %s → %s", or(name[l.From], l.From), or(name[l.To], l.To)),
				[]string{"etcd_network_active_peers on " + or(name[l.From], l.From) + " has no entry for " + l.To},
				[]string{"peer down", "peer port 2380 blocked", "peer TLS failure"},
				[]string{"hawarden dcs"})
		}
	}

	// --- Patroni maintenance / restart ------------------------------------
	if snap.Patroni.Paused != nil && *snap.Patroni.Paused {
		add("CLUSTER_PAUSED", "WARN", "Patroni maintenance mode is ON — auto-failover disabled",
			[]string{"pause flag set in DCS config"},
			[]string{"operator enabled maintenance mode"},
			[]string{"hawarden cluster"})
	}
	for _, m := range snap.Patroni.Members {
		if m.PendingRestart {
			add("PENDING_RESTART", "WARN",
				"member "+m.Name+" needs a restart for pending config",
				[]string{"pending_restart=true on " + m.Name},
				[]string{"dynamic config change requires restart"},
				[]string{"hawarden cluster"})
		}
		if !m.APIReachable && m.State != "" {
			add("API_UNREACHABLE", "WARN",
				"Patroni REST unreachable on "+m.Name,
				[]string{m.Err},
				[]string{"API down; state may be stale"},
				[]string{"hawarden health"})
		}
		if m.State != "" && m.State != "running" && m.State != "streaming" {
			add("MEMBER_STATE", "WARN",
				fmt.Sprintf("%s state=%s", m.Name, m.State),
				[]string{"patroni reported state " + m.State},
				[]string{"member restarting, stopped or creating replica"},
				[]string{"hawarden cluster", "hawarden events"})
		}
	}
	if snap.Patroni.Scheduled != nil {
		add("SCHED_SWITCHOVER", "INFO",
			"switchover scheduled at "+snap.Patroni.Scheduled.At,
			[]string{fmt.Sprintf("%+v", *snap.Patroni.Scheduled)},
			[]string{"operator scheduled a switchover"},
			[]string{"hawarden events"})
	}

	// --- standby health -----------------------------------------------------
	var standbys int
	for _, m := range snap.Patroni.Members {
		if m.Role == "replica" && (m.State == "running" || m.State == "streaming") {
			standbys++
		}
	}
	if snap.Patroni.Available && len(snap.Patroni.Members) > 1 && standbys == 0 {
		add("NO_STANDBYS", "WARN", "no streaming standby members observed",
			[]string{fmt.Sprintf("%d members, 0 streaming replicas", len(snap.Patroni.Members))},
			[]string{"replicas down or still creating", "failover in progress"},
			[]string{"hawarden cluster", "hawarden lag"})
	}

	// --- VIP -----------------------------------------------------------------
	for _, v := range snap.VIP {
		switch {
		case len(v.Holders) > 1:
			add("DUAL_VIP", "CRIT",
				fmt.Sprintf("VIP %s present on %d hosts", v.Address, len(v.Holders)),
				v.Holders,
				[]string{"VRRP split-brain", "stale address not withdrawn"},
				[]string{"hawarden vip", "hawarden topology"})
		case v.Absent:
			add("VIP_ABSENT", "CRIT",
				"VIP "+v.Address+" absent on all reachable keepalived hosts",
				[]string{"no interface carries " + v.Address},
				[]string{"keepalived down everywhere", "VRRP election lost"},
				[]string{"hawarden vip", "hawarden health"})
		case v.Unknown:
			add("VIP_UNKNOWN", "INFO",
				"VIP "+v.Address+" unverified — no reachable keepalived host",
				[]string{"all keepalived sources unreachable"},
				[]string{"insufficient evidence either way"},
				[]string{"hawarden health"})
		case len(v.Holders) == 1 && v.Policy == "leader_only":
			// VIP holder should be the Patroni leader
			if snap.Patroni.Leader != "" && v.Holders[0] != snap.Patroni.Leader {
				add("VIP_ON_STANDBY", "CRIT",
					fmt.Sprintf("VIP %s on %s but Patroni leader is %s", v.Address, v.Holders[0], snap.Patroni.Leader),
					[]string{"leader_only policy observed violated"},
					[]string{"VIP failed to follow leader", "VRRP misconfigured"},
					[]string{"hawarden vip", "hawarden cluster"})
			}
		}
	}
	for _, n := range snap.Keepalived {
		if n.Reachable && !n.ProcessUp {
			add("KEEPALIVED_DOWN", "WARN",
				"keepalived process missing on "+n.Host,
				[]string{"pidof/pgrep returned nothing"},
				[]string{"service stopped or crashed"},
				[]string{"hawarden vip"})
		}
	}

	// --- HAProxy routing -----------------------------------------------------
	if p := snap.Proxy; p != nil {
		for _, l := range p.Listeners {
			subj := fmt.Sprintf("%s :%d (%s)", l.Name, l.Port, l.Check)
			ev := []string{fmt.Sprintf("GET %s → 200 on [%s]", l.Check, strings.Join(l.Eligible, ","))}
			if len(l.Unreachable) > 0 {
				ev = append(ev, "check unreadable on "+strings.Join(l.Unreachable, ","))
			}
			switch {
			case l.Write && len(l.Eligible) == 0:
				add("PROXY_NO_WRITE_TARGET", "CRIT", subj+" has no eligible primary — writes fail", ev,
					[]string{"no leader", "leader Patroni API unreachable"},
					[]string{"hawarden cluster", "hawarden topology"})
			case l.Write && len(l.Eligible) > 1:
				add("PROXY_MULTI_WRITE", "CRIT", subj+" admits more than one primary", ev,
					[]string{"split brain", "stale leader still answering /primary"},
					[]string{"hawarden cluster"})
			case !l.Write && len(l.Eligible) == 0:
				add("PROXY_NO_READ_TARGET", "WARN", subj+" has no eligible member — reads fail", ev,
					[]string{"no streaming replica"},
					[]string{"hawarden cluster", "hawarden lag"})
			}
			for _, v := range snap.VIP {
				if open, ok := l.VIPOpen[v.Address]; ok && !open {
					add("PROXY_VIP_CLOSED", "CRIT",
						fmt.Sprintf("VIP %s:%d not accepting connections", v.Address, l.Port),
						[]string{"TCP connect to " + v.Address + " failed"},
						[]string{"VIP not held", "haproxy down on VIP holder"},
						[]string{"hawarden vip", "hawarden topology"})
				}
			}
		}
		for _, n := range p.Nodes {
			for _, l := range p.Listeners {
				if open, ok := n.Open[l.Name]; ok && !open {
					add("PROXY_LISTENER_DOWN", "WARN",
						fmt.Sprintf("haproxy listener %s closed on %s", l.Name, n.Host),
						[]string{"TCP connect to " + n.Address + " failed"},
						[]string{"haproxy stopped", "bind failed"},
						[]string{"hawarden topology"})
				}
			}
		}
	}

	return out
}

func or(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
