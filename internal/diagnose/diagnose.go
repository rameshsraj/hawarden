// Package diagnose implements the section-4 rule set. A rule fires only on
// evidence present in the snapshot; a missing observation produces an INFO
// "insufficient evidence" note, never a guessed severity.
package diagnose

import (
	"fmt"
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

	return out
}
