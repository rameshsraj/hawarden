package cli

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/rameshsraj/hawarden/internal/model"
)

func tw(w io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
}

func renderStatus(w io.Writer, s *model.Snapshot) {
	fmt.Fprintf(w, "cluster: %s   taken: %s   cycle: %dms   degraded: %v\n",
		s.Cluster, s.Taken.Format("15:04:05"), s.DurationMs, s.Degraded)
	if s.DegradedReason != "" {
		fmt.Fprintf(w, "degraded: %s\n", s.DegradedReason)
	}
	renderCluster(w, s)
	renderEtcd(w, s)
	renderVIP(w, s)
	renderDiagnose(w, s)
}

func renderCluster(w io.Writer, s *model.Snapshot) {
	fmt.Fprintln(w, "\n== patroni ==")
	if !s.Patroni.Available {
		fmt.Fprintf(w, "unavailable: %s\n", s.Patroni.Err)
		return
	}
	paused := "no"
	if s.Patroni.Paused != nil && *s.Patroni.Paused {
		paused = "YES (auto-failover off)"
	}
	fmt.Fprintf(w, "leader: %s   paused: %s\n", orDash(s.Patroni.Leader), paused)
	t := tw(w)
	fmt.Fprintln(t, "NAME\tHOST\tROLE\tSTATE\tTL\tLAG\tRESTART\tAPI")
	for _, m := range s.Patroni.Members {
		fmt.Fprintf(t, "%s\t%s\t%s\t%s\t%s\t%s\t%v\t%v\n",
			m.Name, orDash(m.Host), orDash(m.Role), orDash(m.State),
			intOrDash(m.Timeline), lagOrDash(m.LagBytes), m.PendingRestart, m.APIReachable)
	}
	t.Flush()
}

func renderEtcd(w io.Writer, s *model.Snapshot) {
	fmt.Fprintln(w, "\n== etcd ==")
	if len(s.Etcd.Members) == 0 {
		fmt.Fprintf(w, "unavailable: %s\n", orDash(s.Etcd.Err))
		return
	}
	q := "quorum unknown"
	if s.Etcd.QuorumKnown {
		if s.Etcd.QuorumLost {
			q = "QUORUM LOST"
		} else {
			q = "quorum ok"
		}
	}
	fmt.Fprintf(w, "version: %s   leader: %s   %s\n",
		orDash(s.Etcd.Version), orDash(s.Etcd.LeaderEndpoint), q)
	t := tw(w)
	fmt.Fprintln(t, "ENDPOINT\tHEALTHY\tLEADER\tDB BYTES\tLEADER CHANGES\tERR")
	for _, m := range s.Etcd.Members {
		role := ""
		if m.IsLeader {
			role = "leader"
		} else if m.HasLeader {
			role = "follower"
		}
		fmt.Fprintf(t, "%s\t%v\t%s\t%s\t%s\t%s\n",
			m.Endpoint, m.Healthy, orDash(role), int64OrDash(m.DBSizeBytes),
			int64OrDash(m.LeaderChanges), orDash(m.Err))
	}
	t.Flush()
}

func renderVIP(w io.Writer, s *model.Snapshot) {
	fmt.Fprintln(w, "\n== vip / keepalived ==")
	t := tw(w)
	fmt.Fprintln(t, "HOST\tREACHABLE\tPROC\tVRRP\tVIP\tIFACE\tTRANSITION")
	for _, n := range s.Keepalived {
		fmt.Fprintf(t, "%s\t%v\t%v\t%s\t%v\t%s\t%s\n",
			n.Host, n.Reachable, n.ProcessUp, orDash(n.VRRPState), n.VIPPresent,
			orDash(n.VIPInterface), orDash(n.TransitionEvidence))
	}
	for _, v := range s.VIP {
		st := "unknown"
		if v.Observed {
			st = "held by " + strings.Join(v.Holders, ",")
		} else if v.Absent {
			st = "ABSENT"
		}
		fmt.Fprintf(t, "vip %s (%s)\t\t\t\t%s\t%s\t\n",
			v.Address, orDash(v.Policy), st, orDash(v.Interface))
	}
	t.Flush()
}

func renderTopology(w io.Writer, s *model.Snapshot) {
	fmt.Fprintln(w, "\n== topology ==")
	for _, v := range s.VIP {
		holders := "none"
		if len(v.Holders) > 0 {
			holders = strings.Join(v.Holders, ",")
		}
		fmt.Fprintf(w, "clients -> VIP %s -> keepalived[%s]\n", v.Address, holders)
	}
	for _, m := range s.Patroni.Members {
		fmt.Fprintf(w, "  patroni[%s] role=%s state=%s -> pg[%s]\n", m.Name, m.Role, m.State, m.Host)
	}
	var eps []string
	for _, m := range s.Etcd.Members {
		mark := "up"
		if !m.Reachable {
			mark = "down"
		}
		eps = append(eps, m.Endpoint+"("+mark+")")
	}
	if len(eps) > 0 {
		fmt.Fprintf(w, "  etcd ensemble: %s\n", strings.Join(eps, " <-> "))
	}
}

func renderEvents(w io.Writer, s *model.Snapshot) {
	fmt.Fprintln(w, "\n== events (patroni history) ==")
	if len(s.Patroni.History) == 0 {
		fmt.Fprintln(w, "no history observed")
		return
	}
	t := tw(w)
	fmt.Fprintln(t, "TL\tLSN\tREASON\tTIME\tNEW LEADER")
	for _, e := range s.Patroni.History {
		fmt.Fprintf(t, "%d\t%d\t%s\t%s\t%s\n", e.Timeline, e.LSN, e.Reason, e.Timestamp, e.NewLeader)
	}
	t.Flush()
}

func renderHealth(w io.Writer, s *model.Snapshot) {
	fmt.Fprintln(w, "\n== source health ==")
	t := tw(w)
	fmt.Fprintln(t, "SOURCE\tTARGET\tOK\tMS\tERR")
	for _, h := range s.Sources {
		fmt.Fprintf(t, "%s\t%s\t%v\t%d\t%s\n", h.Source, h.Target, h.OK, h.DurationMs, orDash(h.Err))
	}
	t.Flush()
}

func renderDiagnose(w io.Writer, s *model.Snapshot) {
	fmt.Fprintln(w, "\n== findings ==")
	if len(s.Findings) == 0 {
		fmt.Fprintln(w, "none")
		return
	}
	for _, f := range s.Findings {
		fmt.Fprintf(w, "[%s] %s — %s\n", f.Severity, f.Code, f.Subject)
		for _, e := range f.Evidence {
			fmt.Fprintf(w, "    evidence: %s\n", e)
		}
		for _, n := range f.Next {
			fmt.Fprintf(w, "    next: %s\n", n)
		}
	}
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func intOrDash(n int) string {
	if n < 0 {
		return "-"
	}
	return fmt.Sprint(n)
}

func int64OrDash(n int64) string {
	if n < 0 {
		return "-"
	}
	return fmt.Sprint(n)
}

func lagOrDash(n int64) string {
	if n < 0 {
		return "-"
	}
	return fmt.Sprintf("%dB", n)
}
