// Package backendsql is the optional, least-privilege PostgreSQL replication
// context adapter. It only runs when config sets pg.adapter: ssh-psql, and it
// only queries the narrowly allowlisted WAL/replication fields the spec
// permits — never pg_stat_statements, locks, sessions or tuning data.
package backendsql

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/rameshsraj/hawarden/internal/model"
	"github.com/rameshsraj/hawarden/internal/remote"
)

// Fields queried, in order. Nothing else may be added without a spec change.
const probeSQL = `SELECT pg_is_in_recovery(),
  CASE WHEN pg_is_in_recovery() THEN '0/0' ELSE pg_current_wal_lsn()::text END,
  COALESCE(pg_last_wal_receive_lsn()::text,'0/0'),
  COALESCE(pg_last_wal_replay_lsn()::text,'0/0'),
  COALESCE(EXTRACT(EPOCH FROM now()-pg_last_xact_replay_timestamp())::bigint,-1)`

// Probe reads the replication-context row on one PG host via SSH+psql.
// pgUser/port select the local connection; empty user defaults to postgres.
// dataDir locates pg_wal for the on-disk size observation.
func Probe(ctx context.Context, r remote.Runner, h remote.Host, pgUser string, port int, dataDir string) model.Backend {
	b := model.Backend{Host: h.Name, Timeline: -1, CurrentLSN: -1, ReceiveLSN: -1, ReplayLSN: -1, ReplayDelaySec: -1, WalDirBytes: -1}
	if pgUser == "" {
		pgUser = "postgres"
	}
	if port == 0 {
		port = 5432
	}
	if dataDir == "" {
		dataDir = "/var/lib/postgresql/data"
	}
	cmd := fmt.Sprintf("su %s -c %s 2>/dev/null || psql -h 127.0.0.1 -p %d -U %s -d postgres -Atc %s",
		pgUser, shellQuote(fmt.Sprintf("psql -p %d -d postgres -Atc %s", port, shellQuote(probeSQL))),
		port, pgUser, shellQuote(probeSQL))
	out, err := r.Run(ctx, h, cmd)
	if err != nil {
		b.Err = err.Error()
		return b
	}
	line := strings.TrimSpace(string(out))
	if i := strings.IndexByte(line, '\n'); i >= 0 {
		line = line[:i]
	}
	f := strings.Split(line, "|")
	if len(f) < 5 {
		b.Err = "unexpected psql output: " + line
		return b
	}
	b.Reachable = true
	b.InRecovery = strings.HasPrefix(strings.TrimSpace(f[0]), "t")
	b.CurrentLSN = parseLSN(f[1])
	b.ReceiveLSN = parseLSN(f[2])
	b.ReplayLSN = parseLSN(f[3])
	b.ReplayDelaySec, _ = strconv.Atoi(strings.TrimSpace(f[4]))

	// WAL dir size: du -sb over the data dir's pg_wal (best effort).
	walOut, err := r.Run(ctx, h, "du -sb "+dataDir+"/pg_wal 2>/dev/null | cut -f1 || true")
	if err == nil {
		if n, perr := strconv.ParseInt(strings.TrimSpace(string(walOut)), 10, 64); perr == nil && n > 0 {
			b.WalDirBytes = n
		}
	}
	return b
}

// parseLSN converts "0/1A2B3C4" (or plain int) to bytes; "0/0" → -1.
func parseLSN(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" || s == "0/0" {
		return -1
	}
	if i := strings.IndexByte(s, '/'); i >= 0 {
		hi, err1 := strconv.ParseUint(s[:i], 16, 64)
		lo, err2 := strconv.ParseUint(s[i+1:], 16, 64)
		if err1 == nil && err2 == nil {
			return int64(hi<<32 | lo)
		}
		return -1
	}
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
