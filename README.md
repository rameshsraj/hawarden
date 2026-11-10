# hawarden

Command centre for one **Patroni + keepalived + etcd + PostgreSQL** HA
cluster. One static Go binary a DBA runs over SSH to see what the HA stack is
doing — plus an optional browser console that renders the same data live.

hawarden **observes the HA tooling, not the database engine**. Patroni REST,
etcd client-port HTTP, keepalived via SSH (`ip -j addr`, notify state file,
journald) and host interface counters via `/proc/net/dev` are the default
sources. A separately enabled, least-privilege `ssh-psql` adapter may collect
narrowly allowlisted WAL/replication fields — nothing else is ever queried.

## Deliverables

- `hawarden` — standalone static CLI. One-shot views (`status`, `cluster`,
  `dcs`, `vip`, `topology`, `events`, `health`, `diagnose`), `monitor`
  NDJSON stream, `probe` per-endpoint checks, `config` management.
- `hawardenconsole` — optional web gateway embedding an SPA. It shells out
  to the CLI for every read and mutation; one shared `hawarden monitor`
  process feeds every browser over SSE. CLI-only installs need neither it
  nor Node.

## Consoles

- **Operations** — live monitor pipeline status, source event log and the
  operations journal (every console-driven probe/config change, with actor
  and outcome).
- **Configuration** — the monitoring-connection manager: current settings,
  host inventory, and a four-step *add machine* wizard (identity → roles &
  endpoints → per-check probe → redacted review → save). Saving restarts the
  shared monitor so the machine appears on the live dashboard. Monitoring
  changes never touch Patroni, etcd or keepalived settings on the hosts.

## Quick start (Docker testbed)

See `AGENTS.md` for the local compose lab layout (testbed/ is intentionally
not published). For a real cluster:

```sh
hawarden config init                 # writes /etc/hawarden.yaml
hawarden -c hawarden.yaml status     # one-shot view; exit 10/11 on WARN/CRIT
hawarden -c hawarden.yaml monitor    # NDJSON stream (~2s cadence)
hawardenconsole -token $TOKEN -addr 127.0.0.1:8080 -config hawarden.yaml
```

## Security notes

Passwords/certs are `*_ref` file references only — never stored in config,
logs, audit or browser storage. Console auth: token → HttpOnly session
cookie + CSRF double-submit; binds 127.0.0.1 by default; optional TLS.
