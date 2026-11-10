# hawarden

CLI command centre for one **Patroni + keepalived + etcd + PostgreSQL** HA
cluster, used by DBAs over SSH — plus an optional browser console that renders
the same data live. Pattern and layering rules mirror poolwarden.

## Commands

- `make build` — static CLI into bin/ (CGO_ENABLED=0, -trimpath, version ldflags)
- `make console` — static console gateway into bin/
- `make build-linux` — linux/amd64 + linux/arm64 into dist/
- `make test` — `go test -race ./...`
- `make vet` — `go vet` + `gofmt -l`

## Scope rule

Monitor the HA tooling, not the database engine. PostgreSQL is a contextual
topology node; the only optional collection is the least-privilege
`pg.adapter: ssh-psql` replication context (WAL/LSN fields only). Destructive
cluster controls are out of scope for the default release — the product is
observation plus management of monitoring connections.

## Layering rules

`internal/cli` parses and renders; all logic lives in `internal/service`
(never prints, never reads stdin). Every external system — Patroni HTTP,
etcd HTTP, SSH, keepalived, host counters — sits behind an interface faked in
tests. Writes are atomic (temp + fsync + rename). User-facing errors are
`*exitcode.Error` with an exit code and a `next:` hint; WARN exit 10, CRIT 11.

## Console

`hawardenconsole` (cmd/hawardenconsole + internal/console) is an OPTIONAL
gateway: it embeds the SPA in internal/console/dist (no build step) and
drives everything through the hawarden CLI — one shared `hawarden monitor`
NDJSON process for live data (SSE to browsers), allowlisted argv subprocesses
for reads and config mutations. CLI-only installs need neither Node nor the
console.

## Docker testbed

`testbed/` (local only, not pushed) holds a compose lab: 3×etcd, 3×
Patroni+PostgreSQL, unicast-VRRP keepalived on pg1/pg2, sshd on DB hosts, and
a `console` container running both binaries. Bring it up with
`docker compose -f testbed/docker-compose.yml up -d --build`; browse
http://localhost:8080 (token in testbed/console.env).
