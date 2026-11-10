// Package model holds hawarden's pure snapshot types. Stdlib only: every
// field carries its source and an explicit "unknown" representation (-1,
// empty, omitted) so no screen ever has to invent a value.
package model

import "time"

// SchemaVersion is the snapshot contract version the console checks before
// reusing metric definitions.
const SchemaVersion = 1

// Snapshot is one collection cycle across every configured source.
type Snapshot struct {
	Schema         int              `json:"schema"`
	Cluster        string           `json:"cluster"`
	Taken          time.Time        `json:"taken"`
	DurationMs     int64            `json:"duration_ms"`
	Degraded       bool             `json:"degraded"`
	DegradedReason string           `json:"degraded_reason,omitempty"`
	Patroni        PatroniState     `json:"patroni"`
	Etcd           EtcdState        `json:"etcd"`
	Keepalived     []KeepalivedNode `json:"keepalived"`
	Backends       []Backend        `json:"backends,omitempty"`
	Flows          []FlowSample     `json:"flows,omitempty"`
	VIP            []VIPCheck       `json:"vip"`
	Findings       []Finding        `json:"findings,omitempty"`
	Sources        []SourceHealth   `json:"sources"`
	Errors         []SourceError    `json:"errors,omitempty"`
}

// PatroniState is the cluster view assembled from member API observations.
type PatroniState struct {
	Available bool             `json:"available"`
	Err       string           `json:"err,omitempty"`
	Paused    *bool            `json:"paused,omitempty"`
	Leader    string           `json:"leader,omitempty"`
	Members   []PatroniMember  `json:"members"`
	Scheduled *ScheduledSwitch `json:"scheduled_switchover,omitempty"`
	History   []HistoryEntry   `json:"history,omitempty"`
}

// PatroniMember is one row of the Patroni member table.
type PatroniMember struct {
	Name             string            `json:"name"`
	Host             string            `json:"host"`
	APIURL           string            `json:"api_url"`
	Role             string            `json:"role"` // leader|replica|standby_leader|...
	State            string            `json:"state"`
	Timeline         int               `json:"timeline"`  // -1 unknown
	LagBytes         int64             `json:"lag_bytes"` // -1 unknown
	PendingRestart   bool              `json:"pending_restart"`
	ScheduledRestart string            `json:"scheduled_restart,omitempty"`
	APIReachable     bool              `json:"api_reachable"`
	Err              string            `json:"err,omitempty"`
	Tags             map[string]string `json:"tags,omitempty"`
}

// ScheduledSwitch is a pending Patroni switchover observation.
type ScheduledSwitch struct {
	At   string `json:"at"`
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
}

// HistoryEntry is one Patroni /history row (failover/switchover evidence).
type HistoryEntry struct {
	Timeline  int    `json:"timeline"`
	LSN       int64  `json:"lsn"`
	Reason    string `json:"reason"`
	Timestamp string `json:"timestamp"`
	NewLeader string `json:"new_leader"`
}

// EtcdState is the DCS coordination view.
type EtcdState struct {
	Available      bool         `json:"available"`
	Err            string       `json:"err,omitempty"`
	LeaderID       string       `json:"leader_id,omitempty"`
	LeaderEndpoint string       `json:"leader_endpoint,omitempty"`
	Members        []EtcdMember `json:"members"`
	Alarms         []string     `json:"alarms,omitempty"`
	QuorumLost     bool         `json:"quorum_lost"`
	QuorumKnown    bool         `json:"quorum_known"` // false = insufficient evidence
	Version        string       `json:"version,omitempty"`
}

// EtcdMember is one etcd endpoint observation.
type EtcdMember struct {
	Endpoint      string `json:"endpoint"`
	Name          string `json:"name,omitempty"`
	Healthy       bool   `json:"healthy"`
	Reachable     bool   `json:"reachable"`
	HasLeader     bool   `json:"has_leader"`
	IsLeader      bool   `json:"is_leader"`      // true only when the member self-identifies as raft leader
	LeaderChanges int64  `json:"leader_changes"` // -1 unknown
	DBSizeBytes   int64  `json:"db_size_bytes"`  // -1 unknown
	RaftIndex     int64  `json:"raft_index"`     // -1 unknown
	RaftTerm      int64  `json:"raft_term"`      // -1 unknown
	Err           string `json:"err,omitempty"`
}

// KeepalivedNode is the VRRP observation on one host.
type KeepalivedNode struct {
	Host               string    `json:"host"`
	Reachable          bool      `json:"reachable"`
	Err                string    `json:"err,omitempty"`
	ProcessUp          bool      `json:"process_up"`
	VRRPState          string    `json:"vrrp_state"` // MASTER|BACKUP|FAULT|unknown
	VIPPresent         bool      `json:"vip_present"`
	VIPInterface       string    `json:"vip_interface,omitempty"`
	Priority           int       `json:"priority"` // -1 unknown
	LastTransition     time.Time `json:"last_transition,omitempty"`
	TransitionEvidence string    `json:"transition_evidence,omitempty"`
}

// VIPCheck is the expected-virtual-address observation for one address.
type VIPCheck struct {
	Address   string   `json:"address"`
	Interface string   `json:"interface,omitempty"`
	Policy    string   `json:"policy"`   // leader_only|proxy_tier|user_defined
	Holders   []string `json:"holders"`  // hosts observed holding the address
	Observed  bool     `json:"observed"` // at least one holder evidence
	Absent    bool     `json:"absent"`   // verified absent everywhere reachable
	Unknown   bool     `json:"unknown"`  // insufficient evidence either way
}

// Backend is a light PostgreSQL observation (replication context adapter
// only). Nothing else is ever queried.
type Backend struct {
	Host           string  `json:"host"`
	Reachable      bool    `json:"reachable"`
	Err            string  `json:"err,omitempty"`
	InRecovery     bool    `json:"in_recovery"`
	Timeline       int     `json:"timeline"` // -1 unknown
	CurrentLSN     int64   `json:"current_lsn"`
	ReceiveLSN     int64   `json:"receive_lsn"`      // -1 unknown
	ReplayLSN      int64   `json:"replay_lsn"`       // -1 unknown
	ReplayDelaySec int     `json:"replay_delay_sec"` // -1 unknown
	WalDirBytes    int64   `json:"wal_dir_bytes"`    // -1 unknown
	ReplayBps      float64 `json:"replay_bps,omitempty"`
	WalBps         float64 `json:"wal_bps,omitempty"`
}

// FlowSample is a measured host interface rate window (never a per-edge
// bandwidth claim).
type FlowSample struct {
	Host    string  `json:"host"`
	Iface   string  `json:"iface"`
	RxBps   float64 `json:"rx_bps"` // -1 unknown (first sample)
	TxBps   float64 `json:"tx_bps"` // -1 unknown
	RxBytes uint64  `json:"rx_bytes"`
	TxBytes uint64  `json:"tx_bytes"`
	Window  string  `json:"window"` // collection interval label
}

// SourceHealth is the per-adapter pipeline status shown on Logs & source
// health.
type SourceHealth struct {
	Source     string    `json:"source"` // patroni|etcd|keepalived|pg|host
	Target     string    `json:"target"`
	OK         bool      `json:"ok"`
	LastOK     time.Time `json:"last_ok,omitempty"`
	DurationMs int64     `json:"duration_ms"`
	Err        string    `json:"err,omitempty"`
}

// SourceError is a degraded-source record inside a snapshot.
type SourceError struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Err    string `json:"err"`
}

// Finding is a diagnose rule hit: evidence + ranked causes + next commands.
type Finding struct {
	Code      string    `json:"code"`
	Severity  string    `json:"severity"` // WARN|CRIT|INFO
	Subject   string    `json:"subject"`
	Evidence  []string  `json:"evidence"`
	Causes    []string  `json:"causes"`
	Next      []string  `json:"next"`
	FirstSeen time.Time `json:"first_seen"`
}

// StreamEvent is one line of `hawarden monitor` NDJSON output.
type StreamEvent struct {
	Seq   uint64         `json:"seq"`
	Epoch uint64         `json:"epoch"`
	Type  string         `json:"type"` // snapshot|collect_error|monitor
	At    time.Time      `json:"at"`
	Data  map[string]any `json:"data,omitempty"`
	Err   string         `json:"err,omitempty"`
}

// ProbeCheck is one named result of `hawarden probe`.
type ProbeCheck struct {
	Name   string `json:"name"` // dns|tcp|http|ssh|auth|patroni|etcd|keepalived|pg
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
	MS     int64  `json:"ms"`
}

// ProbeResult is the per-endpoint report the setup wizard renders.
type ProbeResult struct {
	Host    string       `json:"host"`
	Address string       `json:"address"`
	Checks  []ProbeCheck `json:"checks"`
	OK      bool         `json:"ok"`
}
