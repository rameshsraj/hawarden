// Package etcd is the etcd observation adapter. It uses the plain HTTP
// endpoints every etcd exposes on its client port — /health, /version and
// /metrics — so no etcdctl binary or gRPC client is required. Metric names
// are read verbatim from the endpoint; nothing is guessed.
package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rameshsraj/hawarden/internal/model"
)

// Client talks to one etcd endpoint's HTTP surface.
type Client struct {
	Endpoint string // host:port
	Scheme   string
	HTTP     *http.Client
}

func NewClient(endpoint string, timeout time.Duration) *Client {
	return &Client{
		Endpoint: endpoint,
		Scheme:   "http",
		HTTP:     &http.Client{Timeout: timeout},
	}
}

func (c *Client) get(ctx context.Context, path string) ([]byte, error) {
	url := fmt.Sprintf("%s://%s%s", c.Scheme, c.Endpoint, path)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s: HTTP %d", path, resp.StatusCode)
	}
	return body, nil
}

// Health reads GET /health → {"health":"true"|"false", ...}.
func (c *Client) Health(ctx context.Context) (bool, error) {
	body, err := c.get(ctx, "/health")
	if err != nil {
		return false, err
	}
	var h struct {
		Health string `json:"health"`
	}
	if err := json.Unmarshal(body, &h); err != nil {
		return false, fmt.Errorf("parse /health: %w", err)
	}
	return h.Health == "true", nil
}

// Version reads GET /version → {"etcdserver":"3.5.x", ...}.
func (c *Client) Version(ctx context.Context) (string, error) {
	body, err := c.get(ctx, "/version")
	if err != nil {
		return "", err
	}
	var v struct {
		Server string `json:"etcdserver"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return "", fmt.Errorf("parse /version: %w", err)
	}
	return v.Server, nil
}

// Metrics scrapes GET /metrics and extracts only the raft/db gauges hawarden
// uses. Missing metrics stay -1 ("unknown").
func (c *Client) Metrics(ctx context.Context) (hasLeader bool, isLeader bool, leaderChanges, dbSize, raftIndex, raftTerm int64, err error) {
	leaderChanges, dbSize, raftIndex, raftTerm = -1, -1, -1, -1
	body, err := c.get(ctx, "/metrics")
	if err != nil {
		return false, false, -1, -1, -1, -1, err
	}
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, val := splitMetric(line)
		f, ferr := strconv.ParseFloat(val, 64)
		if ferr != nil {
			continue
		}
		switch {
		case name == "etcd_server_has_leader":
			hasLeader = f == 1
		case name == "etcd_server_is_leader":
			isLeader = f == 1
		case name == "etcd_server_leader_changes_seen_total":
			leaderChanges = int64(f)
		case name == "etcd_mvcc_db_total_size_in_bytes":
			dbSize = int64(f)
		case strings.HasPrefix(name, "etcd_server_raft_index"):
			// etcd 3.4 metric; 3.5 dropped it — keep as best-effort
			if raftIndex < 0 {
				raftIndex = int64(f)
			}
		case name == "etcd_server_raft_term":
			if raftTerm < 0 {
				raftTerm = int64(f)
			}
		}
	}
	return hasLeader, isLeader, leaderChanges, dbSize, raftIndex, raftTerm, nil
}

// splitMetric splits "name{labels} value" or "name value" into name+value.
func splitMetric(line string) (string, string) {
	i := strings.LastIndexByte(line, ' ')
	if i < 0 {
		return line, ""
	}
	head, val := line[:i], strings.TrimSpace(line[i+1:])
	if j := strings.IndexByte(head, '{'); j >= 0 {
		head = head[:j]
	}
	return head, val
}

// ProbeEndpoint gathers everything one endpoint can tell us.
func (c *Client) ProbeEndpoint(ctx context.Context, m *model.EtcdMember) {
	t0 := time.Now()
	_ = t0
	healthy, err := c.Health(ctx)
	if err != nil {
		m.Reachable = false
		m.Err = err.Error()
		return
	}
	m.Reachable = true
	m.Healthy = healthy
	ver, err := c.Version(ctx)
	if err == nil {
		// version stored at state level by caller
		_ = ver
	}
	hasLeader, isLeader, lc, db, ri, rt, err := c.Metrics(ctx)
	if err != nil {
		m.Err = "metrics: " + err.Error()
		return
	}
	m.HasLeader = hasLeader
	m.IsLeader = isLeader
	m.LeaderChanges = lc
	m.DBSizeBytes = db
	m.RaftIndex = ri
	m.RaftTerm = rt
}
