// Package etcd is the etcd observation adapter. It uses the plain HTTP
// endpoints every etcd exposes on its client port — /health, /version and
// /metrics — so no etcdctl binary or gRPC client is required. Metric names
// are read verbatim from the endpoint; nothing is guessed.
package etcd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
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

// NewClient returns a client for endpoint. A non-nil tlsCfg switches the
// client to https with that configuration (CA and optional client cert).
func NewClient(endpoint string, timeout time.Duration, tlsCfg *tls.Config) *Client {
	c := &Client{
		Endpoint: endpoint,
		Scheme:   "http",
		HTTP:     &http.Client{Timeout: timeout},
	}
	if tlsCfg != nil {
		c.Scheme = "https"
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.TLSClientConfig = tlsCfg
		c.HTTP.Transport = tr
	}
	return c
}

// TLSConfig builds client TLS from file references. It returns nil (plain
// http) when none of caFile, certFile, keyFile is set.
func TLSConfig(caFile, certFile, keyFile string) (*tls.Config, error) {
	if caFile == "" && certFile == "" && keyFile == "" {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("etcd cacert: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("etcd cacert: no PEM certificates in %s", caFile)
		}
		cfg.RootCAs = pool
	}
	if certFile != "" || keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("etcd client cert: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func (c *Client) get(ctx context.Context, path string) ([]byte, error) {
	return c.do(ctx, "GET", path, nil)
}

// do issues one request. The v3 JSON gateway (/v3/...) takes POST with a
// JSON body; every call hawarden makes there is a read-only RPC.
func (c *Client) do(ctx context.Context, method, path string, body io.Reader) ([]byte, error) {
	url := fmt.Sprintf("%s://%s%s", c.Scheme, c.Endpoint, path)
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s %s: HTTP %d", method, path, resp.StatusCode)
	}
	return out, nil
}

// hexID converts the gateway's decimal uint64 member ID to the lower-case
// hex form etcd uses in metric labels and etcdctl output.
func hexID(dec string) string {
	n, err := strconv.ParseUint(dec, 10, 64)
	if err != nil || n == 0 {
		return ""
	}
	return strconv.FormatUint(n, 16)
}

// Status is one member's own view from /v3/maintenance/status.
type Status struct {
	MemberID  string // hex
	LeaderID  string // hex; empty when the member knows no leader
	RaftTerm  int64
	RaftIndex int64
	DBSize    int64
	Version   string
}

// Status calls the read-only Maintenance.Status RPC on this endpoint.
func (c *Client) Status(ctx context.Context) (Status, error) {
	body, err := c.do(ctx, "POST", "/v3/maintenance/status", strings.NewReader("{}"))
	if err != nil {
		return Status{}, err
	}
	var r struct {
		Header struct {
			MemberID string `json:"member_id"`
		} `json:"header"`
		Version   string `json:"version"`
		DBSize    string `json:"dbSize"`
		Leader    string `json:"leader"`
		RaftIndex string `json:"raftIndex"`
		RaftTerm  string `json:"raftTerm"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return Status{}, fmt.Errorf("parse status: %w", err)
	}
	num := func(s string) int64 {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return -1
		}
		return n
	}
	return Status{
		MemberID: hexID(r.Header.MemberID), LeaderID: hexID(r.Leader),
		RaftTerm: num(r.RaftTerm), RaftIndex: num(r.RaftIndex), DBSize: num(r.DBSize),
		Version: r.Version,
	}, nil
}

// MemberList calls the read-only Cluster.MemberList RPC: the cluster's own
// roster, so members added later are discovered without config changes.
func (c *Client) MemberList(ctx context.Context) ([]model.EtcdRosterEntry, error) {
	body, err := c.do(ctx, "POST", "/v3/cluster/member/list", strings.NewReader("{}"))
	if err != nil {
		return nil, err
	}
	var r struct {
		Members []struct {
			ID         string   `json:"ID"`
			Name       string   `json:"name"`
			PeerURLs   []string `json:"peerURLs"`
			ClientURLs []string `json:"clientURLs"`
			IsLearner  bool     `json:"isLearner"`
		} `json:"members"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("parse member list: %w", err)
	}
	out := make([]model.EtcdRosterEntry, 0, len(r.Members))
	for _, m := range r.Members {
		out = append(out, model.EtcdRosterEntry{
			ID: hexID(m.ID), Name: m.Name, PeerURLs: m.PeerURLs,
			ClientURLs: m.ClientURLs, Learner: m.IsLearner,
		})
	}
	return out, nil
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

// MetricsInfo is the subset of /metrics hawarden uses. -1 means unknown.
type MetricsInfo struct {
	HasLeader, IsLeader                        bool
	LeaderChanges, DBSize, RaftIndex, RaftTerm int64
	PeersKnown                                 bool             // etcd_network_active_peers exported
	ActivePeers                                []string         // remote member IDs (hex) with a live raft stream
	PeerSentBytes                              map[string]int64 // remote member ID -> cumulative bytes sent
}

// Metrics scrapes GET /metrics and extracts only the raft/db/peer series
// hawarden uses.
func (c *Client) Metrics(ctx context.Context) (MetricsInfo, error) {
	mi := MetricsInfo{LeaderChanges: -1, DBSize: -1, RaftIndex: -1, RaftTerm: -1, PeerSentBytes: map[string]int64{}}
	body, err := c.get(ctx, "/metrics")
	if err != nil {
		return mi, err
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
			mi.HasLeader = f == 1
		case name == "etcd_server_is_leader":
			mi.IsLeader = f == 1
		case name == "etcd_server_leader_changes_seen_total":
			mi.LeaderChanges = int64(f)
		case name == "etcd_mvcc_db_total_size_in_bytes":
			mi.DBSize = int64(f)
		case strings.HasPrefix(name, "etcd_server_raft_index"):
			// etcd 3.4 metric; 3.5 dropped it — Status supplies it there
			if mi.RaftIndex < 0 {
				mi.RaftIndex = int64(f)
			}
		case name == "etcd_server_raft_term":
			if mi.RaftTerm < 0 {
				mi.RaftTerm = int64(f)
			}
		case name == "etcd_network_active_peers":
			mi.PeersKnown = true
			if r := label(line, "Remote"); r != "" && f >= 1 {
				mi.ActivePeers = append(mi.ActivePeers, r)
			}
		case name == "etcd_network_peer_sent_bytes_total":
			if to := label(line, "To"); to != "" && to != "0" {
				mi.PeerSentBytes[to] = int64(f)
			}
		}
	}
	sort.Strings(mi.ActivePeers)
	return mi, nil
}

// label extracts one label value from a `name{k="v",...} value` line.
func label(line, key string) string {
	i := strings.Index(line, key+`="`)
	if i < 0 {
		return ""
	}
	rest := line[i+len(key)+2:]
	j := strings.IndexByte(rest, '"')
	if j < 0 {
		return ""
	}
	return rest[:j]
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

// ProbeEndpoint gathers everything one endpoint can tell us. It returns the
// cumulative per-peer sent bytes for the caller's rate computation.
func (c *Client) ProbeEndpoint(ctx context.Context, m *model.EtcdMember) map[string]int64 {
	healthy, err := c.Health(ctx)
	if err != nil {
		m.Reachable = false
		m.Err = err.Error()
		return nil
	}
	m.Reachable = true
	m.Healthy = healthy
	if st, err := c.Status(ctx); err == nil {
		m.ID, m.LeaderID = st.MemberID, st.LeaderID
		m.RaftIndex, m.RaftTerm = st.RaftIndex, st.RaftTerm
	}
	mi, err := c.Metrics(ctx)
	if err != nil {
		m.Err = "metrics: " + err.Error()
		return nil
	}
	m.HasLeader = mi.HasLeader
	m.IsLeader = mi.IsLeader
	m.LeaderChanges = mi.LeaderChanges
	m.DBSizeBytes = mi.DBSize
	if m.RaftIndex < 0 {
		m.RaftIndex = mi.RaftIndex
	}
	if m.RaftTerm < 0 {
		m.RaftTerm = mi.RaftTerm
	}
	m.PeersKnown = mi.PeersKnown
	m.ActivePeers = mi.ActivePeers
	return mi.PeerSentBytes
}
