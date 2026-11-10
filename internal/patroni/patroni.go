// Package patroni is the Patroni REST adapter. It reads /cluster, /patroni
// and /history over the member API and never mutates anything.
package patroni

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

// Client talks to one Patroni REST endpoint.
type Client struct {
	Base string
	User string
	Pass string
	HTTP *http.Client
}

func NewClient(base, user, pass string, timeout time.Duration) *Client {
	return &Client{
		Base: strings.TrimSuffix(base, "/"),
		User: user, Pass: pass,
		HTTP: &http.Client{Timeout: timeout},
	}
}

func (c *Client) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.Base+path, nil)
	if err != nil {
		return nil, err
	}
	if c.User != "" {
		req.SetBasicAuth(c.User, c.Pass)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s: HTTP %d: %s", path, resp.StatusCode, truncate(string(body), 200))
	}
	return body, nil
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

// clusterJSON mirrors the Patroni /cluster document.
type clusterJSON struct {
	Members []struct {
		Name           string            `json:"name"`
		Host           string            `json:"host"`
		Port           int               `json:"port"`
		Role           string            `json:"role"`
		State          string            `json:"state"`
		APIURL         string            `json:"api_url"`
		Timeline       int               `json:"timeline"`
		Lag            json.RawMessage   `json:"lag"`
		PendingRestart bool              `json:"pending_restart"`
		Tags           map[string]string `json:"tags"`
	} `json:"members"`
	Paused              bool `json:"pause"`
	ScheduledSwitchover *struct {
		At   string `json:"at"`
		From string `json:"from"`
		To   string `json:"to"`
	} `json:"scheduled_switchover"`
}

// memberJSON mirrors the per-member /patroni document.
type memberJSON struct {
	State      string `json:"state"`
	Role       string `json:"role"`
	Postmaster *struct {
		State string `json:"state"`
	} `json:"postmaster"`
	PendingRestart   bool   `json:"pending_restart"`
	ReplicationState string `json:"replication_state"`
	XLog             *struct {
		Location int64 `json:"location"`
	} `json:"xlog"`
	Timeline int `json:"timeline"`
}

// Cluster fetches and parses GET /cluster into member rows.
func (c *Client) Cluster(ctx context.Context) ([]model.PatroniMember, bool, *model.ScheduledSwitch, error) {
	body, err := c.get(ctx, "/cluster")
	if err != nil {
		return nil, false, nil, err
	}
	var cj clusterJSON
	if err := json.Unmarshal(body, &cj); err != nil {
		return nil, false, nil, fmt.Errorf("parse /cluster: %w", err)
	}
	var out []model.PatroniMember
	for _, m := range cj.Members {
		lag := int64(-1)
		if len(m.Lag) > 0 && string(m.Lag) != "null" && string(m.Lag) != `"unknown"` {
			var f float64
			if err := json.Unmarshal(m.Lag, &f); err == nil {
				lag = int64(f)
			}
		}
		tl := m.Timeline
		if tl == 0 {
			tl = -1
		}
		out = append(out, model.PatroniMember{
			Name: m.Name, Host: m.Host, APIURL: m.APIURL,
			Role: m.Role, State: m.State, Timeline: tl,
			LagBytes: lag, PendingRestart: m.PendingRestart,
			APIReachable: true, Tags: m.Tags,
		})
	}
	var sched *model.ScheduledSwitch
	if cj.ScheduledSwitchover != nil {
		sched = &model.ScheduledSwitch{
			At: cj.ScheduledSwitchover.At, From: cj.ScheduledSwitchover.From, To: cj.ScheduledSwitchover.To,
		}
	}
	return out, cj.Paused, sched, nil
}

// Member probes one member's /patroni endpoint.
func (c *Client) Member(ctx context.Context) (state, role string, pendingRestart bool, timeline int, err error) {
	body, err := c.get(ctx, "/patroni")
	if err != nil {
		return "", "", false, -1, err
	}
	var mj memberJSON
	if uerr := json.Unmarshal(body, &mj); uerr != nil {
		return "", "", false, -1, fmt.Errorf("parse /patroni: %w", uerr)
	}
	tl := mj.Timeline
	if tl == 0 {
		tl = -1
	}
	return mj.State, mj.Role, mj.PendingRestart, tl, nil
}

// History fetches GET /history (failover evidence).
func (c *Client) History(ctx context.Context) ([]model.HistoryEntry, error) {
	body, err := c.get(ctx, "/history")
	if err != nil {
		return nil, err
	}
	var raw [][]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("parse /history: %w", err)
	}
	var out []model.HistoryEntry
	for _, row := range raw {
		var e model.HistoryEntry
		if len(row) > 0 {
			_ = json.Unmarshal(row[0], &e.Timeline)
		}
		if len(row) > 1 {
			var f float64
			if json.Unmarshal(row[1], &f) == nil {
				e.LSN = int64(f)
			} else {
				var s string
				if json.Unmarshal(row[1], &s) == nil {
					e.LSN, _ = strconv.ParseInt(s, 10, 64)
				}
			}
		}
		if len(row) > 2 {
			_ = json.Unmarshal(row[2], &e.Reason)
		}
		if len(row) > 3 {
			_ = json.Unmarshal(row[3], &e.Timestamp)
		}
		if len(row) > 4 {
			_ = json.Unmarshal(row[4], &e.NewLeader)
		}
		out = append(out, e)
	}
	return out, nil
}
