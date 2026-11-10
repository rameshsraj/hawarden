// Package kalive observes keepalived on a host over SSH: VRRP state from the
// notify-script state file (or journald), VIP presence from `ip -j addr`,
// and process health from pgrep. It never mutates keepalived.
package kalive

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rameshsraj/hawarden/internal/model"
	"github.com/rameshsraj/hawarden/internal/remote"
)

// Probe reads keepalived state on one host.
// stateFile is the notify-script output (e.g. /run/keepalived.hawarden.state)
// written as "STATE <instance> <state> <ts>"; empty disables that source.
func Probe(ctx context.Context, r remote.Runner, h remote.Host, vipAddrs []string, stateFile string) model.KeepalivedNode {
	n := model.KeepalivedNode{Host: h.Name, VRRPState: "unknown", Priority: -1}

	out, err := r.Run(ctx, h, "pidof keepalived || pgrep -x keepalived || true")
	if err != nil {
		n.Reachable = false
		n.Err = err.Error()
		return n
	}
	n.Reachable = true
	n.ProcessUp = strings.TrimSpace(string(out)) != ""

	// VIP presence on any interface (JSON `ip -j addr`).
	addrOut, err := r.Run(ctx, h, "ip -j addr show 2>/dev/null || ip addr show")
	if err == nil {
		for _, vip := range vipAddrs {
			if vip == "" {
				continue
			}
			if vipOnIface(addrOut, vip) {
				n.VIPPresent = true
				n.VIPInterface = ifaceOf(addrOut, vip)
			}
		}
	}

	// Notify-script state file first (works without systemd); journald next.
	if stateFile != "" {
		if st, terr := r.Run(ctx, h, "cat "+stateFile+" 2>/dev/null || true"); terr == nil {
			parseStateFile(&n, string(st))
		}
	}
	if n.VRRPState == "unknown" {
		if j, jerr := r.Run(ctx, h,
			"journalctl -u keepalived -o json -n 50 --no-pager 2>/dev/null | grep -o '\"MESSAGE\":\"[^\"]*\"' | tail -20 || true"); jerr == nil {
			parseJournal(&n, string(j))
		}
	}
	if n.ProcessUp && n.VIPPresent && n.VRRPState == "unknown" {
		n.VRRPState = "MASTER" // holds the VIP: strongest available evidence
	}
	return n
}

// parseStateFile reads lines "STATE <instance> <STATE> <epoch>".
func parseStateFile(n *model.KeepalivedNode, data string) {
	for _, line := range strings.Split(data, "\n") {
		f := strings.Fields(line)
		if len(f) >= 3 && f[0] == "STATE" {
			n.VRRPState = f[2]
			n.TransitionEvidence = strings.TrimSpace(line)
			if len(f) >= 4 {
				var ts int64
				fmt.Sscanf(f[3], "%d", &ts)
				if ts > 0 {
					n.LastTransition = time.Unix(ts, 0)
				}
			}
		}
	}
}

// parseJournal scans journal MESSAGE fields for VRRP transitions.
func parseJournal(n *model.KeepalivedNode, data string) {
	for _, line := range strings.Split(data, "\n") {
		l := strings.ToUpper(line)
		switch {
		case strings.Contains(l, "MASTER STATE") || strings.Contains(l, "ENTERING MASTER"):
			n.VRRPState = "MASTER"
			n.TransitionEvidence = strings.TrimSpace(line)
		case strings.Contains(l, "BACKUP STATE") || strings.Contains(l, "ENTERING BACKUP"):
			n.VRRPState = "BACKUP"
			n.TransitionEvidence = strings.TrimSpace(line)
		case strings.Contains(l, "FAULT STATE") || strings.Contains(l, "ENTERING FAULT"):
			n.VRRPState = "FAULT"
			n.TransitionEvidence = strings.TrimSpace(line)
		}
	}
}

// vipOnIface reports whether ip JSON output shows the address.
func vipOnIface(ipJSON []byte, vip string) bool {
	var ifs []struct {
		IfName   string `json:"ifname"`
		AddrInfo []struct {
			Local string `json:"local"`
		} `json:"addr_info"`
	}
	if json.Unmarshal(ipJSON, &ifs) != nil {
		return strings.Contains(string(ipJSON), vip) // non-JSON fallback
	}
	for _, i := range ifs {
		for _, a := range i.AddrInfo {
			if a.Local == vip {
				return true
			}
		}
	}
	return false
}

// ifaceOf returns the interface name holding vip ("" if unknown).
func ifaceOf(ipJSON []byte, vip string) string {
	var ifs []struct {
		IfName   string `json:"ifname"`
		AddrInfo []struct {
			Local string `json:"local"`
		} `json:"addr_info"`
	}
	if json.Unmarshal(ipJSON, &ifs) != nil {
		return ""
	}
	for _, i := range ifs {
		for _, a := range i.AddrInfo {
			if a.Local == vip {
				return i.IfName
			}
		}
	}
	return ""
}
