// Package netobs holds same-segment network observations that need no
// remote access: TCP listener reachability and the local ARP table.
package netobs

import (
	"bufio"
	"context"
	"net"
	"os"
	"strings"
	"time"
)

// TCPOpen reports whether addr (host:port) accepts a TCP connection.
func TCPOpen(ctx context.Context, addr string, timeout time.Duration) bool {
	d := net.Dialer{Timeout: timeout}
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

// ARPTable returns IP -> MAC for complete entries in /proc/net/arp. Only
// on-link neighbours appear, so a routed address is never matched.
func ARPTable() (map[string]string, error) {
	f, err := os.Open("/proc/net/arp")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	sc.Scan() // header
	for sc.Scan() {
		// IP address  HW type  Flags  HW address  Mask  Device
		fs := strings.Fields(sc.Text())
		if len(fs) < 4 || fs[2] == "0x0" || fs[3] == "00:00:00:00:00:00" {
			continue
		}
		out[fs[0]] = strings.ToLower(fs[3])
	}
	return out, sc.Err()
}

// ResolveIPv4 returns the first IPv4 address of host (literal or name).
func ResolveIPv4(ctx context.Context, host string) string {
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			return v4.String()
		}
		return ""
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return ""
	}
	for _, ip := range ips {
		if v4 := ip.IP.To4(); v4 != nil {
			return v4.String()
		}
	}
	return ""
}
