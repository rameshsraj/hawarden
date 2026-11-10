// Package hostcounters reads /proc/net/dev per monitored host over SSH and
// turns interface byte counters into FlowSamples. Rates are computed by the
// collector from timestamped deltas; first samples carry -1 rates.
package hostcounters

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/rameshsraj/hawarden/internal/model"
	"github.com/rameshsraj/hawarden/internal/remote"
)

// Prev is a rate-computation store kept between collection cycles.
type Prev struct {
	at    time.Time
	stats map[string][2]uint64 // "host/iface" -> rx,tx
}

func NewPrev() *Prev { return &Prev{stats: map[string][2]uint64{}} }

// Probe reads interface counters on h and emits samples with rates computed
// against the previous stored counters (or -1 when absent/reset).
func (p *Prev) Probe(ctx context.Context, r remote.Runner, h remote.Host, window string) ([]model.FlowSample, error) {
	out, err := r.Run(ctx, h, "cat /proc/net/dev")
	if err != nil {
		return nil, err
	}
	now := time.Now()
	dt := now.Sub(p.at).Seconds()
	var samples []model.FlowSample
	for _, line := range strings.Split(string(out), "\n") {
		i := strings.IndexByte(line, ':')
		if i < 0 {
			continue
		}
		iface := strings.TrimSpace(line[:i])
		f := strings.Fields(line[i+1:])
		if len(f) < 9 {
			continue
		}
		rx, _ := strconv.ParseUint(f[0], 10, 64)
		tx, _ := strconv.ParseUint(f[8], 10, 64)
		s := model.FlowSample{
			Host: h.Name, Iface: iface,
			RxBps: -1, TxBps: -1,
			RxBytes: rx, TxBytes: tx, Window: window,
		}
		key := h.Name + "/" + iface
		if prev, ok := p.stats[key]; ok && dt > 0 {
			if rx >= prev[0] {
				s.RxBps = float64(rx-prev[0]) / dt
			}
			if tx >= prev[1] {
				s.TxBps = float64(tx-prev[1]) / dt
			}
		}
		p.stats[key] = [2]uint64{rx, tx}
		samples = append(samples, s)
	}
	p.at = now
	return samples, nil
}
