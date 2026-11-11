// Service control: status/start/stop/restart for patroni, postgresql,
// keepalived and etcd units on a monitored host over SSH. systemd is used
// when present; otherwise a documented process-mode fallback applies
// (containers, labs). Mutations require an exact typed confirmation of the
// host name — the CLI/console binds it to the target before executing.
package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rameshsraj/hawarden/internal/config"
	"github.com/rameshsraj/hawarden/internal/exitcode"
	"github.com/rameshsraj/hawarden/internal/remote"
)

// Unit names accepted by `hawarden service`.
var serviceUnits = map[string]config.Role{
	"patroni":    config.RolePatroni,
	"postgresql": config.RolePG,
	"pg":         config.RolePG,
	"keepalived": config.RoleKeepalived,
	"etcd":       config.RoleEtcd,
}

// ServiceResult is the evidence record of one service action.
type ServiceResult struct {
	Host     string   `json:"host"`
	Unit     string   `json:"unit"`
	Verb     string   `json:"verb"`
	Manager  string   `json:"manager"` // systemd|process
	OK       bool     `json:"ok"`
	Running  bool     `json:"running"`
	PIDs     string   `json:"pids,omitempty"`
	Evidence []string `json:"evidence"`
	Err      string   `json:"err,omitempty"`
}

// UnitRole reports whether unit is valid and which role gates it.
func UnitRole(unit string) (config.Role, bool) {
	r, ok := serviceUnits[strings.ToLower(unit)]
	return r, ok
}

// unitSystemdName resolves the systemctl unit name for a service on a host.
func unitSystemdName(unit string, h config.Host) string {
	switch unit {
	case "patroni":
		if h.Patroni != nil && h.Patroni.Service != "" {
			return h.Patroni.Service
		}
		return "patroni"
	case "keepalived":
		if h.Keepalived != nil && h.Keepalived.Service != "" {
			return h.Keepalived.Service
		}
		return "keepalived"
	case "etcd":
		if h.Etcd != nil && h.Etcd.Service != "" {
			return h.Etcd.Service
		}
		return "etcd"
	default: // postgresql
		if h.PG != nil && h.PG.Service != "" {
			return h.PG.Service
		}
		return "postgresql"
	}
}

// procCmds returns process-mode commands per unit. These are fixed strings
// built from config paths — never user input.
func procCmds(unit string, h config.Host) (status, start, stop, restart string, err error) {
	switch unit {
	case "keepalived":
		conf := "/etc/keepalived/keepalived.conf"
		if h.Keepalived != nil && h.Keepalived.Conf != "" {
			conf = h.Keepalived.Conf
		}
		// status lists only *live* keepalived pids — container parents often
		// leave zombies behind, which pidof/pgrep still report
		return "ps -eo pid=,stat=,comm= | awk '$3==\"keepalived\" && $2!~/^Z/{print $1}'",
			"rm -f /run/keepalived.pid /var/run/keepalived.pid; keepalived -f " + conf,
			"pkill -x keepalived; sleep 1; pkill -9 -x keepalived; true",
			"pkill -x keepalived; sleep 1; pkill -9 -x keepalived; rm -f /run/keepalived.pid /var/run/keepalived.pid; keepalived -f " + conf, nil
	case "patroni":
		conf := "/etc/patroni/patroni.yml"
		if h.Patroni != nil && h.Patroni.Conf != "" {
			conf = h.Patroni.Conf
		}
		return "ps -eo pid=,stat=,args= | awk '/patroni [^ ]*yml/ && !/awk| sh -c/ && $2!~/^Z/{print $1}'",
			fmt.Sprintf("su postgres -c 'nohup patroni %s </dev/null >>/var/log/patroni-boot.log 2>&1 &'", conf),
			"pkill -f 'patroni .*yml'",
			fmt.Sprintf("pkill -f 'patroni .*yml'; sleep 2; su postgres -c 'nohup patroni %s </dev/null >>/var/log/patroni-boot.log 2>&1 &'", conf), nil
	case "etcd":
		return "", "", "", "", exitcode.New(exitcode.Usage, "etcd has no process-mode control").
			WithHint("manage etcd via its systemd unit or container runtime")
	default: // postgresql
		dir := "/var/lib/postgresql/data"
		if h.PG != nil && h.PG.DataDir != "" {
			dir = h.PG.DataDir
		}
		user := "postgres"
		if h.PG != nil && h.PG.User != "" {
			user = h.PG.User
		}
		ctl := fmt.Sprintf("su %s -c 'pg_ctl -D %s", user, dir)
		return "ps -eo pid=,stat=,comm= | awk '$3==\"postgres\" && $2!~/^Z/{print $1}' | head -5",
			ctl + " -w -t 30 -l /tmp/pg-ctl.log start'",
			ctl + " -w -m fast stop'",
			ctl + " -w -m fast -l /tmp/pg-ctl.log restart'", nil
	}
}

// managerDetect reports whether systemd is functional on the host.
func (s *Service) managerDetect(ctx context.Context, r remote.Runner, h remote.Host) string {
	out, err := r.Run(ctx, h, "command -v systemctl >/dev/null 2>&1 && systemctl is-system-running 2>/dev/null || echo nosystemd")
	if err == nil && strings.TrimSpace(string(out)) != "nosystemd" && !strings.Contains(string(out), "offline") {
		return "systemd"
	}
	return "process"
}

// ServiceStatus returns the observed state of one unit on a host.
func (s *Service) ServiceStatus(ctx context.Context, h config.Host, unit string) ServiceResult {
	res := ServiceResult{Host: h.Name, Unit: unit, Verb: "status"}
	if h.SSHDisabled {
		res.Err = "ssh telemetry disabled on this host"
		return res
	}
	r := s.Collector.Runner()
	sh := remote.Host{Name: h.Name, Address: h.Address, User: h.SSHUser}
	res.Manager = s.managerDetect(ctx, r, sh)
	out, err := s.serviceProbe(ctx, r, sh, h, unit, "status", res.Manager)
	res.Evidence = append(res.Evidence, "manager="+res.Manager, trimStr(string(out)))
	if err != nil {
		res.Running = false
		res.Evidence = append(res.Evidence, "probe: "+err.Error())
	} else {
		res.Running = true
		res.PIDs = strings.Join(strings.Fields(string(out)), " ")
	}
	res.OK = true
	return res
}

// ServiceControl executes a mutation on one unit. confirm must equal the
// host name exactly (typed confirmation bound to the target).
func (s *Service) ServiceControl(ctx context.Context, h config.Host, unit, verb, confirm string) (ServiceResult, error) {
	res := ServiceResult{Host: h.Name, Unit: unit, Verb: verb}
	if verb == "status" {
		return s.ServiceStatus(ctx, h, unit), nil
	}
	if verb != "start" && verb != "stop" && verb != "restart" {
		return res, exitcode.New(exitcode.Usage, "unknown verb").WithCause(verb).
			WithHint("service status|start|stop|restart <unit> <host>")
	}
	if confirm != h.Name {
		return res, exitcode.New(exitcode.Usage, "typed confirmation required").
			WithCause(fmt.Sprintf("refusing %s %s on %s without confirmation", verb, unit, h.Name)).
			WithHint(fmt.Sprintf("hawarden service %s %s %s --confirm %s", verb, unit, h.Name, h.Name))
	}
	if h.SSHDisabled {
		return res, exitcode.New(exitcode.Usage, "ssh telemetry disabled on this host").WithCause(h.Name)
	}
	r := s.Collector.Runner()
	sh := remote.Host{Name: h.Name, Address: h.Address, User: h.SSHUser}
	res.Manager = s.managerDetect(ctx, r, sh)

	before, _ := s.serviceProbe(ctx, r, sh, h, unit, "status", res.Manager)
	res.Evidence = append(res.Evidence, "before: "+trimStr(string(before)))

	out, err := s.serviceProbe(ctx, r, sh, h, unit, verb, res.Manager)
	if err != nil {
		res.Err = err.Error()
		res.Evidence = append(res.Evidence, "exec: "+trimStr(string(out)))
		return res, exitcode.New(exitcode.PreflightNoGo, verb+" "+unit+" failed on "+h.Name).
			WithCause(err.Error()).WithHint("hawarden service status " + unit + " " + h.Name)
	}
	time.Sleep(1200 * time.Millisecond) // settle before verify
	after, aerr := s.serviceProbe(ctx, r, sh, h, unit, "status", res.Manager)
	res.Evidence = append(res.Evidence, "after: "+trimStr(string(after)))
	res.Running = aerr == nil
	res.PIDs = strings.Join(strings.Fields(string(after)), " ")
	res.OK = true
	// verify matches intent: running expected for start/restart, down for stop
	wantUp := verb != "stop"
	if res.Running != wantUp {
		res.OK = false
		res.Err = fmt.Sprintf("post-verify: expected %s but state reports running=%v", map[bool]string{true: "up", false: "down"}[wantUp], res.Running)
		res.Evidence = append(res.Evidence, "verify failed")
		return res, exitcode.New(exitcode.ApplyRolledBack, verb+" "+unit+" did not reach expected state on "+h.Name).
			WithCause(res.Err).WithHint("hawarden service status " + unit + " " + h.Name)
	}
	return res, nil
}

// serviceProbe runs one verb's command line for the unit under the detected
// manager. Returns stdout; non-zero exit is an error.
func (s *Service) serviceProbe(ctx context.Context, r remote.Runner, sh remote.Host, h config.Host, unit, verb, manager string) ([]byte, error) {
	var cmd string
	if manager == "systemd" {
		name := unitSystemdName(unit, h)
		switch verb {
		case "status":
			cmd = "systemctl is-active " + name + " && systemctl show -p MainPID --value " + name + " || true"
			out, err := r.Run(ctx, sh, cmd)
			if err != nil {
				return out, err
			}
			if strings.HasPrefix(strings.TrimSpace(string(out)), "active") {
				return out, nil
			}
			return out, fmt.Errorf("unit %s not active: %s", name, trimStr(string(out)))
		default:
			return r.Run(ctx, sh, fmt.Sprintf("systemctl %s %s", verb, name))
		}
	}
	st, start, stop, restart, err := procCmds(unit, h)
	if err != nil {
		return nil, err
	}
	switch verb {
	case "status":
		out, err := r.Run(ctx, sh, st)
		if err != nil || len(strings.TrimSpace(string(out))) == 0 {
			if err == nil {
				err = fmt.Errorf("not running")
			}
			return out, err
		}
		return out, nil
	case "start":
		cmd = start
	case "stop":
		cmd = stop
	case "restart":
		cmd = restart
	}
	return r.Run(ctx, sh, cmd)
}

func trimStr(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 160 {
		return s[:160]
	}
	return s
}
