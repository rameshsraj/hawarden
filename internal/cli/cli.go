// Package cli parses flags and renders output. All logic lives in service;
// this package never touches external systems directly.
package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/rameshsraj/hawarden/internal/config"
	"github.com/rameshsraj/hawarden/internal/exitcode"
	"github.com/rameshsraj/hawarden/internal/model"
	"github.com/rameshsraj/hawarden/internal/service"
)

var (
	cfgPath string
	jsonOut bool
	version = "dev"
)

// SetVersion wires the ldflags-injected build version.
func SetVersion(v string) { version = v }

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "hawarden",
		Short: "command centre for a Patroni + keepalived + etcd HA cluster",
		Long: "hawarden observes the HA tooling around PostgreSQL — Patroni, " +
			"keepalived and etcd — plus optional host telemetry. It never " +
			"mutates the cluster and never queries the database engine unless " +
			"the least-privilege replication context adapter is enabled.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVarP(&cfgPath, "config", "c", config.DefaultPath, "config file")
	root.PersistentFlags().BoolVar(&jsonOut, "json", false, "JSON output")
	return root
}

// Execute runs the CLI and returns the process exit code.
func Execute() int {
	root := rootCmd()
	svc := func(cmd *cobra.Command) (*service.Service, error) {
		cfg, err := config.Load(cfgPath)
		if err != nil {
			return nil, err
		}
		return service.New(cfg, cfgPath), nil
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	snapCmd := func(name, short string, render func(io.Writer, *model.Snapshot)) *cobra.Command {
		return &cobra.Command{
			Use: name, Short: short,
			RunE: func(cmd *cobra.Command, _ []string) error {
				s, err := svc(cmd)
				if err != nil {
					return err
				}
				snap, err := s.Snapshot(ctx)
				if err != nil {
					return err
				}
				if jsonOut {
					return writeJSON(os.Stdout, snap)
				}
				render(os.Stdout, snap)
				return exitFor(snap)
			},
		}
	}

	root.AddCommand(
		snapCmd("status", "one-shot cluster view", renderStatus),
		snapCmd("cluster", "Patroni member table", renderCluster),
		snapCmd("dcs", "etcd state", renderEtcd),
		snapCmd("vip", "keepalived/VIP state", renderVIP),
		snapCmd("topology", "discovered topology", renderTopology),
		snapCmd("events", "failover/switchover timeline", renderEvents),
		snapCmd("health", "per-source probe results", renderHealth),
		snapCmd("diagnose", "findings with evidence and next commands", renderDiagnose),
	)

	// monitor: NDJSON stream
	var count int
	mon := &cobra.Command{
		Use: "monitor", Short: "NDJSON snapshot stream",
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := svc(cmd)
			if err != nil {
				return err
			}
			return s.Monitor(ctx, os.Stdout, count)
		},
	}
	mon.Flags().IntVar(&count, "count", 0, "stop after N snapshots (0=forever)")
	root.AddCommand(mon)

	// probe
	var probeHost string
	probe := &cobra.Command{
		Use: "probe", Short: "per-endpoint connectivity for the setup wizard",
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := svc(cmd)
			if err != nil {
				return err
			}
			if probeHost != "" {
				var h config.Host
				if err := json.Unmarshal([]byte(probeHost), &h); err != nil {
					return exitcode.New(exitcode.Usage, "invalid --host-json").WithCause(err.Error())
				}
				res := s.Probe(ctx, h)
				return writeJSON(os.Stdout, res)
			}
			return writeJSON(os.Stdout, s.ProbeAll(ctx))
		},
	}
	probe.Flags().StringVar(&probeHost, "host-json", "", "probe an ad-hoc host spec instead of the config")
	root.AddCommand(probe)

	// version
	root.AddCommand(&cobra.Command{
		Use: "version", Short: "print version",
		Run: func(*cobra.Command, []string) {
			fmt.Fprintf(os.Stdout, "hawarden %s (schema %d)\n", version, model.SchemaVersion)
		},
	})

	// service: status/start/stop/restart of HA units on monitored hosts
	var confirm string
	svcCmd := &cobra.Command{
		Use:   "service <verb> <unit> <host>",
		Short: "status/start/stop/restart patroni|postgresql|keepalived|etcd on a host",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			verb, unit, hostName := args[0], args[1], args[2]
			s, err := svc(cmd)
			if err != nil {
				return err
			}
			role, ok := service.UnitRole(unit)
			if !ok {
				return exitcode.New(exitcode.Usage, "unknown unit").WithCause(unit).
					WithHint("units: patroni postgresql keepalived etcd")
			}
			h, ok := s.Cfg.HostByName(hostName)
			if !ok {
				return exitcode.New(exitcode.Usage, "unknown host").WithCause(hostName).
					WithHint("hawarden config show")
			}
			if !h.Has(role) {
				return exitcode.New(exitcode.Usage, "host does not run this unit").
					WithCause(fmt.Sprintf("%s has no %s role", hostName, role)).
					WithHint("roles are configured in the Configuration console")
			}
			res, err := s.ServiceControl(ctx, h, unit, verb, confirm)
			if jsonOut {
				_ = writeJSON(os.Stdout, res)
			} else {
				renderService(os.Stdout, res)
			}
			return err
		},
	}
	svcCmd.Flags().StringVar(&confirm, "confirm", "", "typed confirmation (must equal the host name) — required for start/stop/restart")
	root.AddCommand(svcCmd)

	root.AddCommand(configCmds())

	if err := root.Execute(); err != nil {
		return exitcode.Render(os.Stderr, err)
	}
	return 0
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// exitFor maps snapshot findings to WARN/CRIT exit codes.
func exitFor(snap *model.Snapshot) error {
	crit, warn := false, false
	for _, f := range snap.Findings {
		if f.Severity == "CRIT" {
			crit = true
		} else if f.Severity == "WARN" {
			warn = true
		}
	}
	if crit {
		return exitcode.New(exitcode.FindingsCrit, "critical findings present").
			WithHint("hawarden diagnose")
	}
	if warn {
		return exitcode.New(exitcode.FindingsWarn, "warnings present").
			WithHint("hawarden diagnose")
	}
	return nil
}
