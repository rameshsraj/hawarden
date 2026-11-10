package cli

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/rameshsraj/hawarden/internal/config"
	"github.com/rameshsraj/hawarden/internal/exitcode"
	"github.com/rameshsraj/hawarden/internal/service"
)

func configCmds() *cobra.Command {
	cfgCmd := &cobra.Command{Use: "config", Short: "manage hawarden.yaml"}

	cfgCmd.AddCommand(&cobra.Command{
		Use: "init", Short: "write a starter config",
		RunE: func(*cobra.Command, []string) error {
			if err := service.Init(cfgPath); err != nil {
				return err
			}
			fmt.Fprintf(os.Stdout, "wrote %s\n", cfgPath)
			return nil
		},
	})

	cfgCmd.AddCommand(&cobra.Command{
		Use: "validate", Short: "check the config file",
		RunE: func(*cobra.Command, []string) error {
			if _, err := config.Load(cfgPath); err != nil {
				return err
			}
			fmt.Fprintln(os.Stdout, "ok")
			return nil
		},
	})

	cfgCmd.AddCommand(&cobra.Command{
		Use: "show", Short: "print the effective config (secrets redacted)",
		RunE: func(*cobra.Command, []string) error {
			cfg, err := config.Load(cfgPath)
			if err != nil {
				return err
			}
			if jsonOut {
				return writeJSON(os.Stdout, service.Redacted(cfg))
			}
			data, err := service.Redacted(cfg).Marshal()
			if err != nil {
				return err
			}
			fmt.Fprint(os.Stdout, string(data))
			return nil
		},
	})

	var hostJSON string
	addHost := &cobra.Command{
		Use: "add-host", Short: "append a host from a JSON spec",
		RunE: func(*cobra.Command, []string) error {
			var h config.Host
			if err := json.Unmarshal([]byte(hostJSON), &h); err != nil {
				return exitcode.New(exitcode.Usage, "invalid --json host spec").WithCause(err.Error())
			}
			cfg, err := service.AddHost(cfgPath, h)
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stdout, "added %s (%d hosts total)\n", h.Name, len(cfg.Hosts))
			return nil
		},
	}
	addHost.Flags().StringVar(&hostJSON, "json", "", "host spec as JSON (required)")
	_ = addHost.MarkFlagRequired("json")
	cfgCmd.AddCommand(addHost)

	cfgCmd.AddCommand(&cobra.Command{
		Use: "remove-host <name>", Short: "delete a host by name", Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			cfg, err := service.RemoveHost(cfgPath, args[0])
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stdout, "removed %s (%d hosts total)\n", args[0], len(cfg.Hosts))
			return nil
		},
	})

	cfgCmd.AddCommand(&cobra.Command{
		Use: "set <key> <value>", Short: "set a config field", Args: cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			if _, err := service.SetField(cfgPath, args[0], args[1]); err != nil {
				return err
			}
			fmt.Fprintf(os.Stdout, "set %s\n", args[0])
			return nil
		},
	})

	return cfgCmd
}
