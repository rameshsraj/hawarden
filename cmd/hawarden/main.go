// hawarden is the standalone CLI command centre for one
// Patroni + keepalived + etcd + PostgreSQL HA cluster.
package main

import (
	"os"

	"github.com/rameshsraj/hawarden/internal/cli"
)

var version = "dev"

func main() {
	cli.SetVersion(version)
	os.Exit(cli.Execute())
}
