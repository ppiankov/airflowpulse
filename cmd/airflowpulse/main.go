package main

import (
	"os"

	"github.com/ppiankov/airflowpulse/internal/cli"
)

// version is set via ldflags at build time.
var version = "dev"

func main() {
	cli.SetVersion(version)
	if err := cli.Execute(); err != nil {
		os.Exit(1)
	}
}
