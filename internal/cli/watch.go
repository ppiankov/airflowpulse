package cli

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/ppiankov/airflowpulse/internal/config"
)

const defaultWatchInterval = 15 * time.Second

// runWithWatch wraps a command's RunE to support --watch mode.
func runWithWatch(fn func(cmd *cobra.Command, args []string) error) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		watch, _ := cmd.Flags().GetBool("watch")
		if !watch {
			return fn(cmd, args)
		}

		interval, _ := cmd.Flags().GetDuration("interval")
		if interval == 0 {
			// Try to use the configured poll interval.
			if cfg, err := config.Load(); err == nil {
				interval = cfg.PollInterval
			} else {
				interval = defaultWatchInterval
			}
		}

		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

		for {
			// Clear screen.
			fmt.Fprint(os.Stdout, "\033[H\033[2J")
			fmt.Fprintf(cmd.OutOrStdout(), "Last updated: %s  (Ctrl+C to stop, interval: %s)\n\n",
				time.Now().Format("15:04:05"), interval)

			if err := fn(cmd, args); err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
			}

			select {
			case <-sigCh:
				return nil
			case <-time.After(interval):
			}
		}
	}
}
