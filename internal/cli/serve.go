package cli

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/ppiankov/airflowpulse/internal/config"
	"github.com/ppiankov/airflowpulse/internal/engine"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the metrics exporter",
	Long:  "Start the poll loop and HTTP server exposing /metrics and /healthz.",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load()
		if err != nil {
			return err
		}

		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		eng := engine.New(cfg)

		stream, _ := cmd.Flags().GetBool("stream")
		if stream {
			eng.EnableStream()
		}

		return eng.Run(ctx)
	},
}

func init() {
	serveCmd.Flags().Bool("stream", false, "Output JSON-lines event stream to stdout alongside metrics server")
}
