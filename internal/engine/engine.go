package engine

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/collector"
	"github.com/ppiankov/airflowpulse/internal/config"
	"github.com/ppiankov/airflowpulse/internal/metrics"
	"github.com/ppiankov/airflowpulse/internal/retry"
)

// Engine runs the poll loop and HTTP server.
type Engine struct {
	cfg        *config.Config
	clients    []*airflow.Client
	collectors []collector.Collector
}

// New creates an engine from configuration.
func New(cfg *config.Config) *Engine {
	var clients []*airflow.Client
	for _, u := range cfg.APIURLs {
		clients = append(clients, airflow.New(u, cfg.APIUser, cfg.APIPassword))
	}
	return &Engine{
		cfg:        cfg,
		clients:    clients,
		collectors: collector.All(),
	}
}

// Run starts the poll loop and HTTP server, blocking until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", e.cfg.MetricsPort),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("serving metrics on :%d", e.cfg.MetricsPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	// Initial poll before entering loop.
	e.poll(ctx)

	ticker := time.NewTicker(e.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down")
			shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return srv.Shutdown(shutCtx)
		case err := <-errCh:
			return fmt.Errorf("http server: %w", err)
		case <-ticker.C:
			e.poll(ctx)
		}
	}
}

func (e *Engine) poll(ctx context.Context) {
	var wg sync.WaitGroup
	for _, client := range e.clients {
		wg.Add(1)
		go func(c *airflow.Client) {
			defer wg.Done()
			e.pollInstance(ctx, c)
		}(client)
	}
	wg.Wait()
}

func (e *Engine) pollInstance(ctx context.Context, client *airflow.Client) {
	instance := config.InstanceLabel(client.BaseURL())
	start := time.Now()

	var collectErr error
	for _, col := range e.collectors {
		err := retry.Do(ctx, retry.DefaultMaxAttempts, func() error {
			return col.Collect(ctx, client, instance)
		})
		if err != nil {
			log.Printf("collector %s failed for %s: %v", col.Name(), instance, err)
			metrics.ScrapeErrors.WithLabelValues(instance).Inc()
			collectErr = err
		}
	}

	elapsed := time.Since(start).Seconds()
	metrics.ScrapeDuration.WithLabelValues(instance).Set(elapsed)

	if collectErr != nil {
		metrics.Up.WithLabelValues(instance).Set(0)
	} else {
		metrics.Up.WithLabelValues(instance).Set(1)
	}
}
