package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/alert"
	"github.com/ppiankov/airflowpulse/internal/annotate"
	"github.com/ppiankov/airflowpulse/internal/collector"
	"github.com/ppiankov/airflowpulse/internal/config"
	"github.com/ppiankov/airflowpulse/internal/metadb"
	"github.com/ppiankov/airflowpulse/internal/metrics"
	"github.com/ppiankov/airflowpulse/internal/retry"
)

// Engine runs the poll loop and HTTP server.
type Engine struct {
	cfg        *config.Config
	clients    []*airflow.Client
	collectors []collector.Collector
	alerter    *alert.Sender
	annotator  *annotate.Client
	stream     bool
	streamEnc  *json.Encoder

	// Previous state for alerting/streaming.
	prevUp map[string]bool
	mu     sync.Mutex
}

// New creates an engine from configuration.
func New(cfg *config.Config) *Engine {
	var clients []*airflow.Client
	for _, u := range cfg.APIURLs {
		clients = append(clients, airflow.New(u, cfg.APIUser, cfg.APIPassword))
	}

	collectors := collector.All()

	// Add metadata DB collectors if DSN is configured.
	if cfg.MetadataDSN != "" {
		db, err := metadb.New(cfg.MetadataDSN)
		if err != nil {
			log.Printf("metadata DB unavailable: %v (metadata collectors disabled)", err)
		} else {
			collectors = append(collectors,
				&collector.SLAMiss{DB: db},
				&collector.XCom{DB: db},
				&collector.Executor{DB: db},
			)
			log.Printf("metadata DB connected, SLA/XCom/executor collectors enabled")
		}
	}

	alerter := alert.New(alert.Config{
		TelegramBotToken: cfg.TelegramBotToken,
		TelegramChatID:   cfg.TelegramChatID,
		WebhookURL:       cfg.AlertWebhookURL,
		Cooldown:         cfg.AlertCooldown,
	})

	annotator := annotate.New(annotate.Config{
		URL:          cfg.GrafanaURL,
		Token:        cfg.GrafanaToken,
		DashboardUID: cfg.GrafanaDashboardUID,
	})

	return &Engine{
		cfg:        cfg,
		clients:    clients,
		collectors: collectors,
		alerter:    alerter,
		annotator:  annotator,
		prevUp:     make(map[string]bool),
	}
}

// EnableStream turns on JSON-lines event streaming to stdout.
func (e *Engine) EnableStream() {
	e.stream = true
	e.streamEnc = json.NewEncoder(os.Stdout)
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

	isUp := collectErr == nil
	if isUp {
		metrics.Up.WithLabelValues(instance).Set(1)
	} else {
		metrics.Up.WithLabelValues(instance).Set(0)
	}

	// Alert and annotate on state transitions.
	e.mu.Lock()
	wasUp, existed := e.prevUp[instance]
	e.prevUp[instance] = isUp
	e.mu.Unlock()

	if existed && wasUp && !isUp {
		e.fireAlert(ctx, instance, "Instance down", fmt.Sprintf("%s is unreachable", instance))
		e.fireAnnotation(ctx, fmt.Sprintf("airflowpulse: %s went DOWN", instance), []string{"airflowpulse", "down"})
	}
	if existed && !wasUp && isUp {
		e.fireAnnotation(ctx, fmt.Sprintf("airflowpulse: %s recovered", instance), []string{"airflowpulse", "recovered"})
	}
}

func (e *Engine) fireAlert(ctx context.Context, instance, title, message string) {
	if !e.alerter.Enabled() {
		return
	}
	if err := e.alerter.Send(ctx, alert.Alert{
		Key:      "down:" + instance,
		Severity: "critical",
		Instance: instance,
		Title:    title,
		Message:  message,
	}); err != nil {
		log.Printf("alert failed: %v", err)
	}
}

func (e *Engine) fireAnnotation(ctx context.Context, text string, tags []string) {
	if !e.annotator.Enabled() {
		return
	}
	if err := e.annotator.Push(ctx, annotate.Annotation{
		Time: time.Now(),
		Text: text,
		Tags: tags,
	}); err != nil {
		log.Printf("annotation failed: %v", err)
	}
}
