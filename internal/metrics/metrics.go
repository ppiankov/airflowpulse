package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// Scrape health — per instance
	Up = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_up",
		Help: "1 if Airflow API is reachable, 0 otherwise",
	}, []string{"instance"})
	ScrapeDuration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_scrape_duration_seconds",
		Help: "Time taken to collect metrics",
	}, []string{"instance"})
	ScrapeErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "airflow_scrape_errors_total",
		Help: "Cumulative scrape error count",
	}, []string{"instance"})
)

func init() {
	prometheus.MustRegister(Up, ScrapeDuration, ScrapeErrors)
}
