package annotate

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClientEnabled(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want bool
	}{
		{name: "disabled", cfg: Config{}, want: false},
		{name: "missing token", cfg: Config{URL: "https://grafana.example"}, want: false},
		{name: "enabled", cfg: Config{URL: "https://grafana.example", Token: "token"}, want: true},
	}

	for _, tt := range tests {
		if got := New(tt.cfg).Enabled(); got != tt.want {
			t.Fatalf("%s: Enabled() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestClientPush(t *testing.T) {
	var gotAuth string
	var gotPayload map[string]any

	restoreTransport := overrideDefaultTransport(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/annotations" {
			t.Fatalf("path = %q, want /api/annotations", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("content-type = %q, want application/json", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotPayload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
	}))
	defer restoreTransport()

	client := New(Config{
		URL:          "http://grafana.test",
		Token:        "grafana-token",
		DashboardUID: "dash",
	})
	annotation := Annotation{
		Time: time.UnixMilli(1704164645000),
		Text: "airflowpulse: instance down",
		Tags: []string{"airflowpulse", "down"},
	}
	if err := client.Push(context.Background(), annotation); err != nil {
		t.Fatalf("Push() error = %v", err)
	}

	if gotAuth != "Bearer grafana-token" {
		t.Fatalf("authorization = %q, want Bearer grafana-token", gotAuth)
	}
	if gotPayload["text"] != annotation.Text {
		t.Fatalf("payload text = %#v, want %q", gotPayload["text"], annotation.Text)
	}
	if gotPayload["dashboardUID"] != "dash" {
		t.Fatalf("payload dashboardUID = %#v, want dash", gotPayload["dashboardUID"])
	}
	if gotPayload["time"] != float64(annotation.Time.UnixMilli()) {
		t.Fatalf("payload time = %#v, want %d", gotPayload["time"], annotation.Time.UnixMilli())
	}
}

func TestClientPushReturnsServerError(t *testing.T) {
	restoreTransport := overrideDefaultTransport(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer restoreTransport()

	client := New(Config{URL: "http://grafana.test", Token: "grafana-token"})
	err := client.Push(context.Background(), Annotation{Time: time.Now()})
	if err == nil {
		t.Fatal("Push() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "grafana returned 502") {
		t.Fatalf("Push() error = %q, want grafana status", err)
	}
}

func overrideDefaultTransport(t *testing.T, handler http.Handler) func() {
	t.Helper()

	origTransport := http.DefaultTransport
	http.DefaultTransport = roundTripHandler{handler: handler}
	return func() {
		http.DefaultTransport = origTransport
	}
}

type roundTripHandler struct {
	handler http.Handler
}

func (r roundTripHandler) RoundTrip(req *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	r.handler.ServeHTTP(recorder, req)
	return recorder.Result(), nil
}
