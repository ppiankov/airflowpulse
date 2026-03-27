package alert

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSenderEnabled(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want bool
	}{
		{name: "disabled", cfg: Config{}, want: false},
		{name: "webhook only", cfg: Config{WebhookURL: "https://hooks.example"}, want: true},
		{name: "telegram only", cfg: Config{TelegramBotToken: "token", TelegramChatID: "chat"}, want: true},
	}

	for _, tt := range tests {
		if got := New(tt.cfg).Enabled(); got != tt.want {
			t.Fatalf("%s: Enabled() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestSenderSendTelegramAndWebhook(t *testing.T) {
	var mu sync.Mutex
	telegramCalls := 0
	webhookCalls := 0
	telegramText := ""
	webhookAlert := Alert{}

	restoreTransport := overrideDefaultTransport(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/bottoken/sendMessage":
			if r.Method != http.MethodPost {
				t.Fatalf("telegram method = %s, want POST", r.Method)
			}
			if got := r.Header.Get("Content-Type"); got != "application/json" {
				t.Fatalf("telegram content-type = %q, want application/json", got)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode telegram body: %v", err)
			}
			mu.Lock()
			telegramCalls++
			telegramText = body["text"]
			mu.Unlock()
		case "/hook":
			if r.Method != http.MethodPost {
				t.Fatalf("webhook method = %s, want POST", r.Method)
			}
			if got := r.Header.Get("Content-Type"); got != "application/json" {
				t.Fatalf("webhook content-type = %q, want application/json", got)
			}
			var body Alert
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode webhook body: %v", err)
			}
			mu.Lock()
			webhookCalls++
			webhookAlert = body
			mu.Unlock()
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer restoreTransport()

	restore := overrideAlertTestHooks(t, func() time.Time {
		return time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
	}, "http://telegram.test")
	defer restore()

	sender := New(Config{
		TelegramBotToken: "token",
		TelegramChatID:   "chat",
		WebhookURL:       "http://hooks.test/hook",
		Cooldown:         time.Minute,
	})

	alert := Alert{
		Key:      "dag-down",
		Severity: "critical",
		Instance: "airflow.example",
		Title:    "Instance down",
		Message:  "Scheduler unreachable",
	}
	if err := sender.Send(context.Background(), alert); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if telegramCalls != 1 {
		t.Fatalf("telegram calls = %d, want 1", telegramCalls)
	}
	if webhookCalls != 1 {
		t.Fatalf("webhook calls = %d, want 1", webhookCalls)
	}
	if !strings.Contains(telegramText, "[critical] Instance down") {
		t.Fatalf("telegram text = %q", telegramText)
	}
	if !strings.Contains(telegramText, "Instance: airflow.example") {
		t.Fatalf("telegram text = %q", telegramText)
	}
	if webhookAlert != alert {
		t.Fatalf("webhook alert = %#v, want %#v", webhookAlert, alert)
	}
}

func TestSenderCooldownDeduplicates(t *testing.T) {
	var mu sync.Mutex
	webhookCalls := 0

	restoreTransport := overrideDefaultTransport(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		webhookCalls++
		mu.Unlock()
	}))
	defer restoreTransport()

	times := []time.Time{
		time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC),
		time.Date(2024, time.January, 2, 3, 4, 35, 0, time.UTC),
		time.Date(2024, time.January, 2, 3, 5, 6, 0, time.UTC),
	}
	index := 0
	restore := overrideAlertTestHooks(t, func() time.Time {
		current := times[index]
		index++
		return current
	}, "")
	defer restore()

	sender := New(Config{
		WebhookURL: "http://hooks.test",
		Cooldown:   time.Minute,
	})
	alert := Alert{Key: "dedup", Title: "same"}

	for i := 0; i < 3; i++ {
		if err := sender.Send(context.Background(), alert); err != nil {
			t.Fatalf("Send() call %d error = %v", i+1, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if webhookCalls != 2 {
		t.Fatalf("webhook calls = %d, want 2", webhookCalls)
	}
}

func TestSenderTelegramError(t *testing.T) {
	restoreTransport := overrideDefaultTransport(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer restoreTransport()

	restore := overrideAlertTestHooks(t, func() time.Time {
		return time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
	}, "http://telegram.test")
	defer restore()

	sender := New(Config{
		TelegramBotToken: "token",
		TelegramChatID:   "chat",
	})
	err := sender.Send(context.Background(), Alert{Key: "telegram"})
	if err == nil {
		t.Fatal("Send() error = nil, want telegram error")
	}
	if !strings.Contains(err.Error(), "telegram returned 502") {
		t.Fatalf("Send() error = %q, want telegram status", err)
	}
}

func TestSenderWebhookError(t *testing.T) {
	restoreTransport := overrideDefaultTransport(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer restoreTransport()

	restore := overrideAlertTestHooks(t, func() time.Time {
		return time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
	}, "")
	defer restore()

	sender := New(Config{WebhookURL: "http://hooks.test"})
	err := sender.Send(context.Background(), Alert{Key: "webhook"})
	if err == nil {
		t.Fatal("Send() error = nil, want webhook error")
	}
	if !strings.Contains(err.Error(), "webhook returned 500") {
		t.Fatalf("Send() error = %q, want webhook status", err)
	}
}

func overrideAlertTestHooks(t *testing.T, nowFn func() time.Time, telegramURL string) func() {
	t.Helper()

	origNow := alertNow
	origTelegramURL := telegramAPIBaseURL

	alertNow = nowFn
	if telegramURL != "" {
		telegramAPIBaseURL = telegramURL
	}

	return func() {
		alertNow = origNow
		telegramAPIBaseURL = origTelegramURL
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
