package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

var (
	alertNow           = time.Now
	telegramAPIBaseURL = "https://api.telegram.org"
)

// Sender handles alert delivery via Telegram and/or webhook.
type Sender struct {
	telegramToken  string
	telegramChatID string
	webhookURL     string
	cooldown       time.Duration
	lastSent       map[string]time.Time
	mu             sync.Mutex
}

// Config for alert sender.
type Config struct {
	TelegramBotToken string
	TelegramChatID   string
	WebhookURL       string
	Cooldown         time.Duration
}

// New creates an alert sender.
func New(cfg Config) *Sender {
	return &Sender{
		telegramToken:  cfg.TelegramBotToken,
		telegramChatID: cfg.TelegramChatID,
		webhookURL:     cfg.WebhookURL,
		cooldown:       cfg.Cooldown,
		lastSent:       make(map[string]time.Time),
	}
}

// Enabled returns true if at least one alert channel is configured.
func (s *Sender) Enabled() bool {
	return (s.telegramToken != "" && s.telegramChatID != "") || s.webhookURL != ""
}

// Alert describes a single alert to send.
type Alert struct {
	Key      string `json:"key"`
	Severity string `json:"severity"`
	Instance string `json:"instance"`
	Title    string `json:"title"`
	Message  string `json:"message"`
}

// Send dispatches an alert if cooldown allows.
func (s *Sender) Send(ctx context.Context, a Alert) error {
	current := alertNow()

	s.mu.Lock()
	if last, ok := s.lastSent[a.Key]; ok && current.Sub(last) < s.cooldown {
		s.mu.Unlock()
		return nil
	}
	s.lastSent[a.Key] = current
	s.mu.Unlock()

	if s.telegramToken != "" && s.telegramChatID != "" {
		if err := s.sendTelegram(ctx, a); err != nil {
			return fmt.Errorf("telegram: %w", err)
		}
	}

	if s.webhookURL != "" {
		if err := s.sendWebhook(ctx, a); err != nil {
			return fmt.Errorf("webhook: %w", err)
		}
	}

	return nil
}

func (s *Sender) sendTelegram(ctx context.Context, a Alert) error {
	url := fmt.Sprintf("%s/bot%s/sendMessage", telegramAPIBaseURL, s.telegramToken)
	text := fmt.Sprintf("[%s] %s\n%s\nInstance: %s", a.Severity, a.Title, a.Message, a.Instance)

	body, _ := json.Marshal(map[string]string{
		"chat_id": s.telegramChatID,
		"text":    text,
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("telegram returned %d", resp.StatusCode)
	}
	return nil
}

func (s *Sender) sendWebhook(ctx context.Context, a Alert) error {
	body, _ := json.Marshal(a)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.webhookURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook returned %d", resp.StatusCode)
	}
	return nil
}
