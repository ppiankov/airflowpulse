package cli

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/spf13/cobra"
)

func newTestCommand(t *testing.T, setup func(*cobra.Command)) (*cobra.Command, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()

	var out bytes.Buffer
	var errOut bytes.Buffer

	cmd := &cobra.Command{}
	setup(cmd)
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)

	return cmd, &out, &errOut
}

func withDefaultTransportHandler(t *testing.T, handler http.Handler) func() {
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
