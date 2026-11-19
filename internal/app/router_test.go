package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthEndpoints(t *testing.T) {
	for _, tc := range []struct {
		name       string
		path       string
		probeError error
		wantStatus int
		wantCheck  string
	}{
		{name: "live with dependency down", path: "/health/live", probeError: errors.New("not reachable"), wantStatus: http.StatusOK},
		{name: "ready", path: "/health/ready", wantStatus: http.StatusOK, wantCheck: "up"},
		{name: "unready", path: "/health/ready", probeError: errors.New("not reachable"), wantStatus: http.StatusServiceUnavailable, wantCheck: "down"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			router := newRouter(logger, []healthProbe{
				{name: "mysql", check: func(context.Context) error {
					if tc.wantCheck == "" {
						t.Error("liveness must skip dependency checks")
					}
					return tc.probeError
				}},
			})
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if response.Code != tc.wantStatus {
				t.Fatalf("HTTP status = %d, want %d", response.Code, tc.wantStatus)
			}
			if response.Header().Get("X-Request-ID") == "" {
				t.Error("response is missing X-Request-ID")
			}
			var body struct {
				Success bool `json:"success"`
				Data    struct {
					Checks map[string]string `json:"checks"`
				} `json:"data"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode health response: %v", err)
			}
			if body.Success != (tc.wantStatus == http.StatusOK) || body.Data.Checks["mysql"] != tc.wantCheck {
				t.Errorf("unexpected health response: %s", response.Body.String())
			}
		})
	}
}
