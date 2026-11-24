package media

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

type delayedImage struct {
	reading chan struct{}
	release chan struct{}
}

func (d *delayedImage) Read([]byte) (int, error) {
	close(d.reading)
	<-d.release
	return 0, io.EOF
}

func TestUploadBudgetLeavesOtherRoutesAvailable(t *testing.T) {
	store := &Store{}
	router := gin.New()
	router.GET("/health", func(c *gin.Context) { c.Status(http.StatusOK) })
	store.Register(router, func(c *gin.Context) { c.Next() }, slog.Default(), 1)
	body := &delayedImage{reading: make(chan struct{}), release: make(chan struct{})}
	request := httptest.NewRequest(http.MethodPost, "/media/images", body)
	request.Header.Set("Content-Type", "multipart/form-data; boundary=fixture")
	finished := make(chan struct{})
	defer func() { close(body.release); <-finished }()
	go func() {
		defer close(finished)
		router.ServeHTTP(httptest.NewRecorder(), request)
	}()
	select {
	case <-body.reading:
	case <-time.After(time.Second):
		t.Fatal("first upload never started reading")
	}
	result := httptest.NewRecorder()
	router.ServeHTTP(result, httptest.NewRequest(http.MethodPost, "/media/images", nil))
	if result.Code != http.StatusServiceUnavailable || result.Header().Get("Retry-After") != "1" {
		t.Fatalf("second upload status = %v", result.Code)
	}
	result = httptest.NewRecorder()
	router.ServeHTTP(result, httptest.NewRequest(http.MethodGet, "/health", nil))
	if result.Code != http.StatusOK {
		t.Fatalf("other route status = %v", result.Code)
	}
}
