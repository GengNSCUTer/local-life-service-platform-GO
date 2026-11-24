package media

import (
	"bytes"
	"image"
	"image/png"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

type delayedImage struct {
	reading chan struct{}
	release chan struct{}
}

func TestSlowObjectStoreLeavesOtherRoutesAvailable(t *testing.T) {
	reached, release := make(chan struct{}, 1), make(chan struct{})
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = io.WriteString(w, `<LocationConstraint>us-east-1</LocationConstraint>`)
			return
		}
		if r.Method != http.MethodPut || !strings.HasPrefix(r.URL.Path, "/images/blog/") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case reached <- struct{}{}:
		default:
		}
		<-release
		w.Header().Set("ETag", `"fixture"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer storage.Close()
	store, err := New(storage.URL, "test-store", "test-store-secret", "images", storage.URL+"/images")
	if err != nil {
		t.Fatal(err)
	}
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	part, err := form.CreateFormFile("image", "picture.png")
	if err != nil {
		t.Fatal(err)
	}
	if err := png.Encode(part, image.NewRGBA(image.Rect(0, 0, 2, 2))); err != nil {
		t.Fatal(err)
	}
	if err := form.Close(); err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.GET("/blog/:id", func(c *gin.Context) { c.Status(http.StatusOK) })
	store.Register(router, func(c *gin.Context) { c.Next() }, slog.Default(), 1)
	request := httptest.NewRequest(http.MethodPost, "/media/images", &body)
	request.Header.Set("Content-Type", form.FormDataContentType())
	done := make(chan struct{})
	defer func() { close(release); <-done }()
	first := httptest.NewRecorder()
	go func() { defer close(done); router.ServeHTTP(first, request) }()
	select {
	case <-reached:
	case <-time.After(3 * time.Second):
		t.Fatal("upload never reached storage")
	}
	second := httptest.NewRecorder()
	router.ServeHTTP(second, httptest.NewRequest(http.MethodPost, "/media/images", nil))
	if second.Code != http.StatusServiceUnavailable {
		t.Errorf("second upload status = %d", second.Code)
	}
	detail := httptest.NewRecorder()
	router.ServeHTTP(detail, httptest.NewRequest(http.MethodGet, "/blog/42", nil))
	if detail.Code != http.StatusOK {
		t.Errorf("detail status = %d", detail.Code)
	}
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
