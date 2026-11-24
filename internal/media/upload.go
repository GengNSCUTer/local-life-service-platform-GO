package media

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/platform/httpx"
	"github.com/gin-gonic/gin"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const maxImageSize = 5 << 20

type Store struct {
	client *minio.Client
	bucket string
	public *url.URL
}

func New(endpoint, accessKey, secretKey, bucket, publicURL string) (*Store, error) {
	if endpoint == "" {
		return nil, nil
	}
	server, err := url.Parse(endpoint)
	if err != nil || !validURL(server) || (server.Path != "" && server.Path != "/") {
		return nil, errors.New("MEDIA_S3_ENDPOINT must be an HTTP(S) origin")
	}
	public, err := url.Parse(publicURL)
	if err != nil || !validURL(public) || bucket == "" || accessKey == "" || secretKey == "" {
		return nil, errors.New("media requires a public bucket URL, bucket and credentials")
	}
	client, err := minio.New(server.Host, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: server.Scheme == "https"})
	if err != nil {
		return nil, fmt.Errorf("create media client: %w", err)
	}
	return &Store{client: client, bucket: bucket, public: public}, nil
}

func validURL(u *url.URL) bool {
	return (u.Scheme == "https" || u.Scheme == "http") && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == ""
}

func (s *Store) Register(router *gin.Engine, login gin.HandlerFunc, logger *slog.Logger) {
	router.POST("/media/images", login, func(c *gin.Context) {
		if s == nil {
			httpx.Respond(c, http.StatusServiceUnavailable, "image storage is disabled", nil)
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxImageSize+(64<<10))
		reader, err := c.Request.MultipartReader()
		if err != nil {
			httpx.Respond(c, http.StatusBadRequest, "expected multipart image", nil)
			return
		}
		part, err := reader.NextPart()
		if err != nil || part.FormName() != "image" {
			httpx.Respond(c, http.StatusBadRequest, "expected image field", nil)
			return
		}
		data, err := io.ReadAll(io.LimitReader(part, maxImageSize+1))
		if len(data) > maxImageSize {
			httpx.Respond(c, http.StatusRequestEntityTooLarge, "image exceeds 5 MiB", nil)
			return
		}
		if err != nil {
			httpx.Respond(c, http.StatusBadRequest, "invalid image upload", nil)
			return
		}
		format, err := validateImage(data)
		if err != nil {
			httpx.Respond(c, http.StatusUnsupportedMediaType, err.Error(), nil)
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
		defer cancel()
		key := "blog/" + rand.Text() + "." + format
		_, err = s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: "image/" + format, DisableMultipart: true})
		if err != nil {
			logger.Warn("store blog image", "error", err)
			httpx.Respond(c, http.StatusServiceUnavailable, "image storage temporarily unavailable", nil)
			return
		}
		httpx.Respond(c, http.StatusCreated, "", gin.H{"url": s.public.JoinPath(key).String()})
	})
}

func validateImage(data []byte) (string, error) {
	cfg, format, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil || (format != "jpeg" && format != "png") || cfg.Width <= 0 || cfg.Height <= 0 || int64(cfg.Width)*int64(cfg.Height) > 16_000_000 {
		return "", errors.New("expected a JPEG or PNG of at most 16 million pixels")
	}
	if _, _, err := image.Decode(bytes.NewReader(data)); err != nil {
		return "", errors.New("image is incomplete or corrupt")
	}
	return format, nil
}
