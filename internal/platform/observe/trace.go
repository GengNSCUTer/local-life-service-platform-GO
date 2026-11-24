package observe

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func StartTracing(ctx context.Context, service, endpoint string, ratio float64) (func() error, error) {
	if endpoint == "" {
		return func() error { return nil }, nil
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, fmt.Errorf("TRACE_OTLP_ENDPOINT must be an HTTP(S) URL")
	}
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint), otlptracehttp.WithTimeout(2*time.Second))
	if err != nil {
		return nil, fmt.Errorf("configure OTLP traces: %w", err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithMaxQueueSize(2048), sdktrace.WithExportTimeout(2*time.Second)),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", service))),
	)
	otel.SetTracerProvider(provider)
	return func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return provider.Shutdown(ctx)
	}, nil
}

func TraceSeckill(c *gin.Context) {
	if c.Request.Method != http.MethodPost || c.FullPath() != "/voucher-order/seckill/:id" {
		c.Next()
		return
	}
	ctx, span := StartSpan(c.Request.Context(), "POST /voucher-order/seckill/:id", trace.SpanKindServer)
	defer span.End()
	c.Request = c.Request.WithContext(ctx)
	c.Next()
	span.SetAttributes(attribute.Int("http.response.status_code", c.Writer.Status()))
	if c.Writer.Status() >= 500 {
		span.SetStatus(codes.Error, "server request failed")
	}
}

func StartSpan(ctx context.Context, name string, kind trace.SpanKind) (context.Context, trace.Span) {
	return otel.Tracer("hmdp-go/seckill").Start(ctx, name, trace.WithSpanKind(kind))
}

func TraceHeaders(ctx context.Context) map[string]string {
	if !trace.SpanContextFromContext(ctx).IsSampled() {
		return nil
	}
	headers := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, headers)
	return headers
}

func RestoreTrace(ctx context.Context, headers map[string]string) context.Context {
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier(headers))
}
