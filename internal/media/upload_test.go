package media

import (
	"bytes"
	"image"
	"image/jpeg"
	"image/png"
	"testing"
)

func TestValidateImage(t *testing.T) {
	picture := image.NewRGBA(image.Rect(0, 0, 2, 2))
	for _, tt := range []struct {
		format string
		encode func(*bytes.Buffer) error
	}{
		{"png", func(b *bytes.Buffer) error { return png.Encode(b, picture) }},
		{"jpeg", func(b *bytes.Buffer) error { return jpeg.Encode(b, picture, nil) }},
	} {
		t.Run(tt.format, func(t *testing.T) {
			var b bytes.Buffer
			if err := tt.encode(&b); err != nil {
				t.Fatal(err)
			}
			if format, err := validateImage(b.Bytes()); err != nil || format != tt.format {
				t.Fatalf("valid image: %q %v", format, err)
			}
			if _, err := validateImage(b.Bytes()[:b.Len()/2]); err == nil {
				t.Fatal("truncated image accepted")
			}
		})
	}
}
