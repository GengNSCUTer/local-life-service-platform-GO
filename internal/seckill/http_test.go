package seckill

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type reservationTestHook struct{ process redis.ProcessHook }

func (h reservationTestHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h reservationTestHook) ProcessHook(redis.ProcessHook) redis.ProcessHook {
	return h.process
}
func (h reservationTestHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestReserveRetriesOnlyRejectedClusterWrites(t *testing.T) {
	for _, test := range []struct {
		name      string
		firstErr  error
		wantCalls int
	}{
		{"rehashing", errors.New("TRYAGAIN Multiple keys request during rehashing of slot"), 2},
		{"ask", errors.New("ASK 6220 127.0.0.1:6379"), 2},
		{"moved", errors.New("MOVED 6220 127.0.0.1:6379"), 2},
		{"network", io.ErrUnexpectedEOF, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := redis.NewClusterClient(&redis.ClusterOptions{})
			t.Cleanup(func() { _ = client.Close() })
			calls := 0
			var firstArgs []any
			client.AddHook(reservationTestHook{process: func(_ context.Context, command redis.Cmder) error {
				calls++
				if calls == 1 {
					firstArgs = command.Args()
					return test.firstErr
				}
				if !reflect.DeepEqual(command.Args(), firstArgs) {
					t.Fatal("retry changed the Lua keys or reservation arguments")
				}
				command.(*redis.Cmd).SetVal([]any{"accepted", "order-id"})
				return nil
			}})
			service := New(nil, nil, nil, time.Minute, 0, 0, Layout{Sharded: true, Cluster: client})
			result, err := service.reserve(t.Context(), 9601, 7)
			if calls != test.wantCalls {
				t.Fatalf("calls=%d, want %d, err=%v", calls, test.wantCalls, err)
			}
			if test.wantCalls == 1 && !errors.Is(err, test.firstErr) {
				t.Fatalf("err=%v, want %v", err, test.firstErr)
			}
			if test.wantCalls == 2 && (err != nil || !reflect.DeepEqual(result, []string{"accepted", "order-id"})) {
				t.Fatalf("result=%v, err=%v", result, err)
			}
		})
	}
}
