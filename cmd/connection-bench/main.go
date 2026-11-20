// connection-bench opens isolated Linux loopback HTTP/1.1 keep-alive connections.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

const healthRequest = "GET /health/live HTTP/1.1\r\nHost: localhost\r\n\r\n"

func main() {
	basePort := flag.Int("port", 31320, "first loopback API port")
	servers := flag.Int("servers", 8, "number of consecutive API ports")
	connections := flag.Int("connections", 1_000_000, "target concurrent connections")
	workers := flag.Int("workers", 128, "parallel dial and probe workers")
	hold := flag.Duration("hold", 2*time.Minute, "interval to keep the full connection set open")
	flag.Parse()
	if *basePort < 1024 || *servers < 1 || *servers > 32 || *basePort > 65535-*servers+1 ||
		*workers < 1 || *workers > 512 || *connections < 1 || *connections > 1_000_000 || *hold < time.Second {
		log.Fatal("invalid connection benchmark parameters")
	}
	started := time.Now()
	clients := make([]net.Conn, *connections)
	for _, target := range []int{10_000, 100_000, *connections} {
		if target > *connections {
			continue
		}
		previous := 0
		for previous < len(clients) && clients[previous] != nil {
			previous++
		}
		if target <= previous {
			continue
		}
		err := parallel(previous, target, *workers, func(index int) error {
			source := net.IPv4(127, 88, 0, byte(index%64+1))
			dialer := net.Dialer{LocalAddr: &net.TCPAddr{IP: source}, Timeout: 5 * time.Second, KeepAlive: -1}
			port := *basePort + (index/64)%*servers
			connection, err := dialer.Dial("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
			if err != nil {
				return err
			}
			if err := probe(connection); err != nil {
				connection.Close()
				return err
			}
			clients[index] = connection
			return nil
		})
		if err != nil {
			log.Fatalf("reach %d connections: %v", target, err)
		}
		report("connected", target, started)
	}
	if err := parallel(0, len(clients), *workers, func(index int) error { return probe(clients[index]) }); err != nil {
		log.Fatalf("probe at start of hold: %v", err)
	}
	report("hold_start", len(clients), started)
	time.Sleep(*hold)
	if err := parallel(0, len(clients), *workers, func(index int) error { return probe(clients[index]) }); err != nil {
		log.Fatalf("probe at end of hold: %v", err)
	}
	report("hold_confirmed", len(clients), started)
	_ = parallel(0, len(clients), *workers, func(index int) error { return clients[index].Close() })
}

func report(stage string, count int, started time.Time) {
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"stage": stage, "connections": count, "elapsedSeconds": time.Since(started).Seconds(),
	})
}

func probe(conn net.Conn) error {
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	if _, err := io.WriteString(conn, healthRequest); err != nil {
		return err
	}
	response, err := http.ReadResponse(bufio.NewReaderSize(conn, 512), nil)
	if err != nil {
		return err
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if response.StatusCode != http.StatusOK || response.Close {
		return fmt.Errorf("HTTP %d, close=%t", response.StatusCode, response.Close)
	}
	return nil
}

func parallel(first, end, workers int, run func(int) error) error {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	var group sync.WaitGroup
	for worker := range workers {
		group.Go(func() {
			for index := first + worker; index < end && ctx.Err() == nil; index += workers {
				if err := run(index); err != nil {
					cancel(fmt.Errorf("connection %d: %w", index, err))
					return
				}
			}
		})
	}
	group.Wait()
	return context.Cause(ctx)
}
