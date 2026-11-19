// seckill-bench sends a fixed number of requests to a local test activity.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
)

const maxLatencyMS = 5001

type counts struct {
	Accepted   int `json:"accepted"`
	SoldOut    int `json:"soldOut"`
	Duplicate  int `json:"duplicate"`
	Errors     int `json:"errors"`
	latencies  [maxLatencyMS + 1]int
	firstError string
}

func main() {
	target := flag.String("target", "http://127.0.0.1:8080", "base URL of the local test API")
	voucher := flag.Uint64("voucher-id", 0, "activity ID")
	requests := flag.Int("requests", 1_000_000, "total requests")
	workers := flag.Int("workers", 256, "maximum concurrent requests")
	users := flag.Int("users", 16_384, "number of sequentially numbered test sessions")
	prefix := flag.String("token-prefix", "bench-", "prefix of the seeded Redis session tokens")
	flag.Parse()
	base, err := url.Parse(*target)
	if err != nil || base.Scheme != "http" || base.User != nil || base.Path != "" || base.RawQuery != "" || base.Fragment != "" {
		log.Fatal("target must be an HTTP origin on a loopback IP")
	}
	if ip := net.ParseIP(base.Hostname()); ip == nil || !ip.IsLoopback() {
		log.Fatal("target must use a loopback IP")
	}
	if *voucher == 0 || *requests < 1 || *workers < 1 || *users < 1 || *workers > *requests {
		log.Fatal("provide a positive activity ID and positive requests, workers, and users")
	}

	transport := &http.Transport{MaxIdleConns: *workers, MaxIdleConnsPerHost: *workers, MaxConnsPerHost: *workers}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	endpoint := base.String() + "/voucher-order/seckill/" + strconv.FormatUint(*voucher, 10)
	tokens := make([]string, *users)
	for i := range tokens {
		tokens[i] = "Bearer " + *prefix + strconv.Itoa(i+1)
	}

	started := time.Now()
	partials := make(chan counts, *workers)
	var active sync.WaitGroup
	for worker := range *workers {
		active.Go(func() {
			var result counts
			for i := worker; i < *requests; i += *workers {
				start := time.Now()
				result.send(client, endpoint, tokens[i%len(tokens)])
				bucket := min(int((time.Since(start)+time.Millisecond-1)/time.Millisecond), maxLatencyMS)
				result.latencies[bucket]++
			}
			partials <- result
		})
	}
	active.Wait()
	close(partials)
	elapsed := time.Since(started).Seconds()
	var result counts
	for partial := range partials {
		result.Accepted += partial.Accepted
		result.SoldOut += partial.SoldOut
		result.Duplicate += partial.Duplicate
		result.Errors += partial.Errors
		for bucket, count := range partial.latencies {
			result.latencies[bucket] += count
		}
		if result.firstError == "" {
			result.firstError = partial.firstError
		}
	}
	output := struct {
		counts
		Requests int     `json:"requests"`
		Workers  int     `json:"workers"`
		Users    int     `json:"users"`
		Seconds  float64 `json:"seconds"`
		RPS      float64 `json:"requestsPerSecond"`
		P50      int     `json:"p50Ms"`
		P95      int     `json:"p95Ms"`
		P99      int     `json:"p99Ms"`
	}{result, *requests, *workers, *users, elapsed, float64(*requests) / elapsed,
		result.percentile(*requests, 50), result.percentile(*requests, 95), result.percentile(*requests, 99)}
	if err := json.NewEncoder(os.Stdout).Encode(output); err != nil {
		log.Fatal(err)
	}
	if result.Errors > 0 {
		log.Fatal(result.firstError)
	}
}

func (c *counts) send(client *http.Client, endpoint, token string) {
	request, err := http.NewRequest(http.MethodPost, endpoint, nil)
	if err != nil {
		c.recordError(err.Error())
		return
	}
	request.Header.Set("Authorization", token)
	response, err := client.Do(request)
	if err != nil {
		c.recordError(err.Error())
		return
	}
	defer response.Body.Close()
	var body struct {
		Success bool   `json:"success"`
		Message string `json:"errorMsg"`
		Data    struct {
			OrderID string `json:"orderId"`
		} `json:"data"`
	}
	err = json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&body)
	if err != nil {
		c.recordError(err.Error())
		return
	}
	switch {
	case response.StatusCode == http.StatusAccepted && body.Success && body.Data.OrderID != "":
		c.Accepted++
	case response.StatusCode == http.StatusConflict && body.Message == "sold out":
		c.SoldOut++
	case response.StatusCode == http.StatusConflict && body.Message == "already submitted" && body.Data.OrderID != "":
		c.Duplicate++
	default:
		c.recordError(fmt.Sprintf("unexpected HTTP %d %q", response.StatusCode, body.Message))
	}
}

func (c *counts) recordError(message string) {
	c.Errors++
	if c.firstError == "" {
		c.firstError = message
	}
}

func (c *counts) percentile(total, percentile int) int {
	rank := (total*percentile + 99) / 100
	for bucket, count := range c.latencies {
		rank -= count
		if rank <= 0 {
			return bucket
		}
	}
	return maxLatencyMS
}
