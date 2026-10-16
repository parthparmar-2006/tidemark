package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/parthparmar-2006/tidemark/internal/config"
)

type result struct {
	latency time.Duration
	status  int
	failed  bool
}

func main() {
	baseURL := config.String("SERVICE_URL", "http://localhost:18080")
	workers := config.Int("WORKERS", 16)
	accounts := config.Int("ACCOUNTS", 1000)
	duration := config.Duration("DURATION", 30*time.Second)

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: workers,
		},
	}

	log.Printf("load: workers=%d accounts=%d duration=%s", workers, accounts, duration)

	deadline := time.Now().Add(duration)
	results := make(chan result, 10000)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			runWorker(client, baseURL, accounts, deadline, rand.New(rand.NewSource(seed)), results)
		}(int64(i) + time.Now().UnixNano())
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var all []result
	for res := range results {
		all = append(all, res)
	}
	printSummary(all, duration)
}

func runWorker(client *http.Client, baseURL string, accounts int, deadline time.Time, random *rand.Rand, results chan<- result) {
	for time.Now().Before(deadline) {
		accountID := random.Intn(accounts) + 1
		action := "deposit"
		if random.Intn(10) < 3 {
			action = "withdraw"
		}
		amount := random.Intn(100) + 1
		url := fmt.Sprintf("%s/accounts/%d/%s", baseURL, accountID, action)
		body := fmt.Sprintf(`{"amount":%d}`, amount)

		started := time.Now()
		response, err := client.Post(url, "application/json", bytes.NewBufferString(body))
		elapsed := time.Since(started)
		if err != nil {
			results <- result{latency: elapsed, failed: true}
			continue
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		results <- result{latency: elapsed, status: response.StatusCode}
	}
}

func printSummary(all []result, duration time.Duration) {
	var ok, conflicts, failures int
	var latencies []time.Duration
	for _, res := range all {
		if res.failed {
			failures++
			continue
		}
		switch res.status {
		case http.StatusOK:
			ok++
		case http.StatusConflict:
			conflicts++
		default:
			failures++
			continue
		}
		latencies = append(latencies, res.latency)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	fmt.Printf("requests      : %d\n", len(all))
	fmt.Printf("committed     : %d (%.0f/s)\n", ok, float64(ok)/duration.Seconds())
	fmt.Printf("rejected (409): %d\n", conflicts)
	fmt.Printf("failed        : %d\n", failures)
	fmt.Printf("latency p50   : %s\n", percentile(latencies, 0.50))
	fmt.Printf("latency p99   : %s\n", percentile(latencies, 0.99))
	fmt.Printf("latency max   : %s\n", percentile(latencies, 1.0))
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted)-1) * p)
	return sorted[index]
}
