package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/parthparmar-2006/tidemark/internal/config"
)

func main() {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, config.DatabaseURL())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)

	measure(ctx, conn, "SELECT 1 round trip", func() error {
		var one int
		return conn.QueryRow(ctx, "SELECT 1").Scan(&one)
	})

	_, err = conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS probe_durable (id int)")
	if err != nil {
		log.Fatal(err)
	}

	measure(ctx, conn, "insert+commit, synchronous_commit=on", func() error {
		_, err := conn.Exec(ctx, "INSERT INTO probe_durable VALUES (1)")
		return err
	})

	_, err = conn.Exec(ctx, "SET synchronous_commit = off")
	if err != nil {
		log.Fatal(err)
	}
	measure(ctx, conn, "insert+commit, synchronous_commit=off", func() error {
		_, err := conn.Exec(ctx, "INSERT INTO probe_durable VALUES (1)")
		return err
	})

	_, err = conn.Exec(ctx, "DROP TABLE probe_durable")
	if err != nil {
		log.Fatal(err)
	}
}

func measure(ctx context.Context, conn *pgx.Conn, name string, operation func() error) {
	var samples []time.Duration
	for i := 0; i < 500; i++ {
		started := time.Now()
		err := operation()
		if err != nil {
			log.Fatal(err)
		}
		samples = append(samples, time.Since(started))
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	fmt.Printf("%-40s p50=%-12s p99=%s\n", name, samples[len(samples)/2], samples[len(samples)*99/100])
}
