package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/parthparmar-2006/tidemark/internal/config"
)

const selectBatchSQL = `
SELECT id, aggregate_id, payload
FROM outbox
ORDER BY id
LIMIT $1
FOR UPDATE SKIP LOCKED`

const deleteBatchSQL = `DELETE FROM outbox WHERE id = ANY($1)`

type outboxRow struct {
	id          int64
	aggregateID int64
	payload     []byte
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	pool, err := pgxpool.New(ctx, config.DatabaseURL())
	if err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	brokers := strings.Split(config.KafkaBrokers(), ",")
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(config.Topic()),
	)
	if err != nil {
		log.Fatalf("create kafka client: %v", err)
	}
	defer client.Close()

	batchSize := config.Int("BATCH_SIZE", 500)
	pollInterval := config.Duration("POLL_INTERVAL", 50*time.Millisecond)
	retryDelay := config.Duration("RETRY_DELAY", time.Second)

	log.Printf("polling relay started: batch=%d poll=%s", batchSize, pollInterval)
	for ctx.Err() == nil {
		count, err := relayBatch(ctx, pool, client, batchSize)
		if err != nil {
			log.Printf("relay batch failed: %v", err)
			sleep(ctx, retryDelay)
			continue
		}
		if count < batchSize {
			sleep(ctx, pollInterval)
		}
	}
	log.Printf("polling relay stopped")
}

func relayBatch(ctx context.Context, pool *pgxpool.Pool, client *kgo.Client, batchSize int) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, selectBatchSQL, batchSize)
	if err != nil {
		return 0, err
	}
	var batch []outboxRow
	for rows.Next() {
		var row outboxRow
		err = rows.Scan(&row.id, &row.aggregateID, &row.payload)
		if err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, row)
	}
	rows.Close()
	if rows.Err() != nil {
		return 0, rows.Err()
	}
	if len(batch) == 0 {
		return 0, nil
	}

	records := make([]*kgo.Record, 0, len(batch))
	ids := make([]int64, 0, len(batch))
	for _, row := range batch {
		record := &kgo.Record{
			Key:   []byte(strconv.FormatInt(row.aggregateID, 10)),
			Value: row.payload,
		}
		records = append(records, record)
		ids = append(ids, row.id)
	}

	results := client.ProduceSync(ctx, records...)
	err = results.FirstErr()
	if err != nil {
		return 0, err
	}

	_, err = tx.Exec(ctx, deleteBatchSQL, ids)
	if err != nil {
		return 0, err
	}
	err = tx.Commit(ctx)
	if err != nil {
		return 0, err
	}
	return len(batch), nil
}

func sleep(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
