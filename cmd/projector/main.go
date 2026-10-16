package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/parthparmar-2006/tidemark/internal/config"
	"github.com/parthparmar-2006/tidemark/internal/ledger"
)

const insertFirstSQL = `
INSERT INTO account_balances (account_id, balance, version)
VALUES ($1, $2, 1)
ON CONFLICT (account_id) DO NOTHING`

const applyNextSQL = `
UPDATE account_balances
SET balance = balance + $2,
    version = $3,
    updated_at = now()
WHERE account_id = $1 AND version = $3 - 1`

const currentVersionSQL = `SELECT version FROM account_balances WHERE account_id = $1`

var errGap = errors.New("version gap")

type stats struct {
	applied    int
	duplicates int
	latencies  []time.Duration
	batches    int
	applyTime  time.Duration
	lastReport time.Time
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	pool, err := pgxpool.New(ctx, config.ProjectionDatabaseURL())
	if err != nil {
		log.Fatalf("connect to projection database: %v", err)
	}
	defer pool.Close()

	group := config.String("GROUP", "balance-projector")
	instance := config.String("INSTANCE_ID", "balance-projector-1")
	brokers := strings.Split(config.KafkaBrokers(), ",")
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.InstanceID(instance),
		kgo.ConsumeTopics(config.Topic()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatalf("create kafka client: %v", err)
	}
	defer client.Close()

	retryDelay := config.Duration("RETRY_DELAY", time.Second)
	reportEvery := config.Duration("REPORT_EVERY", 10*time.Second)
	applyMode := config.String("APPLY_MODE", "pipelined")
	s := &stats{lastReport: time.Now()}

	log.Printf("projector started: group=%s instance=%s apply_mode=%s", group, instance, applyMode)
	for ctx.Err() == nil {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("fetch error on %s/%d: %v", topic, partition, err)
		})

		events := decodeEvents(fetches.Records())
		if len(events) > 0 {
			applyUntilDone(ctx, pool, events, applyMode, s, retryDelay)
			err = client.CommitUncommittedOffsets(ctx)
			if err != nil {
				log.Printf("commit offsets failed: %v", err)
			}
		}

		if time.Since(s.lastReport) >= reportEvery {
			s.report()
		}
	}
	s.report()
	log.Printf("projector stopped")
}

func decodeEvents(records []*kgo.Record) []ledger.Event {
	events := make([]ledger.Event, 0, len(records))
	for _, record := range records {
		var event ledger.Event
		err := json.Unmarshal(record.Value, &event)
		if err != nil {
			log.Fatalf("undecodable record at partition %d offset %d: %v", record.Partition, record.Offset, err)
		}
		events = append(events, event)
	}
	return events
}

func applyUntilDone(ctx context.Context, pool *pgxpool.Pool, events []ledger.Event, applyMode string, s *stats, retryDelay time.Duration) {
	for ctx.Err() == nil {
		started := time.Now()
		applied, duplicates, err := applyBatch(ctx, pool, events, applyMode)
		if errors.Is(err, errGap) {
			log.Fatalf("stopping: %v", err)
		}
		if err == nil {
			s.record(events, applied, duplicates, time.Since(started))
			return
		}
		log.Printf("apply batch failed, retrying: %v", err)
		sleep(ctx, retryDelay)
	}
}

func applyBatch(ctx context.Context, pool *pgxpool.Pool, events []ledger.Event, applyMode string) (int, int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback(ctx)

	var applied, duplicates int
	if applyMode == "sequential" {
		applied, duplicates, err = applySequential(ctx, tx, events)
	} else {
		applied, duplicates, err = applyPipelined(ctx, tx, events)
	}
	if err != nil {
		return 0, 0, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return 0, 0, err
	}
	return applied, duplicates, nil
}

func applySequential(ctx context.Context, tx pgx.Tx, events []ledger.Event) (int, int, error) {
	applied := 0
	duplicates := 0
	for _, event := range events {
		sql, args := statementFor(event)
		result, err := tx.Exec(ctx, sql, args...)
		if err != nil {
			return 0, 0, err
		}
		if result.RowsAffected() == 1 {
			applied++
			continue
		}
		err = classifyNotApplied(ctx, tx, event)
		if err != nil {
			return 0, 0, err
		}
		duplicates++
	}
	return applied, duplicates, nil
}

func applyPipelined(ctx context.Context, tx pgx.Tx, events []ledger.Event) (int, int, error) {
	batch := &pgx.Batch{}
	for _, event := range events {
		sql, args := statementFor(event)
		batch.Queue(sql, args...)
	}

	results := tx.SendBatch(ctx, batch)
	applied := 0
	var notApplied []ledger.Event
	for _, event := range events {
		result, err := results.Exec()
		if err != nil {
			results.Close()
			return 0, 0, err
		}
		if result.RowsAffected() == 1 {
			applied++
		} else {
			notApplied = append(notApplied, event)
		}
	}
	err := results.Close()
	if err != nil {
		return 0, 0, err
	}

	for _, event := range notApplied {
		err = classifyNotApplied(ctx, tx, event)
		if err != nil {
			return 0, 0, err
		}
	}
	return applied, len(notApplied), nil
}

func statementFor(event ledger.Event) (string, []any) {
	if event.Version == 1 {
		return insertFirstSQL, []any{event.AccountID, event.Amount}
	}
	return applyNextSQL, []any{event.AccountID, event.Amount, event.Version}
}

func classifyNotApplied(ctx context.Context, tx pgx.Tx, event ledger.Event) error {
	var current int64
	err := tx.QueryRow(ctx, currentVersionSQL, event.AccountID).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		current = 0
	} else if err != nil {
		return err
	}

	if event.Version <= current {
		return nil
	}
	return fmt.Errorf("%w: account %d has version %d, received %d", errGap, event.AccountID, current, event.Version)
}

func (s *stats) record(events []ledger.Event, applied int, duplicates int, applyTime time.Duration) {
	s.applied += applied
	s.duplicates += duplicates
	s.batches++
	s.applyTime += applyTime
	now := time.Now()
	for _, event := range events {
		s.latencies = append(s.latencies, now.Sub(event.OccurredAt))
	}
}

func (s *stats) report() {
	sort.Slice(s.latencies, func(i, j int) bool { return s.latencies[i] < s.latencies[j] })
	busy := 0.0
	elapsed := time.Since(s.lastReport)
	if elapsed > 0 {
		busy = 100 * s.applyTime.Seconds() / elapsed.Seconds()
	}
	averageBatch := 0
	if s.batches > 0 {
		averageBatch = len(s.latencies) / s.batches
	}
	log.Printf("applied=%d duplicates_skipped=%d window_events=%d batches=%d avg_batch=%d apply_busy=%.0f%% e2e_p50=%s e2e_p99=%s e2e_max=%s",
		s.applied, s.duplicates, len(s.latencies), s.batches, averageBatch, busy,
		percentile(s.latencies, 0.50), percentile(s.latencies, 0.99), percentile(s.latencies, 1.0))
	s.latencies = s.latencies[:0]
	s.batches = 0
	s.applyTime = 0
	s.lastReport = time.Now()
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted)-1) * p)
	return sorted[index].Round(time.Millisecond)
}

func sleep(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
