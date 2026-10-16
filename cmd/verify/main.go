package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/parthparmar-2006/tidemark/internal/config"
	"github.com/parthparmar-2006/tidemark/internal/ledger"
)

type entryKey struct {
	accountID int64
	version   int64
}

type entry struct {
	amount       int64
	balanceAfter int64
}

type accountProgress struct {
	lastVersion int64
	seen        map[int64]bool
}

type report struct {
	committedEntries int
	unrelayed        int64
	recordsRead      int
	delivered        int
	duplicates       int
	phantoms         int
	mismatched       int
	outOfOrder       int
	lost             int
	accountsInDB     int
	projectionWrong  int
}

func main() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, config.DatabaseURL())
	if err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	brokers := strings.Split(config.KafkaBrokers(), ",")
	topic := config.Topic()

	var r report
	entries, err := loadEntries(ctx, pool)
	if err != nil {
		log.Fatalf("load entries: %v", err)
	}
	r.committedEntries = len(entries)

	err = pool.QueryRow(ctx, "SELECT count(*) FROM outbox").Scan(&r.unrelayed)
	if err != nil {
		log.Fatalf("count outbox: %v", err)
	}

	endOffsets, err := loadEndOffsets(ctx, brokers, topic)
	if err != nil {
		log.Fatalf("load end offsets: %v", err)
	}

	progress := make(map[int64]*accountProgress)
	err = consumeAll(ctx, brokers, topic, endOffsets, func(event ledger.Event) {
		checkEvent(event, entries, progress, &r)
	})
	if err != nil {
		log.Fatalf("consume: %v", err)
	}

	countLost(entries, progress, &r)

	err = checkProjection(ctx, pool, &r)
	if err != nil {
		log.Fatalf("check projection: %v", err)
	}

	printReport(r)
	if r.hasViolations() {
		os.Exit(1)
	}
}

func countLost(entries map[entryKey]entry, progress map[int64]*accountProgress, r *report) {
	for key := range entries {
		account := progress[key.accountID]
		if account == nil || !account.seen[key.version] {
			r.lost++
		}
	}
}

func (r report) hasViolations() bool {
	return r.lost > 0 || r.phantoms > 0 || r.mismatched > 0 || r.outOfOrder > 0 || r.projectionWrong > 0
}

func loadEntries(ctx context.Context, pool *pgxpool.Pool) (map[entryKey]entry, error) {
	rows, err := pool.Query(ctx, "SELECT account_id, version, amount, balance_after FROM account_entries")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := make(map[entryKey]entry)
	for rows.Next() {
		var key entryKey
		var value entry
		err = rows.Scan(&key.accountID, &key.version, &value.amount, &value.balanceAfter)
		if err != nil {
			return nil, err
		}
		entries[key] = value
	}
	return entries, rows.Err()
}

func loadEndOffsets(ctx context.Context, brokers []string, topic string) (map[int32]int64, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	listed, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}

	endOffsets := make(map[int32]int64)
	listed.Each(func(offset kadm.ListedOffset) {
		endOffsets[offset.Partition] = offset.Offset
	})
	return endOffsets, nil
}

func consumeAll(ctx context.Context, brokers []string, topic string, endOffsets map[int32]int64, handle func(ledger.Event)) error {
	remaining := 0
	for _, end := range endOffsets {
		if end > 0 {
			remaining++
		}
	}
	if remaining == 0 {
		return nil
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	done := make(map[int32]bool)
	for remaining > 0 {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		for _, fetchErr := range fetches.Errors() {
			return fetchErr.Err
		}
		fetches.EachRecord(func(record *kgo.Record) {
			if done[record.Partition] {
				return
			}
			var event ledger.Event
			err := json.Unmarshal(record.Value, &event)
			if err != nil {
				log.Printf("bad record at partition %d offset %d: %v", record.Partition, record.Offset, err)
			} else {
				handle(event)
			}
			if record.Offset+1 >= endOffsets[record.Partition] {
				done[record.Partition] = true
				remaining--
			}
		})
	}
	return nil
}

func checkEvent(event ledger.Event, entries map[entryKey]entry, progress map[int64]*accountProgress, r *report) {
	r.recordsRead++

	expected, exists := entries[entryKey{accountID: event.AccountID, version: event.Version}]
	if !exists {
		r.phantoms++
		return
	}
	if expected.amount != event.Amount || expected.balanceAfter != event.BalanceAfter {
		r.mismatched++
		return
	}

	account := progress[event.AccountID]
	if account == nil {
		account = &accountProgress{seen: make(map[int64]bool)}
		progress[event.AccountID] = account
	}

	if account.seen[event.Version] {
		r.duplicates++
		return
	}
	if event.Version != account.lastVersion+1 {
		r.outOfOrder++
	}

	account.seen[event.Version] = true
	if event.Version > account.lastVersion {
		account.lastVersion = event.Version
	}
	r.delivered++
}

func printReport(r report) {
	fmt.Printf("committed entries in DB : %d\n", r.committedEntries)
	fmt.Printf("still in outbox         : %d\n", r.unrelayed)
	fmt.Printf("records read from Kafka : %d\n", r.recordsRead)
	fmt.Printf("distinct delivered      : %d\n", r.delivered)
	fmt.Printf("duplicates              : %d\n", r.duplicates)
	fmt.Printf("phantoms                : %d\n", r.phantoms)
	fmt.Printf("content mismatches      : %d\n", r.mismatched)
	fmt.Printf("out of order            : %d\n", r.outOfOrder)
	fmt.Printf("lost                    : %d\n", r.lost)
	fmt.Printf("accounts in DB          : %d\n", r.accountsInDB)
	fmt.Printf("projection wrong        : %d\n", r.projectionWrong)
}
