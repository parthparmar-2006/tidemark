package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/parthparmar-2006/tidemark/internal/config"
	"github.com/parthparmar-2006/tidemark/internal/ledger"
)

type counts struct {
	mu         sync.Mutex
	committed  int
	rolledBack int
	failed     int
}

func (c *counts) add(committed bool, failed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if failed {
		c.failed++
	} else if committed {
		c.committed++
	} else {
		c.rolledBack++
	}
}

func main() {
	ctx := context.Background()

	workers := config.Int("WORKERS", 4)
	accounts := config.Int("ACCOUNTS", 1000)
	rollbackPercent := config.Int("ROLLBACK_PERCENT", 50)
	maxHold := config.Duration("MAX_HOLD", 500*time.Millisecond)
	duration := config.Duration("DURATION", 30*time.Second)

	pool, err := pgxpool.New(ctx, config.DatabaseURL())
	if err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	log.Printf("rogue writer: workers=%d rollback=%d%% max_hold=%s duration=%s", workers, rollbackPercent, maxHold, duration)

	deadline := time.Now().Add(duration)
	var c counts
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			random := rand.New(rand.NewSource(seed))
			for time.Now().Before(deadline) {
				committed, err := oneTransaction(ctx, pool, random, accounts, rollbackPercent, maxHold)
				if err != nil {
					log.Printf("transaction failed: %v", err)
					time.Sleep(200 * time.Millisecond)
				}
				c.add(committed, err != nil)
			}
		}(int64(i) + time.Now().UnixNano())
	}
	wg.Wait()

	fmt.Printf("rogue committed  : %d\n", c.committed)
	fmt.Printf("rogue rolled back: %d\n", c.rolledBack)
	fmt.Printf("rogue failed     : %d\n", c.failed)
}

func oneTransaction(ctx context.Context, pool *pgxpool.Pool, random *rand.Rand, accounts int, rollbackPercent int, maxHold time.Duration) (bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	accountID := int64(random.Intn(accounts) + 1)
	amount := int64(random.Intn(100) + 1)
	_, err = ledger.WriteDeposit(ctx, tx, accountID, amount)
	if err != nil {
		return false, err
	}

	hold := time.Duration(random.Int63n(int64(maxHold)))
	time.Sleep(hold)

	if random.Intn(100) < rollbackPercent {
		return false, tx.Rollback(ctx)
	}
	return true, tx.Commit(ctx)
}
