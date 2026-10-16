package main

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/parthparmar-2006/tidemark/internal/config"
)

type accountState struct {
	balance int64
	version int64
}

func checkProjection(ctx context.Context, ledgerPool *pgxpool.Pool, r *report) error {
	projectionPool, err := pgxpool.New(ctx, config.ProjectionDatabaseURL())
	if err != nil {
		return err
	}
	defer projectionPool.Close()

	accounts, err := loadStates(ctx, ledgerPool, "SELECT id, balance, version FROM accounts")
	if err != nil {
		return err
	}
	projection, err := loadStates(ctx, projectionPool, "SELECT account_id, balance, version FROM account_balances")
	if err != nil {
		return err
	}

	r.accountsInDB = len(accounts)
	r.projectionWrong = compareProjection(accounts, projection)
	return nil
}

func loadStates(ctx context.Context, pool *pgxpool.Pool, query string) (map[int64]accountState, error) {
	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	states := make(map[int64]accountState)
	for rows.Next() {
		var id int64
		var state accountState
		err = rows.Scan(&id, &state.balance, &state.version)
		if err != nil {
			return nil, err
		}
		states[id] = state
	}
	return states, rows.Err()
}

func compareProjection(accounts map[int64]accountState, projection map[int64]accountState) int {
	wrong := 0
	for id, expected := range accounts {
		actual, exists := projection[id]
		if !exists {
			log.Printf("projection missing account %d (expected balance %d version %d)", id, expected.balance, expected.version)
			wrong++
			continue
		}
		if actual != expected {
			log.Printf("projection wrong for account %d: got balance %d version %d, expected balance %d version %d",
				id, actual.balance, actual.version, expected.balance, expected.version)
			wrong++
		}
	}
	for id := range projection {
		_, exists := accounts[id]
		if !exists {
			log.Printf("projection has account %d that does not exist in the ledger", id)
			wrong++
		}
	}
	return wrong
}
