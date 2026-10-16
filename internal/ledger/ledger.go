package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrInsufficientFunds = errors.New("insufficient funds")

type Event struct {
	AccountID    int64     `json:"account_id"`
	Version      int64     `json:"version"`
	Type         string    `json:"type"`
	Amount       int64     `json:"amount"`
	BalanceAfter int64     `json:"balance_after"`
	OccurredAt   time.Time `json:"occurred_at"`
}

const depositSQL = `
INSERT INTO accounts (id, balance, version)
VALUES ($1, $2, 1)
ON CONFLICT (id) DO UPDATE
SET balance = accounts.balance + EXCLUDED.balance,
    version = accounts.version + 1
RETURNING balance, version`

const withdrawSQL = `
UPDATE accounts
SET balance = balance - $2,
    version = version + 1
WHERE id = $1 AND balance >= $2
RETURNING balance, version`

const insertEntrySQL = `
INSERT INTO account_entries (account_id, version, amount, balance_after)
VALUES ($1, $2, $3, $4)`

const insertOutboxSQL = `
INSERT INTO outbox (aggregate_id, event_type, payload)
VALUES ($1, $2, $3)`

type Ledger struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Ledger {
	return &Ledger{pool: pool}
}

func (l *Ledger) Deposit(ctx context.Context, accountID int64, amount int64) (Event, error) {
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return Event{}, err
	}
	defer tx.Rollback(ctx)

	event, err := WriteDeposit(ctx, tx, accountID, amount)
	if err != nil {
		return Event{}, err
	}
	err = tx.Commit(ctx)
	if err != nil {
		return Event{}, err
	}
	return event, nil
}

func (l *Ledger) Withdraw(ctx context.Context, accountID int64, amount int64) (Event, error) {
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return Event{}, err
	}
	defer tx.Rollback(ctx)

	event, err := WriteWithdraw(ctx, tx, accountID, amount)
	if err != nil {
		return Event{}, err
	}
	err = tx.Commit(ctx)
	if err != nil {
		return Event{}, err
	}
	return event, nil
}

func WriteDeposit(ctx context.Context, tx pgx.Tx, accountID int64, amount int64) (Event, error) {
	var balance, version int64
	err := tx.QueryRow(ctx, depositSQL, accountID, amount).Scan(&balance, &version)
	if err != nil {
		return Event{}, err
	}

	event := newEvent(accountID, version, "deposited", amount, balance)
	err = recordEvent(ctx, tx, event)
	if err != nil {
		return Event{}, err
	}
	return event, nil
}

func WriteWithdraw(ctx context.Context, tx pgx.Tx, accountID int64, amount int64) (Event, error) {
	var balance, version int64
	err := tx.QueryRow(ctx, withdrawSQL, accountID, amount).Scan(&balance, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return Event{}, ErrInsufficientFunds
	}
	if err != nil {
		return Event{}, err
	}

	event := newEvent(accountID, version, "withdrawn", -amount, balance)
	err = recordEvent(ctx, tx, event)
	if err != nil {
		return Event{}, err
	}
	return event, nil
}

func newEvent(accountID int64, version int64, eventType string, amount int64, balance int64) Event {
	return Event{
		AccountID:    accountID,
		Version:      version,
		Type:         eventType,
		Amount:       amount,
		BalanceAfter: balance,
		OccurredAt:   time.Now().UTC(),
	}
}

func recordEvent(ctx context.Context, tx pgx.Tx, event Event) error {
	_, err := tx.Exec(ctx, insertEntrySQL, event.AccountID, event.Version, event.Amount, event.BalanceAfter)
	if err != nil {
		return err
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, insertOutboxSQL, event.AccountID, event.Type, string(payload))
	return err
}
