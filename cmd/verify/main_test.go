package main

import (
	"testing"

	"github.com/parthparmar-2006/tidemark/internal/ledger"
)

func committedEntries() map[entryKey]entry {
	return map[entryKey]entry{
		{accountID: 1, version: 1}: {amount: 100, balanceAfter: 100},
		{accountID: 1, version: 2}: {amount: -30, balanceAfter: 70},
		{accountID: 1, version: 3}: {amount: 50, balanceAfter: 120},
		{accountID: 2, version: 1}: {amount: 10, balanceAfter: 10},
	}
}

func event(accountID int64, version int64, amount int64, balanceAfter int64) ledger.Event {
	return ledger.Event{AccountID: accountID, Version: version, Amount: amount, BalanceAfter: balanceAfter}
}

func runChecks(events []ledger.Event) report {
	entries := committedEntries()
	progress := make(map[int64]*accountProgress)
	var r report
	for _, e := range events {
		checkEvent(e, entries, progress, &r)
	}
	countLost(entries, progress, &r)
	return r
}

func allCorrectEvents() []ledger.Event {
	return []ledger.Event{
		event(1, 1, 100, 100),
		event(2, 1, 10, 10),
		event(1, 2, -30, 70),
		event(1, 3, 50, 120),
	}
}

func TestCleanStreamHasNoViolations(t *testing.T) {
	r := runChecks(allCorrectEvents())
	if r.hasViolations() {
		t.Fatalf("expected no violations, got %+v", r)
	}
	if r.delivered != 4 {
		t.Fatalf("expected 4 delivered, got %d", r.delivered)
	}
}

func TestMissingEventIsLost(t *testing.T) {
	events := []ledger.Event{
		event(1, 1, 100, 100),
		event(1, 2, -30, 70),
		event(1, 3, 50, 120),
	}
	r := runChecks(events)
	if r.lost != 1 {
		t.Fatalf("expected 1 lost, got %+v", r)
	}
}

func TestEventForUncommittedVersionIsPhantom(t *testing.T) {
	events := append(allCorrectEvents(), event(1, 4, 999, 1119))
	r := runChecks(events)
	if r.phantoms != 1 {
		t.Fatalf("expected 1 phantom, got %+v", r)
	}
}

func TestRolledBackEventReusingVersionIsMismatch(t *testing.T) {
	events := []ledger.Event{
		event(1, 1, 100, 100),
		event(2, 1, 10, 10),
		event(1, 2, -80, 20),
		event(1, 2, -30, 70),
		event(1, 3, 50, 120),
	}
	r := runChecks(events)
	if r.mismatched != 1 {
		t.Fatalf("expected 1 mismatch, got %+v", r)
	}
	if !r.hasViolations() {
		t.Fatalf("a rolled-back event must count as a violation")
	}
}

func TestRepeatedEventIsDuplicateNotViolation(t *testing.T) {
	events := append(allCorrectEvents(), event(1, 2, -30, 70))
	r := runChecks(events)
	if r.duplicates != 1 {
		t.Fatalf("expected 1 duplicate, got %+v", r)
	}
	if r.hasViolations() {
		t.Fatalf("duplicates alone must not be a violation, got %+v", r)
	}
}

func TestSwappedVersionsAreOutOfOrder(t *testing.T) {
	events := []ledger.Event{
		event(1, 1, 100, 100),
		event(1, 3, 50, 120),
		event(1, 2, -30, 70),
		event(2, 1, 10, 10),
	}
	r := runChecks(events)
	if r.outOfOrder == 0 {
		t.Fatalf("expected out of order, got %+v", r)
	}
	if r.lost != 0 {
		t.Fatalf("late event still arrived, expected 0 lost, got %+v", r)
	}
}

func TestReplayedBatchAfterCrashIsOnlyDuplicates(t *testing.T) {
	events := []ledger.Event{
		event(1, 1, 100, 100),
		event(1, 2, -30, 70),
		event(1, 1, 100, 100),
		event(1, 2, -30, 70),
		event(1, 3, 50, 120),
		event(2, 1, 10, 10),
	}
	r := runChecks(events)
	if r.duplicates != 2 {
		t.Fatalf("expected 2 duplicates, got %+v", r)
	}
	if r.hasViolations() {
		t.Fatalf("a replayed prefix must not be a violation, got %+v", r)
	}
}
