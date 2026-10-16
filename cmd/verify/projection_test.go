package main

import "testing"

func ledgerAccounts() map[int64]accountState {
	return map[int64]accountState{
		1: {balance: 120, version: 3},
		2: {balance: 10, version: 1},
	}
}

func TestMatchingProjectionIsCorrect(t *testing.T) {
	projection := map[int64]accountState{
		1: {balance: 120, version: 3},
		2: {balance: 10, version: 1},
	}
	wrong := compareProjection(ledgerAccounts(), projection)
	if wrong != 0 {
		t.Fatalf("expected 0 wrong, got %d", wrong)
	}
}

func TestDuplicateAppliedTwiceIsWrong(t *testing.T) {
	projection := map[int64]accountState{
		1: {balance: 170, version: 3},
		2: {balance: 10, version: 1},
	}
	wrong := compareProjection(ledgerAccounts(), projection)
	if wrong != 1 {
		t.Fatalf("expected 1 wrong, got %d", wrong)
	}
}

func TestMissedEventIsWrong(t *testing.T) {
	projection := map[int64]accountState{
		1: {balance: 70, version: 2},
		2: {balance: 10, version: 1},
	}
	wrong := compareProjection(ledgerAccounts(), projection)
	if wrong != 1 {
		t.Fatalf("expected 1 wrong, got %d", wrong)
	}
}

func TestMissingAndExtraAccountsAreWrong(t *testing.T) {
	projection := map[int64]accountState{
		1: {balance: 120, version: 3},
		3: {balance: 5, version: 1},
	}
	wrong := compareProjection(ledgerAccounts(), projection)
	if wrong != 2 {
		t.Fatalf("expected 2 wrong (account 2 missing, account 3 extra), got %d", wrong)
	}
}
