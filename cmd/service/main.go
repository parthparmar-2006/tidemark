package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/parthparmar-2006/tidemark/internal/config"
	"github.com/parthparmar-2006/tidemark/internal/ledger"
)

type amountRequest struct {
	Amount int64 `json:"amount"`
}

type server struct {
	ledger *ledger.Ledger
}

func main() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, config.DatabaseURL())
	if err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	s := &server{ledger: ledger.New(pool)}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /accounts/{id}/deposit", s.handleDeposit)
	mux.HandleFunc("POST /accounts/{id}/withdraw", s.handleWithdraw)

	addr := config.String("HTTP_ADDR", ":8080")
	log.Printf("service listening on %s", addr)
	err = http.ListenAndServe(addr, mux)
	if err != nil {
		log.Fatal(err)
	}
}

func (s *server) handleDeposit(w http.ResponseWriter, r *http.Request) {
	accountID, amount, ok := readRequest(w, r)
	if !ok {
		return
	}
	event, err := s.ledger.Deposit(r.Context(), accountID, amount)
	writeResult(w, event, err)
}

func (s *server) handleWithdraw(w http.ResponseWriter, r *http.Request) {
	accountID, amount, ok := readRequest(w, r)
	if !ok {
		return
	}
	event, err := s.ledger.Withdraw(r.Context(), accountID, amount)
	writeResult(w, event, err)
}

func readRequest(w http.ResponseWriter, r *http.Request) (int64, int64, bool) {
	accountID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid account id", http.StatusBadRequest)
		return 0, 0, false
	}

	var body amountRequest
	err = json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return 0, 0, false
	}
	if body.Amount <= 0 {
		http.Error(w, "amount must be positive", http.StatusBadRequest)
		return 0, 0, false
	}

	return accountID, body.Amount, true
}

func writeResult(w http.ResponseWriter, event ledger.Event, err error) {
	if errors.Is(err, ledger.ErrInsufficientFunds) {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if err != nil {
		log.Printf("ledger error: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(event)
}
