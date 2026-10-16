CREATE TABLE accounts (
    id         BIGINT PRIMARY KEY,
    balance    BIGINT NOT NULL,
    version    BIGINT NOT NULL
);

CREATE TABLE account_entries (
    account_id    BIGINT NOT NULL REFERENCES accounts (id),
    version       BIGINT NOT NULL,
    amount        BIGINT NOT NULL,
    balance_after BIGINT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, version)
);

CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    aggregate_id BIGINT NOT NULL,
    event_type   TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
