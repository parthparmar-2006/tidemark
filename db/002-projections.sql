CREATE DATABASE projections;

\connect projections

CREATE TABLE account_balances (
    account_id BIGINT PRIMARY KEY,
    balance    BIGINT NOT NULL,
    version    BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
