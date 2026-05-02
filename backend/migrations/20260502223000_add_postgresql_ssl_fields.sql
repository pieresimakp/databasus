-- +goose Up
-- Add SSL fields to postgresql_databases table
ALTER TABLE postgresql_databases
    ADD COLUMN ssl_mode TEXT NOT NULL DEFAULT 'disable',
    ADD COLUMN ssl_ca   TEXT NOT NULL DEFAULT '',
    ADD COLUMN ssl_cert TEXT NOT NULL DEFAULT '',
    ADD COLUMN ssl_key  TEXT NOT NULL DEFAULT '';

-- +goose Down
-- Remove SSL fields from postgresql_databases table
ALTER TABLE postgresql_databases
    DROP COLUMN ssl_mode,
    DROP COLUMN ssl_ca,
    DROP COLUMN ssl_cert,
    DROP COLUMN ssl_key;
