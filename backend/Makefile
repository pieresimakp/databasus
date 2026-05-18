-include ../.env

run:
	go run cmd/main.go

test:
	# cleanup_test_db resets the metadata DB AND each test PG container
	# (drops leftover roles/databases and recreates testdb) so containers
	# stay usable across runs - tests that REVOKE on public or create
	# per-run roles would otherwise poison subsequent pg_dump calls.
	go run ./cmd/cleanup_test_db
	goose -dir ./migrations postgres "$(GOOSE_TEST_DBSTRING)" up
	go test -p=1 -count=1 -failfast -timeout 15m ./internal/...

lint:
	golangci-lint fmt ./cmd/... ./internal/... && golangci-lint run ./cmd/... ./internal/...

migration-create:
	goose -dir ./migrations create $(name) sql

migration-up:
	goose -dir ./migrations postgres "$(GOOSE_DBSTRING)" up

migration-up-test:
	goose -dir ./migrations postgres "$(GOOSE_TEST_DBSTRING)" up

migration-down:
	goose -dir ./migrations postgres "$(GOOSE_DBSTRING)" down

swagger:
	swag init -g ./cmd/main.go -o swagger
