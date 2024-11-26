.PHONY: lint
lint: ## Lint source code
	@echo "Linting source code..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.60.1
	@golangci-lint run

.PHONY: test
test:
	@go test -coverprofile=coverage -timeout 10m -race -cover -failfast ./...

.PHONY: integration-test
integration-test:
	@PGSTREAM_INTEGRATION_TESTS=true go test -timeout 90s github.com/xataio/pgstream/pkg/stream/integration

.PHONY: license-check
license-check:
	@curl -s https://raw.githubusercontent.com/lluissm/license-header-checker/master/install.sh | bash
	@./bin/license-header-checker -a -r ./license-header.txt . go

.PHONY: gen-migrations
gen-migrations:
	@go install github.com/go-bindata/go-bindata/...
	@go-bindata -o migrations/postgres/migrations.go -pkg pgmigrations -ignore migrations.go -prefix "migrations/postgres/" migrations/postgres/
