.PHONY: lint
lint: ## Lint source code
	@echo "Linting source code..."
	@go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.0.2
	@golangci-lint run

.PHONY: test
test:
	@go test -coverprofile=coverage -timeout 10m -race -cover -failfast ./...

.PHONY: integration-test
integration-test:
	@PGSTREAM_INTEGRATION_TESTS=true go test -timeout 180s github.com/xataio/pgstream/pkg/stream/integration

.PHONY: license-check
license-check:
	@curl -s https://raw.githubusercontent.com/lluissm/license-header-checker/master/install.sh | bash
	@./bin/license-header-checker -a -r ./license-header.txt . go

.PHONY: gen-migrations
gen-migrations:
	@go install github.com/go-bindata/go-bindata/...
	@go-bindata -o migrations/postgres/migrations.go -pkg pgmigrations -ignore migrations.go -prefix "migrations/postgres/" migrations/postgres/

.PHONY: generate
generate:
	# Generate the cli-definition.json file
	go run tools/build-cli-definition.go
	go run tools/transformer-definition/build-transformers-definition.go

GIT_COMMIT := $(shell git rev-parse --short HEAD)
.PHONY: build
build:
	@go build -ldflags "-X github.com/xataio/pgstream/cmd.Env=development -X github.com/xataio/pgstream/cmd.Version=$(GIT_COMMIT)" .
