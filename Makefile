.PHONY: lint
lint: ## Lint source code
	@echo "Linting source code..."
	@go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.5.0
	@golangci-lint run

.PHONY: test
test:
	@go test -coverprofile=coverage -timeout 10m -race -cover -failfast ./...

.PHONY: integration-test
integration-test:
	@PGSTREAM_INTEGRATION_TESTS=true go test -timeout 180s github.com/xataio/pgstream/pkg/stream/integration

.PHONY: fuzz
fuzz:
	@echo "Fuzzing YAML to Stream Config"
	@go test -fuzz=FuzzYAMLToStreamConfig --fuzztime 30s github.com/xataio/pgstream/cmd/config
	@echo "Fuzzing YAML Config Structure"
	@go test -fuzz=FuzzYAMLConfigStructure --fuzztime 30s github.com/xataio/pgstream/cmd/config
	@echo "Fuzzing YAML Config Properties"
	@go test -fuzz=FuzzYAMLConfigProperties --fuzztime 30s github.com/xataio/pgstream/cmd/config

.PHONY: license-check
license-check:
	@curl -s https://raw.githubusercontent.com/lluissm/license-header-checker/master/install.sh | bash
	@./bin/license-header-checker -a -r ./license-header.txt . go

.PHONY: gen-migrations
gen-migrations:
	@go install github.com/go-bindata/go-bindata/v3/go-bindata@latest
	@go-bindata -o migrations/postgres/core/migrations.go -pkg pgcoremigrations -ignore migrations.go -prefix "migrations/postgres/core" migrations/postgres/core/
	@go-bindata -o migrations/postgres/injector/migrations.go -pkg pginjectormigrations -ignore migrations.go -prefix "migrations/postgres/injector" migrations/postgres/injector/
.PHONY: generate
generate:
	# Generate the cli-definition.json file
	go run tools/build-cli-definition.go
	go run tools/transformer-definition/build-transformers-definition.go

GIT_COMMIT := $(shell git rev-parse --short HEAD)
.PHONY: build
build:
	@go build -ldflags "-X github.com/xataio/pgstream/cmd.Env=development -X github.com/xataio/pgstream/cmd.Version=$(GIT_COMMIT)" .

.PHONY: build-linux-amd64
build-linux-amd64:
	@GOOS=linux GOARCH=amd64 go build -ldflags "-X github.com/xataio/pgstream/cmd.Env=development -X github.com/xataio/pgstream/cmd.Version=$(GIT_COMMIT)" .

.PHONY: build-linux-arm64
build-linux-arm64:
	@GOOS=linux GOARCH=arm64 go build -ldflags "-X github.com/xataio/pgstream/cmd.Env=development -X github.com/xataio/pgstream/cmd.Version=$(GIT_COMMIT)" .
