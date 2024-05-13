.PHONY: lint
lint: ## Lint source code
	@echo "Linting source code..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@golangci-lint run

.PHONY: test
test:
	@go test -timeout 1m -race -failfast -v ./...
