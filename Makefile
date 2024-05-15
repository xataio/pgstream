.PHONY: lint
lint: ## Lint source code
	@echo "Linting source code..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@golangci-lint run

.PHONY: test
test:
	@go test -timeout 10m -race -cover -failfast ./...

.PHONY: license-check
license-check:
	@curl -s https://raw.githubusercontent.com/lluissm/license-header-checker/master/install.sh | bash 
	@./bin/license-header-checker -a -r .github/license-header.txt . go && [[ -z `git status -s` ]]
