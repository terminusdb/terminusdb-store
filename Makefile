.PHONY: help build test test-unit test-integration clippy check fmt check-fmt package clean pr

help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

build: ## Build the library
	cargo build

test: ## Run the full test suite
	cargo test

test-unit: ## Run unit tests (all tests currently live in-module)
	cargo test --lib

test-integration: ## Run integration-level tests (pack import/export suites)
	cargo test --lib storage::pack

clippy: ## Run clippy on lib and tests with all features (benches need nightly)
	cargo clippy --lib --tests --all-features

check: ## Run cargo check
	cargo check

fmt: ## Format all Rust code
	cargo fmt

check-fmt: ## Check formatting without changing files
	cargo fmt --check

package: ## Package the crate, like CI
	cargo package --allow-dirty --no-verify

clean: ## Remove build artifacts
	cargo clean

pr: check-fmt clippy build test ## Run pre-PR checks (fmt, clippy, build, tests)
