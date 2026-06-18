# Makefile for pond-rs.
#
# Convenience wrappers around the cargo commands used in development.
# Run `make` or `make help` to list targets.
#
# Note: `--all-features` is avoided on purpose — the `tokio-console` feature
# requires RUSTFLAGS="--cfg tokio_unstable" and so has its own target (`console`).

# The feature set that builds without special RUSTFLAGS.
FEATURES := runtime-tokio,blocking,tracing

# Examples that need a non-default feature to compile (see the per-example case
# in the `example` target below).

.DEFAULT_GOAL := help

.PHONY: help test bench example examples build check clippy fmt fmt-check doc console clean all

help: ## List available targets
	@echo "pond-rs make targets:"
	@echo "  make test       Run all tests"
	@echo "  make bench       Run all benchmarks (Criterion)"
	@echo "  make example     Run all examples"
	@echo "  make build       Build the library and all targets"
	@echo "  make check       Type-check without producing binaries"
	@echo "  make clippy      Lint with clippy (warnings = errors)"
	@echo "  make fmt         Format the code"
	@echo "  make fmt-check   Check formatting without modifying files"
	@echo "  make doc         Build the API docs"
	@echo "  make console     Check the tokio-console feature (needs nightly cfg)"
	@echo "  make clean       Remove the target directory"
	@echo "  make all         fmt-check + clippy + test"

test: ## Run all tests
	cargo test --features $(FEATURES)

bench: ## Run all benchmarks
	cargo bench

# Run every example. Plain examples build on default features; a few need an
# extra feature, mapped per-example below.
example examples: ## Run all examples
	@for ex in $$(ls examples/*.rs | sed -e 's|examples/||' -e 's|\.rs$$||'); do \
		case "$$ex" in \
			tracing_debug) feat="--features tracing" ;; \
			metrics_export) feat="--features metrics-export" ;; \
			*) feat="" ;; \
		esac; \
		echo "=== running example: $$ex ==="; \
		cargo run --example $$ex $$feat || exit 1; \
	done

build: ## Build the library and all targets
	cargo build --all-targets --features $(FEATURES)

check: ## Type-check without producing binaries
	cargo check --all-targets --features $(FEATURES)

clippy: ## Lint with clippy (warnings = errors)
	cargo clippy --all-targets --features $(FEATURES) -- -D warnings

fmt: ## Format the code
	cargo fmt --all

fmt-check: ## Check formatting without modifying files
	cargo fmt --all --check

doc: ## Build the API docs
	cargo doc --no-deps --features $(FEATURES)

console: ## Check the tokio-console feature (needs the tokio_unstable cfg)
	RUSTFLAGS="--cfg tokio_unstable" cargo check --features tokio-console

clean: ## Remove the target directory
	cargo clean

all: fmt-check clippy test ## Run fmt-check, clippy, and test
