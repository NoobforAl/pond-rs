# Makefile for pond-rs.
#
# Convenience wrappers around the cargo commands used in development.
# Run `make` or `make help` to list targets.

# The feature set that builds without special RUSTFLAGS.
FEATURES := runtime-tokio,blocking

# Open a file in the default app/browser. On Windows, `make` runs under Git Bash
# which has no bare `start`, so go through cmd.exe (NOT wsl). Each use is guarded
# with `|| true`, and the path is always printed, so a failed open never breaks
# the build.
ifeq ($(OS),Windows_NT)
  OPEN := cmd //c start ""
else ifeq ($(shell uname -s),Darwin)
  OPEN := open
else
  OPEN := xdg-open
endif

.DEFAULT_GOAL := help

.PHONY: help test test-all bench example examples build check clippy fmt fmt-check doc \
	report coverage flamegraph memprofile clean all

help: ## List available targets
	@echo "pond-rs make targets:"
	@echo "  make test         Run tests on the default feature set"
	@echo "  make test-all     Run tests + doctests on ALL features (every runtime)"
	@echo "  make bench         Run all benchmarks (Criterion)"
	@echo "  make report        Open the Criterion benchmark report in your browser"
	@echo "  make coverage      Generate + open an HTML code-coverage report (cargo-llvm-cov)"
	@echo "  make flamegraph    CPU flamegraph of the benches (Windows: run as Administrator)"
	@echo "  make memprofile    dhat heap profile of a submit/await workload (no admin needed)"
	@echo "  make example       Run all examples"
	@echo "  make build         Build the library and all targets"
	@echo "  make check         Type-check without producing binaries"
	@echo "  make clippy        Lint with clippy (warnings = errors)"
	@echo "  make fmt           Format the code"
	@echo "  make fmt-check     Check formatting without modifying files"
	@echo "  make doc           Build the API docs"
	@echo "  make clean         Remove the target directory"
	@echo "  make all           fmt-check + clippy + test-all"

test: ## Run tests on the default feature set
	cargo test --features $(FEATURES)

test-all: ## Run tests + doctests on every feature (all runtimes)
	cargo test --all-features
	cargo test --doc --all-features

bench: ## Run all benchmarks
	cargo bench

# --- Profiling / reports (saved to files and opened in the browser) ---

report: ## Open the Criterion benchmark report (run `make bench` first)
	@if [ -f target/criterion/report/index.html ]; then \
		echo "Criterion report: target/criterion/report/index.html"; \
		$(OPEN) "target/criterion/report/index.html" 2>/dev/null || true; \
	else \
		echo "No report yet — run 'make bench' first."; exit 1; \
	fi

coverage: ## Generate + open an HTML coverage report (needs: cargo install cargo-llvm-cov)
	cargo llvm-cov --all-features --html
	@echo "Coverage report: target/llvm-cov/html/index.html"
	@$(OPEN) "target/llvm-cov/html/index.html" 2>/dev/null || true

flamegraph: ## CPU flamegraph of the benches (needs cargo-flamegraph; on Windows run as Administrator)
	CARGO_PROFILE_BENCH_DEBUG=true cargo flamegraph --bench pool_bench --features $(FEATURES) -o target/flamegraph.svg -- --bench
	@echo "Flamegraph: target/flamegraph.svg"
	@$(OPEN) "target/flamegraph.svg" 2>/dev/null || true

memprofile: ## dhat heap profile -> target/dhat-heap.json (view in dh_view.html)
	cargo test --release --test heap_profile --features $(FEATURES) -- --ignored --nocapture
	@echo "Heap profile: target/dhat-heap.json (view at https://nnethercote.github.io/dh_view/dh_view.html)"

# Run every example. Plain examples build on default features; a few need an
# extra feature, mapped per-example below.
example examples: ## Run all examples
	@for ex in $$(ls examples/*.rs | sed -e 's|examples/||' -e 's|\.rs$$||'); do \
		case "$$ex" in \
			smol_runtime) feat="--features runtime-smol" ;; \
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

clean: ## Remove the target directory
	cargo clean

all: fmt-check clippy test-all ## Run fmt-check, clippy, and the full-feature test suite
