# Makefile for pond-rs.
#
# Convenience wrappers around the cargo commands used in development.
# Run `make` or `make help` to list targets.
#
# Note: `--all-features` is avoided on purpose — the `tokio-console` feature
# requires RUSTFLAGS="--cfg tokio_unstable" and so has its own target (`console`).

# The feature set that builds without special RUSTFLAGS.
FEATURES := runtime-tokio,blocking,tracing

# Every safe feature (all runtimes + monitoring). Excludes only tokio-console,
# which needs the tokio_unstable cfg (see the `console` target).
ALL_FEATURES := runtime-tokio,runtime-smol,runtime-async-std,blocking,tracing,metrics-export

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

# Examples that need a non-default feature to compile (see the per-example case
# in the `example` target below).

.DEFAULT_GOAL := help

.PHONY: help test test-all bench example examples build check clippy fmt fmt-check doc console \
	report coverage flamegraph memprofile tracy metrics-dump clean all

help: ## List available targets
	@echo "pond-rs make targets:"
	@echo "  make test         Run tests on the safe feature set"
	@echo "  make test-all     Run tests + doctests on ALL features (every runtime + monitoring)"
	@echo "  make bench         Run all benchmarks (Criterion)"
	@echo "  make report        Open the Criterion benchmark report in your browser"
	@echo "  make coverage      Generate + open an HTML code-coverage report (cargo-llvm-cov)"
	@echo "  make flamegraph    CPU flamegraph of the benches (Windows: run as Administrator)"
	@echo "  make memprofile    Heap memory profile -> JSON for the dh_view web viewer"
	@echo "  make tracy         Live memory + task spans in the Tracy GUI (rich UI)"
	@echo "  make metrics-dump  Run a workload and write Prometheus metrics to target/metrics.prom"
	@echo "  make example       Run all examples"
	@echo "  make build         Build the library and all targets"
	@echo "  make check         Type-check without producing binaries"
	@echo "  make clippy        Lint with clippy (warnings = errors)"
	@echo "  make fmt           Format the code"
	@echo "  make fmt-check     Check formatting without modifying files"
	@echo "  make doc           Build the API docs"
	@echo "  make console       Check the tokio-console feature (needs nightly cfg)"
	@echo "  make clean         Remove the target directory"
	@echo "  make all           fmt-check + clippy + test-all"

test: ## Run tests on the safe feature set
	cargo test --features $(FEATURES)

test-all: ## Run tests + doctests on every safe feature (all runtimes + monitoring)
	cargo test --features $(ALL_FEATURES)
	cargo test --doc --features $(ALL_FEATURES)

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
	cargo llvm-cov --features $(ALL_FEATURES) --html
	@echo "Coverage report: target/llvm-cov/html/index.html"
	@$(OPEN) "target/llvm-cov/html/index.html" 2>/dev/null || true

flamegraph: ## CPU flamegraph of the benches (needs cargo-flamegraph; on Windows run as Administrator)
	CARGO_PROFILE_BENCH_DEBUG=true cargo flamegraph --bench pool_bench --features $(FEATURES) -o target/flamegraph.svg -- --bench
	@echo "Flamegraph: target/flamegraph.svg"
	@$(OPEN) "target/flamegraph.svg" 2>/dev/null || true

memprofile: ## Heap memory profile -> target/dhat-heap.json (view at the dh_view web app)
	cargo run --release --example mem_profile --features dhat-heap
	@echo "Heap profile: target/dhat-heap.json"
	@echo "View it at:   https://nnethercote.github.io/dh_view/dh_view.html"

tracy: ## Live memory + spans in the Tracy GUI (download Tracy.exe, click Connect, then run this)
	@echo "Launch the Tracy GUI (https://github.com/wolfpld/tracy/releases) and click Connect."
	cargo run --release --example tracy_profile --features tracy

metrics-dump: ## Write the monitoring (Prometheus) output to target/metrics.prom
	@cargo run --quiet --example metrics_export --features metrics-export > target/metrics.prom
	@echo "wrote target/metrics.prom:"
	@cat target/metrics.prom

# Run every example. Plain examples build on default features; a few need an
# extra feature, mapped per-example below.
example examples: ## Run all examples
	@for ex in $$(ls examples/*.rs | sed -e 's|examples/||' -e 's|\.rs$$||'); do \
		case "$$ex" in \
			tracing_debug) feat="--features tracing" ;; \
			metrics_export) feat="--features metrics-export" ;; \
			smol_runtime) feat="--features runtime-smol" ;; \
			mem_profile) feat="--features dhat-heap" ;; \
			tracy_profile) feat="--features tracy" ;; \
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

all: fmt-check clippy test-all ## Run fmt-check, clippy, and the full-feature test suite
