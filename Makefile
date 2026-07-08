.PHONY: build debug release release-thin test test-asan test-tsan test-stack fmt clippy clean help

# 默认目标
.DEFAULT_GOAL := help

# 可覆盖的变量
CARGO ?= cargo

# 构建调试版本
build:
	@echo "Building debug version..."
	@$(CARGO) build --features deadlock_detection

debug: build

release:
	@echo "Building release binary (standard profile)..."
	@$(CARGO) build --release

release-thin:
	@echo "Building minimal binary via release-thin profile..."
	@$(CARGO) build --profile release-thin --no-default-features

# 运行测试
test:
	@echo "Running tests..."
	@$(CARGO) test --features deadlock_detection
	@echo "Running tests for sub-crates..."
	@cd src/datatypes && $(CARGO) test --features deadlock_detection
	@cd src/flow && $(CARGO) test --features deadlock_detection
	@cd src/manager && $(CARGO) test --features deadlock_detection
	@cd src/parser && $(CARGO) test --features deadlock_detection
	@echo "Building veloflux binary for integration tests..."
	@$(CARGO) build --bin veloflux --features deadlock_detection
	@echo "Running distros/sdv tests..."
	@cd distros/sdv && $(CARGO) test

# --- AC-3: Memory and Concurrency Safety ---

# AddressSanitizer (requires nightly)
# Usage: make test-asan [TARGET=x86_64-unknown-linux-gnu]
test-asan:
	@echo "Running workspace-wide tests with AddressSanitizer (excluding benches)..."
	@RUSTFLAGS="-Zsanitizer=address -Clink-arg=-fuse-ld=lld" $(CARGO) test -Zbuild-std --workspace --lib --tests $(if $(TARGET),--target $(TARGET)) --no-default-features --features allocator-system,metrics -- --test-threads=1

# Data Race Detection (requires nightly)
# Usage: make test-tsan [TARGET=x86_64-unknown-linux-gnu]
test-tsan:
	@echo "Running workspace-wide tests with ThreadSanitizer (excluding benches)..."
	@TSAN_OPTIONS="suppressions=$(CURDIR)/scripts/tsan_suppressions.txt" RUSTFLAGS="-Zsanitizer=thread -Clink-arg=-fuse-ld=lld" $(CARGO) test -Zbuild-std --workspace --lib --tests $(if $(TARGET),--target $(TARGET)) --no-default-features --features allocator-system,metrics -- --test-threads=1

# Stack Overflow Detection
# Runs tests with a restricted stack size to expose unbounded recursion
test-stack:
	@echo "Running tests with restricted stack size (1MB)..."
	@ulimit -s 1024 && $(CARGO) test --workspace --lib --tests

bump-version:
	@if [ -z "$(VERSION)" ]; then echo "Usage: make bump-version VERSION=x.y.z"; exit 1; fi
	@./scripts/bump_version.sh $(VERSION)

# 格式化代码
fmt:
	@echo "Formatting code..."
	@$(CARGO) fmt
	@cd distros/sdv && $(CARGO) fmt

# 运行 Clippy 静态检查
clippy:
	@echo "Running Clippy checks..."
	@$(CARGO) clippy --features deadlock_detection -- -D warnings -D clippy::await_holding_lock
	@echo "Running Clippy checks for sub-crates..."
	@cd src/datatypes && $(CARGO) clippy --features deadlock_detection -- -D warnings -D clippy::await_holding_lock
	@cd src/flow && $(CARGO) clippy --features deadlock_detection -- -D warnings -D clippy::await_holding_lock
	@cd src/parser && $(CARGO) clippy --features deadlock_detection -- -D warnings -D clippy::await_holding_lock
	@echo "Running Clippy for distros/sdv..."
	@cd distros/sdv && $(CARGO) clippy -- -D warnings -D clippy::await_holding_lock

# 清理构建文件
clean:
	@echo "Cleaning build files..."
	@$(CARGO) clean

# 显示帮助信息
help:
	@echo "Available targets:"
	@echo "  build   - Build debug version"
	@echo "  debug   - Alias for build"
	@echo "  release - Build release version"
	@echo "  test    - Run tests"
	@echo "  test-asan - Run tests with AddressSanitizer (leaks)"
	@echo "  test-tsan - Run tests with ThreadSanitizer (races)"
	@echo "  test-stack - Run tests with restricted stack"
	@echo "  fmt     - Format code"
	@echo "  clippy  - Run Clippy static analysis"
	@echo "  clean   - Clean build files"
	@echo "  help    - Show this help message"
