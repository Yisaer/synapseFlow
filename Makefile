.PHONY: build release release-thin test fmt clippy clean help

# 默认目标
.DEFAULT_GOAL := help

# 构建调试版本
build:
	@echo "Building debug version..."
	@cargo build

release:
	@echo "Building release binary (standard profile)..."
	@cargo build --release

release-thin:
	@echo "Building minimal binary via release-thin profile..."
	@cargo build --profile release-thin --no-default-features

# 运行测试
test:
	@echo "Running tests..."
	@cargo test
	@echo "Running tests for sub-crates..."
	@cd src/datatypes && cargo test
	@cd src/flow && cargo test
	@cd src/parser && cargo test

# 格式化代码
fmt:
	@echo "Formatting code..."
	@cargo fmt

# 运行 Clippy 静态检查
clippy:
	@echo "Running Clippy checks..."
	@cargo clippy -- -D warnings
	@echo "Running Clippy checks for sub-crates..."
	@cd src/datatypes && cargo clippy -- -D warnings
	@cd src/flow && cargo clippy -- -D warnings
	@cd src/parser && cargo clippy -- -D warnings

# 清理构建文件
clean:
	@echo "Cleaning build files..."
	@cargo clean

# 显示帮助信息
help:
	@echo "Available targets:"
	@echo "  build   - Build debug version"
	@echo "  release - Build release version"
	@echo "  test    - Run tests"
	@echo "  fmt     - Format code"
	@echo "  clippy  - Run Clippy static analysis"
	@echo "  clean   - Clean build files"
	@echo "  help    - Show this help message"
