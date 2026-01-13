# Contributing to veloFlux

Thank you for your interest in contributing! Please follow these guidelines to ensure your contributions pass CI checks.

## Development Requirements

- Rust (stable toolchain)
- `rustfmt` and `clippy` components

## Code Quality Standards

### Formatting

All code must be formatted with `rustfmt`:

```bash
cargo fmt --all
```

To check formatting without making changes:

```bash
cargo fmt --all -- --check
```

### Linting

Code must pass Clippy with no warnings:

```bash
make clippy
```

### Testing

All tests must pass:

```bash
make test
```

### Code Coverage

The project maintains a **50% minimum line coverage** requirement. To check coverage locally, install [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov):

```bash
cargo install cargo-llvm-cov
```

Then run:

```bash
cargo llvm-cov --workspace --all-features --fail-under-lines 50
```

## Before Submitting a Pull Request

1. Run `cargo fmt --all`
2. Run `make clippy` and fix any warnings
3. Run `make test` and ensure all tests pass
4. Ensure code coverage is at least 50%

## Pull Request Titles

This project follows [Conventional Commits](https://www.conventionalcommits.org/) specification.

> **Note:** CI validates only the **PR title** (not individual commits), so ensure your PR title follows the conventional commit format. We use squash merge, so the PR title becomes the final commit message.

### Format

```
<type>(<scope>): <subject>
```

### Rules

- Keep PR title under 50 characters
- Use lowercase for type and scope
- Do not end subject line with a period

### Types

| Type       | Description                                      |
|------------|--------------------------------------------------|
| `feat`     | A new feature                                    |
| `fix`      | A bug fix                                        |
| `docs`     | Documentation only changes                       |
| `style`    | Formatting, missing semicolons, etc.             |
| `refactor` | Code change that neither fixes a bug nor adds a feature |
| `perf`     | Performance improvement                          |
| `test`     | Adding or updating tests                         |
| `build`    | Changes to build system or dependencies          |
| `ci`       | Changes to CI configuration                      |
| `chore`    | Other changes that don't modify src or test files|

### Examples

```
feat(decoder): add SPI signal decoding support
fix(encoder): handle null values correctly
docs: update contributing guidelines
```
