# Processor Runtime Contracts

This directory contains processor-level execution and channel contracts.

## Processor error handling contract

Unless a processor-specific document states otherwise, runtime processing errors are handled
locally by the processor that encounters them.

The default processor contract is:

- record the error in `ProcessorStats`
- emit logs for diagnostics when appropriate
- keep the pipeline running when the error is non-fatal and the processor can continue safely

Tests and downstream runtime documents should not assume that a processor forwards such errors as
`StreamData::Error`. `StreamData::Error` exists as a pipeline message type, but it is not the
default mechanism for surfacing ordinary processor-local runtime failures.

For runtime test design, prefer assertions such as:

- the pipeline continues running after the bad input
- valid follow-up input still produces output
- processor stats and logs reflect the error

Only add a downstream `StreamData::Error` expectation when a specific processor contract explicitly
documents that behavior.

### Recoverable input error boundary

A processor records and skips an invalid input only when the error is classified before any
persistent state mutation. Examples include an invalid partition key or a timestamp that cannot be
represented by the target window.

Errors that may follow a partial state mutation continue to terminate the processor task. The same
applies to unreachable invariant failures, downstream channel failures, startup or configuration
failures, control lifecycle failures, panics, and unexpected task exits. The processor does not
clear active state in an attempt to recover from these errors.
