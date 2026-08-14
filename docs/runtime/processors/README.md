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

A processor records and skips an invalid input when the error is classified before any persistent
state mutation. Examples include an invalid partition key or a timestamp that cannot be represented
by the target window.

Row-transform processors should keep processing later rows in the same collection after a
recoverable row-level error. Valid rows from that collection may still be emitted; the invalid row is
not forwarded.

Stateful row processors should update processor-local state only after the row has reached the
state mutation point successfully. A row that fails while resolving keys, filters, arguments, or
tracked values is skipped without advancing that row-scoped state.

Stateful function processors prepare each per-call state update for a row and commit those prepared
updates only after every stateful call in the row succeeds. If a later call fails, earlier prepared
updates from the same row are discarded with the row.

Aggregation processors prepare all accumulator updates for a row before committing any of them. If
one aggregate update fails, the row is skipped and no accumulator for that row is advanced.

Window processors may also recover from errors that are isolated to the current row, requested
window, or active partition state. In those cases the processor records the error, drops the invalid
row or window state, and continues with later input.

Downstream channel failures, startup or configuration failures, control lifecycle failures,
unreachable invariant failures, panics, and unexpected task exits continue to terminate the
processor task.

Managed pipeline runtimes treat a processor task exit as a pipeline-fatal event unless the pipeline
is already in a stop or delete shutdown path. The pipeline supervisor records the failed processor,
aborts the remaining processor tasks, and reports the pipeline runtime as failed. This contract does
not attempt data replay or state recovery.
