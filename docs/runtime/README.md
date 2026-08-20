# Runtime Docs

This section contains runtime architecture and execution behavior.

- `instances/`: Flow instance hosting and cgroup binding.
- `startup/`: Startup-time bootstrap and cache/lifecycle behavior.
- `pipelines/`: Pipeline lifecycle, desired-state handling, and runtime observability.
- `checkpoint.md`: Pipeline checkpoint barriers, processor snapshots, graceful shutdown, and
  independent checkpoint persistence on the shared redb database.
- `time/`: Event-time runtime behavior.
- `processors/`: Processor-level runtime protocols and contracts.
- `sources/`: Source connector and shared-source runtime behavior.
- `sinks/`: Sink connector, encoder, and sink-branch output behavior.
- `extensibility/`: Runtime extension mechanisms.
