# Flow Instances

## Background

In veloFlux, a `flow::FlowInstance` is a **process-local runtime container** that owns:

- a catalog (stream definitions)
- pipeline runtime (planning, execution, and lifecycle)
- instance-scoped runtime resources (shared stream runtime, memory pub/sub, shared clients, etc.)

By default, the Manager runs a single in-process instance named `default`.

veloFlux also supports **extra Flow instances** for isolation. Extra instances are hosted
inside the main veloFlux process with their own dedicated Tokio runtime.

## Goals

- Provide isolation between groups of pipelines by binding each pipeline to a `flow_instance_id`.
- Keep the public Manager REST API simple: pipelines bind to an instance by id; instances are not
  created via REST.
- Make multi-instance behavior observable and operationally explicit.

## Non-Goals

- A distributed / multi-host Flow cluster.
- Autoscaling or dynamic instance creation at runtime via REST.
- Cross-instance shared state or cross-instance execution.

## Terminology

- `flow_instance_id`: an identifier that decides **where a pipeline runs**.
- `default`: the in-process Flow instance inside the Manager process.
- **extra instance**: an in-process non-default `flow::FlowInstance` with its own dedicated Tokio
  runtime. Runtime threads may be bound to a dedicated thread-level cgroup.

## Architecture Overview

```text
┌───────────────────────────────────────────────────────────┐
│ Main server process                                       │
│                                                           │
│  ┌──────────────┐                                         │
│  │ Manager HTTP │  REST                                   │
│  │ API (:8080)  │◀──────────────────────────────────────┐ │
│  └──────┬───────┘                                       │ │
│         │                                               │ │
│         │ in-process calls                              │ │
│         ▼                                               │ │
│   FlowInstance(id=default)                              │ │
│   FlowInstance(id=fi_local_1, dedicated Tokio runtime)  │ │
│   FlowInstance(id=fi_local_2, dedicated Tokio runtime)  │ │
│                                                           │
└──────────────────────────────────────────────────────────┘
```

Key points:

- The Manager process always hosts `default` in-process.
- Extra instances are in-process with dedicated Tokio runtimes.

## Configuration Model

Flow instances are declared under `server.flow_instances`.

Each instance must declare:

- `id`
- `backend` (optional, defaults to `in_process`; the only supported value)
- optional `runtime` options (worker threads, thread name prefix)
- optional `cgroup` options for thread-level binding

Constraints:

- `id` must match the resource-id grammar `` `[A-Za-z][A-Za-z0-9_]{0,127}` ``
  (start with an ASCII letter, then ASCII letters, digits, or underscores, up to
  128 bytes); it is case-sensitive and is not trimmed
- an extra instance `id` must not be `default`
- all declared ids must be unique
- `backend` must be `in_process` (the only valid value); removed values such as
  `worker_process` are rejected during config deserialization
- Linux-only cgroup settings must not be configured on unsupported platforms

## Runtime Startup and Lifecycle

### Manager startup

- Main process initializes config, logging, storage, and the in-process `default` FlowInstance.
- Depending on configuration, Manager may also create extra in-process instances.

### Extra instance startup

For each configured extra instance:

1. Manager constructs an in-process `FlowInstance` with a dedicated Tokio runtime.
2. During runtime worker thread startup, each runtime thread joins the configured child threaded
   cgroup, if thread-level binding is enabled for that instance.
3. Manager routes pipeline operations to the appropriate in-process instance.

### Shutdown

- Instance lifecycles are bound to the main process lifecycle.
- Runtime worker threads exit as their Tokio runtime is shut down.

## Resource Model: Global Metadata vs Instance Runtime

The Manager API persists **global metadata** in storage, but runtime installation is instance-aware.

### Streams

- `POST /streams` persists the stream definition in storage.
- The Manager installs the stream into in-process instances.

### Memory topics and shared connector configs

The same principle applies:

- definitions are persisted globally
- runtime installation is in-process for all instances

### Pipelines

A pipeline is persisted in storage as a spec that includes `flow_instance_id` (optional in REST;
defaults to `default`).

At runtime, Manager routes the operation directly to the local `FlowInstance`.

## Isolation and Trade-offs

What you gain with extra instances:

- workload partitioning by `flow_instance_id`
- dedicated Tokio runtime per extra instance
- runtime worker thread CPU isolation through thread-level cgroup binding

What extra instances do not provide:

- horizontal scaling of a single pipeline across hosts
- implicit coordination or shared state across instances
- process-level fault isolation

## Known Limitations

- Cross-instance memory topic delivery is not supported.

## When to Use Extra Instances

Use an extra instance when you need:

- dedicated Tokio runtime scheduling inside the main process
- thread-level CPU control for a specific instance
- co-location with Manager while separating runtime CPU budgets
