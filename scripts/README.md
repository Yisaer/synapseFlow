# Scripts: Heap Flamegraph Tooling (Ubuntu)

This directory contains helper scripts to capture a jemalloc heap dump from a running SynapseFlow process and convert it into a flamegraph SVG.

## Other scripts

- `scripts/sf_rest_tool.md`: How to create a stream/pipeline via the REST API using `scripts/sf_rest_tool.py`.

## What you get

- `scripts/setup_heap_tools_ubuntu.sh`: Installs the required tooling on Ubuntu:
  - `python3`
  - `libjemalloc-dev` (typically provides `jeprof`)
  - `flamegraph.pl` (from `apt` package `flamegraph` when available, otherwise installed from `https://github.com/brendangregg/FlameGraph`)
- `scripts/heap_to_svg.sh`: Converts a jemalloc `heap_v2` dump to `heap.svg` using `jeprof` + `flamegraph.pl`.

## Capture heap dump from a running process

The profiling server exposes `GET /debug/pprof/heap` (jemalloc `heap_v2` dump):

```bash
curl -sS http://<host>:6060/debug/pprof/heap -o heap.1
```

Note: jemalloc heap profiling must be enabled at process start, e.g.:

```bash
nohup env _RJEM_MALLOC_CONF='prof:true,prof_active:true' ./veloFlux-linux-x86_64 --data-dir ./data/ --config ./etc/config.example.yaml > veloFlux.out 2>&1 &
```

## Install tools on Ubuntu

Run:

```bash
./scripts/setup_heap_tools_ubuntu.sh
```

## Convert heap dump to flamegraph SVG

You need the same Linux binary that produced the heap dump (ideally not stripped), plus the downloaded `heap.1`.

Bytes (default):

```bash
./scripts/heap_to_svg.sh --binary ./veloFlux-linux-x86_64 --heap ./heap.1 --out ./heap.svg
```

Objects:

```bash
./scripts/heap_to_svg.sh --binary ./veloFlux-linux-x86_64 --heap ./heap.1 --out ./heap.svg --metric objects
```

## Download an SVG from a server via scp

```bash
scp root@115.190.54.189:/root/gaosong/flow/heap.svg ./heap.svg
```

## Troubleshooting

- If `jeprof` is missing after installing `libjemalloc-dev`, check where the package installed it:
  - `dpkg -L libjemalloc-dev | grep jeprof`
- If the output shows only addresses, your deployed binary is likely stripped (missing `.symtab`), and function-level symbolization will be limited.
