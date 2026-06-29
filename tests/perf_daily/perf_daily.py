#!/usr/bin/env python3

import argparse
import json
import os
import random
import re
import socket
import string
import sys
import threading
import time
import urllib.parse
import urllib.request
import urllib.error
from typing import Any, Dict, List, Optional

from mqtt_qos0_pub import MqttError, parse_tcp_broker_url, publish_qos1_with_timing


class PerfDailyError(RuntimeError):
    pass


DEFAULT_STREAM_NAME = "perf_daily_stream"
DEFAULT_PIPELINE_ID = "perf_daily_pipeline"

DEFAULT_METRICS_URL = "http://127.0.0.1:8080/metrics"
DEFAULT_OUT_DIR = "tmp/perf_daily"

FLOW_INSTANCE_LABEL = "flow_instance"
PIPELINE_ID_LABEL = "pipeline_id"
PROCESSOR_ID_LABEL = "processor_id"

RUNTIME_CPU_USAGE_METRIC = "veloflux_runtime_cpu_usage_percent"
RUNTIME_MEMORY_USAGE_METRIC = "veloflux_runtime_memory_usage_bytes"
RUNTIME_HEAP_IN_USE_METRIC = "veloflux_runtime_heap_in_use_bytes"
RUNTIME_HEAP_IN_ALLOCATOR_METRIC = "veloflux_runtime_heap_in_allocator_bytes"
PROCESSOR_RECORDS_IN_METRIC = "veloflux_processor_records_in_total"
PROCESSOR_RECORDS_OUT_METRIC = "veloflux_processor_records_out_total"

DATASOURCE_PROCESSOR_ID_PREFIX = "datasource_"
RESULT_COLLECT_PROCESSOR_ID_PREFIX = "result_collect_"

TRACKED_METRICS = (
    RUNTIME_CPU_USAGE_METRIC,
    RUNTIME_MEMORY_USAGE_METRIC,
    RUNTIME_HEAP_IN_USE_METRIC,
    RUNTIME_HEAP_IN_ALLOCATOR_METRIC,
    PROCESSOR_RECORDS_IN_METRIC,
)

_SAMPLE_RE = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+]?(?:\d+\.?\d*|\d*\.?\d+)(?:[eE][-+]?\d+)?)(?:\s+(\d+))?$"
)


def _ignore_not_found(fn) -> None:
    try:
        fn()
    except ApiError as e:
        if e.status_code == 404:
            return
        raise


def _encode_json(obj: Any) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _join_url(base_url: str, path: str) -> str:
    base = base_url.rstrip("/") + "/"
    return urllib.parse.urljoin(base, path.lstrip("/"))


class ApiError(RuntimeError):
    def __init__(self, method: str, path: str, status_code: int, message: str) -> None:
        super().__init__(f"{method} {path} failed: HTTP {status_code}: {message}")
        self.method = method
        self.path = path
        self.status_code = status_code
        self.message = message


class Client:
    def __init__(self, base_url: str, timeout_secs: float) -> None:
        self.base_url = base_url
        self.timeout_secs = timeout_secs

    def request_json(self, method: str, path: str, body: Optional[Any]) -> Any:
        url = _join_url(self.base_url, path)
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = _encode_json(body)
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url=url, method=method, data=data, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_secs) as resp:
                raw = resp.read()
                if not raw:
                    return None
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as e:  # type: ignore[attr-defined]
            raw = e.read()
            message = raw.decode("utf-8", errors="replace") if raw else str(e)
            raise ApiError(method, path, e.code, message) from None
        except urllib.error.URLError as e:  # type: ignore[attr-defined]
            raise ApiError(method, path, 0, str(e)) from None

    def request_text(self, method: str, path: str, body: Optional[Any] = None) -> str:
        url = _join_url(self.base_url, path)
        data = None
        headers = {"Accept": "text/plain"}
        if body is not None:
            data = _encode_json(body)
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url=url, method=method, data=data, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_secs) as resp:
                raw = resp.read()
                return raw.decode("utf-8", errors="replace") if raw else ""
        except urllib.error.HTTPError as e:  # type: ignore[attr-defined]
            raw = e.read()
            message = raw.decode("utf-8", errors="replace") if raw else str(e)
            raise ApiError(method, path, e.code, message) from None
        except urllib.error.URLError as e:  # type: ignore[attr-defined]
            raise ApiError(method, path, 0, str(e)) from None


def delete_all_pipelines(client: "Client") -> None:
    """
    Delete all pipelines in manager storage.

    This keeps perf cases isolated (no leftover pipelines affecting metrics) and
    makes it safe to reuse the same stream across cases.
    """
    items = client.request_json("GET", "/pipelines", None) or []
    ids: List[str] = []
    for it in items:
        if isinstance(it, dict) and isinstance(it.get("id"), str):
            ids.append(it["id"])
    if ids:
        print(f"force: deleting {len(ids)} pipeline(s): {', '.join(ids)}", file=sys.stderr)
    for pid in ids:
        _ignore_not_found(lambda pid=pid: client.request_text("DELETE", f"/pipelines/{urllib.parse.quote(pid)}"))


def build_columns(count: int) -> List[Dict[str, str]]:
    return [{"name": f"a{i}", "data_type": "string"} for i in range(1, count + 1)]


def build_select_sql(stream_name: str, column_count: int) -> str:
    cols = [f"a{i}" for i in range(1, column_count + 1)]
    return f"select {','.join(cols)} from {stream_name}"


def build_select_sql_by_mode(stream_name: str, column_count: int, sql_mode: str) -> str:
    if sql_mode == "star":
        return f"select * from {stream_name}"
    if sql_mode == "explicit":
        return build_select_sql(stream_name, column_count)
    raise PerfDailyError(f"unknown --sql-mode: {sql_mode}")


def create_stream_body(
    stream_name: str,
    column_count: int,
    broker_url: str,
    topic: str,
    qos: int,
) -> Dict[str, Any]:
    return {
        "name": stream_name,
        "type": "mqtt",
        "props": {"broker_url": broker_url, "topic": topic, "qos": qos},
        "schema": {"type": "json", "props": {"columns": build_columns(column_count)}},
        "decoder": {"type": "json", "props": {}},
        "shared": False,
    }


def create_pipeline_body(pipeline_id: str, sql: str) -> Dict[str, Any]:
    # Ensure JSON encoder config is present.
    return {
        "id": pipeline_id,
        "sql": sql,
        "sinks": [
            {
                "type": "nop",
                "props": {"log": False},
                "common_sink_props": {"batch_duration": 100, "batch_count": 50},
                "encoder": {"type": "json", "props": {}},
            }
        ],
        "options": {"eventtime": {"enabled": False}},
    }


def wait_for_manager(base_url: str, timeout_secs: float, wait_secs: float = 60.0) -> None:
    client = Client(base_url, timeout_secs)
    deadline = time.time() + wait_secs
    last_err: Optional[str] = None
    while time.time() < deadline:
        try:
            client.request_json("GET", "/streams", None)
            return
        except Exception as e:
            last_err = str(e)
            time.sleep(1.0)
    raise PerfDailyError(f"manager not ready after {wait_secs}s: {last_err}")


def wait_for_tcp(host: str, port: int, wait_secs: float = 60.0) -> None:
    deadline = time.time() + wait_secs
    last_err: Optional[str] = None
    while time.time() < deadline:
        s = socket.socket()
        try:
            s.settimeout(2.0)
            s.connect((host, port))
            return
        except Exception as e:
            last_err = str(e)
            time.sleep(1.0)
        finally:
            try:
                s.close()
            except Exception:
                pass
    raise PerfDailyError(f"broker {host}:{port} not ready after {wait_secs}s: {last_err}")


def provision(
    base_url: str,
    timeout_secs: float,
    stream_name: str,
    pipeline_id: str,
    columns: int,
    broker_url: str,
    topic: str,
    qos: int,
    force: bool,
    no_start: bool,
    dry_run: bool,
    sql_mode: str,
) -> None:
    client = Client(base_url, timeout_secs)
    sql = build_select_sql_by_mode(stream_name, columns, sql_mode=sql_mode)
    stream_req = create_stream_body(stream_name, columns, broker_url, topic, qos)
    pipeline_req = create_pipeline_body(pipeline_id, sql)

    if dry_run:
        print(f"sql bytes: {len(sql.encode('utf-8'))}", file=sys.stderr)
        print(f"stream body bytes: {len(_encode_json(stream_req))}", file=sys.stderr)
        print(f"pipeline body bytes: {len(_encode_json(pipeline_req))}", file=sys.stderr)
        return

    if force:
        # Do NOT delete the stream: multiple cases share the same stream, and manager
        # will reject deleting a referenced stream (HTTP 409).
        #
        # Instead, delete all pipelines before creating the requested case pipeline.
        delete_all_pipelines(client)

    try:
        client.request_json("POST", "/streams", stream_req)
    except ApiError as e:
        if e.status_code != 409:
            raise

    # Recreate pipeline.
    _ignore_not_found(lambda: client.request_text("DELETE", f"/pipelines/{urllib.parse.quote(pipeline_id)}"))
    try:
        client.request_json("POST", "/pipelines", pipeline_req)
    except ApiError as e:
        if e.status_code != 409:
            raise

    if not no_start:
        client.request_text("POST", f"/pipelines/{urllib.parse.quote(pipeline_id)}/start")


def _alnum_rand_str(rng: random.Random, length: int) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(rng.choices(alphabet, k=length))


def generate_payload_cases(column_count: int, cases: int, str_len: int) -> List[bytes]:
    if cases <= 0:
        return []
    keys = [f"a{i}" for i in range(1, column_count + 1)]
    rng = random.Random()

    payloads: List[bytes] = []
    for _ in range(cases):
        obj: Dict[str, Any] = {}
        for k in keys:
            obj[k] = _alnum_rand_str(rng, str_len)
        payloads.append(_encode_json(obj))
    return payloads


def scrape_prometheus_text(url: str, timeout_secs: float) -> str:
    req = urllib.request.Request(url=url, method="GET", headers={"Accept": "text/plain"})
    with urllib.request.urlopen(req, timeout=timeout_secs) as resp:
        raw = resp.read()
        return raw.decode("utf-8", errors="replace") if raw else ""


def _ensure_flow_instance_label(labels: Optional[str], flow_instance: str) -> str:
    if labels is None:
        return f'{{{FLOW_INSTANCE_LABEL}="{flow_instance}"}}'
    if f"{FLOW_INSTANCE_LABEL}=" in labels:
        return labels
    inner = labels[1:-1].strip()
    if not inner:
        return f'{{{FLOW_INSTANCE_LABEL}="{flow_instance}"}}'
    return f'{{{FLOW_INSTANCE_LABEL}="{flow_instance}",{inner}}}'


def _labels_have_kv(labels: str, key: str, value: str) -> bool:
    # Simple substring check is sufficient for Prometheus label format in our dumps.
    return f'{key}="{value}"' in labels


def _labels_have_processor_id_prefix(labels: str, prefix: str) -> bool:
    return f'{PROCESSOR_ID_LABEL}="{prefix}' in labels


def extract_openmetrics_block(text: str, ts_ms: int, instance: str, include_meta: bool) -> str:
    out_lines: List[str] = []
    for line in text.splitlines():
        if not line:
            continue
        if include_meta and (line.startswith("# HELP ") or line.startswith("# TYPE ")):
            parts = line.split(" ", 3)
            if len(parts) >= 3 and parts[2] in TRACKED_METRICS:
                out_lines.append(line)
            continue
        m = _SAMPLE_RE.match(line)
        if not m:
            continue
        name = m.group(1)
        if name not in TRACKED_METRICS:
            continue
        labels = _ensure_flow_instance_label(m.group(2), flow_instance=instance)
        # Only keep datasource input counter; this is the closest approximation of "ingress throughput".
        if (
            name == PROCESSOR_RECORDS_IN_METRIC
            and not _labels_have_processor_id_prefix(labels, DATASOURCE_PROCESSOR_ID_PREFIX)
        ):
            continue
        value = m.group(3)
        out_lines.append(f"{name}{labels} {value} {ts_ms}")
    return "\n".join(out_lines) + ("\n" if out_lines else "")


class OpenMetricsDumper:
    def __init__(
        self,
        metrics_url: str,
        timeout_secs: float,
        interval_ms: int,
        out_path: str,
        instance: str,
    ) -> None:
        self.metrics_url = metrics_url
        self.timeout_secs = timeout_secs
        self.interval_ms = interval_ms
        self.out_path = out_path
        self.instance = instance
        self.stop = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.samples = 0
        self.errors = 0

    def start(self) -> None:
        os.makedirs(os.path.dirname(self.out_path), exist_ok=True)
        t = threading.Thread(target=self._run, name="metrics-dumper", daemon=True)
        self.thread = t
        t.start()

    def finish(self) -> None:
        self.stop.set()
        if self.thread is not None:
            self.thread.join(timeout=10.0)
        try:
            with open(self.out_path, "a", encoding="utf-8") as f:
                f.write("# EOF\n")
        except Exception:
            pass

    def _run(self) -> None:
        interval = max(0.05, self.interval_ms / 1000.0)
        # Truncate the output file at start so a rerun doesn't append on top of old data.
        include_meta = True
        with open(self.out_path, "w", encoding="utf-8") as f:
            while not self.stop.is_set():
                try:
                    ts_ms = int(time.time() * 1000)
                    text = scrape_prometheus_text(self.metrics_url, timeout_secs=self.timeout_secs)
                    block = extract_openmetrics_block(
                        text,
                        ts_ms=ts_ms,
                        instance=self.instance,
                        include_meta=include_meta,
                    )
                    if block:
                        f.write(block)
                        f.flush()
                    include_meta = False
                    self.samples += 1
                except Exception:
                    self.errors += 1
                time.sleep(interval)

    def result(self) -> Dict[str, Any]:
        return {
            "metrics_url": self.metrics_url,
            "scrape_interval_ms": self.interval_ms,
            "samples": self.samples,
            "errors": self.errors,
            "openmetrics_path": self.out_path,
            "instance": self.instance,
        }


def trim_openmetrics_to_window(in_path: str, start_ms: int, end_ms: int) -> None:
    """
    Keep only tracked metrics samples within [start_ms, end_ms] in-place.

    This makes the dump "window-aligned" so downstream plotting doesn't need to
    know about pre/post noise.
    """
    if start_ms <= 0 or end_ms <= 0 or end_ms < start_ms:
        return
    try:
        with open(in_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.read().splitlines()
    except FileNotFoundError:
        return

    meta_seen: set[str] = set()
    out: List[str] = []
    for line in lines:
        if not line or line == "# EOF":
            continue
        if line.startswith("# HELP ") or line.startswith("# TYPE "):
            parts = line.split(" ", 3)
            if len(parts) >= 3 and parts[2] in TRACKED_METRICS and line not in meta_seen:
                out.append(line)
                meta_seen.add(line)
            continue
        m = _SAMPLE_RE.match(line)
        if not m:
            continue
        name = m.group(1)
        if name not in TRACKED_METRICS:
            continue
        ts = m.group(4)
        if ts is None:
            continue
        try:
            ts_ms = int(ts)
        except ValueError:
            continue
        if start_ms <= ts_ms <= end_ms:
            out.append(line)

    with open(in_path, "w", encoding="utf-8") as f:
        if out:
            f.write("\n".join(out))
            f.write("\n")
        f.write("# EOF\n")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Daily perf scenario: 15k-column MQTT stream + nop pipeline.")
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    p.add_argument("--timeout-secs", type=float, default=60.0)

    p.add_argument("--stream-name", default=DEFAULT_STREAM_NAME)
    p.add_argument("--pipeline-id", default=DEFAULT_PIPELINE_ID)
    p.add_argument("--columns", type=int, default=15000)
    p.add_argument("--sql-mode", choices=("explicit", "star"), default="explicit")

    p.add_argument("--broker-url", default="tcp://127.0.0.1:1883",
                   help="MQTT broker URL used by this script for publishing. "
                        "Also used as the stream broker_url unless --stream-broker-url is set.")
    p.add_argument("--stream-broker-url", default=None,
                   help="MQTT broker URL passed to veloflux when creating the stream "
                        "(defaults to --broker-url). Use this when veloflux runs in a "
                        "different network namespace (e.g. QEMU guest).")
    p.add_argument("--topic", default="/perf/daily")
    p.add_argument("--qos", type=int, default=1)

    p.add_argument("--cases", type=int, default=20)
    p.add_argument("--str-len", type=int, default=10)
    p.add_argument("--duration-secs", type=int, default=300)
    p.add_argument("--rate", type=int, default=50, help="Messages per second (default: 50)")

    p.add_argument("--metrics-url", default=DEFAULT_METRICS_URL)
    p.add_argument("--scrape-interval-ms", type=int, default=15000)
    p.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    p.add_argument("--no-metrics", action="store_true")

    p.add_argument("--force", action="store_true")
    p.add_argument("--no-start", action="store_true")
    p.add_argument("--dry-run", action="store_true")

    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("provision")
    sub.add_parser("publish")
    sub.add_parser("run")
    return p


def main(argv: List[str]) -> int:
    args = build_parser().parse_args(argv)

    if args.columns <= 0:
        raise PerfDailyError("--columns must be > 0")
    if args.cases < 0:
        raise PerfDailyError("--cases must be >= 0")
    if args.str_len <= 0:
        raise PerfDailyError("--str-len must be > 0")
    if args.duration_secs < 0:
        raise PerfDailyError("--duration-secs must be >= 0")
    if args.rate < 0:
        raise PerfDailyError("--rate must be >= 0")
    if args.qos != 1:
        raise PerfDailyError("--qos must be 1 for publish (QoS1 only)")

    wait_for_manager(args.base_url, timeout_secs=args.timeout_secs, wait_secs=60.0)
    broker = parse_tcp_broker_url(args.broker_url)
    wait_for_tcp(broker.host, broker.port, wait_secs=60.0)

    stream_broker_url = args.stream_broker_url or args.broker_url

    if args.command in ("provision", "run"):
        provision(
            base_url=args.base_url,
            timeout_secs=args.timeout_secs,
            stream_name=args.stream_name,
            pipeline_id=args.pipeline_id,
            columns=args.columns,
            broker_url=stream_broker_url,
            topic=args.topic,
            qos=args.qos,
            force=args.force,
            no_start=args.no_start,
            dry_run=args.dry_run,
            sql_mode=args.sql_mode,
        )

    if args.command in ("publish", "run"):
        if args.dry_run:
            payloads = generate_payload_cases(5, min(args.cases, 1), args.str_len)
            print(f"sample payload bytes: {len(payloads[0]) if payloads else 0}", file=sys.stderr)
            return 0

        # Generate payloads before starting metrics collection, to align "measurement window" to publish window.
        payloads = generate_payload_cases(args.columns, args.cases, args.str_len)
        payload_bytes = len(payloads[0]) if payloads else 0

        dumper = None
        os.makedirs(args.out_dir, exist_ok=True)
        openmetrics_path = os.path.join(args.out_dir, "metrics.openmetrics")

        publish_start_ts_ms = 0
        publish_end_ts_ms = 0
        if args.command == "run" and not args.no_metrics:
            dumper = OpenMetricsDumper(
                metrics_url=args.metrics_url,
                timeout_secs=args.timeout_secs,
                interval_ms=args.scrape_interval_ms,
                out_path=openmetrics_path,
                instance="local",
            )
            dumper.start()

        pub = None
        try:
            pub = publish_qos1_with_timing(
                broker_url=args.broker_url,
                topic=args.topic,
                payloads=payloads,
                duration_secs=args.duration_secs,
                rate_per_sec=args.rate,
                client_id=f"perf-daily-{os.getpid()}",
                keepalive_secs=60,
            )
        except MqttError as e:
            raise PerfDailyError(str(e)) from None
        finally:
            if pub is None:
                publish_start_ts_ms = int(time.time() * 1000)
                publish_end_ts_ms = publish_start_ts_ms
            else:
                publish_start_ts_ms = pub.start_ts_ms
                publish_end_ts_ms = pub.end_ts_ms
            if dumper is not None:
                dumper.finish()

        if dumper is not None:
            trim_openmetrics_to_window(openmetrics_path, start_ms=publish_start_ts_ms, end_ms=publish_end_ts_ms)
            duration_s = max(0.001, (publish_end_ts_ms - publish_start_ts_ms) / 1000.0)
            result_path = os.path.join(args.out_dir, "result.json")
            result = {
                "publish_start_ts_ms": publish_start_ts_ms,
                "publish_end_ts_ms": publish_end_ts_ms,
                "publish_duration_secs": duration_s,
                "sent_messages": pub.sent if pub is not None else 0,
                "effective_rate_mps": (pub.sent / duration_s) if pub is not None else 0.0,
                "payload_bytes": payload_bytes,
                "config": {
                    "base_url": args.base_url,
                    "stream_name": args.stream_name,
                    "pipeline_id": args.pipeline_id,
                    "columns": args.columns,
                    "sql_mode": args.sql_mode,
                    "broker_url": args.broker_url,
                    "stream_broker_url": stream_broker_url,
                    "topic": args.topic,
                    "qos": args.qos,
                    "cases": args.cases,
                    "str_len": args.str_len,
                    "duration_secs": args.duration_secs,
                    "rate": args.rate,
                },
                "metrics": dumper.result(),
            }
            with open(result_path, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            print(f"result_json: {result_path}")
            print(f"openmetrics: {openmetrics_path}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except (PerfDailyError, ApiError) as e:
        print(f"error: {e}", file=sys.stderr)
        raise SystemExit(2)
