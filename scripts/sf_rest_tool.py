#!/usr/bin/env python3

import argparse
import json
import sys
from typing import Any, Dict, List, Optional, Tuple
import urllib.error
import urllib.parse
import urllib.request


def _join_url(base_url: str, path: str) -> str:
    base = base_url.rstrip("/") + "/"
    return urllib.parse.urljoin(base, path.lstrip("/"))


class ApiError(RuntimeError):
    pass


class Client:
    def __init__(self, base_url: str, timeout_secs: float) -> None:
        self.base_url = base_url
        self.timeout_secs = timeout_secs

    def request_json(self, method: str, path: str, body: Optional[Any]) -> Tuple[int, Any]:
        url = _join_url(self.base_url, path)
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url=url, method=method, data=data, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_secs) as resp:
                raw = resp.read()
                if not raw:
                    return resp.status, None
                return resp.status, json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as e:
            raw = e.read()
            message = raw.decode("utf-8", errors="replace") if raw else str(e)
            raise ApiError(f"{method} {path} failed: HTTP {e.code}: {message}") from None
        except urllib.error.URLError as e:
            raise ApiError(f"{method} {path} failed: {e}") from None

    def request_text(self, method: str, path: str, body: Optional[Any] = None) -> Tuple[int, str]:
        url = _join_url(self.base_url, path)
        data = None
        headers = {"Accept": "text/plain"}
        if body is not None:
            data = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url=url, method=method, data=data, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_secs) as resp:
                raw = resp.read()
                text = raw.decode("utf-8", errors="replace") if raw else ""
                return resp.status, text
        except urllib.error.HTTPError as e:
            raw = e.read()
            message = raw.decode("utf-8", errors="replace") if raw else str(e)
            raise ApiError(f"{method} {path} failed: HTTP {e.code}: {message}") from None
        except urllib.error.URLError as e:
            raise ApiError(f"{method} {path} failed: {e}") from None


def build_columns(count: int) -> List[Dict[str, str]]:
    columns: List[Dict[str, str]] = []
    for i in range(1, count + 1):
        columns.append({"name": f"a{i}", "data_type": "string"})
    return columns


def build_select_sql(stream_name: str, column_count: int) -> str:
    parts: List[str] = []
    for i in range(1, column_count + 1):
        parts.append(f"a{i}")
    return f"select {','.join(parts)} from {stream_name}"


def create_stream_body(
    stream_name: str,
    column_count: int,
    broker_url: str,
    topic: str,
    qos: int,
) -> Dict[str, Any]:
    columns = build_columns(column_count)
    return {
        "name": stream_name,
        "type": "mqtt",
        "props": {"broker_url": broker_url, "topic": topic, "qos": qos},
        "schema": {"type": "json", "props": {"columns": columns}},
        "decoder": {"type": "json", "props": {}},
        "shared": False,
    }


def create_pipeline_body(pipeline_id: str, sql: str) -> Dict[str, Any]:
    return {
        "id": pipeline_id,
        "sql": sql,
        "sinks": [
            {
                "type": "nop",
                "props": {"log": False},
                "commonSinkProps": {"batchDuration": 100, "batchCount": 50},
            }
        ],
        "options": {"plan_cache": {"enabled": False}, "eventtime": {"enabled": False}},
    }


def cmd_provision(args: argparse.Namespace) -> int:
    client = Client(args.base_url, args.timeout_secs)
    sql = build_select_sql(args.stream_name, args.columns)
    stream_req = create_stream_body(
        args.stream_name,
        args.columns,
        args.broker_url,
        args.topic,
        args.qos,
    )
    pipeline_req = create_pipeline_body(args.pipeline_id, sql)

    if args.dry_run:
        print(json.dumps(stream_req)[:512] + " ...", file=sys.stderr)
        print(f"stream body bytes: {len(json.dumps(stream_req).encode('utf-8'))}", file=sys.stderr)
        print(f"sql bytes: {len(sql.encode('utf-8'))}", file=sys.stderr)
        print(json.dumps(pipeline_req)[:512] + " ...", file=sys.stderr)
        return 0

    if args.force:
        cmd_cleanup(args)

    client.request_json("POST", "/streams", stream_req)
    _ignore_not_found(
        lambda: client.request_text(
            "DELETE",
            f"/pipelines/{urllib.parse.quote(args.pipeline_id)}",
        )
    )
    client.request_json("POST", "/pipelines", pipeline_req)
    if not args.no_start:
        client.request_text("POST", f"/pipelines/{urllib.parse.quote(args.pipeline_id)}/start")
    return 0


def _ignore_not_found(fn):
    try:
        return fn()
    except ApiError as e:
        if "HTTP 404" in str(e):
            return None
        raise


def cmd_cleanup(args: argparse.Namespace) -> int:
    client = Client(args.base_url, args.timeout_secs)
    pipeline_id = urllib.parse.quote(args.pipeline_id)
    stream_name = urllib.parse.quote(args.stream_name)

    _ignore_not_found(lambda: client.request_text("DELETE", f"/pipelines/{pipeline_id}"))
    _ignore_not_found(lambda: client.request_text("DELETE", f"/streams/{stream_name}"))
    return 0


def cmd_recreate_pipeline(args: argparse.Namespace) -> int:
    client = Client(args.base_url, args.timeout_secs)
    sql = build_select_sql(args.stream_name, args.columns)
    pipeline_req = create_pipeline_body(args.pipeline_id, sql)

    if args.dry_run:
        print(f"sql bytes: {len(sql.encode('utf-8'))}", file=sys.stderr)
        print(json.dumps(pipeline_req)[:512] + " ...", file=sys.stderr)
        return 0

    _ignore_not_found(
        lambda: client.request_text(
            "DELETE",
            f"/pipelines/{urllib.parse.quote(args.pipeline_id)}",
        )
    )
    client.request_json("POST", "/pipelines", pipeline_req)
    if not args.no_start:
        client.request_text("POST", f"/pipelines/{urllib.parse.quote(args.pipeline_id)}/start")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="SynapseFlow REST helper: create/start/delete a 15k-column MQTT stream and a nop-sink pipeline.",
    )
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    p.add_argument("--stream-name", default="stream")
    p.add_argument("--pipeline-id", default="test")
    p.add_argument("--columns", type=int, default=15000)
    p.add_argument("--broker-url", default="tcp://127.0.0.1:1883")
    p.add_argument("--topic", default="/yisa/data")
    p.add_argument("--qos", type=int, default=0)
    p.add_argument("--timeout-secs", type=float, default=60.0)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force", action="store_true")
    p.add_argument("--no-start", action="store_true")

    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("provision")
    sub.add_parser("cleanup")
    sub.add_parser("recreate-pipeline")
    return p


def main(argv: List[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.columns <= 0:
        raise ApiError("--columns must be > 0")

    if args.command == "provision":
        return cmd_provision(args)
    if args.command == "cleanup":
        return cmd_cleanup(args)
    if args.command == "recreate-pipeline":
        return cmd_recreate_pipeline(args)

    raise ApiError(f"unknown command: {args.command}")


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except ApiError as e:
        print(f"error: {e}", file=sys.stderr)
        raise SystemExit(2)
