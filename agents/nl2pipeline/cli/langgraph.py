#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

if __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from agents.nl2pipeline.core.driver import LangGraphEngine  # noqa: E402
from agents.nl2pipeline.core.state import TurnRequest  # noqa: E402
from agents.nl2pipeline.shared.config import load_config  # noqa: E402


def _print_help() -> None:
    print(
        "\n".join(
            [
                "Commands:",
                "  /help",
                "  /trace",
                "  /exit",
                "",
                "Otherwise, type a natural-language message and press Enter.",
                "The agent will route it to either:",
                "  - list streams (GET /streams)",
                "  - describe stream schema (GET /streams/describe/:name)",
                "  - NL→SQL pipeline drafting (draft→validate→explain loop)",
            ]
        ),
        file=sys.stderr,
    )


def _format_streams_table(streams: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for s in streams:
        if not isinstance(s, dict):
            continue
        name = str(s.get("name", "")).strip()
        cols = (((s.get("schema") or {}).get("columns")) or []) if isinstance(s, dict) else []
        col_count = len(cols) if isinstance(cols, list) else 0
        sample: List[str] = []
        if isinstance(cols, list):
            for c in cols[:5]:
                if isinstance(c, dict) and c.get("name"):
                    sample.append(str(c["name"]))
        suffix = f" cols={col_count}"
        if sample:
            suffix += f" sample={','.join(sample)}"
        lines.append(f"- {name}{suffix}")
    return "\n".join(lines) if lines else "(no streams)"


def _render_schema(schema: Dict[str, Any]) -> str:
    cols = schema.get("columns") or []
    if not isinstance(cols, list) or not cols:
        return "(no columns)"
    lines: List[str] = []
    for c in cols:
        if isinstance(c, dict):
            lines.append(f"- {c.get('name','')}: {c.get('data_type','')}")
    return "\n".join(lines)


def _render_pipeline(sql: str, explain: str, pipeline_json: Dict[str, Any]) -> None:
    print("\nSQL:\n" + (sql or ""), file=sys.stdout)
    print("\nEXPLAIN:\n" + (explain or ""), file=sys.stdout)
    print(
        "\nPipeline JSON (manual apply):\n"
        + json.dumps(pipeline_json or {}, indent=2, ensure_ascii=False),
        file=sys.stdout,
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Interactive NL→pipeline agent (LangGraph)")
    p.add_argument("--config", required=True, help="Path to TOML config file")
    return p


def main(argv: list[str]) -> int:
    args = build_parser().parse_args(argv)
    cfg = load_config(args.config)
    try:
        engine = LangGraphEngine.new(cfg)
    except RuntimeError as e:
        print(str(e), file=sys.stderr)
        return 1

    session_id = engine.create_session()

    print("Type /help for commands. Enter requirements in plain text.", file=sys.stderr)

    while True:
        try:
            line = input("nl2pipeline> ").strip()
        except EOFError:
            print("", file=sys.stderr)
            break
        except KeyboardInterrupt:
            print("", file=sys.stderr)
            break
        if not line:
            continue

        if line == "/exit":
            break
        if line == "/help":
            _print_help()
            continue
        if line == "/trace":
            trace = engine.veloflux.trace()
            if not trace:
                print("(no trace)", file=sys.stderr)
            else:
                for t in trace[-30:]:
                    print(json.dumps(t, ensure_ascii=False), file=sys.stderr)
            continue

        # Normal user text.
        resp = engine.run_turn(session_id, TurnRequest(text=line))
        if resp.intent:
            print(f"[ROUTER] intent={resp.intent}", file=sys.stderr)

        if resp.streams is not None:
            print(_format_streams_table(resp.streams))
            continue

        if resp.schema is not None:
            print(_render_schema(resp.schema))
            continue

        if resp.interrupt and resp.interrupt.get("type") == "need_stream":
            print(resp.interrupt.get("question") or "Which stream should be used?", file=sys.stderr)
            choices = resp.interrupt.get("choices") or []
            if choices:
                print("Available streams:", file=sys.stderr)
                for name in choices:
                    print(f"- {name}", file=sys.stderr)
            selected = input("Stream name: ").strip()
            if not selected:
                continue
            # Push selection; workflow should resume the pending action using the stored last requirement.
            resp = engine.run_turn(session_id, TurnRequest(text=selected))

        if resp.interrupt and resp.interrupt.get("type") == "need_clarification":
            print(resp.interrupt.get("question") or "Please clarify.", file=sys.stderr)
            clarification = input("Reply: ").strip()
            if not clarification:
                continue
            resp = engine.run_turn(session_id, TurnRequest(text=clarification))

        if resp.explain_pretty and resp.pipeline_request_json and resp.sql:
            _render_pipeline(resp.sql, resp.explain_pretty, resp.pipeline_request_json)

        if resp.interrupt and resp.interrupt.get("type") == "need_confirm":
            feedback = input("\nIs this correct? (y=done / n=revise): ").strip().lower()
            if feedback in ("y", "yes"):
                engine.run_turn(session_id, TurnRequest(confirm=True))
                continue
            revision_text = input("What should be changed? ").strip()
            if not revision_text:
                continue
            print("[LLM] revising SQL...", file=sys.stderr, flush=True)
            resp = engine.run_turn(session_id, TurnRequest(feedback=revision_text))
            if resp.explain_pretty and resp.pipeline_request_json and resp.sql:
                _render_pipeline(resp.sql, resp.explain_pretty, resp.pipeline_request_json)
            continue

        if resp.interrupt and resp.interrupt.get("question"):
            print(resp.interrupt.get("question"), file=sys.stderr)
            continue

        # Fallback: make "no output" visible.
        last_nodes = [t.get("node", "") for t in (resp.trace or []) if isinstance(t, dict)]
        suffix = ""
        if last_nodes:
            suffix = " last_nodes=" + "→".join([n for n in last_nodes[-5:] if n])
        print(f"(no output){suffix}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
