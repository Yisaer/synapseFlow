from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ..shared.mcp_client import McpError, VeloFluxMcpClient
from .router import Intent, route_intent
from .workflow import EventKind, PipelineCandidate, TurnContext, TurnInput, Workflow


@dataclass
class ReplState:
    active_stream: Optional[str]
    stream_schema: Optional[Dict[str, Any]]
    last_sql: Optional[str] = None
    last_explain: Optional[str] = None
    last_pipeline_json: Optional[Dict[str, Any]] = None


def _print_help() -> None:
    print(
        "\n".join(
            [
                "Commands:",
                "  /help",
                "  /streams",
                "  /use <stream_name>",
                "  /show sql",
                "  /show explain",
                "  /show pipeline",
                "  /exit",
                "",
                "Otherwise, type a natural-language message and press Enter.",
                "The agent will route it to either:",
                "  - list streams (GET /streams)",
                "  - describe stream schema (GET /streams/describe/:name)",
                "  - NL→SQL pipeline drafting (draft→validate→explain loop)",
            ]
        )
    )


def _print_streams(streams: List[Dict[str, Any]]) -> None:
    for s in streams:
        print(str(s.get("name", "")))


def _format_streams_table(streams: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for s in streams:
        if not isinstance(s, dict):
            continue
        name = str(s.get("name", "")).strip()
        cols = (((s.get("schema") or {}).get("columns")) or []) if isinstance(s, dict) else []
        col_count = len(cols) if isinstance(cols, list) else 0
        sample = []
        if isinstance(cols, list):
            for c in cols[:5]:
                if isinstance(c, dict) and c.get("name"):
                    sample.append(str(c["name"]))
        suffix = f" cols={col_count}"
        if sample:
            suffix += f" sample={','.join(sample)}"
        lines.append(f"- {name}{suffix}")
    return "\n".join(lines)


def _summarize_stream_names(streams: List[Dict[str, Any]], max_items: int = 20) -> str:
    names: List[str] = []
    for s in streams:
        if isinstance(s, dict) and s.get("name"):
            names.append(str(s["name"]).strip())
    names = [n for n in names if n]
    if len(names) <= max_items:
        return ", ".join(names)
    head = ", ".join(names[:max_items])
    return f"{head}, ... (+{len(names) - max_items} more)"


def _summarize_schema(schema: Dict[str, Any], max_cols: int = 12) -> str:
    cols = schema.get("columns") or []
    if not isinstance(cols, list) or not cols:
        return "(no columns)"
    parts: List[str] = []
    for c in cols[:max_cols]:
        if not isinstance(c, dict):
            continue
        name = str(c.get("name", "")).strip()
        dtype = str(c.get("data_type", "")).strip()
        if name:
            parts.append(f"{name}:{dtype}" if dtype else name)
    if len(cols) > max_cols:
        return ", ".join(parts) + f", ... (+{len(cols) - max_cols} more)"
    return ", ".join(parts)


def _select_stream_interactively(streams: List[Dict[str, Any]]) -> str:
    print("Available streams:", file=sys.stderr)
    for idx, s in enumerate(streams, start=1):
        name = s.get("name", "")
        cols = (((s.get("schema") or {}).get("columns")) or []) if isinstance(s, dict) else []
        sample_cols = [c.get("name", "") for c in cols[:5] if isinstance(c, dict)]
        suffix = f" cols={len(cols)} sample={','.join([x for x in sample_cols if x])}" if cols else ""
        print(f"  {idx}) {name}{suffix}", file=sys.stderr)

    while True:
        raw = input("Select stream by number or name: ").strip()
        if not raw:
            continue
        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(streams):
                return str(streams[idx - 1].get("name", "")).strip()
        for s in streams:
            if str(s.get("name", "")).strip() == raw:
                return raw
        print("Invalid selection.", file=sys.stderr)


def _get_stream_schema(manager: VeloFluxMcpClient, stream: str) -> Dict[str, Any]:
    desc = manager.streams_describe(stream)
    return (desc.get("spec") or {}).get("schema") or {}


def _render_result(result: PipelineCandidate) -> None:
    print("\nSQL:\n" + result.sql)
    if result.assumptions:
        print("\nAssumptions:")
        for a in result.assumptions:
            print(f"- {a}")
    print("\nEXPLAIN:\n" + result.explain_pretty)
    print(
        "\nPipeline JSON (manual apply):\n"
        + json.dumps(result.create_pipeline_request, indent=2, ensure_ascii=False)
    )


def run_repl(
    manager: VeloFluxMcpClient,
    workflow: Workflow,
    router_model: str,
    initial_stream_name: str,
    check_max_attempts: int,
) -> int:
    streams = manager.streams_list()
    if not streams:
        raise McpError(code="not_found", message="no streams found (create one first via Manager)")

    active_stream = initial_stream_name.strip()

    state = ReplState(active_stream=None, stream_schema=None)
    workflow.seed_session()

    if active_stream:
        state = ReplState(
            active_stream=active_stream,
            stream_schema=_get_stream_schema(manager, active_stream),
        )
        workflow.update_active_stream(
            TurnContext(active_stream=state.active_stream, stream_schema=state.stream_schema)
        )

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
        if line == "/streams":
            _print_streams(manager.streams_list())
            continue
        if line == "/trace":
            trace = manager.trace()
            if not trace:
                print("(no trace)", file=sys.stderr)
            else:
                for t in trace[-30:]:
                    print(json.dumps(t, ensure_ascii=False), file=sys.stderr)
            continue
        if line.startswith("/use "):
            name = line[len("/use ") :].strip()
            streams = manager.streams_list()
            if not any(s.get("name") == name for s in streams):
                print(f"stream not found: {name}", file=sys.stderr)
                continue
            state = ReplState(
                active_stream=name,
                stream_schema=_get_stream_schema(manager, name),
                last_sql=state.last_sql,
                last_explain=state.last_explain,
                last_pipeline_json=state.last_pipeline_json,
            )
            workflow.update_active_stream(TurnContext(active_stream=name, stream_schema=state.stream_schema))
            print(f"active stream set to {state.active_stream}", file=sys.stderr)
            continue
        if line == "/show sql":
            print(state.last_sql or "")
            continue
        if line == "/show explain":
            print(state.last_explain or "")
            continue
        if line == "/show pipeline":
            print(json.dumps(state.last_pipeline_json or {}, indent=2, ensure_ascii=False))
            continue

        user_text = line
        streams = manager.streams_list()
        try:
            decision = route_intent(
                llm=workflow.llm,
                model=router_model,
                user_text=user_text,
                streams=streams,
                active_stream=state.active_stream,
                json_mode=workflow.llm_json_mode,
            )
        except Exception as e:
            print(f"[ROUTER] failed: {e}", file=sys.stderr)
            continue
        print(f"[ROUTER] intent={decision.intent.value}", file=sys.stderr)

        if decision.intent == Intent.ListStreams:
            print(_format_streams_table(streams))
            workflow.add_context_note(
                f"Stream list requested; available streams: {_summarize_stream_names(streams)}."
            )
            continue

        if decision.intent == Intent.DescribeStream:
            target = decision.stream or state.active_stream
            if not target:
                question = decision.question or "Which stream?"
                print(question, file=sys.stderr)
                target = _select_stream_interactively(streams)
            schema = _get_stream_schema(manager, target)
            print(f"stream: {target}")
            cols = (schema.get("columns") or []) if isinstance(schema, dict) else []
            if not isinstance(cols, list) or not cols:
                print("(no columns)")
            else:
                for c in cols:
                    if isinstance(c, dict):
                        print(f"- {c.get('name','')}: {c.get('data_type','')}")
            workflow.add_context_note(
                f"Schema requested; stream={target} columns: {_summarize_schema(schema)}."
            )
            state = ReplState(
                active_stream=target,
                stream_schema=schema,
                last_sql=state.last_sql,
                last_explain=state.last_explain,
                last_pipeline_json=state.last_pipeline_json,
            )
            workflow.update_active_stream(
                TurnContext(active_stream=state.active_stream, stream_schema=state.stream_schema)
            )
            continue

        if decision.intent == Intent.AskUser:
            print(decision.question or "I need more information.", file=sys.stderr)
            continue

        # NL→SQL.
        if decision.stream and decision.stream != state.active_stream:
            schema = _get_stream_schema(manager, decision.stream)
            state = ReplState(
                active_stream=decision.stream,
                stream_schema=schema,
                last_sql=state.last_sql,
                last_explain=state.last_explain,
                last_pipeline_json=state.last_pipeline_json,
            )
            workflow.update_active_stream(
                TurnContext(active_stream=state.active_stream, stream_schema=state.stream_schema)
            )

        if state.active_stream is None or state.stream_schema is None:
            question = decision.question or "Which stream should be used?"
            print(question, file=sys.stderr)
            selected = _select_stream_interactively(streams)
            schema = _get_stream_schema(manager, selected)
            state = ReplState(
                active_stream=selected,
                stream_schema=schema,
                last_sql=state.last_sql,
                last_explain=state.last_explain,
                last_pipeline_json=state.last_pipeline_json,
            )
            workflow.update_active_stream(
                TurnContext(active_stream=state.active_stream, stream_schema=state.stream_schema)
            )

        original_prompt = user_text
        ctx = TurnContext(active_stream=state.active_stream, stream_schema=state.stream_schema)
        turn = TurnInput(
            prompt=user_text,
            max_attempts=check_max_attempts,
            previous_sql=state.last_sql,
        )

        last_result: Optional[PipelineCandidate] = None
        needs_user_input: Optional[List[str]] = None
        final_error: Optional[str] = None

        preview_started = False
        preview_emitted = False

        for ev in workflow.run_turn(ctx, turn):
            if ev.kind == EventKind.PhaseChanged and ev.phase is not None:
                if ev.phase.value == "draft_sql" and not preview_started:
                    preview_started = True
                    print("[LLM] drafting SQL", file=sys.stderr)

            if ev.kind == EventKind.DraftPreviewDelta and ev.text_delta:
                if not preview_emitted:
                    print("[LLM] SQL preview:", file=sys.stderr)
                    preview_emitted = True
                print(ev.text_delta, end="", file=sys.stderr, flush=True)

            if ev.kind == EventKind.NeedUserInput and ev.candidate is not None:
                needs_user_input = ev.candidate.questions
            if ev.kind == EventKind.Explained and ev.result is not None:
                last_result = ev.result
            if ev.kind == EventKind.Failed:
                final_error = ev.error
            if ev.kind == EventKind.CandidateGenerated and preview_emitted:
                print("", file=sys.stderr)

        if needs_user_input:
            print("Questions:", file=sys.stderr)
            for q in needs_user_input:
                print(f"- {q}", file=sys.stderr)
            continue

        if final_error and not last_result:
            print(f"Failed to produce a valid pipeline: {final_error}", file=sys.stderr)
            continue

        if not last_result:
            print("No result.", file=sys.stderr)
            continue

        _render_result(last_result)
        state.last_sql = last_result.sql
        state.last_explain = last_result.explain_pretty
        state.last_pipeline_json = last_result.create_pipeline_request

        feedback = input("\nIs this correct? (y=done / n=revise): ").strip().lower()
        if feedback in ("y", "yes"):
            continue

        revision_text = input("What should be changed? ").strip()
        if revision_text:
            revised_prompt = f"{original_prompt}\nUser feedback: {revision_text}"
            turn = TurnInput(
                prompt=revised_prompt,
                max_attempts=check_max_attempts,
                previous_sql=state.last_sql,
            )
            for ev in workflow.run_turn(ctx, turn):
                if ev.kind == EventKind.Explained and ev.result is not None:
                    last_result = ev.result
                if ev.kind == EventKind.Failed:
                    final_error = ev.error
            if last_result:
                _render_result(last_result)
                state.last_sql = last_result.sql
                state.last_explain = last_result.explain_pretty
                state.last_pipeline_json = last_result.create_pipeline_request

    return 0
