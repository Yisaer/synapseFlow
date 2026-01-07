from __future__ import annotations

import json
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

from ..shared.catalogs import CapabilitiesDigest
from ..shared.history import HistoryBuffer
from ..shared.mcp_client import McpError, SynapseFlowMcpClient
from ..shared.prompts import default_assistant_instructions
from ..shared.requests import build_create_pipeline_request
from ..shared.router import Intent, route_intent
from .state import State


def _now_ms() -> int:
    return int(time.time() * 1000)


def _is_conflict_error(msg: str) -> bool:
    m = msg.lower()
    return "http 409" in m or "already exists" in m or "conflict" in m


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


def _get_history(state: State) -> HistoryBuffer:
    h = state.get("history") or {}
    summary = str(h.get("summary", "") or "")
    tail = h.get("tail") or []
    max_tail = int(h.get("max_tail") or 20)
    if not isinstance(tail, list):
        tail = []
    return HistoryBuffer(summary=summary, tail=list(tail), max_tail=max_tail)


def _save_history(state: State, history: HistoryBuffer) -> None:
    state["history"] = {
        "summary": history.summary,
        "tail": list(history.tail),
        "max_tail": int(history.max_tail),
    }


def _ensure_state_defaults(state: State) -> None:
    state.setdefault("notes", [])
    state.setdefault("trace", [])
    if "history" not in state:
        state["history"] = {"summary": "", "tail": [], "max_tail": 20}


def _append_trace(state: State, node: str, *, ok: bool, latency_ms: int, error: str | None = None) -> None:
    trace = state.get("trace") or []
    if not isinstance(trace, list):
        trace = []
    trace.append({"node": node, "ok": ok, "latency_ms": latency_ms, "error": error})
    state["trace"] = trace


def _traced(node_name: str, fn: Callable[[State], Dict[str, Any] | State]) -> Callable[[State], Dict[str, Any]]:
    def _wrapped(state: State) -> Dict[str, Any]:
        _ensure_state_defaults(state)
        start = _now_ms()
        try:
            out = fn(state)
            next_state: Dict[str, Any] = out if isinstance(out, dict) else dict(out)
            _ensure_state_defaults(next_state)  # ensure trace list exists in returned state
            latency = _now_ms() - start
            _append_trace(next_state, node_name, ok=True, latency_ms=latency, error=None)
            return next_state
        except Exception as e:
            next_state = dict(state)
            _ensure_state_defaults(next_state)
            latency = _now_ms() - start
            _append_trace(next_state, node_name, ok=False, latency_ms=latency, error=str(e))
            raise

    return _wrapped


def build_langgraph_workflow(
    *,
    synapse: SynapseFlowMcpClient,
    llm: Any,
    llm_router_model: str,
    llm_preview_model: str,
    llm_draft_model: str,
    llm_json_mode: bool,
    sink_broker_url: str,
    sink_topic: str,
    sink_qos: int,
    default_max_attempts: int,
) -> Any:
    """
    Returns a compiled LangGraph graph.

    `llm` is expected to be a ChatCompletionsClient-like object providing:
      - complete_text(model, messages, temperature)
      - complete_json(model, messages, temperature, response_format_json, stream, on_stream_delta)
    """

    try:
        from langgraph.graph import END, StateGraph  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "langgraph is not installed. Install with: pip install -r agents/nl2pipeline/requirements.txt"
        ) from e

    def init_session(state: State) -> Dict[str, Any]:
        if state.get("initialized") is True:
            return dict(state)

        if "catalog_digest" not in state:
            digest = synapse.build_capabilities_digest()
            state["catalog_digest"] = digest.to_json()

        streams = synapse.streams_list()
        state["available_streams"] = streams

        # If a default stream is set, eagerly fetch schema once.
        if state.get("active_stream") and state.get("stream_schema") is None:
            try:
                desc = synapse.streams_describe(str(state["active_stream"]))
                state["stream_schema"] = (desc.get("spec") or {}).get("schema") or {}
            except McpError:
                state["active_stream"] = None
                state["stream_schema"] = None

        # Seed a deterministic note for later turns (kept in summary).
        history = _get_history(state)
        history.add_note(f"Available streams: {_summarize_stream_names(streams)}.")
        _save_history(state, history)
        state["initialized"] = True
        return dict(state)

    def pre_turn(state: State) -> Dict[str, Any]:
        # `confirmed` is per-turn; reset for each turn.
        state["confirmed"] = False
        ui = state.get("user_input") or {}
        state["user_input"] = ui
        if "validation" not in state:
            state["validation"] = {"attempt": 1, "max_attempts": default_max_attempts}
        else:
            v = state["validation"]
            if isinstance(v, dict):
                v.setdefault("attempt", 1)
                v.setdefault("max_attempts", default_max_attempts)

        # Refresh streams every turn for correctness.
        state["available_streams"] = synapse.streams_list()

        interrupt = state.get("interrupt")
        # New user request: clear per-turn outputs so we never leak stale SQL/EXPLAIN across turns.
        # If we're currently in an interrupt/resume flow, keep the previous draft/explain intact.
        if not interrupt:
            user_text = str(ui.get("text") or "").strip()
            if user_text:
                state["draft"] = {"preview_text": None, "sql": None, "assumptions": [], "questions": []}
                state["explain"] = {"pretty": None, "pipeline_request_json": None}
                v = state.get("validation") or {}
                if not isinstance(v, dict):
                    v = {}
                v["tmp_pipeline_id"] = None
                v["error"] = None
                v["attempt"] = 1
                v["max_attempts"] = int(v.get("max_attempts") or default_max_attempts)
                state["validation"] = v  # type: ignore[assignment]
                state["pending_action"] = None
                state["intent"] = None
                state["intent_stream"] = None
                state["intent_question"] = None

        if interrupt and isinstance(interrupt, dict):
            itype = str(interrupt.get("type", "")).strip()
            pending = str(state.get("pending_action") or "").strip()

            if itype == "need_confirm":
                if ui.get("confirm") is True:
                    state["confirmed"] = True
                    state["interrupt"] = None
                    state["pending_action"] = None
                    return dict(state)
                feedback = str(ui.get("feedback") or "").strip()
                if feedback:
                    # Continue the workflow via the revise loop.
                    state["interrupt"] = None
                    state["pending_action"] = "revise"
                    return dict(state)
                return dict(state)

            if itype == "need_stream":
                answer = str(ui.get("text") or "").strip()
                names = [str(s.get("name", "")).strip() for s in state.get("available_streams") or []]
                if answer and answer in names:
                    desc = synapse.streams_describe(answer)
                    schema = (desc.get("spec") or {}).get("schema") or {}
                    state["active_stream"] = answer
                    state["stream_schema"] = schema
                    state["interrupt"] = None
                    state["pending_action"] = pending or "nl2sql"
                    history = _get_history(state)
                    history.add_note(f"Active stream set to {answer}.")
                    _save_history(state, history)
                    return dict(state)
                # Keep interrupt; ask again.
                state["interrupt"] = {
                    "type": "need_stream",
                    "question": interrupt.get("question") or "Which stream should be used?",
                    "choices": names,
                }
                return dict(state)

            if itype == "need_clarification":
                clarification = str(ui.get("text") or "").strip()
                if clarification:
                    base = str(state.get("last_requirement") or "").strip()
                    state["last_requirement"] = (base + "\n" + clarification).strip() if base else clarification
                    state["interrupt"] = None
                    state["pending_action"] = pending or "nl2sql"
                return dict(state)

        return dict(state)

    def _pre_turn_next(state: State) -> str:
        interrupt = state.get("interrupt")
        ui = state.get("user_input") or {}
        pending = str(state.get("pending_action") or "").strip()

        # If the user is responding to a confirmation prompt (confirm=true), `pre_turn` will clear
        # the interrupt. Some providers require we avoid routing on this "control-only" turn.
        if ui.get("confirm") is True and state.get("confirmed") is True:
            return "end"

        if interrupt and isinstance(interrupt, dict):
            itype = str(interrupt.get("type", "")).strip()
            if itype == "need_confirm" and ui.get("confirm") is True:
                return "end"
            if pending == "revise":
                return "revise"
            if itype in ("need_stream", "need_clarification"):
                return pending or "route"
            return "end"
        if pending == "revise":
            return "revise"
        if pending == "describe_stream":
            return "describe_stream"
        if pending == "nl2sql":
            return "nl2sql"
        return "route"

    def do_route_intent(state: State) -> Dict[str, Any]:
        ui = state.get("user_input") or {}
        user_text = str(ui.get("text") or "").strip()
        if not user_text:
            state["interrupt"] = {
                "type": "need_clarification",
                "question": "Please enter a request.",
            }
            return dict(state)

        streams = state.get("available_streams") or []
        decision = route_intent(
            llm=llm,
            model=llm_router_model,
            user_text=user_text,
            streams=streams,
            active_stream=state.get("active_stream"),
            json_mode=llm_json_mode,
        )
        state["intent"] = decision.intent.value  # type: ignore[assignment]
        state["intent_stream"] = decision.stream  # type: ignore[assignment]
        state["intent_question"] = decision.question  # type: ignore[assignment]
        return dict(state)

    def _route_next(state: State) -> str:
        intent = str(state.get("intent") or "").strip()
        if intent == Intent.ListStreams.value:
            return "list_streams"
        if intent == Intent.DescribeStream.value:
            return "describe_stream"
        if intent == Intent.Nl2Sql.value:
            return "ensure_stream"
        return "ask_user"

    def handle_list_streams(state: State) -> Dict[str, Any]:
        streams = synapse.streams_list()
        state["available_streams"] = streams
        history = _get_history(state)
        history.add_note(f"Stream list requested; available streams: {_summarize_stream_names(streams)}.")
        _save_history(state, history)
        state["interrupt"] = None
        state["pending_action"] = None
        return dict(state)

    def handle_describe_stream(state: State) -> Dict[str, Any]:
        target = state.get("intent_stream") or state.get("active_stream")
        if not target:
            state["interrupt"] = {
                "type": "need_stream",
                "question": state.get("intent_question") or "Which stream?",
                "choices": [s.get("name", "") for s in state.get("available_streams") or []],
            }
            state["pending_action"] = "describe_stream"
            return dict(state)
        desc = synapse.streams_describe(str(target))
        schema = (desc.get("spec") or {}).get("schema") or {}
        state["active_stream"] = str(target)
        state["stream_schema"] = schema
        history = _get_history(state)
        history.add_note(f"Schema requested; stream={target}.")
        _save_history(state, history)
        state["interrupt"] = None
        state["pending_action"] = None
        return dict(state)

    def handle_ask_user(state: State) -> Dict[str, Any]:
        q = state.get("intent_question") or "I need more information."
        state["interrupt"] = {"type": "need_clarification", "question": q}
        state["pending_action"] = None
        return dict(state)

    def ensure_active_stream(state: State) -> Dict[str, Any]:
        if state.get("active_stream") and state.get("stream_schema") is not None:
            return dict(state)
        candidate = state.get("intent_stream")
        if candidate:
            desc = synapse.streams_describe(str(candidate))
            schema = (desc.get("spec") or {}).get("schema") or {}
            state["active_stream"] = str(candidate)
            state["stream_schema"] = schema
            return dict(state)
        state["interrupt"] = {
            "type": "need_stream",
            "question": state.get("intent_question") or "Which stream should be used?",
            "choices": [s.get("name", "") for s in state.get("available_streams") or []],
        }
        state["pending_action"] = "nl2sql"
        return dict(state)

    def draft_preview_sql(state: State) -> Dict[str, Any]:
        ui = state.get("user_input") or {}
        user_text = str(ui.get("text") or "").strip()
        if str(state.get("pending_action") or "").strip() == "nl2sql" and state.get("last_requirement"):
            # Resume after an interrupt (e.g. stream selection). Keep the original requirement.
            user_text = str(state.get("last_requirement") or "").strip()
        else:
            state["last_requirement"] = user_text

        history = _get_history(state)
        payload = {
            "mode": "preview_sql",
            "nl": user_text,
            "active_stream": state.get("active_stream") or "",
            "stream_schema": state.get("stream_schema") or {},
            "output_rules": "Output a single SQL statement OR a single line `QUESTION: ...`. No extra text.",
        }
        messages = history.to_messages(
            system_prompt=default_assistant_instructions(),
            digest=state.get("catalog_digest"),
            extra_context=payload,
        )
        preview = llm.complete_text(model=llm_preview_model, messages=messages, temperature=0.0)
        preview = preview.strip()
        state.setdefault("draft", {})
        state["draft"]["preview_text"] = preview  # type: ignore[index]

        # Keep history small and unambiguous: only store the user's natural-language request.
        # The structured payload (including `mode` and full schema) is provided as per-turn context.
        if user_text.strip():
            history.add_user(user_text)
        history.add_assistant(preview)
        _save_history(state, history)

        lower = preview.lower().strip()
        if lower.startswith("question:"):
            q = preview.split(":", 1)[1].strip() if ":" in preview else preview.strip()
            state["draft"]["questions"] = [q]  # type: ignore[index]
            state["interrupt"] = {"type": "need_clarification", "question": q or "Please clarify."}
            state["pending_action"] = "nl2sql"
            return dict(state)

        sql = preview.strip().rstrip(";").strip() + ";"
        state["draft"]["sql"] = sql  # type: ignore[index]
        state["draft"]["questions"] = []  # type: ignore[index]
        state["draft"].setdefault("assumptions", [])  # type: ignore[index]
        state["pending_action"] = None
        return dict(state)

    def _create_tmp_pipeline_with_retry(state: State, sql: str) -> str:
        for _ in range(3):
            tmp_id = f"__nl2pipeline_tmp__{uuid.uuid4().hex}"
            req = build_create_pipeline_request(
                pipeline_id=tmp_id,
                sql=sql,
                sink_broker_url=sink_broker_url,
                sink_topic=sink_topic,
                sink_qos=sink_qos,
            )
            try:
                synapse.pipelines_create(req)
                return tmp_id
            except McpError as e:
                if _is_conflict_error(str(e)):
                    continue
                raise
        raise McpError(
            code="planner_error",
            message="POST /pipelines failed: could not allocate a unique temporary pipeline id",
        )

    def validate_sql(state: State) -> Dict[str, Any]:
        sql = ((state.get("draft") or {}).get("sql") or "").strip()
        if not sql:
            state["interrupt"] = {
                "type": "need_clarification",
                "question": "No SQL draft was produced. Please rephrase the request.",
            }
            return dict(state)

        state.setdefault("validation", {"attempt": 1, "max_attempts": default_max_attempts})
        tmp_id: Optional[str] = None
        try:
            tmp_id = _create_tmp_pipeline_with_retry(state, sql)
        except McpError as e:
            state["validation"]["error"] = str(e)  # type: ignore[index]
            state["validation"]["tmp_pipeline_id"] = None  # type: ignore[index]
            return dict(state)

        state["validation"]["tmp_pipeline_id"] = tmp_id  # type: ignore[index]
        state["validation"]["error"] = None  # type: ignore[index]
        return dict(state)

    def _validate_next(state: State) -> str:
        err = ((state.get("validation") or {}).get("error") or "").strip()
        if err:
            attempt = int((state.get("validation") or {}).get("attempt") or 1)
            max_attempts = int((state.get("validation") or {}).get("max_attempts") or default_max_attempts)
            if attempt >= max_attempts:
                return "give_up"
            return "revise"
        return "explain"

    def revise_sql_json(state: State) -> Dict[str, Any]:
        ui = state.get("user_input") or {}
        feedback = str(ui.get("feedback") or "").strip()
        requirement = str(state.get("last_requirement") or str(ui.get("text") or "")).strip()
        if feedback:
            requirement = (requirement + "\nUser feedback: " + feedback).strip()

        previous_sql = ((state.get("draft") or {}).get("sql") or "").strip()
        previous_error = ((state.get("validation") or {}).get("error") or "").strip()

        history = _get_history(state)
        payload = {
            "mode": "json_candidate",
            "nl": requirement,
            "active_stream": state.get("active_stream") or "",
            "stream_schema": state.get("stream_schema") or {},
            "previous_sql": previous_sql,
            "previous_error": previous_error,
            "output_schema": {
                "sql": "string (required)",
                "questions": "string[] (optional)",
                "assumptions": "string[] (optional)",
            },
        }
        messages = history.to_messages(
            system_prompt=default_assistant_instructions(),
            digest=state.get("catalog_digest"),
            extra_context=payload,
        )
        parsed = llm.complete_json(
            model=llm_draft_model,
            messages=messages,
            temperature=0.0,
            response_format_json=llm_json_mode,
            stream=False,
            on_stream_delta=None,
        )
        sql = str(parsed.get("sql", "")).strip()
        questions = parsed.get("questions", []) or []
        assumptions = parsed.get("assumptions", []) or []

        state.setdefault("draft", {})
        state["draft"]["sql"] = sql.rstrip(";").strip() + ";" if sql else ""  # type: ignore[index]
        state["draft"]["questions"] = [str(x) for x in questions]  # type: ignore[index]
        state["draft"]["assumptions"] = [str(x) for x in assumptions]  # type: ignore[index]

        v = state.get("validation") or {}
        attempt = int(v.get("attempt") or 1) + 1
        v["attempt"] = attempt
        state["validation"] = v  # type: ignore[assignment]
        state["pending_action"] = None
        return dict(state)

    def explain_tmp_pipeline(state: State) -> Dict[str, Any]:
        tmp_id = ((state.get("validation") or {}).get("tmp_pipeline_id") or "").strip()
        if not tmp_id:
            raise McpError(code="invalid_state", message="missing tmp_pipeline_id for explain")
        explain = synapse.pipelines_explain(tmp_id)
        state["explain"] = {
            "pretty": explain,
            "pipeline_request_json": build_create_pipeline_request(
                pipeline_id="REPLACE_ME",
                sql=((state.get("draft") or {}).get("sql") or "").strip(),
                sink_broker_url=sink_broker_url,
                sink_topic=sink_topic,
                sink_qos=sink_qos,
            ),
        }
        return dict(state)

    def cleanup_tmp_pipeline(state: State) -> Dict[str, Any]:
        tmp_id = ((state.get("validation") or {}).get("tmp_pipeline_id") or "").strip()
        if tmp_id:
            try:
                synapse.pipelines_delete(tmp_id)
            except McpError:
                pass
        v = state.get("validation") or {}
        if isinstance(v, dict):
            v["tmp_pipeline_id"] = None
            state["validation"] = v  # type: ignore[assignment]
        return dict(state)

    def confirm_interrupt(state: State) -> Dict[str, Any]:
        state["interrupt"] = {"type": "need_confirm", "question": "Is this correct?"}
        state["pending_action"] = None
        return dict(state)

    def give_up(state: State) -> Dict[str, Any]:
        err = ((state.get("validation") or {}).get("error") or "").strip()
        state["interrupt"] = {
            "type": "need_clarification",
            "question": f"Failed to produce a valid pipeline after retries. Last error: {err}",
        }
        state["pending_action"] = None
        return dict(state)

    graph = StateGraph(State)

    graph.add_node("init_session", _traced("init_session", init_session))
    graph.add_node("pre_turn", _traced("pre_turn", pre_turn))
    graph.add_node("route_intent", _traced("route_intent", do_route_intent))
    graph.add_node("list_streams", _traced("list_streams", handle_list_streams))
    graph.add_node("describe_stream", _traced("describe_stream", handle_describe_stream))
    graph.add_node("ask_user", _traced("ask_user", handle_ask_user))
    graph.add_node("ensure_stream", _traced("ensure_stream", ensure_active_stream))
    graph.add_node("draft_preview_sql", _traced("draft_preview_sql", draft_preview_sql))
    graph.add_node("validate_sql", _traced("validate_sql", validate_sql))
    graph.add_node("revise_sql", _traced("revise_sql", revise_sql_json))
    graph.add_node("explain_tmp", _traced("explain_tmp", explain_tmp_pipeline))
    graph.add_node("cleanup_tmp", _traced("cleanup_tmp", cleanup_tmp_pipeline))
    graph.add_node("confirm", _traced("confirm", confirm_interrupt))
    graph.add_node("give_up", _traced("give_up", give_up))

    graph.set_entry_point("init_session")
    graph.add_edge("init_session", "pre_turn")

    graph.add_conditional_edges(
        "pre_turn",
        _pre_turn_next,
        {
            "route": "route_intent",
            "revise": "revise_sql",
            "end": END,
            "describe_stream": "describe_stream",
            "nl2sql": "draft_preview_sql",
        },
    )

    graph.add_conditional_edges(
        "route_intent",
        _route_next,
        {
            "list_streams": "list_streams",
            "describe_stream": "describe_stream",
            "ensure_stream": "ensure_stream",
            "ask_user": "ask_user",
        },
    )

    graph.add_edge("list_streams", END)
    graph.add_edge("describe_stream", END)
    graph.add_edge("ask_user", END)

    graph.add_edge("ensure_stream", "draft_preview_sql")
    graph.add_edge("draft_preview_sql", "validate_sql")

    graph.add_conditional_edges(
        "validate_sql",
        _validate_next,
        {
            "explain": "explain_tmp",
            "revise": "revise_sql",
            "give_up": "give_up",
        },
    )

    graph.add_edge("revise_sql", "validate_sql")
    graph.add_edge("give_up", END)

    graph.add_edge("explain_tmp", "cleanup_tmp")
    graph.add_edge("cleanup_tmp", "confirm")
    graph.add_edge("confirm", END)

    return graph.compile()


__all__ = ["build_langgraph_workflow"]
