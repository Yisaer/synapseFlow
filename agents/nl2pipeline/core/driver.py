from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

from ..shared.chat_client import ChatCompletionsClient, LlmError
from ..shared.config import AppConfig
from ..shared.manager_client import ManagerClient
from ..shared.mcp import EmbeddedMcpRuntime, McpRegistry, register_veloflux_mcp
from ..shared.mcp_client import McpError, VeloFluxMcpClient
from .session_store import InMemorySessionStore
from .state import State, TurnRequest, TurnResponse
from .workflow import build_langgraph_workflow


@dataclass
class LangGraphEngine:
    veloflux: VeloFluxMcpClient
    llm: ChatCompletionsClient
    cfg: AppConfig
    store: InMemorySessionStore
    graph: Any

    @classmethod
    def new(cls, cfg: AppConfig) -> "LangGraphEngine":
        manager = ManagerClient.new(cfg.manager.url, cfg.manager.timeout_secs)
        registry = McpRegistry()
        register_veloflux_mcp(registry, manager)
        runtime = EmbeddedMcpRuntime.new(registry)
        veloflux = VeloFluxMcpClient(runtime=runtime)
        llm = ChatCompletionsClient.new(cfg.llm.base_url, cfg.llm.api_key, cfg.llm.timeout_secs)
        store = InMemorySessionStore.new()
        graph = build_langgraph_workflow(
            veloflux=veloflux,
            llm=llm,
            llm_router_model=cfg.llm.router_model,
            llm_preview_model=cfg.llm.preview_model,
            llm_draft_model=cfg.llm.draft_model,
            llm_json_mode=cfg.llm.json_mode,
            sink_broker_url=cfg.sink.broker_url,
            sink_topic=cfg.sink.topic,
            sink_qos=cfg.sink.qos,
            default_max_attempts=cfg.repl.check_max_attempts,
        )
        return cls(veloflux=veloflux, llm=llm, cfg=cfg, store=store, graph=graph)

    def create_session(self) -> str:
        # Minimal initial state; init_session node will populate catalogs/streams.
        initial: State = {
            "initialized": False,
            "active_stream": self.cfg.stream.default.strip() or None,
            "stream_schema": None,
            "available_streams": [],
            "notes": [],
            "history": {"summary": "", "tail": [], "max_tail": 20},
            "draft": {"preview_text": None, "sql": None, "assumptions": [], "questions": []},
            "validation": {"attempt": 1, "max_attempts": self.cfg.repl.check_max_attempts},
            "explain": {"pretty": None, "pipeline_request_json": None},
            "interrupt": None,
            "pending_action": None,
            "confirmed": False,
            "trace": [],
            "user_input": {"text": ""},
            "last_requirement": None,
        }
        session_id = self.store.create(initial)
        return session_id

    def run_turn(self, session_id: str, req: TurnRequest) -> TurnResponse:
        state = self.store.get(session_id)
        state["user_input"] = {
            "text": req.text or "",
            "confirm": req.confirm,
            "feedback": req.feedback,
            "stream": bool(req.stream),
        }
        state = self._invoke(state)
        self.store.save(session_id, state)
        return self._to_response(state)

    def get_trace(self, session_id: str) -> list[Dict[str, Any]]:
        return self.store.list_trace(session_id)

    def _invoke(self, state: State) -> State:
        try:
            out = self.graph.invoke(state)
            # Be defensive: different graph implementations may return either the full state or
            # just a dict of updates. Merge into the input state so callers always get a full state.
            next_state = dict(state)
            updates = out if isinstance(out, dict) else dict(out)
            next_state.update(updates)
            return next_state
        except (McpError, LlmError) as e:
            state = dict(state)
            state["interrupt"] = {"type": "need_clarification", "question": str(e)}
            return state
        except Exception as e:
            state = dict(state)
            state["interrupt"] = {
                "type": "need_clarification",
                "question": f"internal error: {e}",
            }
            return state

    def _to_response(self, state: State) -> TurnResponse:
        interrupt = state.get("interrupt")
        explain = state.get("explain") or {}
        draft = state.get("draft") or {}
        streams = None
        schema = None

        intent = str(state.get("intent") or "").strip()
        if intent == "list_streams":
            streams = state.get("available_streams") or []
        if intent == "describe_stream":
            schema = state.get("stream_schema")

        done = bool(state.get("confirmed") is True)

        return TurnResponse(
            session_id=str(state.get("session_id") or ""),
            done=done,
            interrupt=interrupt if isinstance(interrupt, dict) else None,
            intent=intent or None,
            sql=str(draft.get("sql") or "") or None,
            explain_pretty=str(explain.get("pretty") or "") or None,
            pipeline_request_json=explain.get("pipeline_request_json")
            if isinstance(explain.get("pipeline_request_json"), dict)
            else None,
            streams=streams,
            schema=schema if isinstance(schema, dict) else None,
            trace=list(state.get("trace") or []),
        )


def response_to_json(resp: TurnResponse) -> Dict[str, Any]:
    return {
        "session_id": resp.session_id,
        "done": resp.done,
        "interrupt": resp.interrupt,
        "intent": resp.intent,
        "sql": resp.sql,
        "explain_pretty": resp.explain_pretty,
        "pipeline_request_json": resp.pipeline_request_json,
        "streams": resp.streams,
        "schema": resp.schema,
        "trace": resp.trace,
    }


def parse_turn_request(obj: Dict[str, Any]) -> TurnRequest:
    return TurnRequest(
        text=str(obj.get("text", "") or ""),
        confirm=obj.get("confirm"),
        feedback=str(obj.get("feedback", "") or "") or None,
        stream=bool(obj.get("stream", False)),
    )


__all__ = ["LangGraphEngine", "parse_turn_request", "response_to_json"]
