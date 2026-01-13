from __future__ import annotations

import json
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterator, List, Optional

from ..shared.chat_client import ChatCompletionsClient, LlmError
from ..shared.catalogs import CapabilitiesDigest
from ..shared.prompts import default_assistant_instructions
from ..shared.requests import build_create_pipeline_request, build_create_stream_request
from ..shared.mcp_client import McpError, VeloFluxMcpClient


class Phase(str, Enum):
    DraftSql = "draft_sql"
    ValidatePipeline = "validate_pipeline"
    ExplainPipeline = "explain_pipeline"
    Done = "done"
    Failed = "failed"


@dataclass(frozen=True)
class TurnContext:
    active_stream: str
    stream_schema: Dict[str, Any]


@dataclass(frozen=True)
class Candidate:
    sql: str
    questions: List[str]
    assumptions: List[str]


@dataclass(frozen=True)
class PipelineCandidate:
    sql: str
    explain_pretty: str
    create_pipeline_request: Dict[str, Any]
    assumptions: List[str]


@dataclass(frozen=True)
class TurnInput:
    prompt: str
    max_attempts: int
    previous_sql: Optional[str] = None


class EventKind(str, Enum):
    PhaseChanged = "phase_changed"
    DraftPreviewDelta = "draft_preview_delta"
    CandidateGenerated = "candidate_generated"
    PlanningFailed = "planning_failed"
    Explained = "explained"
    NeedUserInput = "need_user_input"
    Failed = "failed"


@dataclass(frozen=True)
class WorkflowEvent:
    kind: EventKind
    phase: Optional[Phase] = None
    attempt: Optional[int] = None
    text_delta: Optional[str] = None
    candidate: Optional[Candidate] = None
    error: Optional[str] = None
    result: Optional[PipelineCandidate] = None


def _candidate_from_json(obj: Dict[str, Any]) -> Candidate:
    sql = str(obj.get("sql", "")).strip()
    if not sql:
        raise LlmError(f"LLM response missing sql: {obj}")
    questions = obj.get("questions", []) or []
    assumptions = obj.get("assumptions", []) or []
    return Candidate(
        sql=sql,
        questions=[str(x) for x in questions],
        assumptions=[str(x) for x in assumptions],
    )


def _strip_code_fences(text: str) -> str:
    raw = text.strip()
    if raw.startswith("```"):
        # Drop the first fence line and the last fence if present.
        lines = raw.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        raw = "\n".join(lines).strip()
    return raw


def _extract_sql_from_preview(text: str) -> str:
    raw = _strip_code_fences(text).strip()
    if raw.lower().startswith("sql:"):
        raw = raw[4:].strip()
    # Heuristic: take from first SQL keyword if there is leading chatter.
    lowered = raw.lower()
    for kw in ("with", "select", "insert", "create", "delete", "update"):
        idx = lowered.find(kw)
        if idx != -1:
            raw = raw[idx:].strip()
            break
    return raw.strip().rstrip(";").strip() + ";"


def _candidate_from_preview_text(text: str) -> Candidate:
    raw = _strip_code_fences(text).strip()
    if raw.lower().startswith("question:"):
        q = raw[len("question:") :].strip()
        return Candidate(sql="", questions=[q] if q else ["Which stream/fields should be used?"], assumptions=[])
    sql = _extract_sql_from_preview(raw)
    if not sql or sql == ";":
        raise LlmError(f"LLM preview did not produce SQL: {text}")
    return Candidate(sql=sql, questions=[], assumptions=[])


class Workflow:
    """
    The core NL→SQL→(planning validate)→explain loop.

    This module intentionally avoids REPL/printing so the state machine remains readable and testable.
    """

    def __init__(
        self,
        veloflux: VeloFluxMcpClient,
        llm: ChatCompletionsClient,
        llm_preview_model: str,
        llm_draft_model: str,
        digest: CapabilitiesDigest,
        sink_broker_url: str,
        sink_topic: str,
        sink_qos: int,
        max_history_messages: int = 30,
        llm_json_mode: bool = True,
        llm_stream: bool = False,
    ) -> None:
        self.veloflux = veloflux
        self.llm = llm
        self.llm_preview_model = llm_preview_model
        self.llm_draft_model = llm_draft_model
        self.digest = digest
        self.sink_broker_url = sink_broker_url
        self.sink_topic = sink_topic
        self.sink_qos = sink_qos
        self.max_history_messages = max_history_messages
        self.llm_json_mode = llm_json_mode
        self.llm_stream = llm_stream

        self._messages: List[Dict[str, str]] = []
        self._seeded = False

    def seed_session(self) -> None:
        if self._seeded:
            return
        self._messages = [
            {"role": "system", "content": default_assistant_instructions()},
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "type": "context",
                        "capabilities_digest": self.digest.to_json(),
                    },
                    ensure_ascii=False,
                ),
            },
        ]
        self._seeded = True

    def update_active_stream(self, ctx: TurnContext) -> None:
        self._append_user_json(
            {
                "type": "active_stream_update",
                "active_stream": ctx.active_stream,
                "stream_schema": ctx.stream_schema,
            }
        )

    def _append_user_json(self, payload: Dict[str, Any]) -> None:
        self._messages.append(
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}
        )
        self._trim_history()

    def add_context_note(self, note: str) -> None:
        """
        Add a short, user-visible context note into the chat history.

        Use this for deterministic meta-query results (e.g. list streams) so later turns
        can reference them without needing to re-ask or re-fetch.
        """
        if not note.strip():
            return
        self._append_user_json({"type": "context_note", "note": note.strip()})

    def _append_assistant_text(self, text: str) -> None:
        self._messages.append({"role": "assistant", "content": text})
        self._trim_history()

    def _trim_history(self) -> None:
        if self.max_history_messages <= 0:
            return
        if len(self._messages) <= self.max_history_messages:
            return
        # Keep system message and most recent messages.
        system = self._messages[0:1]
        tail = self._messages[-(self.max_history_messages - 1) :]
        self._messages = system + tail

    def run_turn(self, ctx: TurnContext, turn: TurnInput) -> Iterator[WorkflowEvent]:
        if not self._seeded:
            self.seed_session()

        previous_error: Optional[str] = None
        previous_sql = turn.previous_sql
        explain_summary: Optional[str] = None

        max_attempts = max(1, turn.max_attempts)
        for attempt in range(1, max_attempts + 1):
            yield WorkflowEvent(kind=EventKind.PhaseChanged, phase=Phase.DraftSql, attempt=attempt)

            # Attempt 1: fast preview SQL (streamable). Later attempts: structured repair JSON.
            if attempt == 1:
                llm_payload = {
                    "mode": "preview_sql",
                    "nl": turn.prompt,
                    "active_stream": ctx.active_stream,
                    "stream_schema": ctx.stream_schema,
                    "previous_sql": previous_sql,
                    "previous_error": previous_error,
                    "explain_summary": explain_summary,
                    "output_rules": "Output a single SQL statement OR a single line `QUESTION: ...`. No extra text.",
                }
                self._append_user_json(llm_payload)
                chunks: List[str] = []
                try:
                    if self.llm_stream:
                        for piece in self.llm.iter_text_deltas(
                            model=self.llm_preview_model,
                            messages=self._messages,
                            temperature=0.0,
                        ):
                            chunks.append(piece)
                            yield WorkflowEvent(
                                kind=EventKind.DraftPreviewDelta,
                                phase=Phase.DraftSql,
                                attempt=attempt,
                                text_delta=piece,
                            )
                        full = "".join(chunks).strip()
                    else:
                        full = self.llm.complete_text(
                            model=self.llm_preview_model,
                            messages=self._messages,
                            temperature=0.0,
                        ).strip()
                        if full:
                            yield WorkflowEvent(
                                kind=EventKind.DraftPreviewDelta,
                                phase=Phase.DraftSql,
                                attempt=attempt,
                                text_delta=full,
                            )
                except LlmError as e:
                    yield WorkflowEvent(kind=EventKind.Failed, phase=Phase.Failed, error=str(e))
                    return

                self._append_assistant_text(full)
                try:
                    candidate = _candidate_from_preview_text(full)
                except LlmError as e:
                    yield WorkflowEvent(kind=EventKind.Failed, phase=Phase.Failed, error=str(e))
                    return
            else:
                llm_payload = {
                    "mode": "json_candidate",
                    "nl": turn.prompt,
                    "active_stream": ctx.active_stream,
                    "stream_schema": ctx.stream_schema,
                    "previous_sql": previous_sql,
                    "previous_error": previous_error,
                    "explain_summary": explain_summary,
                    "output_schema": {
                        "sql": "string (required)",
                        "questions": "string[] (optional)",
                        "assumptions": "string[] (optional)",
                    },
                }
                self._append_user_json(llm_payload)
                try:
                    parsed = self.llm.complete_json(
                        model=self.llm_draft_model,
                        messages=self._messages,
                        temperature=0.0,
                        response_format_json=self.llm_json_mode,
                        # Streaming is disabled for JSON mode to avoid provider-specific SSE quirks.
                        stream=False,
                        on_stream_delta=None,
                    )
                except LlmError as e:
                    yield WorkflowEvent(kind=EventKind.Failed, phase=Phase.Failed, error=str(e))
                    return

                # Keep raw output in history (best-effort) for multi-turn consistency.
                self._append_assistant_text(json.dumps(parsed, ensure_ascii=False))
                candidate = _candidate_from_json(parsed)

            yield WorkflowEvent(
                kind=EventKind.CandidateGenerated,
                phase=Phase.DraftSql,
                attempt=attempt,
                candidate=candidate,
            )

            if candidate.questions:
                yield WorkflowEvent(
                    kind=EventKind.NeedUserInput,
                    phase=Phase.DraftSql,
                    attempt=attempt,
                    candidate=candidate,
                )
                return

            yield WorkflowEvent(
                kind=EventKind.PhaseChanged, phase=Phase.ValidatePipeline, attempt=attempt
            )

            temp_id = f"__nl2pipeline_tmp__{int(time.time())}"
            pipeline_req = build_create_pipeline_request(
                pipeline_id=temp_id,
                sql=candidate.sql,
                sink_broker_url=self.sink_broker_url,
                sink_topic=self.sink_topic,
                sink_qos=self.sink_qos,
            )
            try:
                self.veloflux.pipelines_create(pipeline_req)
            except McpError as e:
                previous_error = str(e)
                previous_sql = candidate.sql
                yield WorkflowEvent(
                    kind=EventKind.PlanningFailed,
                    phase=Phase.ValidatePipeline,
                    attempt=attempt,
                    candidate=candidate,
                    error=previous_error,
                )
                continue

            yield WorkflowEvent(
                kind=EventKind.PhaseChanged, phase=Phase.ExplainPipeline, attempt=attempt
            )

            try:
                explain = self.veloflux.pipelines_explain(temp_id)
            finally:
                try:
                    self.veloflux.pipelines_delete(temp_id)
                except McpError:
                    pass

            result = PipelineCandidate(
                sql=candidate.sql,
                explain_pretty=explain,
                create_pipeline_request=build_create_pipeline_request(
                    pipeline_id="REPLACE_ME",
                    sql=candidate.sql,
                    sink_broker_url=self.sink_broker_url,
                    sink_topic=self.sink_topic,
                    sink_qos=self.sink_qos,
                ),
                assumptions=candidate.assumptions,
            )
            yield WorkflowEvent(
                kind=EventKind.Explained,
                phase=Phase.ExplainPipeline,
                attempt=attempt,
                result=result,
            )
            yield WorkflowEvent(kind=EventKind.PhaseChanged, phase=Phase.Done, attempt=attempt)
            return

        yield WorkflowEvent(kind=EventKind.Failed, phase=Phase.Failed, error=previous_error)
