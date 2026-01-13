from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from .chat_client import ChatCompletionsClient, LlmError


class Intent(str, Enum):
    ListStreams = "list_streams"
    DescribeStream = "describe_stream"
    Nl2Sql = "nl2sql"
    AskUser = "ask_user"


@dataclass(frozen=True)
class RouterDecision:
    intent: Intent
    stream: Optional[str]
    question: Optional[str]
    raw: Dict[str, Any]


def _safe_stream_names(streams: List[Dict[str, Any]], max_items: int = 50) -> List[str]:
    names: List[str] = []
    for s in streams:
        if not isinstance(s, dict):
            continue
        name = str(s.get("name", "")).strip()
        if name:
            names.append(name)
    names = names[:max_items]
    return names


def _instructions() -> str:
    return (
        "You are a router for a VeloFlux CLI.\n"
        "Choose which action to take based on the user's message.\n"
        "\n"
        "Allowed intents:\n"
        "- list_streams: user asks what streams exist / list streams.\n"
        "- describe_stream: user asks schema/columns of a stream.\n"
        "- nl2sql: user wants a pipeline SQL (filter/aggregate/window/etc.).\n"
        "- ask_user: missing required info (e.g. which stream to use).\n"
        "\n"
        "Rules:\n"
        "- Do NOT generate SQL.\n"
        "- If the user asks about streams list, pick list_streams.\n"
        "- If the user asks about columns/schema, pick describe_stream.\n"
        "- If intent is describe_stream and the stream is not clear, use ask_user and write a short question.\n"
        "- If intent is nl2sql and stream is not clear and active_stream is empty, use ask_user.\n"
        "\n"
        "Output ONLY JSON: {\"intent\": string, \"stream\": string|null, \"question\": string|null}\n"
    )


def route_intent(
    llm: ChatCompletionsClient,
    model: str,
    user_text: str,
    streams: List[Dict[str, Any]],
    active_stream: Optional[str],
    json_mode: bool,
) -> RouterDecision:
    stream_names = _safe_stream_names(streams)
    payload = {
        "user_text": user_text,
        "active_stream": active_stream or "",
        "available_streams": stream_names,
    }
    messages = [
        {"role": "system", "content": _instructions()},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ]
    raw = llm.complete_json(
        model=model,
        messages=messages,
        temperature=0.0,
        response_format_json=json_mode,
    )
    intent_raw = str(raw.get("intent", "")).strip()
    stream = str(raw.get("stream", "")).strip() or None
    question = str(raw.get("question", "")).strip() or None

    try:
        intent = Intent(intent_raw)
    except Exception as e:
        raise LlmError(f"invalid router intent: {intent_raw} raw={raw}") from e

    # Normalize: if router picked a stream not in catalog, treat as missing.
    if stream is not None and stream not in stream_names:
        stream = None

    return RouterDecision(intent=intent, stream=stream, question=question, raw=raw)


__all__ = ["Intent", "RouterDecision", "route_intent"]
