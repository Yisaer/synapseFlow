import os
import tomllib
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class ManagerConfig:
    url: str
    timeout_secs: float


@dataclass(frozen=True)
class LlmConfig:
    base_url: str
    timeout_secs: float
    api_key: str
    # Default model. If per-stage models are not set, this is used for all calls.
    model: str
    # Per-stage models (optional; fall back to `model`).
    router_model: str
    preview_model: str
    draft_model: str
    stream: bool
    json_mode: bool


@dataclass(frozen=True)
class StreamConfig:
    default: str


@dataclass(frozen=True)
class SinkConfig:
    broker_url: str
    topic: str
    qos: int


@dataclass(frozen=True)
class ReplConfig:
    check_max_attempts: int


@dataclass(frozen=True)
class AppConfig:
    manager: ManagerConfig
    llm: LlmConfig
    stream: StreamConfig
    sink: SinkConfig
    repl: ReplConfig


def _require_non_empty(value: str, path: str) -> str:
    if not value.strip():
        raise ValueError(f"config missing {path}")
    return value.strip()


def _read_api_key(llm_section: Dict[str, Any]) -> str:
    api_key = str(llm_section.get("api_key", "")).strip()
    env_key = str(llm_section.get("api_key_env", "")).strip()
    if not api_key and env_key:
        api_key = str(os.environ.get(env_key, "")).strip()
    if not api_key:
        raise ValueError("missing LLM API key (set [llm].api_key_env or [llm].api_key)")
    return api_key


def _read_columns(stream_section: Dict[str, Any]) -> List[Dict[str, str]]:
    raw = stream_section.get("columns", []) or []
    columns: List[Dict[str, str]] = []
    if not isinstance(raw, list):
        return columns
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        data_type = str(item.get("data_type", "")).strip()
        if name and data_type:
            columns.append({"name": name, "data_type": data_type})
    return columns


def load_config(path: str) -> AppConfig:
    with open(path, "rb") as f:
        raw = tomllib.load(f)

    manager_section = raw.get("manager", {}) or {}
    llm_section = raw.get("llm", {}) or {}
    stream_section = raw.get("stream", {}) or {}
    sink_section = raw.get("sink", {}) or {}
    repl_section = raw.get("repl", {}) or {}

    manager = ManagerConfig(
        url=_require_non_empty(str(manager_section.get("url", "")), "[manager].url"),
        timeout_secs=float(manager_section.get("timeout_secs", 10.0)),
    )

    default_model = _require_non_empty(str(llm_section.get("model", "")), "[llm].model")
    router_model = str(llm_section.get("router_model", "")).strip() or default_model
    preview_model = str(llm_section.get("preview_model", "")).strip() or router_model
    draft_model = str(llm_section.get("draft_model", "")).strip() or default_model

    llm = LlmConfig(
        base_url=_require_non_empty(str(llm_section.get("base_url", "")), "[llm].base_url"),
        timeout_secs=float(llm_section.get("timeout_secs", 30.0)),
        api_key=_read_api_key(llm_section),
        model=default_model,
        router_model=_require_non_empty(router_model, "[llm].router_model"),
        preview_model=_require_non_empty(preview_model, "[llm].preview_model"),
        draft_model=_require_non_empty(draft_model, "[llm].draft_model"),
        stream=bool(llm_section.get("stream", False)),
        json_mode=bool(llm_section.get("json_mode", True)),
    )

    stream = StreamConfig(
        default=str(stream_section.get("default", "")).strip(),
    )

    sink = SinkConfig(
        broker_url=str(sink_section.get("broker_url", "tcp://127.0.0.1:1883")).strip(),
        topic=str(sink_section.get("topic", "/nl2pipeline/out")).strip(),
        qos=int(sink_section.get("qos", 0)),
    )

    repl = ReplConfig(check_max_attempts=int(repl_section.get("check_max_attempts", 3)))

    return AppConfig(manager=manager, llm=llm, stream=stream, sink=sink, repl=repl)
