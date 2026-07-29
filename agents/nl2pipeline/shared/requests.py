from __future__ import annotations

from typing import Any, Dict, List


def build_create_stream_request(
    name: str,
    broker_url: str,
    topic: str,
    qos: int,
    columns: List[Dict[str, str]],
) -> Dict[str, Any]:
    return {
        "name": name,
        "revision": 1,
        "type": "mqtt",
        "props": {"broker_url": broker_url, "topic": topic, "qos": qos},
        "schema": {"type": "json", "props": {"columns": columns}},
        "decoder": {"type": "json", "props": {}},
        "shared": False,
    }


def build_create_pipeline_request(
    pipeline_id: str,
    sql: str,
    sink_broker_url: str,
    sink_topic: str,
    sink_qos: int,
) -> Dict[str, Any]:
    return {
        "id": pipeline_id,
        "revision": 1,
        "sql": sql,
        "sinks": [
            {
                "type": "mqtt",
                "props": {
                    "broker_url": sink_broker_url,
                    "topic": sink_topic,
                    "qos": sink_qos,
                    "retain": False,
                },
                "encoder": {"type": "json", "props": {}},
            }
        ],
        "options": {"eventtime": {"enabled": False}},
    }


__all__ = ["build_create_pipeline_request", "build_create_stream_request"]
