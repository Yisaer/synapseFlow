#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

if __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from agents.nl2pipeline.legacy.repl import run_repl  # noqa: E402
from agents.nl2pipeline.legacy.workflow import Workflow  # noqa: E402
from agents.nl2pipeline.shared.manager_client import ManagerClient  # noqa: E402
from agents.nl2pipeline.shared.chat_client import ChatCompletionsClient  # noqa: E402
from agents.nl2pipeline.shared.config import load_config  # noqa: E402
from agents.nl2pipeline.shared.mcp import (  # noqa: E402
    EmbeddedMcpRuntime,
    McpRegistry,
    register_veloflux_mcp,
)
from agents.nl2pipeline.shared.mcp_client import VeloFluxMcpClient  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Interactive NL→pipeline agent (Chat Completions)")
    p.add_argument("--config", required=True, help="Path to TOML config file")
    return p


def main(argv: list[str]) -> int:
    args = build_parser().parse_args(argv)
    cfg = load_config(args.config)

    manager = ManagerClient.new(cfg.manager.url, cfg.manager.timeout_secs)
    registry = McpRegistry()
    register_veloflux_mcp(registry, manager)
    runtime = EmbeddedMcpRuntime.new(registry)
    veloflux = VeloFluxMcpClient(runtime=runtime)
    digest = veloflux.build_capabilities_digest()

    llm = ChatCompletionsClient.new(cfg.llm.base_url, cfg.llm.api_key, cfg.llm.timeout_secs)

    workflow = Workflow(
        veloflux=veloflux,
        llm=llm,
        llm_preview_model=cfg.llm.preview_model,
        llm_draft_model=cfg.llm.draft_model,
        digest=digest,
        sink_broker_url=cfg.sink.broker_url,
        sink_topic=cfg.sink.topic,
        sink_qos=cfg.sink.qos,
        llm_json_mode=cfg.llm.json_mode,
        llm_stream=cfg.llm.stream,
    )

    return run_repl(
        manager=veloflux,
        workflow=workflow,
        router_model=cfg.llm.router_model,
        initial_stream_name=cfg.stream.default,
        check_max_attempts=cfg.repl.check_max_attempts,
    )


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except ApiError as e:
        print(str(e), file=sys.stderr)
        raise SystemExit(1)
