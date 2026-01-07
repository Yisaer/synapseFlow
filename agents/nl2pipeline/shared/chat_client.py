import json
import socket
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterator, List, Optional
import urllib.error
import urllib.request

from .http_client import ApiError, HttpJsonClient, join_url


class LlmError(RuntimeError):
    pass


def _normalize_messages(messages: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Some providers reject consecutive messages with the same role (especially assistant).
    Merge consecutive messages of the same role to keep the history valid.
    """

    out: List[Dict[str, str]] = []
    for m in messages:
        role = str(m.get("role", "") or "")
        content = str(m.get("content", "") or "")
        if not role:
            continue
        if not out:
            out.append({"role": role, "content": content})
            continue
        if out[-1]["role"] == role:
            prev = out[-1].get("content", "") or ""
            if prev and content:
                out[-1]["content"] = prev + "\n" + content
            elif content:
                out[-1]["content"] = content
            continue
        out.append({"role": role, "content": content})

    # Drop empty non-system messages.
    cleaned: List[Dict[str, str]] = []
    for m in out:
        role = str(m.get("role", "") or "")
        content = str(m.get("content", "") or "")
        if role != "system" and not content.strip():
            continue
        cleaned.append({"role": role, "content": content})
    return cleaned


@dataclass
class ChatCompletionsClient:
    http: HttpJsonClient
    base_url: str

    @classmethod
    def new(cls, base_url: str, api_key: str, timeout_secs: float) -> "ChatCompletionsClient":
        headers = {"Authorization": f"Bearer {api_key}"}
        http = HttpJsonClient(
            base_url=base_url.rstrip("/"), timeout_secs=timeout_secs, headers=headers
        )
        return cls(http=http, base_url=base_url.rstrip("/"))

    def _v1_path(self, path: str) -> str:
        return path

    def complete_text(
        self,
        model: str,
        messages: List[Dict[str, str]],
        temperature: float = 0.0,
    ) -> str:
        messages = _normalize_messages(messages)
        body: Dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
        }
        _, resp = self.http.request_json("POST", self._v1_path("/chat/completions"), body)
        try:
            content = resp["choices"][0]["message"]["content"]
        except Exception as e:
            raise LlmError(f"unexpected chat completion response shape: {resp}") from e
        if not isinstance(content, str) or not content.strip():
            raise LlmError(f"empty chat completion content: {resp}")
        return content

    def iter_text_deltas(
        self,
        model: str,
        messages: List[Dict[str, str]],
        temperature: float = 0.0,
    ) -> Iterator[str]:
        messages = _normalize_messages(messages)
        body: Dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "stream": True,
        }

        url = join_url(self.http.base_url, self._v1_path("/chat/completions"))
        data = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8", errors="replace"
        )
        headers = {
            **self.http.headers,
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        }
        req = urllib.request.Request(url=url, method="POST", data=data, headers=headers)
        yielded_any = False
        try:
            with urllib.request.urlopen(req, timeout=self.http.timeout_secs) as resp:
                while True:
                    line = resp.readline()
                    if not line:
                        break
                    text = line.decode("utf-8", errors="replace").strip()
                    if not text or not text.startswith("data:"):
                        continue
                    payload = text[len("data:") :].strip()
                    if payload == "[DONE]":
                        break
                    try:
                        event = json.loads(payload)
                    except Exception:
                        continue
                    choices = event.get("choices") or []
                    if not isinstance(choices, list):
                        continue
                    for choice in choices:
                        if not isinstance(choice, dict):
                            continue
                        delta = choice.get("delta") or {}
                        if not isinstance(delta, dict):
                            continue
                        piece = delta.get("content")
                        if isinstance(piece, str) and piece:
                            yielded_any = True
                            yield piece
        except (TimeoutError, socket.timeout) as e:
            raise LlmError(f"LLM request timed out after {self.http.timeout_secs}s") from e
        except urllib.error.HTTPError as e:
            raw = e.read()
            message = raw.decode("utf-8", errors="replace") if raw else str(e)
            raise LlmError(f"LLM request failed: HTTP {e.code}: {message}") from None
        except urllib.error.URLError as e:
            raise LlmError(f"LLM request failed: {e}") from None

        if not yielded_any:
            # Some providers/models do not stream `delta.content`; fall back to non-streaming.
            yield self.complete_text(model=model, messages=messages, temperature=temperature)

    def complete_json(
        self,
        model: str,
        messages: List[Dict[str, str]],
        temperature: float = 0.0,
        response_format_json: bool = False,
        stream: bool = False,
        on_stream_delta: Optional[Callable[[str], None]] = None,
        _stream_fallback_used: bool = False,
    ) -> Dict[str, Any]:
        messages = _normalize_messages(messages)
        body: Dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
        }
        if response_format_json:
            body["response_format"] = {"type": "json_object"}

        if not stream:
            # Do not hardcode a version prefix (e.g. /v1). Different providers may expose
            # chat completions under /v1 or /beta. Users should include that in base_url.
            _, resp = self.http.request_json("POST", self._v1_path("/chat/completions"), body)
            try:
                content = resp["choices"][0]["message"]["content"]
            except Exception as e:
                raise LlmError(f"unexpected chat completion response shape: {resp}") from e
            if not isinstance(content, str) or not content.strip():
                raise LlmError(f"empty chat completion content: {resp}")
            return self.parse_json_content(content)

        body["stream"] = True
        url = join_url(self.http.base_url, self._v1_path("/chat/completions"))
        data = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8", errors="replace"
        )
        headers = {
            **self.http.headers,
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        }
        req = urllib.request.Request(url=url, method="POST", data=data, headers=headers)
        chunks: List[str] = []
        try:
            with urllib.request.urlopen(req, timeout=self.http.timeout_secs) as resp:
                while True:
                    line = resp.readline()
                    if not line:
                        break
                    text = line.decode("utf-8", errors="replace").strip()
                    if not text or not text.startswith("data:"):
                        continue
                    payload = text[len("data:") :].strip()
                    if payload == "[DONE]":
                        break
                    try:
                        event = json.loads(payload)
                    except Exception:
                        continue
                    choices = event.get("choices") or []
                    if not isinstance(choices, list):
                        continue
                    for choice in choices:
                        if not isinstance(choice, dict):
                            continue
                        delta = choice.get("delta") or {}
                        if not isinstance(delta, dict):
                            continue
                        piece = delta.get("content")
                        if isinstance(piece, str) and piece:
                            chunks.append(piece)
                            if on_stream_delta is not None:
                                on_stream_delta(piece)
        except (TimeoutError, socket.timeout) as e:
            raise LlmError(f"LLM request timed out after {self.http.timeout_secs}s") from e
        except urllib.error.HTTPError as e:
            raw = e.read()
            message = raw.decode("utf-8", errors="replace") if raw else str(e)
            raise LlmError(f"LLM request failed: HTTP {e.code}: {message}") from None
        except urllib.error.URLError as e:
            raise LlmError(f"LLM request failed: {e}") from None

        full = "".join(chunks).strip()
        if not full:
            # Some providers/models may ignore `stream=true` or may not stream `content` deltas
            # (especially when JSON mode is enabled). Fall back to non-streaming once.
            if not _stream_fallback_used:
                return self.complete_json(
                    model=model,
                    messages=messages,
                    temperature=temperature,
                    response_format_json=response_format_json,
                    stream=False,
                    on_stream_delta=None,
                    _stream_fallback_used=True,
                )
            raise LlmError(
                "empty streaming completion content (provider may not stream content; try llm.stream=false)"
            )
        return self.parse_json_content(full)

    @staticmethod
    def parse_json_content(text: str) -> Dict[str, Any]:
        raw = text.strip()
        if raw.startswith("```"):
            raw = raw.strip("`")
        try:
            return json.loads(raw)
        except Exception as e:
            raise LlmError(f"LLM did not return valid JSON: {text}") from e


__all__ = ["ApiError", "LlmError", "ChatCompletionsClient"]
