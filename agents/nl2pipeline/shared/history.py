from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

Message = Dict[str, str]  # {"role": "system"|"user"|"assistant", "content": "..."}


@dataclass
class HistoryBuffer:
    """
    History strategy: keep a lightweight `summary` plus the most recent `max_tail` messages.

    v1 summary updates are deterministic (notes concatenation). No LLM summarization yet.
    """

    summary: str
    tail: List[Message]
    max_tail: int = 20

    def add_user(self, text: str) -> None:
        self._append({"role": "user", "content": text})

    def add_assistant(self, text: str) -> None:
        self._append({"role": "assistant", "content": text})

    def add_note(self, note: str) -> None:
        note = note.strip()
        if not note:
            return
        if self.summary:
            self.summary = self.summary.rstrip() + "\n" + note
        else:
            self.summary = note

    def to_messages(
        self,
        *,
        system_prompt: str,
        digest: Optional[Dict[str, Any]],
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> List[Message]:
        messages: List[Message] = [{"role": "system", "content": system_prompt}]

        if digest is not None:
            messages.append(
                {
                    "role": "user",
                    "content": json.dumps(
                        {"type": "context", "capabilities_digest": digest},
                        ensure_ascii=False,
                    ),
                }
            )

        if self.summary.strip():
            messages.append(
                {
                    "role": "user",
                    "content": json.dumps(
                        {"type": "history_summary", "summary": self.summary.strip()},
                        ensure_ascii=False,
                    ),
                }
            )

        # Append conversation tail first, then the current turn's extra context as the last user
        # message. Some providers require the final message to be user.
        messages.extend(self.tail)

        if extra_context is not None:
            messages.append({"role": "user", "content": json.dumps(extra_context, ensure_ascii=False)})
        return messages

    def _append(self, msg: Message) -> None:
        self.tail.append(msg)
        self._trim()

    def _trim(self) -> None:
        if self.max_tail <= 0:
            self.tail = []
            return
        if len(self.tail) <= self.max_tail:
            return
        self.tail = self.tail[-self.max_tail :]


__all__ = ["HistoryBuffer"]
