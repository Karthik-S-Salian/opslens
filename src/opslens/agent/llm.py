"""LLM boundary. Ollama in dev, plus a scripted fake so the loop is testable offline."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

import httpx
import orjson


@dataclass(slots=True)
class ToolCall:
    name: str
    arguments: dict[str, Any]


@dataclass(slots=True)
class Delta:
    content: str = ""
    tool_calls: list[ToolCall] = field(default_factory=list)
    tokens_in: int = 0
    tokens_out: int = 0
    done: bool = False


class LLM(Protocol):
    async def chat(
        self, messages: Sequence[dict[str, Any]], tools: Sequence[dict[str, Any]]
    ) -> AsyncIterator[Delta]: ...

    async def embed(self, texts: Sequence[str]) -> list[list[float]]: ...


class OllamaLLM:
    def __init__(self, http: httpx.AsyncClient, chat_model: str, embed_model: str) -> None:
        self.http = http
        self.chat_model = chat_model
        self.embed_model = embed_model

    async def chat(
        self, messages: Sequence[dict[str, Any]], tools: Sequence[dict[str, Any]]
    ) -> AsyncIterator[Delta]:
        payload: dict[str, Any] = {
            "model": self.chat_model,
            "messages": list(messages),
            "stream": True,
            "options": {"temperature": 0.1, "num_ctx": 16384},
        }
        if tools:
            payload["tools"] = list(tools)

        async with self.http.stream("POST", "/api/chat", json=payload) as resp:
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if not line.strip():
                    continue
                chunk = orjson.loads(line)
                message = chunk.get("message") or {}
                calls = []
                for raw in message.get("tool_calls") or []:
                    function = raw.get("function") or {}
                    arguments = function.get("arguments") or {}
                    if isinstance(arguments, str):
                        try:
                            arguments = orjson.loads(arguments)
                        except orjson.JSONDecodeError:
                            arguments = {}
                    if function.get("name"):
                        calls.append(ToolCall(name=function["name"], arguments=arguments))
                content = message.get("content") or ""
                if content or calls or chunk.get("done"):
                    yield Delta(
                        content=content,
                        tool_calls=calls,
                        tokens_in=int(chunk.get("prompt_eval_count") or 0),
                        tokens_out=int(chunk.get("eval_count") or 0),
                        done=bool(chunk.get("done")),
                    )
                if chunk.get("done"):
                    break

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        if not texts:
            return []
        resp = await self.http.post(
            "/api/embed", json={"model": self.embed_model, "input": list(texts)}
        )
        resp.raise_for_status()
        return resp.json()["embeddings"]


class FakeLLM:
    """Scripted turns; once exhausted, cites whatever tool evidence is in context."""

    def __init__(self, turns: Sequence[Sequence[Delta]], dim: int = 1024) -> None:
        self.turns = list(turns)
        self.calls: list[list[dict[str, Any]]] = []
        self.dim = dim

    async def chat(
        self, messages: Sequence[dict[str, Any]], tools: Sequence[dict[str, Any]]
    ) -> AsyncIterator[Delta]:
        self.calls.append(list(messages))
        if not self.turns:
            yield Delta(content=self._final_answer(messages), done=True)
            return
        for delta in self.turns.pop(0):
            yield delta

    @staticmethod
    def _final_answer(messages: Sequence[dict[str, Any]]) -> str:
        for message in reversed(messages):
            if message.get("role") != "tool":
                continue
            match = re.search(r"evidence_id: ([0-9a-f-]{36})", str(message.get("content", "")))
            if match:
                return (
                    "Observed:\n"
                    "- errors correlate with the failing service and window "
                    f"[ev:{match.group(1)}]\n"
                    "Hypothesis:\n- upstream dependency exhaustion\n"
                    "Next actions:\n- roll back the last deploy if onset matches"
                )
        return "Observed:\n- no tool evidence available\nHypothesis:\n- insufficient data"

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        # Deterministic bag-of-chars vector: close enough to test plumbing, never used for ranking.
        out = []
        for text in texts:
            vector = [0.0] * self.dim
            for index, char in enumerate(text.encode()[: self.dim]):
                vector[index % self.dim] += (char % 17) / 17
            out.append(vector)
        return out
