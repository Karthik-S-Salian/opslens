import gzip
import io
import json

import httpx
import pytest

from opslens.ingest import vl as vl_module
from opslens.ingest.vl import VictoriaLogs
from opslens.ingest.worker import PrefixedReader, _open


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url="http://vl", transport=httpx.MockTransport(handler))


async def test_insert_uses_jsonline_contract():
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["params"] = dict(request.url.params)
        seen["body"] = request.content
        seen["content_type"] = request.headers["content-type"]
        return httpx.Response(200)

    async with _client(handler) as http:
        await VictoriaLogs(http).insert(
            [
                {"_time": "2026-11-24T10:00:00Z", "service": "a", "message": "one"},
                {"_time": "2026-11-24T10:00:01Z", "service": "a", "message": "two"},
            ]
        )

    assert seen["path"] == "/insert/jsonline"
    assert seen["params"] == {
        "_stream_fields": "service,env",
        "_time_field": "_time",
        "_msg_field": "message",
    }
    assert seen["content_type"] == "application/stream+json"
    lines = seen["body"].split(b"\n")
    assert len(lines) == 2
    assert json.loads(lines[1])["message"] == "two"


async def test_insert_retries_on_500(monkeypatch):
    calls = []

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(vl_module.asyncio, "sleep", no_sleep)

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(500 if len(calls) == 1 else 200)

    async with _client(handler) as http:
        await VictoriaLogs(http).insert([{"message": "x"}])

    assert len(calls) == 2


async def test_insert_does_not_retry_4xx():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(400, text="bad line")

    async with _client(handler) as http:
        with pytest.raises(httpx.HTTPStatusError):
            await VictoriaLogs(http).insert([{"message": "x"}])

    assert len(calls) == 1


async def test_query_parses_json_lines():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["query"] == "service:payment"
        assert request.url.params["limit"] == "5"
        body = '{"_msg":"a","service":"payment"}\n{"_msg":"b"}\n'
        return httpx.Response(200, text=body)

    async with _client(handler) as http:
        rows = await VictoriaLogs(http).query("service:payment", start="1h", end="now", limit=5)

    assert [r["_msg"] for r in rows] == ["a", "b"]


def test_gzip_stream_is_detected_and_decompressed():
    payload = gzip.compress(b'{"message":"one"}\n{"message":"two"}\n')
    body = io.BytesIO(payload)
    head = body.read(1 << 20)

    stream = _open(body, head)
    assert stream.read().decode() == '{"message":"one"}\n{"message":"two"}\n'


def test_prefixed_reader_replays_prefix():
    reader = PrefixedReader(b"abc", io.BytesIO(b"def"))

    assert reader.read(2) == b"ab"
    assert reader.read(10) == b"cdef"
    assert reader.read(10) == b""
