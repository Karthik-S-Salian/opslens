import pytest

from opslens.agent.evidence import validate_citations
from opslens.agent.llm import Delta, FakeLLM, ToolCall
from opslens.agent.loop import _coerce_arguments
from opslens.agent.tools import TOOL_SCHEMAS, quote
from opslens.ingest.docs import chunk

KNOWN = {"11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"}


def test_quote_escapes_logsql_metacharacters():
    assert quote('a"b') == '"a\\"b"'
    assert quote("a\\b") == '"a\\\\b"'
    assert quote("line\nbreak") == '"line break"'
    assert quote("pool timeout") == '"pool timeout"'


def test_quote_prevents_query_injection():
    injected = 'x" OR _secret:"y" | delete'
    quoted = quote(injected)

    assert quoted[0] == quoted[-1] == '"'
    # every interior quote is escaped, so nothing can close the string early
    assert quoted[1:-1].replace('\\"', "").count('"') == 0


def test_coerce_arguments_clamps_types():
    coerced = _coerce_arguments(
        {
            "minutes_ago": "60",
            "limit": "1000",
            "k": "not-a-number",
            "query": "x" * 600,
            "service": "p",
        }
    )
    assert coerced["minutes_ago"] == 60
    assert coerced["limit"] == 1000
    assert "k" not in coerced  # non-numeric silently dropped, tool default applies
    assert len(coerced["query"]) == 500


def test_tool_schemas_cover_the_documented_tools():
    names = {schema["function"]["name"] for schema in TOOL_SCHEMAS}
    assert names == {
        "search_logs",
        "get_error_spikes",
        "fetch_trace",
        "list_deployments",
        "search_runbooks",
        "search_past_incidents",
    }
    for schema in TOOL_SCHEMAS:
        assert schema["function"]["parameters"]["type"] == "object"


def test_citations_keep_known_ids_in_order_and_mark_unknown():
    text = f"a [ev:{sorted(KNOWN)[0]}] b [ev:not-a-uuid] c [ev:{sorted(KNOWN)[1]}]"
    cleaned, used = validate_citations(text, KNOWN)

    assert used == sorted(KNOWN)
    assert (
        "[ev:not-a-uuid]" in cleaned
    )  # unknown-looking text is left as prose, not silently deleted
    assert cleaned.startswith("a [ev:")


def test_unknown_uuid_citation_is_replaced():
    ghost = "33333333-3333-3333-3333-333333333333"
    cleaned, used = validate_citations(f"claim [ev:{ghost}]", KNOWN)
    assert used == []
    assert "[ev:unknown]" in cleaned


def test_abbreviated_citation_resolves_and_is_normalized():
    full = sorted(KNOWN)[0]
    cleaned, used = validate_citations(f"claim [ev:{full[:8]}]", KNOWN)

    assert used == [full]
    assert f"[ev:{full}]" in cleaned


def test_ambiguous_prefix_is_not_guessed():
    ambiguous = {
        "aaaaaaaa-0000-0000-0000-000000000001",
        "aaaaaaaa-0000-0000-0000-000000000002",
    }
    cleaned, used = validate_citations("claim [ev:aaaaaaaa]", ambiguous)
    assert used == [] and "[ev:unknown]" in cleaned


def test_no_citations_is_allowed():
    cleaned, used = validate_citations("just prose", KNOWN)
    assert cleaned == "just prose"
    assert used == []


async def test_fake_llm_scripts_turns_then_cites_tool_evidence():
    evidence_id = "44444444-4444-4444-4444-444444444444"
    fake = FakeLLM(
        [[Delta(tool_calls=[ToolCall(name="search_logs", arguments={"service": "payment"})])]]
    )
    messages = [{"role": "tool", "content": f"evidence_id: {evidence_id}\nrows: 1"}]

    first = [delta async for delta in fake.chat([], TOOL_SCHEMAS)]
    second = [delta async for delta in fake.chat(messages, TOOL_SCHEMAS)]

    assert first[0].tool_calls[0].name == "search_logs"
    assert f"[ev:{evidence_id}]" in second[0].content


def test_chunk_respects_size_and_overlap():
    text = "\n\n".join(f"paragraph {i} " + "x" * 300 for i in range(10))
    pieces = chunk(text, size=700, overlap=100)

    assert all(len(piece) <= 700 for piece in pieces)
    assert "".join(pieces).count("paragraph") >= 10


def test_chunk_short_document_single_piece():
    assert chunk("short runbook") == ["short runbook"]
    assert chunk("   ") == []


@pytest.mark.parametrize("size", [1000, 1200, 2000])
def test_chunk_never_empty(size):
    assert all(piece.strip() for piece in chunk("a\n\n" * 500, size=size))
