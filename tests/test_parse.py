from datetime import UTC, datetime

from opslens.ingest.parse import (
    MAX_CONTINUATION_LINES,
    LineAssembler,
    normalize_severity,
    normalize_time,
    parse_line,
    sniff,
)


def test_plain_text_with_prefix():
    rec = parse_line(
        "2026-11-24T10:36:01Z ERROR [payment] pool timeout trace_id=abc123",
        defaults={"env": "prod"},
    )
    assert rec["severity"] == "ERROR"
    assert rec["service"] == "payment"
    assert rec["env"] == "prod"
    assert rec["message"] == "pool timeout trace_id=abc123"
    assert rec["time"] == datetime(2026, 11, 24, 10, 36, 1, tzinfo=UTC)


def test_json_line_flat_keys():
    rec = parse_line(
        '{"timestamp":"2026-11-24T10:36:01.500+02:00","level":"warn","service":"auth",'
        '"trace_id":"t-1","request_id":"r-1","msg":"token expired"}',
        defaults={},
    )
    assert rec["severity"] == "WARN"
    assert rec["service"] == "auth"
    assert rec["message"] == "token expired"
    assert rec["time"] == datetime(2026, 11, 24, 8, 36, 1, 500000, tzinfo=UTC)


def test_docker_json_wrapper():
    rec = parse_line(
        '{"log":"upstream 502\\n","stream":"stderr","time":"2026-11-24T10:00:00Z"}',
        defaults={"service": "gateway"},
    )
    assert rec["message"] == "upstream 502\n"
    assert rec["time"] == datetime(2026, 11, 24, 10, 0, tzinfo=UTC)


def test_otel_style_attributes_are_flattened():
    rec = parse_line(
        '{"body":"connection reset","severityText":"ERROR","timeUnixNano":"1700000000000000000",'
        '"attributes":{"service.name":"orders","traceId":"abc"}}',
        defaults={},
    )
    assert rec["message"] == "connection reset"
    assert rec["severity"] == "ERROR"
    assert rec["service"] == "orders"
    assert rec["trace_id"] == "abc"
    assert rec["time"] == datetime(2023, 11, 14, 22, 13, 20, tzinfo=UTC)


def test_malformed_json_falls_back_to_text():
    rec = parse_line('{"broken": "msg"', defaults={})
    assert rec["message"] == '{"broken": "msg"'
    assert rec["parse_error"] is True


def test_blank_line_skipped():
    assert parse_line("   \n", defaults={}) is None


def test_multiline_traceback_is_one_record():
    asm = LineAssembler(defaults={"service": "orders"})
    lines = [
        '{"level":"ERROR","message":"worker crashed"}',
        "Traceback (most recent call last):",
        '  File "/app/main.py", line 10, in run',
        "ValueError: bad payload",
        '{"level":"INFO","message":"next"}',
    ]
    out = [rec for line in lines for rec in asm.feed(line)] + list(asm.flush())

    assert len(out) == 2
    assert out[0]["message"].startswith("worker crashed\nTraceback")
    assert 'File "/app/main.py"' in out[0]["message"]
    assert out[0]["message"].endswith("ValueError: bad payload")
    assert out[1]["message"] == "next"


def test_continuation_cap_yields_extra_records():
    asm = LineAssembler(defaults={})
    lines = ["start", *["  frame"] * (MAX_CONTINUATION_LINES + 5)]
    recs = [rec for line in lines for rec in asm.feed(line)]
    recs += list(asm.flush())

    assert len(recs) == 2
    assert recs[0]["message"].count("frame") == MAX_CONTINUATION_LINES


def test_epoch_millis():
    expected = datetime(2023, 11, 14, 22, 13, 20, 123000, tzinfo=UTC)
    assert normalize_time(1_700_000_000_123) == expected


def test_severity_mapping():
    assert normalize_severity("warning") == "WARN"
    assert normalize_severity("crit") == "FATAL"
    assert normalize_severity(None) is None


def test_sniff():
    assert sniff(b'{"a":1}\n{"b":2}\n') == "json"
    assert sniff(b"2026-11-24 ERROR boom\n") == "text"
