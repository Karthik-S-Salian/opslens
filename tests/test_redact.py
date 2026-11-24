import pytest

from opslens.ingest.redact import redact

SECRETS = [
    ("Authorization: Bearer abcdefgh12345678.token", "abcdefgh12345678"),
    ("jwt=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJl", "eyJhbGci"),
    ("password=hunter2", "hunter2"),
    ('secret: "s3cr3t-value"', "s3cr3t-value"),
    ("api_key: abcdef123456", "abcdef123456"),
    ("aws key AKIAIOSFODNN7EXAMPLE", "AKIAIOSFODNN7EXAMPLE"),
    ("token ghp_abcdefghijklmnopqrstuvwxyz0123456789", "ghp_abcdefghijklmnopqrstuvwxyz0123456789"),
    ("openai sk-proj-abcdefghijklmnopqrstuvwxyz", "sk-proj-abcdefghijklmnop"),
    ("postgres://opslens:sup3rs3cret@db:5432/opslens", "sup3rs3cret"),
]


@pytest.mark.parametrize(("raw", "leak"), SECRETS)
def test_secrets_are_removed(raw, leak):
    cleaned = redact(raw)
    assert leak not in cleaned
    assert "[redacted" in cleaned


@pytest.mark.parametrize(
    "text",
    [
        "token expired after 30m",
        "trace_id=abc123 request_id=r-9",
        "connection pool exhausted",
        '{"level":"ERROR","message":"charge failed"}',
        "at com.example.PaymentService.charge(PaymentService.java:42)",
        "postgres://db:5432/opslens timed out",
    ],
)
def test_ordinary_log_text_survives(text):
    assert redact(text) == text
