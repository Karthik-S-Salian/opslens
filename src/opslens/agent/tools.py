"""Read-only investigation tools. The model supplies structured args; the server builds LogsQL."""

from __future__ import annotations

import statistics
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import orjson
from sqlalchemy import text

from opslens.agent.llm import LLM
from opslens.ingest.vl import VictoriaLogs
from opslens.store.resources import Resources

MAX_TEXT = 4000
MAX_ROWS = 50


@dataclass(slots=True)
class ToolContext:
    resources: Resources
    llm: LLM
    vl: VictoriaLogs
    run_id: str
    org_id: str


@dataclass(slots=True)
class ToolResult:
    kind: str
    text: str
    ref: dict[str, Any]
    excerpt: str


def quote(value: str) -> str:
    """Escape a value for a LogsQL quoted string. Never interpolate raw model text unquoted."""
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ") + '"'


def _trim(text_value: str) -> str:
    return text_value if len(text_value) <= MAX_TEXT else text_value[:MAX_TEXT] + "\n[truncated]"


async def search_logs(
    ctx: ToolContext,
    service: str | None = None,
    severity: str | None = None,
    text_contains: str | None = None,
    trace_id: str | None = None,
    minutes_ago: int = 60,
    limit: int = 20,
) -> ToolResult:
    filters = []
    if service:
        filters.append(f"service:{quote(service)}")
    if severity:
        filters.append(f"severity:{quote(severity.upper())}")
    if text_contains:
        filters.append(f"_msg:{quote(text_contains)}")
    if trace_id:
        filters.append(f"trace_id:{quote(trace_id)}")
    query = " AND ".join(filters) or "*"
    limit = max(1, min(int(limit), MAX_ROWS))
    minutes_ago = max(1, min(int(minutes_ago), 7 * 24 * 60))

    rows = await ctx.vl.query(query, start=f"{minutes_ago}m", end="now", limit=limit)
    for row in rows:
        row.pop("_stream_id", None)
        row.pop("_stream", None)

    lines = [
        f"{row.get('_time', '?')} {row.get('severity', '-')} {row.get('service', '-')} "
        f"{row.get('_msg', '')}"
        for row in rows
    ]
    body = f"query: {query} (last {minutes_ago}m)\nmatched: {len(rows)}\n" + "\n".join(lines)
    return ToolResult(
        kind="log",
        text=_trim(body),
        ref={"query": query, "start": f"{minutes_ago}m", "end": "now", "rows": len(rows)},
        excerpt=lines[0] if lines else "no matching log lines",
    )


async def get_error_spikes(ctx: ToolContext, minutes_ago: int = 60, step: str = "5m") -> ToolResult:
    minutes_ago = max(5, min(int(minutes_ago), 24 * 60))
    params = {
        "query": 'severity:"ERROR"',
        "start": f"{minutes_ago}m",
        "end": "now",
        "step": step,
        "field": "service",
    }
    resp = await ctx.resources.http.get("/select/logsql/hits", params=params)
    resp.raise_for_status()
    hits = resp.json().get("hits", [])

    summary: list[tuple[str, int, datetime | None, float]] = []
    for hit in hits:
        values = [int(v) for v in hit.get("values", [])]
        times = [
            datetime.fromisoformat(t.replace("Z", "+00:00")) for t in hit.get("timestamps", [])
        ]
        if not values:
            continue
        baseline = statistics.median(values[:-2]) if len(values) > 3 else 0.0
        threshold = max(3.0, baseline * 3)
        onset = next((times[i] for i, v in enumerate(values) if v >= threshold), None)
        summary.append((hit.get("fields", {}).get("service", "?"), sum(values), onset, baseline))
    summary.sort(
        key=lambda item: (item[2] is None, item[2] or datetime.max.replace(tzinfo=UTC), -item[1])
    )

    lines = [
        f"{service}: {total} errors, baseline {baseline:.0f}/bucket, "
        f"onset {onset.isoformat() if onset else 'n/a'}"
        for service, total, onset, baseline in summary[:8]
    ]
    if not lines:
        lines = ["no ERROR logs in window"]

    samples = await ctx.vl.query('severity:"ERROR"', start=f"{minutes_ago}m", end="now", limit=5)
    sample_lines = [
        f"{row.get('_time', '?')} {row.get('service', '-')} {row.get('_msg', '')[:200]}"
        for row in samples
    ]
    body = (
        f"error spikes (last {minutes_ago}m, step {step}):\n"
        + "\n".join(lines)
        + "\n\nsample errors:\n"
        + "\n".join(sample_lines)
    )
    return ToolResult(
        kind="spike",
        text=_trim(body),
        ref={"start": f"{minutes_ago}m", "step": step, "services": [s[0] for s in summary[:8]]},
        excerpt=lines[0],
    )


async def fetch_trace(ctx: ToolContext, trace_id: str, minutes_ago: int = 120) -> ToolResult:
    minutes_ago = max(1, min(int(minutes_ago), 7 * 24 * 60))
    query = f"trace_id:{quote(trace_id)} | sort by (_time)"
    rows = await ctx.vl.query(query, start=f"{minutes_ago}m", end="now", limit=MAX_ROWS)

    lines = [
        f"{row.get('_time', '?')} {row.get('service', '-')} {row.get('severity', '-')} "
        f"{row.get('_msg', '')[:300]}"
        for row in rows
    ]
    body = f"trace {trace_id}: {len(rows)} log lines across services\n" + "\n".join(lines)
    return ToolResult(
        kind="trace",
        text=_trim(body),
        ref={"trace_id": trace_id, "rows": len(rows)},
        excerpt=lines[0] if lines else f"no logs for trace {trace_id}",
    )


async def list_deployments(
    ctx: ToolContext, service: str | None = None, minutes_ago: int = 180
) -> ToolResult:
    minutes_ago = max(1, min(int(minutes_ago), 30 * 24 * 60))
    async with ctx.resources.db.connect() as conn:
        rows = (
            (
                await conn.execute(
                    text(
                        """
                    SELECT service, env, version, commit_sha, deployed_at, source
                    FROM deployments
                    WHERE org_id = :org
                      AND deployed_at > now() - make_interval(mins => :minutes)
                      AND (CAST(:service AS text) IS NULL OR service = CAST(:service AS text))
                    ORDER BY deployed_at DESC
                    LIMIT 50
                    """
                    ),
                    {"org": ctx.org_id, "minutes": minutes_ago, "service": service},
                )
            )
            .mappings()
            .all()
        )

    lines = [
        f"{row['deployed_at'].isoformat()} {row['service']} {row['env']} "
        f"{row['version'] or '-'} {row['commit_sha'] or '-'} ({row['source']})"
        for row in rows
    ]
    body = f"deployments (last {minutes_ago}m): {len(rows)}\n" + "\n".join(lines)
    return ToolResult(
        kind="deploy",
        text=_trim(body),
        ref={"service": service, "minutes_ago": minutes_ago, "rows": len(rows)},
        excerpt=lines[0] if lines else "no deployments in window",
    )


async def search_runbooks(ctx: ToolContext, query: str, k: int = 5) -> ToolResult:
    return await _search_docs(ctx, query, k, kinds=("runbook",))


async def search_past_incidents(ctx: ToolContext, query: str, k: int = 5) -> ToolResult:
    return await _search_docs(ctx, query, k, kinds=("postmortem",))


async def _search_docs(ctx: ToolContext, query: str, k: int, kinds: tuple[str, ...]) -> ToolResult:
    k = max(1, min(int(k), 10))
    vector = (await ctx.llm.embed([query]))[0]
    async with ctx.resources.db.connect() as conn:
        rows = (
            (
                await conn.execute(
                    text(
                        """
                    WITH sem AS (
                        SELECT id,
                            row_number() OVER (ORDER BY embedding <=> CAST(:vec AS halfvec)) AS rn
                        FROM doc_chunks
                        WHERE org_id = :org AND embedding IS NOT NULL
                        ORDER BY embedding <=> CAST(:vec AS halfvec)
                        LIMIT 50
                    ), lex AS (
                        SELECT id, row_number() OVER (
                            ORDER BY ts_rank_cd(tsv, websearch_to_tsquery('english', :q)) DESC
                        ) AS rn
                        FROM doc_chunks
                        WHERE org_id = :org AND tsv @@ websearch_to_tsquery('english', :q)
                        LIMIT 50
                    )
                    SELECT c.id, c.content, d.title, d.kind, sum(1.0 / (60 + f.rn)) AS score
                    FROM (SELECT id, rn FROM sem UNION ALL SELECT id, rn FROM lex) f
                    JOIN doc_chunks c ON c.id = f.id
                    JOIN documents d ON d.id = c.document_id
                    WHERE d.kind = ANY(:kinds)
                    GROUP BY c.id, c.content, d.title, d.kind
                    ORDER BY score DESC
                    LIMIT :k
                    """
                    ),
                    {
                        "vec": "[" + ",".join(f"{v:.6f}" for v in vector) + "]",
                        "q": query,
                        "org": ctx.org_id,
                        "kinds": list(kinds),
                        "k": k,
                    },
                )
            )
            .mappings()
            .all()
        )

    blocks = [
        f"[{row['kind']}] {row['title']} (chunk {row['id']})\n{row['content']}" for row in rows
    ]
    body = f"query: {query}\n" + ("\n\n---\n\n".join(blocks) if blocks else "no matching documents")
    return ToolResult(
        kind="doc",
        text=_trim(body),
        ref={"query": query, "documents": [row["title"] for row in rows]},
        excerpt=blocks[0][:400] if blocks else "no matching documents",
    )


TOOL_FUNCTIONS: dict[str, Callable[..., Any]] = {
    "search_logs": search_logs,
    "get_error_spikes": get_error_spikes,
    "fetch_trace": fetch_trace,
    "list_deployments": list_deployments,
    "search_runbooks": search_runbooks,
    "search_past_incidents": search_past_incidents,
}


def _schema(name: str, description: str, properties: dict[str, Any], required: list[str]) -> dict:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {"type": "object", "properties": properties, "required": required},
        },
    }


TOOL_SCHEMAS: list[dict[str, Any]] = [
    _schema(
        "search_logs",
        "Search logs. Returns matching lines with timestamps. Prefer narrow time windows.",
        {
            "service": {"type": "string", "description": "service name, e.g. payment"},
            "severity": {"type": "string", "description": "ERROR, WARN, INFO, DEBUG"},
            "text_contains": {"type": "string", "description": "words to find in the message"},
            "trace_id": {"type": "string"},
            "minutes_ago": {
                "type": "integer",
                "description": "lookback window in minutes (default 60)",
            },
            "limit": {"type": "integer", "description": "max lines, default 20, max 50"},
        },
        [],
    ),
    _schema(
        "get_error_spikes",
        "Error counts per service over time, with baseline and onset time. Start here.",
        {
            "minutes_ago": {"type": "integer", "description": "lookback in minutes (default 60)"},
            "step": {"type": "string", "description": "bucket size like 1m or 5m"},
        },
        [],
    ),
    _schema(
        "fetch_trace",
        "All log lines for one trace_id across services, ordered in time.",
        {
            "trace_id": {"type": "string"},
            "minutes_ago": {"type": "integer", "description": "lookback in minutes (default 120)"},
        },
        ["trace_id"],
    ),
    _schema(
        "list_deployments",
        "Recent deployments from CI/CD, newest first.",
        {
            "service": {"type": "string"},
            "minutes_ago": {"type": "integer", "description": "lookback in minutes (default 180)"},
        },
        [],
    ),
    _schema(
        "search_runbooks",
        "Search runbooks and service docs. Use for known failure modes and remediation steps.",
        {
            "query": {"type": "string"},
            "k": {"type": "integer", "description": "max chunks, default 5"},
        },
        ["query"],
    ),
    _schema(
        "search_past_incidents",
        "Search historical postmortems for similar outages.",
        {
            "query": {"type": "string"},
            "k": {"type": "integer", "description": "max chunks, default 5"},
        },
        ["query"],
    ),
]


def compact_summary(name: str, result: ToolResult) -> dict[str, Any]:
    return {"tool": name, "kind": result.kind, "summary": result.excerpt[:300], "ref": result.ref}


def dumps(value: Any) -> str:
    return orjson.dumps(value).decode()
