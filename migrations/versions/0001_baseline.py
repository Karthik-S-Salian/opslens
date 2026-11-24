"""baseline schema

Revision ID: 0001
Revises:
Create Date: 2026-11-24
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    op.execute(
        """
        CREATE TABLE incidents (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id text NOT NULL,
            title text NOT NULL,
            severity text NOT NULL DEFAULT 'unknown',
            status text NOT NULL DEFAULT 'open',
            started_at timestamptz,
            detected_at timestamptz NOT NULL DEFAULT now(),
            services text[] NOT NULL DEFAULT '{}',
            created_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX incidents_org_created_idx ON incidents (org_id, created_at DESC)")

    op.execute(
        """
        CREATE TABLE runs (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            incident_id uuid NOT NULL REFERENCES incidents (id) ON DELETE CASCADE,
            parent_run_id uuid REFERENCES runs (id) ON DELETE SET NULL,
            status text NOT NULL DEFAULT 'queued',
            stop_reason text,
            budget jsonb NOT NULL DEFAULT '{}'::jsonb,
            usage jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_by text,
            started_at timestamptz,
            ended_at timestamptz,
            error text,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT runs_parent_not_self CHECK (parent_run_id IS NULL OR parent_run_id <> id)
        )
        """
    )
    op.execute("CREATE INDEX runs_incident_created_idx ON runs (incident_id, created_at DESC)")

    op.execute(
        """
        CREATE TABLE run_events (
            run_id uuid NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
            seq bigserial NOT NULL,
            type text NOT NULL,
            payload jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (run_id, seq)
        )
        """
    )

    op.execute(
        """
        CREATE TABLE run_messages (
            run_id uuid NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
            seq bigserial NOT NULL,
            role text NOT NULL,
            content text NOT NULL DEFAULT '',
            tool_calls jsonb,
            tokens_in integer,
            tokens_out integer,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (run_id, seq)
        )
        """
    )

    op.execute(
        """
        CREATE TABLE evidence (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            run_id uuid NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
            kind text NOT NULL,
            ref jsonb NOT NULL DEFAULT '{}'::jsonb,
            excerpt text,
            created_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX evidence_run_idx ON evidence (run_id)")

    op.execute(
        """
        CREATE TABLE deployments (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id text NOT NULL,
            service text NOT NULL,
            env text NOT NULL,
            version text,
            commit_sha text,
            deployed_at timestamptz NOT NULL,
            source text NOT NULL,
            external_id text NOT NULL,
            payload jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT deployments_source_external_key UNIQUE (org_id, source, external_id)
        )
        """
    )
    op.execute(
        "CREATE INDEX deployments_lookup_idx ON deployments (org_id, service, deployed_at DESC)"
    )

    op.execute(
        """
        CREATE TABLE ingest_jobs (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id text NOT NULL,
            kind text NOT NULL,
            object_key text,
            external_id text,
            status text NOT NULL DEFAULT 'queued',
            attempts integer NOT NULL DEFAULT 0,
            stats jsonb NOT NULL DEFAULT '{}'::jsonb,
            error text,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX ingest_jobs_status_idx ON ingest_jobs (status, created_at)")
    op.execute(
        "CREATE UNIQUE INDEX ingest_jobs_external_key"
        " ON ingest_jobs (org_id, kind, external_id) WHERE external_id IS NOT NULL"
    )

    op.execute(
        """
        CREATE TABLE documents (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id text NOT NULL,
            kind text NOT NULL,
            title text NOT NULL,
            service text,
            source_key text NOT NULL,
            metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT documents_source_key UNIQUE (org_id, source_key)
        )
        """
    )

    op.execute(
        """
        CREATE TABLE doc_chunks (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            document_id uuid NOT NULL REFERENCES documents (id) ON DELETE CASCADE,
            org_id text NOT NULL,
            chunk_index integer NOT NULL,
            content text NOT NULL,
            tsv tsvector GENERATED ALWAYS AS (to_tsvector('english', content)) STORED,
            embedding halfvec(1024),
            CONSTRAINT doc_chunks_doc_index_key UNIQUE (document_id, chunk_index)
        )
        """
    )
    op.execute("CREATE INDEX doc_chunks_tsv_idx ON doc_chunks USING gin (tsv)")
    op.execute(
        "CREATE INDEX doc_chunks_embedding_idx ON doc_chunks"
        " USING hnsw (embedding halfvec_cosine_ops)"
    )
    op.execute("CREATE INDEX doc_chunks_org_idx ON doc_chunks (org_id)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS doc_chunks")
    op.execute("DROP TABLE IF EXISTS documents")
    op.execute("DROP TABLE IF EXISTS ingest_jobs")
    op.execute("DROP TABLE IF EXISTS deployments")
    op.execute("DROP TABLE IF EXISTS evidence")
    op.execute("DROP TABLE IF EXISTS run_messages")
    op.execute("DROP TABLE IF EXISTS run_events")
    op.execute("DROP TABLE IF EXISTS runs")
    op.execute("DROP TABLE IF EXISTS incidents")
