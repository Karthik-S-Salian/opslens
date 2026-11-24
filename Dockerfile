FROM python:3.13-slim

# pip instead of COPY --from=ghcr.io/astral-sh/uv: ghcr is unreachable from this network.
RUN pip install --no-cache-dir uv==0.12.13

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/usr/local

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY . .
RUN uv sync --frozen --no-dev

EXPOSE 8000
CMD ["uvicorn", "opslens.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
