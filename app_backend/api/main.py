"""FastAPI application entry point.

Endpoints
---------
GET    /                          — health check
GET    /pipelines                 — list available pipelines
POST   /run                       — run a named pipeline
GET    /chats/*                   — chat engine (mounted from api/chats.py)
GET    /logs/ai                   — log_llm_call query
GET    /logs/pipeline             — log_pipeline_run query
GET    /logs/errors               — log_error unified journal
"""
from __future__ import annotations

# Load .env before any other import so DATABASE_URL and other env vars are
# available when db.database (and psycopg2) initialise at import time.
from dotenv import load_dotenv
load_dotenv()

import asyncio
import contextlib
import traceback
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel

from api.chats import router as chats_router
from db import database
from engine import pipeline_runner


@contextlib.asynccontextmanager
async def _lifespan(app: FastAPI):
    """Start RabbitMQ listener on startup; disconnect on shutdown."""
    try:
        from rabbit.rabbit import listen, disconnect
        asyncio.ensure_future(listen())
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning(
            "RabbitMQ listener could not start: %s — HTTP API continues.", exc
        )
    yield
    try:
        from rabbit.rabbit import disconnect
        await disconnect()
    except Exception:
        pass


app = FastAPI(title="ms-mysoly-flow", version="3.0.0", lifespan=_lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(chats_router)


# ---------------------------------------------------------------------------
# Global exception handler
# ---------------------------------------------------------------------------

@app.exception_handler(Exception)
async def _global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    tb = traceback.format_exc()
    database.insert_error(
        source="http",
        error_type=type(exc).__name__,
        error_message=str(exc),
        severity="error",
        engine_type="http",
        engine_id="",
        traceback=tb,
        context={"path": str(request.url)},
    )
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
    )


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/")
async def root() -> dict:
    return {"status": "ok", "version": "3.0.0"}


# ---------------------------------------------------------------------------
# Pipeline endpoints
# ---------------------------------------------------------------------------

@app.get("/pipelines")
async def list_pipelines() -> dict:
    """Return all discoverable pipeline definitions."""
    return {"pipelines": pipeline_runner.list_pipelines()}


class RunRequest(BaseModel):
    pipeline: str
    input: dict[str, Any] = {}
    dry_run: bool = False
    prod_name: str = "default"


@app.get("/pipelines/{pipeline_name}/diagram")
async def get_pipeline_diagram(
    pipeline_name: str,
    format: str = Query(default="json", description="'json' or 'text'"),
) -> dict:
    """Return a Mermaid flowchart for the named pipeline."""
    import json as _json
    from pathlib import Path

    from engine.flow_schema import FlowDef
    from engine.mermaid_builder import flow_to_mermaid
    from pydantic import ValidationError

    flow_path = Path(__file__).parent.parent / "pipelines" / pipeline_name / "flow.json"
    if not flow_path.exists():
        return JSONResponse(
            status_code=404,
            content={"detail": f"Pipeline '{pipeline_name}' not found."},
        )

    try:
        raw = _json.loads(flow_path.read_text(encoding="utf-8"))
        flow = FlowDef.model_validate(raw)
    except (_json.JSONDecodeError, ValidationError) as exc:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    mermaid = flow_to_mermaid(flow)
    if format == "text":
        return PlainTextResponse(mermaid)
    return {"pipeline": pipeline_name, "mermaid": mermaid}


@app.post("/run")
async def run_pipeline(request: RunRequest) -> dict:
    """Execute a named pipeline.

    ``prod_name`` is an optional runtime label (defaults to ``"default"``).
    All log rows produced by this run are tagged with this value.
    """
    from fastapi.concurrency import run_in_threadpool

    try:
        result = await run_in_threadpool(
            lambda: pipeline_runner.run(
                pipeline_name=request.pipeline,
                input_data=request.input,
                dry_run=request.dry_run,
                prod_name=request.prod_name,
            )
        )
    except FileNotFoundError as exc:
        return JSONResponse(status_code=404, content={"detail": str(exc)})
    except ValueError as exc:
        return JSONResponse(status_code=422, content={"detail": str(exc)})
    except Exception as exc:
        database.insert_error(
            source="http",
            error_type=type(exc).__name__,
            error_message=str(exc),
            severity="error",
            engine_type="pipeline",
            engine_id="",
            traceback=traceback.format_exc(),
            context={"path": "/run", "pipeline": request.pipeline},
            prod_name=request.prod_name,
        )
        return JSONResponse(status_code=500, content={"detail": str(exc)})

    return {
        "run_id": result.get("run_id"),
        "output": result.get("output", {}),
        "dry_run": result.get("dry_run", False),
    }


# ---------------------------------------------------------------------------
# Log query endpoints
# ---------------------------------------------------------------------------

@app.get("/logs/ai")
async def get_ai_logs(
    engine_type: str | None = Query(default=None, description="'pipeline' or 'chat'"),
    engine_id: str | None = Query(default=None, description="Filter by specific engine_id"),
    agent: str | None = Query(default=None, description="Filter by agent name"),
    prod_name: str | None = Query(default=None, description="Filter by project name"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Query ``log_llm_calls``."""
    conditions: list[str] = []
    params: list[Any] = []

    if engine_type:
        conditions.append("engine_type = %s")
        params.append(engine_type)
    if engine_id:
        conditions.append("engine_id = %s")
        params.append(engine_id)
    if agent:
        conditions.append("agent = %s")
        params.append(agent)
    if prod_name:
        conditions.append("prod_name = %s")
        params.append(prod_name)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT * FROM log_llm_call {where} ORDER BY id DESC"

    from fastapi.concurrency import run_in_threadpool
    rows = await run_in_threadpool(database.query, sql, tuple(params), limit, offset)
    return {"total": len(rows), "offset": offset, "logs": rows}


@app.get("/logs/pipeline")
async def get_pipeline_logs(
    run_id: str | None = Query(default=None, description="Filter by pipeline run_id"),
    pipeline_name: str | None = Query(default=None, description="Filter by pipeline name"),
    triggered_by: str | None = Query(default=None, description="'http', 'chat', or 'rabbit'"),
    status: str | None = Query(default=None, description="'running', 'success', or 'error'"),
    prod_name: str | None = Query(default=None, description="Filter by project name"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Query ``log_pipeline_run`` — single table, no JOIN required."""
    conditions: list[str] = []
    params: list[Any] = []

    if run_id:
        conditions.append("id = %s")
        params.append(run_id)
    if pipeline_name:
        conditions.append("pipeline_name = %s")
        params.append(pipeline_name)
    if triggered_by:
        conditions.append("triggered_by = %s")
        params.append(triggered_by)
    if status:
        conditions.append("status = %s")
        params.append(status)
    if prod_name:
        conditions.append("prod_name = %s")
        params.append(prod_name)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT * FROM log_pipeline_run {where} ORDER BY started_at DESC"

    from fastapi.concurrency import run_in_threadpool
    rows = await run_in_threadpool(database.query, sql, tuple(params), limit, offset)
    return {"total": len(rows), "offset": offset, "logs": rows}


@app.get("/logs/errors")
async def get_error_logs(
    source: str | None = Query(default=None, description="'http','pipeline','chat','llm','tool','rabbit','db'"),
    severity: str | None = Query(default=None, description="'warning','error','critical'"),
    engine_type: str | None = Query(default=None, description="'pipeline','chat','http','system'"),
    engine_id: str | None = Query(default=None, description="Filter by engine_id"),
    agent: str | None = Query(default=None, description="Filter by agent name (LLM errors)"),
    step_id: str | None = Query(default=None, description="Filter by step_id (LLM/pipeline errors)"),
    prod_name: str | None = Query(default=None, description="Filter by project name"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Query the unified ``log_errors`` table.

    To retrieve only LLM errors (previously ``/logs/ai/errors``), pass
    ``?source=llm``.
    """
    conditions: list[str] = []
    params: list[Any] = []

    if source:
        conditions.append("source = %s")
        params.append(source)
    if severity:
        conditions.append("severity = %s")
        params.append(severity)
    if engine_type:
        conditions.append("engine_type = %s")
        params.append(engine_type)
    if engine_id:
        conditions.append("engine_id = %s")
        params.append(engine_id)
    if agent:
        conditions.append("agent = %s")
        params.append(agent)
    if step_id:
        conditions.append("step_id = %s")
        params.append(step_id)
    if prod_name:
        conditions.append("prod_name = %s")
        params.append(prod_name)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT * FROM log_error {where} ORDER BY id DESC"

    from fastapi.concurrency import run_in_threadpool
    rows = await run_in_threadpool(database.query, sql, tuple(params), limit, offset)
    return {"total": len(rows), "offset": offset, "errors": rows}
