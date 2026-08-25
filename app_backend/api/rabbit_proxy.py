"""RabbitMQ proxy router.

HTTP endpoints that forward requests to the internal RabbitMQ bus and return
the reply.  Useful for testing the full RabbitMQ round-trip from a REST client
without needing a separate AMQP client.

Endpoint overview
-----------------
POST /rabbit/send          — generic pattern + data, returns reply
POST /rabbit/pipeline/run  — shorthand for pipeline.run
GET  /rabbit/pipeline/list — shorthand for pipeline.list
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from rabbit.rabbit import RabbitMQError, send_message_for_reply

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/rabbit", tags=["RabbitMQ Proxy"])


# ── Request models ────────────────────────────────────────────────────────────

class SendRequest(BaseModel):
    pattern: str
    data: Any = None
    timeout: int | None = None


class PipelineRunRequest(BaseModel):
    pipeline: str
    input: dict = {}
    dry_run: bool = False
    timeout: int | None = None


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _send(pattern: str, data: Any, timeout: int | None) -> dict:
    """Send a message via RabbitMQ and return the reply payload."""
    queue = pattern.split(".")[0]
    name = f"{queue}.{'.'.join(pattern.split('.')[1:])}" if "." in pattern else queue

    try:
        result = await send_message_for_reply(name, data, timeout=timeout)
    except RabbitMQError as exc:
        raise HTTPException(status_code=exc.status, detail={"message": str(exc), "code": exc.code})
    except TimeoutError as exc:
        raise HTTPException(status_code=504, detail={"message": str(exc), "code": "TIMEOUT"})
    except Exception as exc:
        logger.exception(f"RabbitMQ proxy error for pattern '{pattern}': {exc}")
        raise HTTPException(status_code=503, detail={"message": "RabbitMQ unavailable", "code": "RABBIT_ERROR"})

    return {"pattern": pattern, "data": result}


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/send")
async def rabbit_send(body: SendRequest):
    """Send any pattern to RabbitMQ and return the reply.

    Example body:
    ```json
    {
      "pattern": "pipeline.run",
      "data": { "pipeline": "text_summarizer", "input": { "text": "Hello" } }
    }
    ```
    """
    return await _send(body.pattern, body.data, body.timeout)


@router.post("/pipeline/run")
async def rabbit_pipeline_run(body: PipelineRunRequest):
    """Send pipeline.run via RabbitMQ and return the result.

    Example body:
    ```json
    {
      "pipeline": "text_summarizer",
      "input": { "text": "Hello world" },
      "dry_run": false
    }
    ```
    """
    data = {"pipeline": body.pipeline, "input": body.input, "dry_run": body.dry_run}
    return await _send("pipeline.run", data, body.timeout)


@router.get("/pipeline/list")
async def rabbit_pipeline_list():
    """Fetch the pipeline list via RabbitMQ."""
    return await _send("pipeline.list", {}, None)
