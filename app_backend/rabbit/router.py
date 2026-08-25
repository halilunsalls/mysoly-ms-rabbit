"""Central RabbitMQ message router.

Maps incoming message patterns (queue.pattern) to the same engine functions
that the HTTP API uses.  The HTTP endpoints in api/main.py and api/chats.py
are not modified — this module is an additional entry-point that calls the
same underlying business logic.

Message format (Node.js compatible):
  Incoming: { "pattern": "run", "data": { ... } }
  Reply ok: { "data": { "payload": { ... } } }
  Reply err: { "data": { "error": { "message": "...", "code": "...", "status": 500 } } }

Pattern → handler mapping:
  pipeline.run              POST   /run
  pipeline.list             GET    /pipelines
  pipeline.diagram          GET    /pipelines/{name}/diagram
  chat.diagram              GET    /chats/{name}/diagram
  chat.list                 GET    /chats
  chat.get                  GET    /chats/{name}
  chat.session.create       POST   /chats/{name}/sessions
  chat.session.message      POST   /chats/{name}/sessions/{id}/messages
  chat.session.messages     GET    /chats/{name}/sessions/{id}/messages
  chat.session.end          DELETE /chats/{name}/sessions/{id}
  chat.session.list         GET    /chats/{name}/sessions
  chat.session.get          GET    /chats/{name}/sessions/{id}
  chat.session.vars         PATCH  /chats/{name}/sessions/{id}/vars
  logs.ai                   GET    /logs/ai
  logs.pipeline             GET    /logs/pipeline
  logs.chat                 GET    /logs/chat
  logs.errors               GET    /logs/errors
"""
from __future__ import annotations

import json
import logging
import traceback as _traceback
from typing import Any

from db import database

logger = logging.getLogger(__name__)


async def central_router(name: str, data: Any) -> dict:
    """Route a RabbitMQ message to the appropriate handler.

    Args:
        name: Fully-qualified event name, e.g. "pipeline.run".
        data: The deserialized payload from the message body.

    Returns:
        A dict in the shape { "payload": ... } or { "error": ... }.
    """
    data = data or {}

    try:
        handler = _ROUTES.get(name)
        if handler is None:
            return _err(f"No handler registered for '{name}'", "NO_HANDLER", 404)
        return await handler(data)
    except Exception as exc:
        logger.exception(f"Router error for '{name}': {exc}")
        database.insert_error(
            source="rabbit",
            error_type=type(exc).__name__,
            error_message=str(exc),
            severity="error",
            traceback=_traceback.format_exc(),
            context={"pattern": name},
        )
        return _err(str(exc), getattr(exc, "code", "INTERNAL_ERROR"), 500)


# ── Pipeline handlers ─────────────────────────────────────────────────────────

async def _pipeline_run(data: dict) -> dict:
    """pipeline.run → pipeline_runner.run()

    Expected data:
      { "pipeline": "text_to_sql", "input": {...}, "dry_run": false, "prod_name": "default" }
    """
    import asyncio
    from engine import pipeline_runner

    pipeline_name = data.get("pipeline")
    input_data = data.get("input", {})
    dry_run = bool(data.get("dry_run", False))
    prod_name = data.get("prod_name", "default")

    if not pipeline_name:
        return _err("'pipeline' field is required", "VALIDATION_ERROR", 422)

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        lambda: pipeline_runner.run(
            pipeline_name=pipeline_name,
            input_data=input_data,
            dry_run=dry_run,
            prod_name=prod_name,
            triggered_by="rabbit",
        ),
    )
    return _ok(result)


async def _pipeline_list(_data: dict) -> dict:
    """pipeline.list → pipeline_runner.list_pipelines()"""
    from engine import pipeline_runner

    return _ok({"pipelines": pipeline_runner.list_pipelines()})


async def _pipeline_diagram(data: dict) -> dict:
    """pipeline.diagram → flow_to_mermaid()

    Expected data: { "pipeline": "text_to_sql", "format": "json" }
    """
    import json as _json
    from pathlib import Path
    from engine.flow_schema import FlowDef
    from engine.mermaid_builder import flow_to_mermaid
    from pydantic import ValidationError

    pipeline_name = data.get("pipeline")
    if not pipeline_name:
        return _err("'pipeline' field is required", "VALIDATION_ERROR", 422)

    pipelines_dir = Path(__file__).parent.parent / "pipelines"
    flow_path = pipelines_dir / pipeline_name / "flow.json"
    if not flow_path.exists():
        return _err(f"Pipeline '{pipeline_name}' not found", "NOT_FOUND", 404)

    try:
        raw = _json.loads(flow_path.read_text(encoding="utf-8"))
        flow = FlowDef.model_validate(raw)
    except (_json.JSONDecodeError, ValidationError) as exc:
        database.insert_error(
            source="rabbit",
            error_type=type(exc).__name__,
            error_message=str(exc),
            severity="error",
            context={"pattern": "pipeline.diagram", "pipeline": pipeline_name},
        )
        return _err(str(exc), "INVALID_FLOW", 422)

    mermaid = flow_to_mermaid(flow)
    fmt = data.get("format", "json")
    if fmt == "text":
        return _ok({"pipeline": pipeline_name, "mermaid": mermaid, "format": "text"})
    return _ok({"pipeline": pipeline_name, "mermaid": mermaid})


# ── Chat handlers ─────────────────────────────────────────────────────────────

async def _chat_diagram(data: dict) -> dict:
    """chat.diagram → chat_runner.load_chat + chat_to_mermaid

    Expected data: { "chat_name": "my_chat" }

    Loads the chat definition, resolves the on_message pipeline, and renders
    an inline subgraph mermaid diagram that includes the pipeline flow.
    """
    import asyncio
    from pathlib import Path
    from engine import chat_runner
    from engine.mermaid_builder import chat_to_mermaid

    chat_name = data.get("chat_name")
    if not chat_name:
        return _err("'chat_name' field is required", "VALIDATION_ERROR", 422)

    pipelines_dir = Path(__file__).parent.parent / "pipelines"

    try:
        loop = asyncio.get_event_loop()
        raw = await loop.run_in_executor(None, chat_runner._load_chat, chat_name)
        chat_def = await loop.run_in_executor(None, chat_runner._validate_chat, chat_name, raw)
    except FileNotFoundError as exc:
        return _err(str(exc), "NOT_FOUND", 404)
    except ValueError as exc:
        return _err(str(exc), "VALIDATION_ERROR", 422)

    try:
        mermaid = chat_to_mermaid(chat_def, pipelines_dir=pipelines_dir)
    except Exception as exc:
        database.insert_error(
            source="rabbit",
            error_type=type(exc).__name__,
            error_message=str(exc),
            severity="error",
            context={"pattern": "chat.diagram", "chat_name": chat_name},
        )
        return _err(str(exc), "DIAGRAM_ERROR", 500)

    return _ok({"chat": chat_name, "mermaid": mermaid})


async def _chat_list(_data: dict) -> dict:
    """chat.list → chat_runner.list_chats()"""
    from engine import chat_runner

    return _ok({"chats": chat_runner.list_chats()})


async def _chat_run(data: dict) -> dict:
    """chat.run → create session if needed, then send message.

    Expected data:
      {
        "chat_name": "my_bot",        required
        "prod_name": "default",       required
        "account_id": "...",          required
        "group_id": "...",            required
        "message": "Hello",           required
        "session_id": "..."           optional — omit or null to create a new session
      }
    """
    import asyncio
    from engine import chat_runner
    from db import database

    chat_name = data.get("chat_name")
    prod_name = data.get("prod_name")
    account_id = data.get("account_id")
    group_id = data.get("group_id")
    message = data.get("message")
    session_id = data.get("session_id") or None

    if not chat_name:
        return _err("'chat_name' field is required", "VALIDATION_ERROR", 422)
    if not prod_name:
        return _err("'prod_name' field is required", "VALIDATION_ERROR", 422)
    if not account_id:
        return _err("'account_id' field is required", "VALIDATION_ERROR", 422)
    if not group_id:
        return _err("'group_id' field is required", "VALIDATION_ERROR", 422)
    if not message:
        return _err("'message' field is required", "VALIDATION_ERROR", 422)

    loop = asyncio.get_event_loop()

    if session_id:
        session = await loop.run_in_executor(None, database.get_chat_session, session_id)
        if session is None:
            return _err(f"Session '{session_id}' not found.", "NOT_FOUND", 404)
        if session.get("status") != "active":
            return _err(
                f"Session '{session_id}' is not active (status='{session.get('status')}').",
                "SESSION_NOT_ACTIVE",
                422,
            )
    else:
        try:
            session_id = await loop.run_in_executor(
                None, chat_runner.create_session,
                chat_name, account_id, group_id, None, prod_name,
            )
        except FileNotFoundError as exc:
            return _err(str(exc), "NOT_FOUND", 404)
        except Exception as exc:
            database.insert_error(
                source="rabbit",
                error_type=type(exc).__name__,
                error_message=str(exc),
                severity="error",
                engine_type="chat",
                engine_id="",
                traceback=_traceback.format_exc(),
                context={"pattern": "chat.run", "action": "create_session", "chat_name": chat_name},
                prod_name=prod_name or "",
            )
            return _err(str(exc), "ERROR", 500)

    try:
        result = await loop.run_in_executor(
            None, chat_runner.send_message, chat_name, session_id, message
        )
    except Exception as exc:
        database.insert_error(
            source="rabbit",
            error_type=type(exc).__name__,
            error_message=str(exc),
            severity="error",
            engine_type="chat",
            engine_id=session_id or "",
            traceback=_traceback.format_exc(),
            context={"pattern": "chat.run", "action": "send_message", "chat_name": chat_name},
            prod_name=prod_name or "",
        )
        return _err(str(exc), "ERROR", 500)

    return _ok({
        "session_id": result["session_id"],
        "response": result["response"],
    })


async def _chat_session_end(data: dict) -> dict:
    """chat.session.end → chat_runner.end_session()

    Expected data: { "chat_name": "my_bot", "session_id": "..." }
    """
    import asyncio
    from engine import chat_runner

    chat_name = data.get("chat_name")
    session_id = data.get("session_id")
    if not chat_name or not session_id:
        return _err("'chat_name' and 'session_id' are required", "VALIDATION_ERROR", 422)

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, chat_runner.end_session, session_id, chat_name)
    return _ok({"session_id": session_id, "status": "ended"})


async def _chat_session_list(data: dict) -> dict:
    """chat.session.list → database.query(chat_session)

    Expected data:
      {
        "chat_name": "my_bot",
        "status": "active",
        "prod_name": "...",
        "group_id": "...",
        "account_id": "...",
        "limit": 50,
        "offset": 0
      }
    """
    import asyncio

    chat_name = data.get("chat_name")
    if not chat_name:
        return _err("'chat_name' is required", "VALIDATION_ERROR", 422)

    status = data.get("status")
    prod_name = data.get("prod_name")
    group_id = data.get("group_id")
    account_id = data.get("account_id")
    limit = int(data.get("limit", 50))
    offset = int(data.get("offset", 0))

    conditions = ["chat_name = %s"]
    params: list = [chat_name]
    if status:
        conditions.append("status = %s")
        params.append(status)
    if prod_name:
        conditions.append("prod_name = %s")
        params.append(prod_name)
    if group_id:
        conditions.append("group_id = %s")
        params.append(group_id)
    if account_id:
        conditions.append("account_id = %s")
        params.append(account_id)

    sql = f"SELECT * FROM chat_session WHERE {' AND '.join(conditions)} ORDER BY created_at DESC"
    loop = asyncio.get_event_loop()
    rows = await loop.run_in_executor(None, database.query, sql, tuple(params), limit, offset)

    return _ok({"chat_name": chat_name, "total": len(rows), "offset": offset, "sessions": rows})


async def _chat_session_get(data: dict) -> dict:
    """chat.session.get → database.get_chat_session()

    Expected data: { "session_id": "..." }
    """
    import asyncio

    session_id = data.get("session_id")
    if not session_id:
        return _err("'session_id' is required", "VALIDATION_ERROR", 422)

    loop = asyncio.get_event_loop()
    session = await loop.run_in_executor(None, database.get_chat_session, session_id)
    if session is None:
        return _err(f"Session '{session_id}' not found", "NOT_FOUND", 404)

    return _ok(dict(session))


async def _chat_get(data: dict) -> dict:
    """chat.get → chat_runner._load_chat() + _validate_chat()

    Expected data: { "chat_name": "my_bot" }
    """
    import asyncio
    from engine import chat_runner

    chat_name = data.get("chat_name")
    if not chat_name:
        return _err("'chat_name' is required", "VALIDATION_ERROR", 422)

    try:
        loop = asyncio.get_event_loop()
        raw = await loop.run_in_executor(None, chat_runner._load_chat, chat_name)
        chat_def = await loop.run_in_executor(None, chat_runner._validate_chat, chat_name, raw)
    except FileNotFoundError as exc:
        return _err(str(exc), "NOT_FOUND", 404)
    except ValueError as exc:
        return _err(str(exc), "VALIDATION_ERROR", 422)

    active_sessions = await asyncio.get_event_loop().run_in_executor(
        None, database.count_chat_sessions, chat_name, "active"
    )

    return _ok({
        "name": chat_def.name,
        "description": chat_def.description,
        "on_message": chat_def.on_message.model_dump(),
        "history": chat_def.history.model_dump(),
        "hooks": chat_def.hooks.model_dump(),
        "session_vars_schema": chat_def.session_vars,
        "extract_vars": chat_def.extract_vars,
        "active_sessions": active_sessions,
    })


async def _chat_session_messages(data: dict) -> dict:
    """chat.session.messages → database.get_chat_messages()

    Expected data:
      { "chat_name": "my_bot", "session_id": "...", "limit": 50, "offset": 0 }
    """
    import asyncio

    session_id = data.get("session_id")
    if not session_id:
        return _err("'session_id' is required", "VALIDATION_ERROR", 422)

    session = await asyncio.get_event_loop().run_in_executor(
        None, database.get_chat_session, session_id
    )
    if session is None:
        return _err(f"Session '{session_id}' not found", "NOT_FOUND", 404)

    chat_name = data.get("chat_name")
    if chat_name and session.get("chat_name") != chat_name:
        return _err(
            f"Session '{session_id}' not found for chat '{chat_name}'",
            "NOT_FOUND",
            404,
        )

    limit = int(data.get("limit", 50))
    offset = int(data.get("offset", 0))

    loop = asyncio.get_event_loop()
    messages = await loop.run_in_executor(
        None, database.get_chat_messages, session_id, limit, offset
    )
    return _ok({
        "session_id": session_id,
        "total": len(messages),
        "offset": offset,
        "messages": messages,
    })


async def _chat_session_vars(data: dict) -> dict:
    """chat.session.vars → update_chat_session() with merged vars

    Expected data:
      { "chat_name": "my_bot", "session_id": "...", "vars": { "key": "value" } }
    """
    import asyncio
    from datetime import datetime, timezone

    session_id = data.get("session_id")
    vars_patch = data.get("vars")
    if not session_id:
        return _err("'session_id' is required", "VALIDATION_ERROR", 422)
    if not isinstance(vars_patch, dict):
        return _err("'vars' must be a JSON object", "VALIDATION_ERROR", 422)

    loop = asyncio.get_event_loop()
    session = await loop.run_in_executor(None, database.get_chat_session, session_id)
    if session is None:
        return _err(f"Session '{session_id}' not found", "NOT_FOUND", 404)

    chat_name = data.get("chat_name")
    if chat_name and session.get("chat_name") != chat_name:
        return _err(
            f"Session '{session_id}' not found for chat '{chat_name}'",
            "NOT_FOUND",
            404,
        )

    raw_vars = session.get("session_vars") or {}
    current: dict = raw_vars if isinstance(raw_vars, dict) else {}
    updated = {**current, **vars_patch}
    ts = datetime.now(timezone.utc).isoformat()

    await loop.run_in_executor(
        None, database.update_chat_session, session_id, None, updated, ts, 0
    )
    return _ok({"session_id": session_id, "session_vars": updated})


# ── Log handlers ──────────────────────────────────────────────────────────────

async def _logs_ai(data: dict) -> dict:
    """logs.ai → SELECT * FROM log_llm_call

    Expected data (all optional):
      { "engine_type": "...", "engine_id": "...", "agent": "...",
        "prod_name": "...", "limit": 50, "offset": 0 }
    """
    import asyncio

    conditions: list[str] = []
    params: list = []

    if data.get("engine_type"):
        conditions.append("engine_type = %s")
        params.append(data["engine_type"])
    if data.get("engine_id"):
        conditions.append("engine_id = %s")
        params.append(data["engine_id"])
    if data.get("agent"):
        conditions.append("agent = %s")
        params.append(data["agent"])
    if data.get("prod_name"):
        conditions.append("prod_name = %s")
        params.append(data["prod_name"])

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT * FROM log_llm_call {where} ORDER BY id DESC"
    limit = int(data.get("limit", 50))
    offset = int(data.get("offset", 0))

    loop = asyncio.get_event_loop()
    rows = await loop.run_in_executor(None, database.query, sql, tuple(params), limit, offset)
    return _ok({"total": len(rows), "offset": offset, "logs": rows})


async def _logs_pipeline(data: dict) -> dict:
    """logs.pipeline → SELECT * FROM log_pipeline_run

    Expected data (all optional):
      { "run_id": "...", "pipeline_name": "...", "status": "running|success|error",
        "triggered_by": "...", "prod_name": "...", "limit": 50, "offset": 0 }
    """
    import asyncio

    conditions: list[str] = []
    params: list = []

    if data.get("run_id"):
        conditions.append("id = %s")
        params.append(data["run_id"])
    if data.get("pipeline_name"):
        conditions.append("pipeline_name = %s")
        params.append(data["pipeline_name"])
    if data.get("triggered_by"):
        conditions.append("triggered_by = %s")
        params.append(data["triggered_by"])
    if data.get("prod_name"):
        conditions.append("prod_name = %s")
        params.append(data["prod_name"])
    if data.get("status"):
        conditions.append("status = %s")
        params.append(data["status"])

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT * FROM log_pipeline_run {where} ORDER BY started_at DESC"
    limit = int(data.get("limit", 50))
    offset = int(data.get("offset", 0))

    loop = asyncio.get_event_loop()
    rows = await loop.run_in_executor(None, database.query, sql, tuple(params), limit, offset)
    return _ok({"total": len(rows), "offset": offset, "logs": rows})


async def _logs_errors(data: dict) -> dict:
    """logs.errors → SELECT * FROM log_error

    Expected data (all optional):
      { "source": "...", "severity": "...", "engine_id": "...", "engine_type": "...",
        "agent": "...", "step_id": "...", "prod_name": "...", "limit": 50, "offset": 0 }
    """
    import asyncio

    conditions: list[str] = []
    params: list = []

    if data.get("source"):
        conditions.append("source = %s")
        params.append(data["source"])
    if data.get("severity"):
        conditions.append("severity = %s")
        params.append(data["severity"])
    if data.get("engine_type"):
        conditions.append("engine_type = %s")
        params.append(data["engine_type"])
    if data.get("engine_id"):
        conditions.append("engine_id = %s")
        params.append(data["engine_id"])
    if data.get("agent"):
        conditions.append("agent = %s")
        params.append(data["agent"])
    if data.get("step_id"):
        conditions.append("step_id = %s")
        params.append(data["step_id"])
    if data.get("prod_name"):
        conditions.append("prod_name = %s")
        params.append(data["prod_name"])

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT * FROM log_error {where} ORDER BY id DESC"
    limit = int(data.get("limit", 50))
    offset = int(data.get("offset", 0))

    loop = asyncio.get_event_loop()
    rows = await loop.run_in_executor(None, database.query, sql, tuple(params), limit, offset)
    return _ok({"total": len(rows), "offset": offset, "errors": rows})


async def _logs_chat(data: dict) -> dict:
    """logs.chat → SELECT * FROM chat_session

    Expected data (all optional):
      { "chat_name": "...", "status": "...", "prod_name": "...",
        "group_id": "...", "account_id": "...", "limit": 50, "offset": 0 }
    """
    import asyncio

    conditions: list[str] = []
    params: list = []

    if data.get("chat_name"):
        conditions.append("chat_name = %s")
        params.append(data["chat_name"])
    if data.get("status"):
        conditions.append("status = %s")
        params.append(data["status"])
    if data.get("prod_name"):
        conditions.append("prod_name = %s")
        params.append(data["prod_name"])
    if data.get("group_id"):
        conditions.append("group_id = %s")
        params.append(data["group_id"])
    if data.get("account_id"):
        conditions.append("account_id = %s")
        params.append(data["account_id"])

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT * FROM chat_session {where} ORDER BY created_at DESC"
    limit = int(data.get("limit", 50))
    offset = int(data.get("offset", 0))

    loop = asyncio.get_event_loop()
    rows = await loop.run_in_executor(None, database.query, sql, tuple(params), limit, offset)
    return _ok({"total": len(rows), "offset": offset, "sessions": rows})


# ── Route table ───────────────────────────────────────────────────────────────

_ROUTES: dict[str, Any] = {
    # Pipeline
    "pipeline.run": _pipeline_run,
    "pipeline.list": _pipeline_list,
    "pipeline.diagram": _pipeline_diagram,
    # Chat
    "chat.diagram": _chat_diagram,
    "chat.list": _chat_list,
    "chat.get": _chat_get,
    "chat.run": _chat_run,
    "chat.session.messages": _chat_session_messages,
    "chat.session.end": _chat_session_end,
    "chat.session.list": _chat_session_list,
    "chat.session.get": _chat_session_get,
    "chat.session.vars": _chat_session_vars,
    # Logs
    "logs.ai": _logs_ai,
    "logs.pipeline": _logs_pipeline,
    "logs.chat": _logs_chat,
    "logs.errors": _logs_errors,
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ok(payload: Any) -> dict:
    """Wrap a successful result in the standard reply envelope."""
    return {"payload": payload}


def _err(message: str, code: str = "ERROR", status: int = 500) -> dict:
    """Wrap an error in the standard reply envelope."""
    return {"error": {"message": message, "code": code, "status": status}}
