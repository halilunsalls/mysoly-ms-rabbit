"""Chat engine API router.

All chat endpoints are defined here and mounted onto the main FastAPI app
via ``app.include_router(chats_router)`` in ``api/main.py``.

Endpoint overview
-----------------
POST   /chat/run                                       — run a chat turn (creates session if needed)
GET    /chats                                          — list all available chats
GET    /chats/{chat_name}                              — get chat definition + active session count
GET    /chats/{chat_name}/diagram                      — Mermaid flowchart for the chat (+ inline pipelines)
GET    /chats/{chat_name}/sessions                     — list sessions for a chat
GET    /chats/{chat_name}/sessions/{session_id}        — get a single session
DELETE /chats/{chat_name}/sessions/{session_id}        — end a session
PATCH  /chats/{chat_name}/sessions/{session_id}/vars   — manually update session_vars
GET    /chats/{chat_name}/sessions/{session_id}/messages — paginated message history
GET    /logs/chat                                      — query chat_session log
"""
from __future__ import annotations

import traceback
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from db import database
from engine import chat_runner

router = APIRouter(tags=["chats"])


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class ChatRunRequest(BaseModel):
    chat_name: str
    prod_name: str
    account_id: str
    group_id: str
    session_id: str | None = None
    message: str


class ChatRunResponse(BaseModel):
    session_id: str
    response: str


class UpdateVarsRequest(BaseModel):
    vars: dict[str, Any]


# ---------------------------------------------------------------------------
# Chat run — unified session-create + message endpoint
# ---------------------------------------------------------------------------

@router.post("/chat/run", response_model=ChatRunResponse)
async def chat_run(request: ChatRunRequest) -> ChatRunResponse:
    """Send a message, creating a new session automatically if session_id is omitted.

    - ``session_id`` absent or null → new session is created first, then the
      message is processed.
    - ``session_id`` present → the existing session is continued.  Returns 404
      if the session does not exist; 422 if the session is not active.
    """
    session_id = request.session_id

    if session_id:
        session = await run_in_threadpool(database.get_chat_session, session_id)
        if session is None:
            raise HTTPException(
                status_code=404,
                detail=f"Session '{session_id}' not found.",
            )
        if session.get("status") != "active":
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Session '{session_id}' is not active "
                    f"(status='{session.get('status')}')."
                ),
            )
    else:
        try:
            session_id = await run_in_threadpool(
                chat_runner.create_session,
                request.chat_name,
                request.account_id,
                request.group_id,
                None,
                request.prod_name,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:
            database.insert_error(
                source="http",
                error_type=type(exc).__name__,
                error_message=str(exc),
                severity="error",
                engine_type="chat",
                engine_id="",
                traceback=traceback.format_exc(),
                context={"path": "/chat/run", "action": "create_session", "chat_name": request.chat_name},
                prod_name=request.prod_name,
            )
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    try:
        result = await run_in_threadpool(
            chat_runner.send_message,
            request.chat_name,
            session_id,
            request.message,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        database.insert_error(
            source="http",
            error_type=type(exc).__name__,
            error_message=str(exc),
            severity="error",
            engine_type="chat",
            engine_id=session_id or "",
            traceback=traceback.format_exc(),
            context={"path": "/chat/run", "action": "send_message", "chat_name": request.chat_name},
            prod_name=request.prod_name,
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return ChatRunResponse(
        session_id=result["session_id"],
        response=result["response"],
    )


# ---------------------------------------------------------------------------
# Chat discovery
# ---------------------------------------------------------------------------

@router.get("/chats")
async def list_chats() -> dict:
    """Return all discoverable chats (directories with a chat.json)."""
    return {"chats": chat_runner.list_chats()}


@router.get("/chats/{chat_name}")
async def get_chat(chat_name: str) -> dict:
    """Return the parsed chat.json definition and active session count."""
    try:
        raw = await run_in_threadpool(chat_runner._load_chat, chat_name)
        chat_def = await run_in_threadpool(chat_runner._validate_chat, chat_name, raw)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    active_sessions = await run_in_threadpool(
        database.count_chat_sessions, chat_name, "active"
    )

    return {
        "name": chat_def.name,
        "description": chat_def.description,
        "on_message": chat_def.on_message.model_dump(),
        "history": chat_def.history.model_dump(),
        "hooks": chat_def.hooks.model_dump(),
        "session_vars_schema": chat_def.session_vars,
        "extract_vars": chat_def.extract_vars,
        "active_sessions": active_sessions,
    }


# ---------------------------------------------------------------------------
# Chat diagram
# ---------------------------------------------------------------------------

@router.get("/chats/{chat_name}/diagram")
async def get_chat_diagram(
    chat_name: str,
    format: str = Query(default="json", description="'json' or 'text'"),
) -> dict:
    """Return a Mermaid flowchart for the named chat.

    Any pipeline referenced in ``on_message`` is rendered inline as a
    labelled subgraph so the full execution path is visible in one diagram.
    """
    from pathlib import Path
    from engine.mermaid_builder import chat_to_mermaid

    try:
        raw = await run_in_threadpool(chat_runner._load_chat, chat_name)
        chat_def = await run_in_threadpool(chat_runner._validate_chat, chat_name, raw)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    pipelines_dir = Path(__file__).parent.parent / "pipelines"
    mermaid = await run_in_threadpool(chat_to_mermaid, chat_def, pipelines_dir)

    if format == "text":
        return PlainTextResponse(mermaid)
    return {"chat": chat_name, "mermaid": mermaid}


# ---------------------------------------------------------------------------
# Session lifecycle
# ---------------------------------------------------------------------------

@router.get("/chats/{chat_name}/sessions")
async def list_sessions(
    chat_name: str,
    status: str | None = Query(default=None, description="'active', 'ended', 'error', or 'expired'"),
    prod_name: str | None = Query(default=None, description="Filter by project name"),
    group_id: str | None = Query(default=None, description="Filter by group"),
    account_id: str | None = Query(default=None, description="Filter by account"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Return paginated sessions for a chat, newest first."""
    conditions = ["chat_name = %s"]
    params: list[Any] = [chat_name]
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

    where = "WHERE " + " AND ".join(conditions)
    sql = f"SELECT * FROM chat_session {where} ORDER BY created_at DESC"

    rows = await run_in_threadpool(database.query, sql, tuple(params), limit, offset)
    return {"chat_name": chat_name, "total": len(rows), "offset": offset, "sessions": rows}


@router.get("/chats/{chat_name}/sessions/{session_id}")
async def get_session(chat_name: str, session_id: str) -> dict:
    """Return session metadata, vars, and message count."""
    session = await run_in_threadpool(database.get_chat_session, session_id)
    if session is None or session.get("chat_name") != chat_name:
        raise HTTPException(
            status_code=404,
            detail=f"Session '{session_id}' not found for chat '{chat_name}'.",
        )
    return dict(session)


@router.delete("/chats/{chat_name}/sessions/{session_id}")
async def end_session(chat_name: str, session_id: str) -> dict:
    """End a chat session."""
    session = await run_in_threadpool(database.get_chat_session, session_id)
    if session is None or session.get("chat_name") != chat_name:
        raise HTTPException(
            status_code=404,
            detail=f"Session '{session_id}' not found for chat '{chat_name}'.",
        )
    if session.get("status") == "ended":
        return {"session_id": session_id, "status": "ended", "detail": "already ended"}

    try:
        await run_in_threadpool(chat_runner.end_session, session_id, chat_name)
    except Exception as exc:
        database.insert_error(
            source="http",
            error_type=type(exc).__name__,
            error_message=str(exc),
            severity="error",
            engine_type="chat",
            engine_id=session_id,
            traceback=traceback.format_exc(),
            context={"path": f"/chats/{chat_name}/sessions/{session_id}", "action": "end_session"},
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {"session_id": session_id, "status": "ended"}


@router.patch("/chats/{chat_name}/sessions/{session_id}/vars")
async def update_session_vars(
    chat_name: str,
    session_id: str,
    request: UpdateVarsRequest,
) -> dict:
    """Manually merge vars into session_vars."""
    session = await run_in_threadpool(database.get_chat_session, session_id)
    if session is None or session.get("chat_name") != chat_name:
        raise HTTPException(
            status_code=404,
            detail=f"Session '{session_id}' not found for chat '{chat_name}'.",
        )

    raw_vars = session.get("session_vars") or {}
    current: dict[str, Any] = raw_vars if isinstance(raw_vars, dict) else {}
    updated = {**current, **request.vars}
    ts = datetime.now(timezone.utc).isoformat()

    await run_in_threadpool(
        database.update_chat_session,
        session_id,
        None,
        updated,
        ts,
        0,
    )

    return {"session_id": session_id, "session_vars": updated}


# ---------------------------------------------------------------------------
# Messaging — read-only (write goes through /chat/run)
# ---------------------------------------------------------------------------

@router.get("/chats/{chat_name}/sessions/{session_id}/messages")
async def get_messages(
    chat_name: str,
    session_id: str,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Return paginated conversation history (oldest first)."""
    session = await run_in_threadpool(database.get_chat_session, session_id)
    if session is None or session.get("chat_name") != chat_name:
        raise HTTPException(
            status_code=404,
            detail=f"Session '{session_id}' not found for chat '{chat_name}'.",
        )

    messages = await run_in_threadpool(
        database.get_chat_messages,
        session_id,
        limit,
        offset,
    )
    return {
        "session_id": session_id,
        "total": len(messages),
        "offset": offset,
        "messages": messages,
    }


# ---------------------------------------------------------------------------
# Chat logs
# ---------------------------------------------------------------------------

@router.get("/logs/chat")
async def get_chat_logs(
    chat_name: str | None = Query(default=None, description="Filter by chat name"),
    status: str | None = Query(default=None, description="'active', 'ended', or 'expired'"),
    prod_name: str | None = Query(default=None, description="Filter by project name"),
    group_id: str | None = Query(default=None, description="Filter by group"),
    account_id: str | None = Query(default=None, description="Filter by account"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Query ``chat_session``."""
    conditions: list[str] = []
    params: list[Any] = []

    if chat_name:
        conditions.append("chat_name = %s")
        params.append(chat_name)
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

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT * FROM chat_session {where} ORDER BY created_at DESC"

    rows = await run_in_threadpool(
        database.query, sql, tuple(params), limit, offset
    )
    return {"total": len(rows), "offset": offset, "sessions": rows}
