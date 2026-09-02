import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from typing import Any
from fastapi.responses import FileResponse

import aio_pika
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

load_dotenv()
logger = logging.getLogger(__name__)


class RabbitMQConfig:
    def __init__(self):
        self.host     = os.getenv("RABBIT_HOST", os.getenv("RABBITMQ_HOST", "localhost")).strip()
        self.port     = int(os.getenv("RABBIT_PORT", os.getenv("RABBITMQ_PORT", "5672")).strip())
        self.user     = os.getenv("RABBIT_USER", os.getenv("RABBITMQ_USER", "guest")).strip()
        self.password = os.getenv("RABBIT_PASS", os.getenv("RABBITMQ_PASSWORD", "guest")).strip()
        self.vhost    = os.getenv("RABBIT_VHOST", os.getenv("RABBITMQ_VHOST", "/")).strip()
        self.timeout  = int(os.getenv("RABBIT_TIMEOUT", "600"))

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k) and v is not None:
                setattr(self, k, v)

    def dsn(self) -> str:
        vhost = "%2F" if self.vhost == "/" else self.vhost
        return f"amqp://{self.user}:{self.password}@{self.host}:{self.port}/{vhost}"

    def to_dict(self) -> dict:
        return {
            "host": self.host, "port": self.port,
            "user": self.user, "password": self.password,
            "vhost": self.vhost, "timeout": self.timeout,
        }


config = RabbitMQConfig()
templates = Jinja2Templates(directory="templates")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(title="mysoly RabbitMQ Client", lifespan=lifespan)

# Route keys must match microservice rabbit/router.py _ROUTES:
# queue + "." + pattern == key (e.g. chat + run → chat.run).

KNOWN_ROUTES: dict[str, dict] = {
    # ── Pipeline ───────────────────────────────────────────────────────────
    "pipeline.run": {
        "queue": "pipeline", "pattern": "run",
        "desc": "Run a pipeline (triggered_by=rabbit on server)",
        "http": "POST /run",
        "template": {
            "pipeline": "my_pipeline",
            "input": {"user_message": "Hello"},
            "dry_run": False,
            "prod_name": "default",
        },
        "capture": [],
    },
    "pipeline.list": {
        "queue": "pipeline", "pattern": "list",
        "desc": "List available pipelines",
        "http": "GET /pipelines",
        "template": {},
        "capture": [],
    },
    "pipeline.diagram": {
        "queue": "pipeline", "pattern": "diagram",
        "desc": "Get pipeline Mermaid diagram",
        "http": "GET /pipelines/{name}/diagram",
        "template": {"pipeline": "my_pipeline", "format": "json"},
        "capture": [],
    },
    # ── Chat (matches router _ROUTES order) ───────────────────────────────
    "chat.diagram": {
        "queue": "chat", "pattern": "diagram",
        "desc": "Get chat pipeline Mermaid diagram",
        "http": "GET /chats/{name}/diagram",
        "template": {"chat_name": "my_chat"},
        "capture": [],
    },
    "chat.list": {
        "queue": "chat", "pattern": "list",
        "desc": "List available chats",
        "http": "GET /chats",
        "template": {},
        "capture": [],
    },
    "chat.get": {
        "queue": "chat", "pattern": "get",
        "desc": "Chat definition + active session count",
        "http": "GET /chats/{chat_name}",
        "template": {"chat_name": "my_chat"},
        "capture": [],
    },
    "chat.run": {
        "queue": "chat", "pattern": "run",
        "desc": "Create session if needed, then send message",
        "http": "POST /chat/run",
        "template": {
            "chat_name": "my_chat",
            "prod_name": "default",
            "account_id": "account-1",
            "group_id": "group-1",
            "message": "Hello",
            "session_id": None,
        },
        "capture": ["session_id"],
    },
    "chat.session.messages": {
        "queue": "chat", "pattern": "session.messages",
        "desc": "Message history",
        "http": "GET /chats/{name}/sessions/{id}/messages",
        "template": {
            "chat_name": "my_chat",
            "session_id": "",
            "limit": 50,
            "offset": 0,
        },
        "capture": [],
    },
    "chat.session.end": {
        "queue": "chat", "pattern": "session.end",
        "desc": "End a chat session",
        "http": "DELETE /chats/{name}/sessions/{id}",
        "template": {"chat_name": "my_chat", "session_id": ""},
        "capture": [],
    },
    "chat.session.list": {
        "queue": "chat", "pattern": "session.list",
        "desc": "List sessions for a chat",
        "http": "GET /chats/{name}/sessions",
        "template": {
            "chat_name": "my_chat",
            "status": "active",
            "prod_name": None,
            "group_id": None,
            "account_id": None,
            "limit": 50,
            "offset": 0,
        },
        "capture": [],
    },
    "chat.session.get": {
        "queue": "chat", "pattern": "session.get",
        "desc": "Get session row",
        "http": "GET /chats/{name}/sessions/{id}",
        "template": {"session_id": ""},
        "capture": [],
    },
    "chat.session.vars": {
        "queue": "chat", "pattern": "session.vars",
        "desc": "Merge vars into session_vars",
        "http": "PATCH /chats/{name}/sessions/{id}/vars",
        "template": {
            "chat_name": "my_chat",
            "session_id": "",
            "vars": {"key": "value"},
        },
        "capture": [],
    },
    # ── Logs (tables: log_llm_call, log_pipeline_run, chat_session, log_error)
    "logs.ai": {
        "queue": "logs", "pattern": "ai",
        "desc": "LLM call logs (log_llm_call)",
        "http": "GET /logs/ai",
        "template": {
            "engine_type": None,
            "engine_id": None,
            "agent": None,
            "prod_name": None,
            "limit": 50,
            "offset": 0,
        },
        "capture": [],
    },
    "logs.pipeline": {
        "queue": "logs", "pattern": "pipeline",
        "desc": "Pipeline run logs (log_pipeline_run)",
        "http": "GET /logs/pipeline",
        "template": {
            "run_id": None,
            "pipeline_name": None,
            "status": None,
            "triggered_by": None,
            "prod_name": None,
            "limit": 50,
            "offset": 0,
        },
        "capture": [],
    },
    "logs.chat": {
        "queue": "logs", "pattern": "chat",
        "desc": "Chat sessions (chat_session)",
        "http": "GET /logs/chat",
        "template": {
            "chat_name": None,
            "status": None,
            "prod_name": None,
            "group_id": None,
            "account_id": None,
            "limit": 50,
            "offset": 0,
        },
        "capture": [],
    },
    "logs.errors": {
        "queue": "logs", "pattern": "errors",
        "desc": "Error logs (log_error)",
        "http": "GET /logs/errors",
        "template": {
            "source": None,
            "severity": None,
            "engine_id": None,
            "engine_type": None,
            "agent": None,
            "step_id": None,
            "prod_name": None,
            "limit": 50,
            "offset": 0,
        },
        "capture": [],
    },
}


class SendRequest(BaseModel):
    name: str
    data: Any = None
    rpc: bool = True
    timeout: int = 600
    env_prefix: str = ""


class ConfigUpdate(BaseModel):
    host: str | None = None
    port: int | None = None
    user: str | None = None
    password: str | None = None
    vhost: str | None = None
    timeout: int | None = None


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "config": config.to_dict(),
        "routes": KNOWN_ROUTES,
    })


@app.get("/api/config")
async def get_config():
    return {**config.to_dict(), "env_prefix": os.getenv("RABBIT_ENV_PREFIX", "").strip()}


@app.post("/api/config")
async def update_config(data: ConfigUpdate):
    config.update(**data.model_dump(exclude_none=True))
    return {"success": True, "config": config.to_dict()}


@app.get("/api/routes")
async def get_routes():
    return KNOWN_ROUTES


@app.post("/api/send")
async def send_message(req: SendRequest):
    env_prefix = req.env_prefix.strip() if req.env_prefix else os.getenv("RABBIT_ENV_PREFIX", "").strip()

    route = KNOWN_ROUTES.get(req.name)
    if route:
        base_queue = route["queue"]
        pattern    = route["pattern"]
    else:
        if "." in req.name:
            base_queue, pattern = req.name.split(".", 1)
        else:
            base_queue, pattern = req.name, None

    queue_name = f"{env_prefix}_{base_queue}" if env_prefix else base_queue

    body_dict = {"pattern": pattern, "data": req.data} if pattern else {"data": req.data}
    body_bytes = json.dumps(body_dict).encode()

    try:
        conn = await aio_pika.connect_robust(config.dsn())
        async with conn:
            channel = await conn.channel()

            if req.rpc:
                rpc_channel = await conn.channel()
                correlation_id = str(uuid.uuid4())
                future: asyncio.Future = asyncio.get_event_loop().create_future()
                consumer_tag = None

                try:
                    reply_queue = await rpc_channel.declare_queue(exclusive=True)

                    async def on_reply(msg: aio_pika.IncomingMessage):
                        async with msg.process():
                            if msg.correlation_id == correlation_id and not future.done():
                                future.set_result(msg.body)

                    consumer_tag = await reply_queue.consume(on_reply)

                    await rpc_channel.default_exchange.publish(
                        aio_pika.Message(
                            body=body_bytes,
                            correlation_id=correlation_id,
                            reply_to=reply_queue.name,
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        ),
                        routing_key=queue_name,
                    )

                    raw_reply = await asyncio.wait_for(future, timeout=req.timeout)
                    reply_dict = json.loads(raw_reply)

                    # Unwrap: { "data": { "payload": ... } } or { "data": { "error": ... } }
                    inner = reply_dict.get("data", reply_dict)
                    if isinstance(inner, dict) and "error" in inner:
                        err = inner["error"]
                        raise HTTPException(status_code=err.get("status", 500), detail=err)

                    payload = inner.get("payload", inner) if isinstance(inner, dict) else inner

                    return {
                        "success": True,
                        "name": req.name,
                        "queue": queue_name,
                        "pattern": pattern,
                        "correlation_id": correlation_id,
                        "envelope_sent": body_dict,
                        "reply_raw": reply_dict,
                        "reply": payload,
                    }

                except asyncio.TimeoutError:
                    raise HTTPException(
                        status_code=504,
                        detail=f"No reply from '{queue_name}' within {req.timeout}s — is RABBIT_QUEUES={queue_name} set on the microservice?",
                    )
                finally:
                    if consumer_tag:
                        try:
                            await reply_queue.cancel(consumer_tag)
                        except Exception:
                            pass
                    try:
                        await rpc_channel.close()
                    except Exception:
                        pass
            else:
                await channel.default_exchange.publish(
                    aio_pika.Message(
                        body=body_bytes,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key=queue_name,
                )
                return {
                    "success": True,
                    "name": req.name,
                    "queue": queue_name,
                    "pattern": pattern,
                    "envelope_sent": body_dict,
                    "bytes": len(body_bytes),
                }

    except HTTPException:
        raise
    except aio_pika.exceptions.AMQPConnectionError as e:
        raise HTTPException(status_code=503, detail=f"RabbitMQ connection failed: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return FileResponse("favicon/api.ico")

@app.get("/api/ping")
async def ping():
    try:
        conn = await aio_pika.connect_robust(config.dsn(), timeout=5)
        await conn.close()
        return {"status": "ok", "dsn": f"amqp://{config.user}:***@{config.host}:{config.port}{config.vhost}"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8005, reload=True)
