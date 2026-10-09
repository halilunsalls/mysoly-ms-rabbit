import asyncio
import json
import logging
import os
import re
import uuid
from datetime import timezone
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
    # ── System ─────────────────────────────────────────────────────────────
    "sys.health": {
        "queue": "sys", "pattern": "health",
        "desc": "Version check (empty body)",
        "http": "GET /",
        "template": {},
        "capture": [],
    },
    # ── Pipeline ───────────────────────────────────────────────────────────
    "pipeline.run": {
        "queue": "pipeline", "pattern": "run",
        "desc": "Run a pipeline (triggered_by=rabbit on server)",
        "http": "POST /run",
        "template": {
            "pipeline": "my_pipeline",
            "input": {"user_message": "Hello"},
            "prod_name": "default",
            "account_id": "account_1",
            "group_id": "group_1",
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
    "chat.run": {
        "queue": "chat", "pattern": "run",
        "desc": "Create session if needed, then send message",
        "http": "POST /chat/run",
        "template": {
            "chat_name": "my_chat",
            "prod_name": "default",
            "account_id": "account_1",
            "group_id": "group_1",
            "message": "Hello",
            "session_id": None,
            "vars": {"content": [], "sections": []},
        },
        "capture": ["session_id"],
    },
}


class SendRequest(BaseModel):
    name: str
    data: Any = None
    rpc: bool = True
    timeout: int = 600
    env_prefix: str = ""


class QueueRunRequest(BaseModel):
    pipeline: str = "careons_take_plan"
    text: str = ""
    prod_name: str = "careons"
    service: str = "ms_flow"
    env_prefix: str = ""
    dry_run: bool = False


class QueuePeekRequest(BaseModel):
    pipeline: str = "careons_take_plan"
    service: str = "ms_flow"
    env_prefix: str = ""
    which: str = "publish"
    limit: int = 20


class QueueConsumeRequest(BaseModel):
    pipeline: str = "careons_take_plan"
    service: str = "ms_flow"
    env_prefix: str = ""
    correlation_id: str = ""
    limit: int = 50


_QUEUE_STEM = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


def _message_stamp(message: aio_pika.IncomingMessage) -> str | None:
    stamp = message.timestamp
    if stamp is None:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.isoformat()


def _dedicated_names(service: str, env_prefix: str, pipeline: str) -> tuple[str, str]:
    service_name = service.strip() or "ms_flow"
    env_name = env_prefix.strip() or "prod"
    pipe = pipeline.strip()
    for label, value in (("service", service_name), ("env", env_name), ("pipeline", pipe)):
        if not _QUEUE_STEM.match(value):
            raise HTTPException(
                status_code=422,
                detail=f"{label} '{value}' is not a valid queue stem",
            )
    stem = f"{service_name}_{env_name}_{pipe}"
    return f"{stem}_listen", f"{stem}_publish"


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


@app.get("/chat", response_class=HTMLResponse)
async def chat_playground(request: Request):
    return templates.TemplateResponse("chat.html", {
        "request": request,
        "config": config.to_dict(),
    })


@app.get("/queues", response_class=HTMLResponse)
async def queue_playground(request: Request):
    return templates.TemplateResponse("queues.html", {
        "request": request,
        "config": config.to_dict(),
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

@app.post("/api/queue/run")
async def queue_run(req: QueueRunRequest):
    """Publish one actie string. Does not read the publish queue."""
    listen, publish = _dedicated_names(req.service, req.env_prefix, req.pipeline)
    run_id = str(uuid.uuid4())
    envelope = {
        "pattern": "run",
        "data": {
            "prod_name": req.prod_name.strip() or "default",
            "run_id": run_id,
            "dry_run": req.dry_run,
            "input": {"text": req.text},
        },
    }
    body = json.dumps(envelope).encode()

    try:
        conn = await aio_pika.connect_robust(config.dsn())
        async with conn:
            channel = await conn.channel()
            await channel.declare_queue(listen, durable=True)
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=body,
                    correlation_id=run_id,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=listen,
            )
    except HTTPException:
        raise
    except aio_pika.exceptions.AMQPConnectionError as exc:
        raise HTTPException(status_code=503, detail=f"RabbitMQ connection failed: {exc}")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return {
        "success": True,
        "listen_queue": listen,
        "publish_queue": publish,
        "run_id": run_id,
        "envelope_sent": envelope,
    }


@app.post("/api/queue/peek")
async def queue_peek(req: QueuePeekRequest):
    """Show messages without removing them. Each one is nacked back onto the queue."""
    listen, publish = _dedicated_names(req.service, req.env_prefix, req.pipeline)
    which = req.which.strip().lower()
    if which not in {"listen", "publish"}:
        raise HTTPException(status_code=422, detail="which must be listen or publish")
    queue_name = listen if which == "listen" else publish
    limit = max(1, min(req.limit, 50))
    messages: list[dict] = []
    held: list[aio_pika.IncomingMessage] = []

    try:
        conn = await aio_pika.connect_robust(config.dsn())
        async with conn:
            channel = await conn.channel()
            try:
                queue = await channel.declare_queue(queue_name, passive=True)
            except Exception as exc:
                raise HTTPException(
                    status_code=404,
                    detail=f"Queue '{queue_name}' was not found: {exc}",
                ) from exc
            declared = getattr(queue, "declaration_result", None)
            message_count = getattr(declared, "message_count", None)
            consumer_count = getattr(declared, "consumer_count", None)
            for _ in range(limit):
                message = await queue.get(fail=False)
                if message is None:
                    break
                held.append(message)
                raw = message.body.decode("utf-8", errors="replace")
                try:
                    body_json = json.loads(raw)
                except json.JSONDecodeError:
                    body_json = None
                messages.append({
                    "correlation_id": message.correlation_id,
                    "timestamp": _message_stamp(message),
                    "body": body_json if body_json is not None else raw,
                })
            for pending in held:
                await pending.nack(requeue=True)
    except HTTPException:
        raise
    except aio_pika.exceptions.AMQPConnectionError as exc:
        raise HTTPException(status_code=503, detail=f"RabbitMQ connection failed: {exc}")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return {
        "success": True,
        "queue": queue_name,
        "which": which,
        "message_count": message_count,
        "consumer_count": consumer_count,
        "shown": len(messages),
        "requeued": True,
        "messages": messages,
    }


@app.post("/api/queue/consume")
async def queue_consume(req: QueueConsumeRequest):
    """Ack one message on the publish queue. Every other message is nacked back."""
    _listen, publish = _dedicated_names(req.service, req.env_prefix, req.pipeline)
    target = req.correlation_id.strip()
    if not target:
        raise HTTPException(status_code=422, detail="correlation_id is required")
    limit = max(1, min(req.limit, 50))
    held: list[aio_pika.IncomingMessage] = []
    consumed: dict | None = None

    try:
        conn = await aio_pika.connect_robust(config.dsn())
        async with conn:
            channel = await conn.channel()
            try:
                queue = await channel.declare_queue(publish, passive=True)
            except Exception as exc:
                raise HTTPException(
                    status_code=404,
                    detail=f"Queue '{publish}' was not found: {exc}",
                ) from exc
            try:
                for _ in range(limit):
                    message = await queue.get(fail=False)
                    if message is None:
                        break
                    if consumed is None and (message.correlation_id or "") == target:
                        raw = message.body.decode("utf-8", errors="replace")
                        try:
                            body_json = json.loads(raw)
                        except json.JSONDecodeError:
                            body_json = raw
                        await message.ack()
                        consumed = {
                            "correlation_id": message.correlation_id,
                            "body": body_json,
                        }
                    else:
                        held.append(message)
            finally:
                for pending in held:
                    await pending.nack(requeue=True)
    except HTTPException:
        raise
    except aio_pika.exceptions.AMQPConnectionError as exc:
        raise HTTPException(status_code=503, detail=f"RabbitMQ connection failed: {exc}")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    if consumed is None:
        raise HTTPException(
            status_code=404,
            detail=f"No publish message with correlation_id '{target}'",
        )
    return {
        "success": True,
        "queue": publish,
        "consumed": True,
        "message": consumed,
    }


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
