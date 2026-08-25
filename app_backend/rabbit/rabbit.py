"""RabbitMQ integration layer.

Python equivalent of the Node.js rabbit.ts sample.
Uses aio-pika (async) to stay compatible with FastAPI's event loop.

Environment variables (same keys as the Node.js side):
  RABBIT_HOST, RABBIT_PORT, RABBIT_USER, RABBIT_PASS,
  RABBIT_VHOST, RABBIT_PROTOCOL,
  RABBIT_TIMEOUT, RABBIT_QUEUES, RABBIT_EXCHANGES
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from typing import Any, Callable, Optional

import aio_pika
from aio_pika import Channel, DeliveryMode, ExchangeType, Message
from aio_pika.abc import AbstractRobustConnection

logger = logging.getLogger(__name__)

# ── Singleton state ───────────────────────────────────────────────────────────
_connection: Optional[AbstractRobustConnection] = None
_channel: Optional[Channel] = None


# ── Connection management ─────────────────────────────────────────────────────

async def connect() -> tuple[AbstractRobustConnection, Channel]:
    """Return (connection, channel), creating them if needed.

    Uses connect_robust so aio-pika handles automatic reconnection
    on transient failures (mirrors the channel 'close' reconnect logic
    in the Node.js rabbit.ts).
    """
    global _connection, _channel

    if _connection is None or _connection.is_closed:
        host = os.getenv("RABBIT_HOST", "localhost").strip()
        port = int(os.getenv("RABBIT_PORT", "5672").strip())
        user = os.getenv("RABBIT_USER", "guest").strip()
        password = os.getenv("RABBIT_PASS", "guest").strip()
        vhost = os.getenv("RABBIT_VHOST", "/").strip()

        _connection = await aio_pika.connect_robust(
            host=host,
            port=port,
            login=user,
            password=password,
            virtualhost=vhost,
        )
        logger.info("RabbitMQ connection established")

    if _channel is None or _channel.is_closed:
        _channel = await _connection.channel()
        await _channel.set_qos(prefetch_count=1)

    return _connection, _channel


async def disconnect() -> None:
    """Close channel and connection gracefully."""
    global _connection, _channel

    if _channel and not _channel.is_closed:
        await _channel.close()
        _channel = None

    if _connection and not _connection.is_closed:
        await _connection.close()
        _connection = None

    logger.info("RabbitMQ disconnected")


# ── Direct queue — fire-and-forget ────────────────────────────────────────────

async def send_message(name: str, data: Any = None) -> None:
    """Send a message to a queue without waiting for a reply.

    Node.js equivalent: sendMessage(name, data)

    name format: "queue" or "queue.pattern"
    e.g. "pipeline.run" → queue="pac/pipeline", pattern="run"
    """
    _, channel = await connect()
    queue_name, pattern = _parse_name(name)
    await channel.declare_queue(queue_name, durable=True)
    body = json.dumps({"pattern": pattern, "data": data}).encode()
    await channel.default_exchange.publish(
        Message(body, delivery_mode=DeliveryMode.PERSISTENT),
        routing_key=queue_name,
    )


# ── Direct queue — RPC (send + wait for reply) ────────────────────────────────

async def send_message_for_reply(
    name: str,
    data: Any = None,
    timeout: Optional[int] = None,
) -> Any:
    """Send a message and block until the consumer replies.

    Node.js equivalent: sendMessageForReply(name, data, callback, options)

    Returns the reply payload dict, or raises TimeoutError / Exception.

    Each call opens its own channel so concurrent RPC calls don't share state.
    The exclusive reply queue is cleaned up by closing the channel (RabbitMQ
    automatically deletes exclusive queues when their owning channel closes),
    which avoids the PRECONDITION_FAILED / "queue in use" error that occurs
    when trying to delete a queue that still has an active consumer.
    """
    conn, _ = await connect()
    timeout_s = timeout or int(os.getenv("RABBIT_TIMEOUT", "30"))

    # Each RPC call gets its own channel — isolates failures between concurrent
    # requests and avoids shared-channel state corruption.
    channel = await conn.channel()

    correlation_id = str(uuid.uuid4())
    future: asyncio.Future = asyncio.get_event_loop().create_future()
    consumer_tag: Optional[str] = None

    try:
        reply_queue = await channel.declare_queue(exclusive=True)

        async def _on_reply(message: aio_pika.IncomingMessage) -> None:
            async with message.process():
                if message.correlation_id == correlation_id and not future.done():
                    content = json.loads(message.body)
                    error = content.get("data", {}).get("error") if isinstance(content.get("data"), dict) else None
                    if error and isinstance(error, dict) and error.get("status", 0) >= 400:
                        future.set_exception(
                            RabbitMQError(
                                error.get("message", "Error"),
                                error.get("code", "REMOTE_ERROR"),
                                error.get("status", 500),
                            )
                        )
                    else:
                        future.set_result(content.get("data"))

        consumer_tag = await reply_queue.consume(_on_reply)

        queue_name, pattern = _parse_name(name)
        body = json.dumps({"pattern": pattern, "data": data}).encode()
        await channel.default_exchange.publish(
            Message(
                body,
                correlation_id=correlation_id,
                reply_to=reply_queue.name,
                delivery_mode=DeliveryMode.PERSISTENT,
            ),
            routing_key=queue_name,
        )

        try:
            return await asyncio.wait_for(future, timeout=timeout_s)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(
                f"No response from service '{name}' within {timeout_s}s"
            ) from exc
    finally:
        # Cancel the consumer first so the queue is no longer "in use", then
        # close the channel. Closing the channel also auto-deletes the
        # exclusive reply queue on the broker side.
        if consumer_tag is not None:
            try:
                await reply_queue.cancel(consumer_tag)
            except Exception:
                pass
        try:
            await channel.close()
        except Exception:
            pass


# ── Direct queue — consumer (with optional reply) ─────────────────────────────

async def receive_message(queue_name: str, callback: Callable) -> None:
    """Start consuming a queue. Calls callback(payload) for each message.

    Node.js equivalent: receiveMessage(queue, callback)

    If the message has a replyTo property, the callback's return value
    is sent back as the reply (RPC server side).
    """
    _, channel = await connect()
    queue = await channel.declare_queue(queue_name, durable=True)

    async def _on_message(message: aio_pika.IncomingMessage) -> None:
        async with message.process():
            try:
                payload = json.loads(message.body)
            except json.JSONDecodeError as exc:
                logger.warning(f"Malformed JSON in RabbitMQ message on queue '{queue_name}': {exc}")
                from db import database as _db
                _db.insert_error(
                    source="rabbit",
                    error_type="JSONDecodeError",
                    error_message=f"Malformed message body on queue '{queue_name}': {exc}",
                    severity="warning",
                )
                payload = {}

            result = await callback(payload)

            if message.reply_to:
                body = json.dumps({"data": result}).encode()
                await channel.default_exchange.publish(
                    Message(
                        body,
                        correlation_id=message.correlation_id,
                        delivery_mode=DeliveryMode.PERSISTENT,
                    ),
                    routing_key=message.reply_to,
                )

    await queue.consume(_on_message)
    logger.info(f"RabbitMQ: listening on queue '{queue_name}'")


# ── Fanout exchange — publish ─────────────────────────────────────────────────

async def publish_message(exchange_name: str, key: str, data: Any = None) -> None:
    """Broadcast a message to all subscribers of a fanout exchange.

    Node.js equivalent: publishMessage(exchange, key, data)
    """
    _, channel = await connect()
    exchange = await channel.declare_exchange(
        exchange_name, ExchangeType.FANOUT, durable=False
    )
    body = json.dumps({"key": key, "data": data}).encode()
    await exchange.publish(Message(body), routing_key="")


# ── Fanout exchange — subscribe ───────────────────────────────────────────────

async def receive_published_message(
    exchange_name: str,
    key: str,
    callback: Callable,
) -> None:
    """Subscribe to a fanout exchange.

    Node.js equivalent: receivePublishedMessage(exchange, key, callback)

    key="*" receives all messages; otherwise only matching key messages
    are forwarded to the callback.
    """
    _, channel = await connect()
    exchange = await channel.declare_exchange(
        exchange_name, ExchangeType.FANOUT, durable=False
    )
    queue = await channel.declare_queue(exclusive=True)
    await queue.bind(exchange)

    async def _on_message(message: aio_pika.IncomingMessage) -> None:
        async with message.process():
            try:
                payload = json.loads(message.body)
            except json.JSONDecodeError as exc:
                logger.warning(f"Malformed JSON in RabbitMQ exchange '{exchange_name}': {exc}")
                from db import database as _db
                _db.insert_error(
                    source="rabbit",
                    error_type="JSONDecodeError",
                    error_message=f"Malformed message body on exchange '{exchange_name}': {exc}",
                    severity="warning",
                )
                payload = {}

            msg_key = payload.get("key")
            if key == "*" or msg_key == key:
                cb_data = payload if key == "*" else payload.get("data")
                await callback(cb_data)

    await queue.consume(_on_message)
    logger.info(f"RabbitMQ: subscribed to exchange '{exchange_name}' (key={key})")


# ── Main listener ─────────────────────────────────────────────────────────────

async def listen() -> None:
    """Start consuming all configured queues and exchanges.

    Node.js equivalent: listen()

    Reads RABBIT_QUEUES and RABBIT_EXCHANGES from env, sets up consumers,
    and routes every incoming message through central_router().
    Auto-reconnects via connect_robust if the broker drops.
    """
    from rabbit.router import central_router  # local import avoids circular dep

    try:
        await connect()
    except Exception as exc:
        logger.warning(f"RabbitMQ not available at startup: {exc}. HTTP API continues.")
        from db import database as _db
        _db.insert_error(
            source="rabbit",
            error_type=type(exc).__name__,
            error_message=f"RabbitMQ connection failed at startup: {exc}",
            severity="warning",
        )
        return

    queues = [
        q.strip()
        for q in os.getenv("RABBIT_QUEUES", "").split(",")
        if q.strip()
    ]
    exchanges = [
        e.strip()
        for e in os.getenv("RABBIT_EXCHANGES", "").split(",")
        if e.strip()
    ]

    env_prefix = os.getenv("RABBIT_ENV_PREFIX", "").strip()

    for queue in queues:
        async def _queue_handler(payload: dict, _q: str = queue, _pfx: str = env_prefix) -> dict:
            pattern = payload.get("pattern")
            if _pfx:
                logical_q = _q.removeprefix(f"{_pfx}_")
            elif "_" in _q:
                logical_q = _q.split("_", 1)[1]  # "local_chat" → "chat"
            else:
                logical_q = _q
            name = f"{logical_q}.{pattern}" if pattern else logical_q
            data = payload.get("data")
            return await central_router(name, data)

        await receive_message(queue, _queue_handler)

    for exchange in exchanges:
        async def _exchange_handler(payload: dict, _ex: str = exchange, _pfx: str = env_prefix) -> None:
            key = payload.get("key", "") if isinstance(payload, dict) else ""
            if _pfx:
                logical_ex = _ex.removeprefix(f"{_pfx}_")
            elif "_" in _ex:
                logical_ex = _ex.split("_", 1)[1]
            else:
                logical_ex = _ex
            data = payload.get("data") if isinstance(payload, dict) else payload
            await central_router(f"{logical_ex}.{key}", data)

        await receive_published_message(exchange, "*", _exchange_handler)

    logger.info("RabbitMQ listener ready")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_name(name: str) -> tuple[str, Optional[str]]:
    """Split 'queue.pattern' → ('queue', 'pattern'), 'queue' → ('queue', None)."""
    if "." in name:
        queue, pattern = name.split(".", 1)
        return queue, pattern
    return name, None


class RabbitMQError(Exception):
    """Structured error returned from a remote service via RabbitMQ reply."""

    def __init__(self, message: str, code: str = "REMOTE_ERROR", status: int = 500) -> None:
        super().__init__(message)
        self.code = code
        self.status = status
