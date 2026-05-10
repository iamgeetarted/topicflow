"""WebSocket pub/sub broker — routes published messages to topic subscribers."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

WebSocket = Any  # websockets.ServerConnection; avoid hard import for testability


@dataclass
class BrokerStats:
    """Running counters maintained by the broker."""

    messages_routed: int = 0
    connections_total: int = 0
    connections_active: int = 0
    topic_message_counts: dict[str, int] = field(default_factory=dict)

    def record_publish(self, topic: str) -> None:
        self.messages_routed += 1
        self.topic_message_counts[topic] = self.topic_message_counts.get(topic, 0) + 1


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class Registry:
    """Thread-safe subscriber registry for the broker."""

    def __init__(self) -> None:
        self._subs: dict[str, set[WebSocket]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def subscribe(self, topic: str, ws: WebSocket) -> None:
        """Register *ws* as a subscriber for *topic*."""
        async with self._lock:
            self._subs[topic].add(ws)

    async def unsubscribe(self, topic: str, ws: WebSocket) -> None:
        """Remove *ws* from *topic* subscribers."""
        async with self._lock:
            self._subs[topic].discard(ws)
            if not self._subs[topic]:
                del self._subs[topic]

    async def unsubscribe_all(self, ws: WebSocket) -> list[str]:
        """Remove *ws* from every topic; return the list of topics it was on."""
        async with self._lock:
            topics = [t for t, subs in self._subs.items() if ws in subs]
            for t in topics:
                self._subs[t].discard(ws)
                if not self._subs[t]:
                    del self._subs[t]
        return topics

    async def broadcast(self, topic: str, envelope: dict) -> int:
        """Send *envelope* to all subscribers of *topic*; return delivery count."""
        msg = json.dumps(envelope)
        async with self._lock:
            subs = list(self._subs.get(topic, set()))
        if not subs:
            return 0
        results = await asyncio.gather(*(ws.send(msg) for ws in subs), return_exceptions=True)
        return sum(1 for r in results if not isinstance(r, Exception))

    async def topic_info(self) -> dict[str, int]:
        """Return {topic: subscriber_count} snapshot."""
        async with self._lock:
            return {t: len(s) for t, s in self._subs.items()}


# ---------------------------------------------------------------------------
# Connection handler
# ---------------------------------------------------------------------------

async def _handle(
    ws: WebSocket,
    registry: Registry,
    stats: BrokerStats,
) -> None:
    """Handle a single WebSocket connection lifecycle."""
    stats.connections_total += 1
    stats.connections_active += 1
    subscriptions: list[str] = []

    try:
        async for raw in ws:
            try:
                msg: dict = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                await ws.send(json.dumps({"type": "error", "detail": "invalid JSON"}))
                continue

            msg_type = msg.get("type", "")

            if msg_type == "subscribe":
                topic = str(msg.get("topic", "")).strip()
                if not topic:
                    continue
                if topic not in subscriptions:
                    await registry.subscribe(topic, ws)
                    subscriptions.append(topic)
                await ws.send(json.dumps({"type": "ack", "action": "subscribe", "topic": topic}))

            elif msg_type == "unsubscribe":
                topic = str(msg.get("topic", "")).strip()
                await registry.unsubscribe(topic, ws)
                subscriptions = [t for t in subscriptions if t != topic]
                await ws.send(json.dumps({"type": "ack", "action": "unsubscribe", "topic": topic}))

            elif msg_type == "publish":
                topic = str(msg.get("topic", "")).strip()
                data = msg.get("data", "")
                if not topic:
                    continue
                envelope = {
                    "type": "message",
                    "topic": topic,
                    "data": data,
                    "id": str(uuid.uuid4())[:8],
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
                delivered = await registry.broadcast(topic, envelope)
                stats.record_publish(topic)
                await ws.send(
                    json.dumps({"type": "ack", "action": "publish", "topic": topic, "delivered": delivered})
                )

            elif msg_type == "topics":
                info = await registry.topic_info()
                await ws.send(json.dumps({"type": "topics", "topics": info}))

            elif msg_type == "stats":
                await ws.send(
                    json.dumps(
                        {
                            "type": "stats",
                            "messages_routed": stats.messages_routed,
                            "connections_total": stats.connections_total,
                            "connections_active": stats.connections_active,
                            "topic_message_counts": stats.topic_message_counts,
                        }
                    )
                )

    except Exception:
        pass
    finally:
        await registry.unsubscribe_all(ws)
        stats.connections_active -= 1


# ---------------------------------------------------------------------------
# Server entry point
# ---------------------------------------------------------------------------

async def serve(
    host: str = "0.0.0.0",
    port: int = 8765,
    on_ready: Callable[[str, int], None] | None = None,
) -> None:
    """Start the pub/sub broker and run until cancelled.

    Args:
        host: Interface to bind to.
        port: TCP port to listen on.
        on_ready: Optional callback fired once the server is accepting connections.
    """
    import websockets

    registry = Registry()
    stats = BrokerStats()

    async def _handler(ws: WebSocket) -> None:
        await _handle(ws, registry, stats)

    async with websockets.serve(_handler, host, port):
        if on_ready:
            on_ready(host, port)
        await asyncio.get_running_loop().create_future()
