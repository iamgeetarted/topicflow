"""Async client helpers for publishing, subscribing, and querying the broker."""

from __future__ import annotations

import asyncio
import json
from typing import AsyncIterator, Callable


def _ws_uri(host: str, port: int) -> str:
    return f"ws://{host}:{port}"


async def publish(host: str, port: int, topic: str, data: str) -> dict:
    """Publish *data* to *topic* on the broker and return the ack envelope.

    Opens a short-lived WebSocket connection, sends one publish message,
    waits for the ack, then closes.

    Args:
        host: Broker hostname or IP.
        port: Broker port.
        topic: Destination topic name.
        data: Message payload (any string; use JSON if structured data is needed).

    Returns:
        The broker ack dict, e.g. ``{"type": "ack", "delivered": 2, ...}``.
    """
    import websockets

    async with websockets.connect(_ws_uri(host, port)) as ws:
        await ws.send(json.dumps({"type": "publish", "topic": topic, "data": data}))
        raw = await ws.recv()
        return json.loads(raw)


async def subscribe(
    host: str,
    port: int,
    topic: str,
    on_message: Callable[[dict], None] | None = None,
) -> AsyncIterator[dict]:
    """Async generator that yields message envelopes from *topic*.

    Usage::

        async for envelope in subscribe("localhost", 8765, "events"):
            print(envelope["data"])

    Args:
        host: Broker hostname or IP.
        port: Broker port.
        topic: Topic to subscribe to.
        on_message: Optional synchronous callback; called for each message
                    in addition to yielding it.

    Yields:
        Message envelopes: ``{"type": "message", "topic": ..., "data": ..., "id": ..., "ts": ...}``
    """
    import websockets

    async with websockets.connect(_ws_uri(host, port)) as ws:
        await ws.send(json.dumps({"type": "subscribe", "topic": topic}))
        # consume the ack
        await ws.recv()

        async for raw in ws:
            envelope: dict = json.loads(raw)
            if envelope.get("type") == "message":
                if on_message:
                    on_message(envelope)
                yield envelope


async def list_topics(host: str, port: int) -> dict[str, int]:
    """Return a {topic: subscriber_count} dict from the broker.

    Args:
        host: Broker hostname or IP.
        port: Broker port.

    Returns:
        Mapping of active topic names to their current subscriber counts.
    """
    import websockets

    async with websockets.connect(_ws_uri(host, port)) as ws:
        await ws.send(json.dumps({"type": "topics"}))
        raw = await ws.recv()
        resp = json.loads(raw)
        return resp.get("topics", {})


async def get_stats(host: str, port: int) -> dict:
    """Return broker statistics.

    Args:
        host: Broker hostname or IP.
        port: Broker port.
    """
    import websockets

    async with websockets.connect(_ws_uri(host, port)) as ws:
        await ws.send(json.dumps({"type": "stats"}))
        raw = await ws.recv()
        return json.loads(raw)


async def subscribe_live(
    host: str,
    port: int,
    topic: str,
    max_rows: int = 200,
) -> None:
    """Subscribe to *topic* and display arriving messages in a live Rich table.

    The display updates in real-time. Press Ctrl-C to stop.

    Args:
        host: Broker hostname or IP.
        port: Broker port.
        topic: Topic to subscribe to.
        max_rows: Maximum number of rows kept in the live table (oldest dropped).
    """
    from rich.live import Live
    from rich.table import Table
    from rich import box as rich_box
    from rich.text import Text

    messages: list[dict] = []

    def _render() -> Table:
        t = Table(
            box=rich_box.ROUNDED,
            show_header=True,
            header_style="bold cyan",
            border_style="cyan",
            expand=True,
            title=f"[bold cyan]topicflow[/bold cyan]  topic=[yellow]{topic}[/yellow]",
            title_justify="left",
        )
        t.add_column("ID", style="dim", width=10, no_wrap=True)
        t.add_column("Timestamp", style="dim", width=28, no_wrap=True)
        t.add_column("Data", no_wrap=False)

        for m in messages[-max_rows:]:
            ts = m.get("ts", "")[:23].replace("T", " ")
            t.add_row(m.get("id", ""), ts, Text(str(m.get("data", ""))))
        return t

    import websockets

    async with websockets.connect(_ws_uri(host, port)) as ws:
        await ws.send(json.dumps({"type": "subscribe", "topic": topic}))
        await ws.recv()  # ack

        with Live(_render(), refresh_per_second=8, screen=False) as live:
            try:
                async for raw in ws:
                    env: dict = json.loads(raw)
                    if env.get("type") == "message":
                        messages.append(env)
                        live.update(_render())
            except (KeyboardInterrupt, asyncio.CancelledError):
                pass
