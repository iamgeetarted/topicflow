"""Async client helpers for publishing, subscribing, and querying the broker."""

from __future__ import annotations

import asyncio
import json
from typing import AsyncIterator, Callable


def _ws_uri(host: str, port: int) -> str:
    return f"ws://{host}:{port}"


async def publish(host: str, port: int, topic: str, data: str) -> dict:
    """Publish *data* to *topic* on the broker and return the ack envelope.

    Args:
        host: Broker hostname or IP.
        port: Broker port.
        topic: Destination topic name.
        data: Message payload.

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
        on_message: Optional synchronous callback called for each message.

    Yields:
        Message envelopes: ``{"type": "message", "topic": ..., "data": ..., "id": ..., "ts": ...}``
    """
    import websockets

    async with websockets.connect(_ws_uri(host, port)) as ws:
        await ws.send(json.dumps({"type": "subscribe", "topic": topic}))
        await ws.recv()  # consume ack

        async for raw in ws:
            envelope: dict = json.loads(raw)
            if envelope.get("type") == "message":
                if on_message:
                    on_message(envelope)
                yield envelope


async def replay(host: str, port: int, topic: str, count: int = 20) -> list[dict]:
    """Fetch the last *count* messages from the broker's in-memory history for *topic*.

    This returns messages published before the caller connected — useful for
    catching up after a restart or inspecting recent activity without waiting.

    Args:
        host: Broker hostname or IP.
        port: Broker port.
        topic: Topic to replay.
        count: Number of most-recent messages to retrieve (server cap: 100).

    Returns:
        List of message envelopes, oldest first.
    """
    import websockets

    async with websockets.connect(_ws_uri(host, port)) as ws:
        await ws.send(json.dumps({"type": "replay", "topic": topic, "count": count}))
        raw = await ws.recv()
        resp = json.loads(raw)
        return resp.get("messages", [])


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
    topics: list[str],
    max_rows: int = 200,
    replay_count: int = 0,
) -> None:
    """Subscribe to one or more *topics* and display arriving messages in a live Rich table.

    Supports multiple topics — messages from all topics appear in a single merged
    view, colour-coded by topic name. Press Ctrl-C to stop.

    Args:
        host: Broker hostname or IP.
        port: Broker port.
        topics: List of topic names to subscribe to (1 or more).
        max_rows: Maximum number of rows kept in the live table (oldest dropped).
        replay_count: If > 0, pre-populate the table with this many historical messages
                      per topic before going live.
    """
    from rich.live import Live
    from rich.table import Table
    from rich import box as rich_box
    from rich.text import Text
    import websockets

    TOPIC_COLOURS = ["cyan", "yellow", "magenta", "green", "blue", "red"]
    topic_colour = {t: TOPIC_COLOURS[i % len(TOPIC_COLOURS)] for i, t in enumerate(topics)}

    messages: list[dict] = []
    title_topics = ", ".join(f"[{topic_colour[t]}]{t}[/{topic_colour[t]}]" for t in topics)

    def _render() -> Table:
        t = Table(
            box=rich_box.ROUNDED,
            show_header=True,
            header_style="bold cyan",
            border_style="cyan",
            expand=True,
            title=f"[bold cyan]topicflow[/bold cyan]  {title_topics}",
            title_justify="left",
        )
        t.add_column("Topic", style="dim", width=18, no_wrap=True)
        t.add_column("ID", style="dim", width=10, no_wrap=True)
        t.add_column("Timestamp", style="dim", width=28, no_wrap=True)
        t.add_column("Data", no_wrap=False)

        for m in messages[-max_rows:]:
            topic = m.get("topic", "")
            colour = topic_colour.get(topic, "cyan")
            ts = m.get("ts", "")[:23].replace("T", " ")
            t.add_row(
                Text(topic, style=colour),
                m.get("id", ""),
                ts,
                Text(str(m.get("data", ""))),
            )
        return t

    async with websockets.connect(_ws_uri(host, port)) as ws:
        # Optionally replay history before subscribing
        if replay_count > 0:
            for topic in topics:
                await ws.send(json.dumps({"type": "replay", "topic": topic, "count": replay_count}))
                raw = await ws.recv()
                resp = json.loads(raw)
                messages.extend(resp.get("messages", []))
            messages.sort(key=lambda m: m.get("ts", ""))

        # Subscribe to all requested topics
        for topic in topics:
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


async def dashboard_live(
    host: str,
    port: int,
    refresh_rate: float = 1.0,
) -> None:
    """Display a live broker dashboard — topics, subscriber counts, and stats.

    Polls the broker once per *refresh_rate* seconds and renders an auto-updating
    Rich display. Press Ctrl-C to exit.

    Args:
        host: Broker hostname or IP.
        port: Broker port.
        refresh_rate: Seconds between stat polls (default 1.0).
    """
    from rich.live import Live
    from rich.layout import Layout
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
    from rich import box as rich_box
    from rich.align import Align
    import time

    start_time = time.monotonic()

    def _stats_panel(stats: dict) -> Panel:
        t = Table(box=rich_box.SIMPLE, show_header=False, pad_edge=False, expand=True)
        t.add_column("Metric", style="cyan")
        t.add_column("Value", justify="right", style="white")

        elapsed = time.monotonic() - start_time
        t.add_row("Uptime (dashboard)", f"{elapsed:.0f}s")
        t.add_row("Messages routed", str(stats.get("messages_routed", 0)))
        t.add_row("Active connections", str(stats.get("connections_active", 0)))
        t.add_row("Total connections", str(stats.get("connections_total", 0)))

        counts = stats.get("topic_message_counts", {})
        if counts:
            t.add_section()
            t.add_row("[dim]── per-topic msgs ──[/dim]", "")
            for topic_name, n in sorted(counts.items(), key=lambda x: -x[1])[:10]:
                t.add_row(f"  {topic_name}", str(n))

        return Panel(t, title="[bold]Broker Stats[/bold]", border_style="cyan", box=rich_box.ROUNDED)

    def _topics_panel(topics: dict) -> Panel:
        t = Table(box=rich_box.SIMPLE, show_header=True, header_style="bold cyan", pad_edge=False, expand=True)
        t.add_column("Topic", style="cyan")
        t.add_column("Subscribers", justify="right", style="yellow")

        if topics:
            for topic_name, count in sorted(topics.items()):
                t.add_row(topic_name, str(count))
        else:
            t.add_row("[dim]No active topics[/dim]", "[dim]—[/dim]")

        return Panel(t, title="[bold]Active Topics[/bold]", border_style="cyan", box=rich_box.ROUNDED)

    def _footer(ts: str) -> Text:
        return Text(
            f"  broker ws://{host}:{port}   refreshed {ts}   Ctrl-C to exit",
            style="dim",
        )

    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body"),
        Layout(name="footer", size=1),
    )
    layout["body"].split_row(
        Layout(name="stats"),
        Layout(name="topics"),
    )

    def _header_panel() -> Panel:
        return Panel(
            Align.center("[bold cyan]topicflow broker dashboard[/bold cyan]"),
            border_style="cyan",
            box=rich_box.ROUNDED,
        )

    layout["header"].update(_header_panel())

    stats: dict = {}
    topics: dict = {}

    with Live(layout, refresh_per_second=int(1 / refresh_rate) + 1, screen=True) as live:
        try:
            while True:
                try:
                    stats = await get_stats(host, port)
                    topics = await list_topics(host, port)
                except Exception:
                    pass

                ts = time.strftime("%H:%M:%S")
                layout["stats"].update(_stats_panel(stats))
                layout["topics"].update(_topics_panel(topics))
                layout["footer"].update(_footer(ts))
                live.refresh()
                await asyncio.sleep(refresh_rate)
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
