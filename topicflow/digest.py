"""AI-powered message digest — subscribes to a topic and streams periodic summaries via Claude."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from typing import Any


def _get_anthropic_client() -> Any:
    """Return an Anthropic client or exit with a helpful message."""
    try:
        import anthropic
    except ImportError:
        print("pip install anthropic  # required for 'topicflow digest'", file=sys.stderr)
        sys.exit(1)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print(
            "ANTHROPIC_API_KEY is not set.\n"
            "Export it:  export ANTHROPIC_API_KEY=sk-ant-...",
            file=sys.stderr,
        )
        sys.exit(1)

    return anthropic.Anthropic(api_key=api_key)


def summarize_batch(
    messages: list[str],
    topic: str,
    model: str = "claude-haiku-4-5-20251001",
) -> None:
    """Stream a Claude summary of *messages* on *topic* to stdout.

    Args:
        messages: Raw data strings collected from the topic.
        topic:    Topic name for context.
        model:    Claude model ID to use.
    """
    client = _get_anthropic_client()

    sample = messages[:50]
    numbered = "\n".join(f"{i + 1}. {m}" for i, m in enumerate(sample))
    omitted = len(messages) - len(sample)
    omit_note = f"\n(+ {omitted} more messages not shown)" if omitted else ""

    prompt = (
        f"You are monitoring a real-time message stream on the topic '{topic}'.\n\n"
        f"Here are the {len(sample)} most recent messages:{omit_note}\n\n"
        f"{numbered}\n\n"
        "Write a concise 2-4 sentence digest:\n"
        "• What is the overall pattern or theme?\n"
        "• Are there any anomalies, spikes, or noteworthy events?\n"
        "• What action, if any, should an operator take?\n\n"
        "Be specific and direct. If the messages are purely noise, say so."
    )

    with client.messages.stream(
        model=model,
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}],
    ) as stream:
        for chunk in stream.text_stream:
            print(chunk, end="", flush=True)
    print()


async def run_digest(
    host: str,
    port: int,
    topic: str,
    interval: int = 30,
    model: str = "claude-haiku-4-5-20251001",
) -> None:
    """Subscribe to *topic* and print an AI digest every *interval* seconds.

    Buffers all messages received during the interval, then sends them to
    Claude for summarization. Runs until Ctrl-C.

    Args:
        host:     Broker hostname or IP.
        port:     Broker port.
        topic:    Topic to monitor.
        interval: Seconds between digest generations.
        model:    Claude model ID to use.
    """
    import websockets

    try:
        from rich.console import Console
        from rich.rule import Rule
        console: Any = Console()
    except ImportError:
        console = None

    def _header(n: int, elapsed: float) -> None:
        ts = time.strftime("%H:%M:%S")
        if console:
            console.print()
            console.print(
                Rule(
                    f"[bold cyan]AI Digest[/bold cyan] [dim]— {n} msg in {elapsed:.0f}s — {ts}[/dim]",
                    style="cyan",
                )
            )
        else:
            print(f"\n--- AI Digest [{n} messages, {ts}] ---")

    uri = f"ws://{host}:{port}"
    buffer: list[str] = []
    last_flush = time.monotonic()

    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps({"type": "subscribe", "topic": topic}))
        await ws.recv()  # ack

        if console:
            console.print(
                f"[dim]Subscribed to [bold]{topic}[/bold] — digest every {interval}s. Ctrl-C to stop.[/dim]"
            )
        else:
            print(f"Subscribed to {topic!r} — digest every {interval}s")

        try:
            while True:
                now = time.monotonic()
                remaining = max(0.0, interval - (now - last_flush))

                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                    env: dict = json.loads(raw)
                    if env.get("type") == "message":
                        buffer.append(str(env.get("data", "")))
                except asyncio.TimeoutError:
                    pass

                now = time.monotonic()
                if now - last_flush >= interval:
                    elapsed = now - last_flush
                    if buffer:
                        _header(len(buffer), elapsed)
                        summarize_batch(buffer, topic, model=model)
                    else:
                        if console:
                            console.print(f"[dim]{time.strftime('%H:%M:%S')} — no messages in last {elapsed:.0f}s[/dim]")
                        else:
                            print(f"{time.strftime('%H:%M:%S')} — no messages in last {elapsed:.0f}s")
                    buffer.clear()
                    last_flush = now

        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
