"""Command-line interface for topicflow."""

from __future__ import annotations

import argparse
import asyncio
import sys

from . import __version__

_DEFAULT_HOST = "localhost"
_DEFAULT_PORT = 8765


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_serve(args: argparse.Namespace) -> int:
    """Start the pub/sub broker."""
    from .broker import serve

    try:
        from rich.console import Console
        from rich.rule import Rule

        console = Console()

        def _on_ready(host: str, port: int) -> None:
            console.print(Rule("[bold cyan]topicflow broker[/bold cyan]"))
            console.print(
                f"  Listening on [bold cyan]ws://{host}:{port}[/bold cyan]  "
                f"(Ctrl-C to stop)"
            )

    except ImportError:
        def _on_ready(host: str, port: int) -> None:  # type: ignore[misc]
            print(f"topicflow broker listening on ws://{host}:{port}")

    try:
        asyncio.run(serve(host=args.host, port=args.port, on_ready=_on_ready))
    except KeyboardInterrupt:
        pass
    return 0


def cmd_pub(args: argparse.Namespace) -> int:
    """Publish a message to a topic."""
    from .client import publish

    data = " ".join(args.data)

    async def _run() -> dict:
        return await publish(args.host, args.port, args.topic, data)

    try:
        result = asyncio.run(_run())
    except OSError as exc:
        print(f"topicflow: cannot connect to {args.host}:{args.port}: {exc}", file=sys.stderr)
        return 1

    delivered = result.get("delivered", "?")
    if args.quiet:
        return 0

    try:
        from rich.console import Console
        Console().print(
            f"[green]✓[/green] Published to [bold]{args.topic}[/bold]  "
            f"[dim]({delivered} subscriber{'s' if delivered != 1 else ''})[/dim]"
        )
    except ImportError:
        print(f"Published to {args.topic!r} ({delivered} subscribers)")

    return 0


def cmd_sub(args: argparse.Namespace) -> int:
    """Subscribe to a topic with a live Rich dashboard."""
    from .client import subscribe_live, subscribe

    if args.format == "json":
        import json

        async def _json_sub() -> None:
            async for env in subscribe(args.host, args.port, args.topic):
                print(json.dumps(env), flush=True)

        try:
            asyncio.run(_json_sub())
        except KeyboardInterrupt:
            pass
        return 0

    # Default: live Rich table
    try:
        asyncio.run(subscribe_live(args.host, args.port, args.topic, max_rows=args.max_rows))
    except KeyboardInterrupt:
        pass
    except OSError as exc:
        print(f"topicflow: cannot connect to {args.host}:{args.port}: {exc}", file=sys.stderr)
        return 1
    return 0


def cmd_digest(args: argparse.Namespace) -> int:
    """Subscribe to a topic and print AI-generated digests."""
    from .digest import run_digest

    try:
        asyncio.run(
            run_digest(
                host=args.host,
                port=args.port,
                topic=args.topic,
                interval=args.interval,
                model=args.model,
            )
        )
    except KeyboardInterrupt:
        pass
    except OSError as exc:
        print(f"topicflow: cannot connect to {args.host}:{args.port}: {exc}", file=sys.stderr)
        return 1
    return 0


def cmd_topics(args: argparse.Namespace) -> int:
    """List active topics on the broker."""
    from .client import list_topics

    async def _run() -> dict[str, int]:
        return await list_topics(args.host, args.port)

    try:
        info = asyncio.run(_run())
    except OSError as exc:
        print(f"topicflow: cannot connect to {args.host}:{args.port}: {exc}", file=sys.stderr)
        return 1

    if not info:
        print("No active topics.")
        return 0

    try:
        from rich.table import Table
        from rich.console import Console
        from rich import box as rich_box

        t = Table(
            box=rich_box.ROUNDED,
            show_header=True,
            header_style="bold cyan",
            border_style="cyan",
        )
        t.add_column("Topic", style="cyan")
        t.add_column("Subscribers", justify="right", style="yellow")
        for topic, count in sorted(info.items()):
            t.add_row(topic, str(count))
        Console().print(t)
    except ImportError:
        for topic, count in sorted(info.items()):
            print(f"  {topic}: {count} subscriber(s)")

    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    """Show broker statistics."""
    from .client import get_stats

    async def _run() -> dict:
        return await get_stats(args.host, args.port)

    try:
        data = asyncio.run(_run())
    except OSError as exc:
        print(f"topicflow: cannot connect to {args.host}:{args.port}: {exc}", file=sys.stderr)
        return 1

    if args.json:
        import json
        print(json.dumps(data, indent=2))
        return 0

    try:
        from rich.table import Table
        from rich.console import Console
        from rich import box as rich_box
        from rich.panel import Panel

        t = Table(box=rich_box.SIMPLE, show_header=False, pad_edge=False)
        t.add_column("Metric", style="cyan")
        t.add_column("Value", justify="right")
        t.add_row("Messages routed", str(data.get("messages_routed", 0)))
        t.add_row("Active connections", str(data.get("connections_active", 0)))
        t.add_row("Total connections", str(data.get("connections_total", 0)))
        counts = data.get("topic_message_counts", {})
        if counts:
            t.add_section()
            for topic, n in sorted(counts.items(), key=lambda x: -x[1]):
                t.add_row(f"  {topic}", str(n))
        Console().print(Panel(t, title="[bold]Broker Stats[/bold]", border_style="cyan"))
    except ImportError:
        for k, v in data.items():
            print(f"  {k}: {v}")

    return 0


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="topicflow",
        description="Async WebSocket pub/sub relay with live Rich dashboard and AI digest.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  topicflow serve                         # start broker on localhost:8765
  topicflow serve --port 9000             # custom port
  topicflow pub events "deploy started"   # publish to topic 'events'
  topicflow sub events                    # subscribe with live Rich display
  topicflow sub events --format json      # newline-delimited JSON output
  topicflow digest events --interval 60   # AI summary every 60s (needs ANTHROPIC_API_KEY)
  topicflow topics                        # list active topics + subscriber counts
  topicflow stats                         # broker statistics
""",
    )
    p.add_argument("--version", action="version", version=f"topicflow {__version__}")
    p.add_argument(
        "--host",
        default=_DEFAULT_HOST,
        metavar="HOST",
        help=f"Broker host (default: {_DEFAULT_HOST})",
    )
    p.add_argument(
        "--port",
        type=int,
        default=_DEFAULT_PORT,
        metavar="PORT",
        help=f"Broker port (default: {_DEFAULT_PORT})",
    )

    sub = p.add_subparsers(dest="command", metavar="COMMAND")

    # serve
    ps = sub.add_parser("serve", help="Start the pub/sub broker")
    ps.add_argument("--host", default=None, metavar="HOST", help="Override interface to bind")
    ps.add_argument("--port", type=int, default=None, metavar="PORT", help="Override TCP port")
    ps.set_defaults(func=cmd_serve)

    # pub
    pp = sub.add_parser("pub", help="Publish a message to a topic")
    pp.add_argument("topic", help="Topic name")
    pp.add_argument("data", nargs="+", help="Message data (words joined with spaces)")
    pp.add_argument("-q", "--quiet", action="store_true", help="Suppress output")
    pp.set_defaults(func=cmd_pub)

    # sub
    psu = sub.add_parser("sub", help="Subscribe to a topic (live Rich table by default)")
    psu.add_argument("topic", help="Topic name")
    psu.add_argument(
        "--format", "-f",
        choices=["table", "json"],
        default="table",
        help="Output format: rich table (default) or newline-delimited JSON",
    )
    psu.add_argument(
        "--max-rows",
        type=int,
        default=200,
        metavar="N",
        help="Max rows in the live table (default: 200)",
    )
    psu.set_defaults(func=cmd_sub)

    # digest
    pd = sub.add_parser(
        "digest",
        help="Subscribe to a topic and print AI-generated periodic summaries",
    )
    pd.add_argument("topic", help="Topic name")
    pd.add_argument(
        "--interval", "-i",
        type=int,
        default=30,
        metavar="SECS",
        help="Seconds between digest generations (default: 30)",
    )
    pd.add_argument(
        "--model",
        default="claude-haiku-4-5-20251001",
        metavar="MODEL",
        help="Claude model ID (default: claude-haiku-4-5-20251001)",
    )
    pd.set_defaults(func=cmd_digest)

    # topics
    pt = sub.add_parser("topics", help="List active topics and subscriber counts")
    pt.set_defaults(func=cmd_topics)

    # stats
    pst = sub.add_parser("stats", help="Show broker statistics")
    pst.add_argument("--json", action="store_true", help="Output as JSON")
    pst.set_defaults(func=cmd_stats)

    return p


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    """Parse arguments and dispatch to the appropriate command."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 0

    # Resolve host/port: subcommand args fall back to top-level args
    if not hasattr(args, "host") or args.host is None:
        args.host = _DEFAULT_HOST
    if not hasattr(args, "port") or args.port is None:
        args.port = _DEFAULT_PORT

    return args.func(args)


def entry_point() -> None:
    sys.exit(main())
