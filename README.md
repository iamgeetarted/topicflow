# topicflow

**Async WebSocket pub/sub relay with live Rich dashboard and AI message digest.**

topicflow is a lightweight message broker you run locally to test event-driven systems, wire up services during development, or monitor live message streams — all from the terminal.

```
┌─────────────────────────────────────────────────────────────────┐
│ topicflow  topic=production-events                              │
├──────────┬──────────────────────────┬───────────────────────────┤
│ ID       │ Timestamp                │ Data                      │
├──────────┼──────────────────────────┼───────────────────────────┤
│ 3a1f9c   │ 2026-05-10 12:00:01.124  │ deploy started: v2.4.1    │
│ 7b2e4d   │ 2026-05-10 12:00:03.887  │ health check OK           │
│ c91f8a   │ 2026-05-10 12:00:07.334  │ memory spike: 94% on web1 │
│ d4a2b1   │ 2026-05-10 12:00:09.001  │ deploy complete           │
└──────────┴──────────────────────────┴───────────────────────────┘
```

## Breakthrough techniques

| Technique | Where |
|---|---|
| **WebSocket client/server** | `websockets` library — broker and all clients communicate over async WebSocket |
| **Full async architecture** | `asyncio` throughout; `asyncio.gather` for concurrent broadcast delivery |
| **Live Rich UI** | `rich.live.Live` subscriber display that updates in real-time as messages arrive |
| **LLM integration** | Claude streams periodic AI digests of buffered message batches |

## Install

```bash
pip install topicflow            # core (broker + subscribe + publish)
pip install "topicflow[ai]"      # + anthropic for AI digest
pip install "topicflow[dev]"     # + test dependencies
```

## Quick start

**Terminal 1 — start the broker:**
```bash
topicflow serve
# topicflow broker listening on ws://0.0.0.0:8765
```

**Terminal 2 — subscribe with live display:**
```bash
topicflow sub production-events
```

**Terminal 3 — publish messages:**
```bash
topicflow pub production-events "deploy started: v2.4.1"
topicflow pub production-events "health check OK"
topicflow pub production-events "memory spike: 94% on web1"
```

## Commands

### `topicflow serve`

Start the pub/sub broker on localhost:8765.

```bash
topicflow serve
topicflow serve --host 0.0.0.0 --port 9000
```

### `topicflow pub <topic> <message>`

Publish a message to a topic. Connects, publishes, and disconnects immediately.

```bash
topicflow pub events "build passed"
topicflow pub alerts "CPU > 90%" --quiet   # suppress output
```

### `topicflow sub <topic>`

Subscribe to a topic. Displays arriving messages in a live Rich table.

```bash
topicflow sub events                    # live Rich table (default)
topicflow sub events --format json      # newline-delimited JSON
topicflow sub events --max-rows 500     # keep 500 rows in display
```

**Sample JSON output (`--format json`):**
```json
{"type": "message", "topic": "events", "data": "deploy started", "id": "3a1f9c", "ts": "2026-05-10T12:00:01.124Z"}
{"type": "message", "topic": "events", "data": "health check OK", "id": "7b2e4d", "ts": "2026-05-10T12:00:03.887Z"}
```

### `topicflow digest <topic>`

Subscribe and stream an **AI-generated summary** of all messages received in each time window. Powered by Claude Haiku — fast and cheap.

```bash
export ANTHROPIC_API_KEY=sk-ant-...
topicflow digest production-events --interval 60   # digest every 60s
topicflow digest alerts --interval 30              # more frequent
```

**Sample digest output:**
```
─────────────────── AI Digest — 14 msg in 60s — 12:01:00 ───────────────────
The production-events stream shows a successful deployment of v2.4.1 followed
by routine health checks. One anomaly: a memory spike to 94% on web1 at 12:00:07
resolved within 30 seconds without operator intervention. No action required.
```

### `topicflow topics`

List all active topics and their subscriber counts.

```bash
topicflow topics
# ╭─────────────────────┬─────────────╮
# │ Topic               │ Subscribers │
# ├─────────────────────┼─────────────┤
# │ alerts              │           1 │
# │ production-events   │           3 │
# ╰─────────────────────┴─────────────╯
```

### `topicflow stats`

Show broker statistics (messages routed, connections, per-topic counts).

```bash
topicflow stats
topicflow stats --json
```

## Using topicflow programmatically

```python
import asyncio
from topicflow.client import publish, subscribe, list_topics

# Publish a message
async def main():
    ack = await publish("localhost", 8765, "events", "hello from Python")
    print(f"Delivered to {ack['delivered']} subscriber(s)")

asyncio.run(main())

# Subscribe and process messages
async def consume():
    async for envelope in subscribe("localhost", 8765, "events"):
        print(f"[{envelope['id']}] {envelope['data']}")

asyncio.run(consume())
```

## Architecture

```
┌──────────────────────────────────────┐
│           topicflow broker           │
│  Registry (asyncio.Lock-protected)   │
│  topic → {ws1, ws2, ws3, ...}       │
│  BrokerStats (messages, connections) │
└────────────┬─────────────────────────┘
             │ WebSocket (ws://)
    ┌────────┼────────┐
    │        │        │
  pub      sub     digest
 (write)  (read)  (read + AI)
```

The broker is a single asyncio event loop. Each client connection runs as a coroutine. Messages are broadcast via `asyncio.gather` for maximum concurrency. The `Registry` uses `asyncio.Lock` to protect the subscriber sets.

## Protocol

All messages are JSON over WebSocket:

| Direction | Message |
|---|---|
| Client → Broker | `{"type": "subscribe", "topic": "events"}` |
| Client → Broker | `{"type": "publish", "topic": "events", "data": "hello"}` |
| Client → Broker | `{"type": "topics"}` |
| Client → Broker | `{"type": "stats"}` |
| Broker → Client | `{"type": "ack", "action": "publish", "topic": "events", "delivered": 2}` |
| Broker → Subscriber | `{"type": "message", "topic": "events", "data": "hello", "id": "3a1f9c", "ts": "..."}` |
| Broker → Client | `{"type": "topics", "topics": {"events": 2}}` |

## Running tests

```bash
pip install "topicflow[dev]"
pytest tests/ -v
```

## License

MIT
