# topicflow

**Async WebSocket pub/sub relay with live Rich dashboard and AI message digest.**

topicflow is a lightweight message broker you run locally to test event-driven systems, wire up services during development, or monitor live message streams — all from the terminal.

```
┌─────────────────────────────────────────────────────────────────┐
│ topicflow  production-events, alerts                            │
├──────────────────┬──────────┬──────────────────────────┬────────┤
│ Topic            │ ID       │ Timestamp                │ Data   │
├──────────────────┼──────────┼──────────────────────────┼────────┤
│ production-events│ 3a1f9c   │ 2026-05-10 12:00:01.124  │ deploy │
│ alerts           │ 7b2e4d   │ 2026-05-10 12:00:03.887  │ CPU>90%│
│ production-events│ c91f8a   │ 2026-05-10 12:00:07.334  │ done   │
└──────────────────┴──────────┴──────────────────────────┴────────┘
```

## What's New — v1.1.0

### Message History & Replay

The broker now stores the last N messages per topic in memory. Subscribers can request a replay of recent history — perfect for catching up after a restart without missing events.

```bash
# Broker stores last 200 messages per topic (default: 100)
topicflow serve --history 200

# Show last 30 messages for a topic without subscribing
topicflow replay events --count 30

# Subscribe AND pre-populate the table with the last 50 messages
topicflow sub events --replay 50
```

Programmatic replay:
```python
from topicflow.client import replay

messages = await replay("localhost", 8765, "events", count=20)
for m in messages:
    print(f"[{m['id']}] {m['ts']} — {m['data']}")
```

### Live Broker Dashboard

A new `topicflow dash` command opens a full-screen live dashboard showing all active topics, subscriber counts, and broker stats — updated every second.

```bash
topicflow dash            # 1s refresh
topicflow dash --refresh 0.5   # faster
```

```
╭────────── topicflow broker dashboard ──────────╮
│  ╭─── Broker Stats ───╮  ╭─── Active Topics ──╮ │
│  │ Uptime        120s  │  │ Topic      Subs    │ │
│  │ Msgs routed   4,821 │  │ alerts        1    │ │
│  │ Active conns     4  │  │ events        3    │ │
│  │ Total conns      9  │  │ metrics       2    │ │
│  ╰─────────────────────╯  ╰────────────────────╯ │
╰────────────────────────────────────────────────╯
```

### Multi-Topic Subscribe

`topicflow sub` now accepts comma-separated topic names. Messages from all topics flow into a single merged live table, colour-coded by topic.

```bash
topicflow sub events,alerts,deploys
topicflow sub events,alerts --replay 25   # with history pre-fill
topicflow sub "metrics,logs,errors" --format json   # JSON mode
```

---

## Breakthrough techniques

| Technique | Where |
|---|---|
| **WebSocket client/server** | `websockets` library — broker and all clients communicate over async WebSocket |
| **Full async architecture** | `asyncio` throughout; `asyncio.gather` for concurrent broadcast delivery |
| **Live Rich UI** | `rich.live.Live` subscriber display + full broker dashboard that updates in real-time |
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
topicflow serve --history 200   # keep 200 messages per topic for replay
```

### `topicflow pub <topic> <message>`

Publish a message to a topic. Connects, publishes, and disconnects immediately.

```bash
topicflow pub events "build passed"
topicflow pub alerts "CPU > 90%" --quiet   # suppress output
```

### `topicflow sub <topic[,topic,...]>`

Subscribe to one or more topics. Displays arriving messages in a live Rich table.

```bash
topicflow sub events                    # live Rich table (default)
topicflow sub events,alerts,deploys     # multi-topic merged view
topicflow sub events --replay 50        # pre-fill with 50 historical messages
topicflow sub events --format json      # newline-delimited JSON
topicflow sub events --max-rows 500     # keep 500 rows in display
```

### `topicflow replay <topic>`

Fetch recent message history without subscribing.

```bash
topicflow replay events                 # last 20 messages (default)
topicflow replay events --count 50      # last 50 messages
topicflow replay events --format json   # as newline-delimited JSON
```

### `topicflow dash`

Live full-screen broker dashboard showing all topics, subscriber counts, and stats.

```bash
topicflow dash
topicflow dash --refresh 0.5   # 500ms refresh
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
from topicflow.client import publish, subscribe, replay, list_topics

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

# Replay recent history
async def catchup():
    messages = await replay("localhost", 8765, "events", count=20)
    for m in messages:
        print(f"[{m['id']}] {m['ts']} — {m['data']}")

asyncio.run(catchup())
```

## Architecture

```
┌──────────────────────────────────────────────┐
│              topicflow broker                │
│  Registry (asyncio.Lock-protected)           │
│  topic → {ws1, ws2, ws3, ...}               │
│  History (deque[100] per topic, for replay)  │
│  BrokerStats (messages, connections)         │
└────────────┬─────────────────────────────────┘
             │ WebSocket (ws://)
    ┌────────┼────────┬────────┐
    │        │        │        │
  pub      sub     digest    dash
 (write)  (read)  (read+AI) (poll)
```

The broker is a single asyncio event loop. Each client connection runs as a coroutine. Messages are broadcast via `asyncio.gather` for maximum concurrency. The `Registry` uses `asyncio.Lock` to protect the subscriber sets. The `History` buffer is a per-topic `deque(maxlen=N)` — O(1) append/evict, O(N) snapshot.

## Protocol

All messages are JSON over WebSocket:

| Direction | Message |
|---|---|
| Client → Broker | `{"type": "subscribe", "topic": "events"}` |
| Client → Broker | `{"type": "publish", "topic": "events", "data": "hello"}` |
| Client → Broker | `{"type": "replay", "topic": "events", "count": 20}` |
| Client → Broker | `{"type": "topics"}` |
| Client → Broker | `{"type": "stats"}` |
| Broker → Client | `{"type": "ack", "action": "publish", "topic": "events", "delivered": 2}` |
| Broker → Subscriber | `{"type": "message", "topic": "events", "data": "hello", "id": "3a1f9c", "ts": "..."}` |
| Broker → Client | `{"type": "replay", "topic": "events", "messages": [...], "count": 5}` |
| Broker → Client | `{"type": "topics", "topics": {"events": 2}}` |

## Running tests

```bash
pip install "topicflow[dev]"
pytest tests/ -v
```

## License

MIT
