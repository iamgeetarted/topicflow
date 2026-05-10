"""Tests for the broker Registry — no real WebSocket connection required."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from topicflow.broker import Registry, BrokerStats


# ---------------------------------------------------------------------------
# Registry tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_subscribe_and_topic_info():
    registry = Registry()
    ws1 = MagicMock()
    ws2 = MagicMock()

    await registry.subscribe("events", ws1)
    await registry.subscribe("events", ws2)
    await registry.subscribe("alerts", ws1)

    info = await registry.topic_info()
    assert info["events"] == 2
    assert info["alerts"] == 1


@pytest.mark.asyncio
async def test_unsubscribe_removes_entry():
    registry = Registry()
    ws = MagicMock()

    await registry.subscribe("logs", ws)
    assert "logs" in await registry.topic_info()

    await registry.unsubscribe("logs", ws)
    assert "logs" not in await registry.topic_info()


@pytest.mark.asyncio
async def test_unsubscribe_all():
    registry = Registry()
    ws = MagicMock()

    await registry.subscribe("a", ws)
    await registry.subscribe("b", ws)
    await registry.subscribe("c", ws)

    removed = await registry.unsubscribe_all(ws)
    assert set(removed) == {"a", "b", "c"}
    info = await registry.topic_info()
    assert not info


@pytest.mark.asyncio
async def test_broadcast_delivers_to_subscribers():
    registry = Registry()

    received: list[str] = []

    async def _fake_send(data: str) -> None:
        received.append(data)

    ws1 = MagicMock()
    ws1.send = AsyncMock(side_effect=_fake_send)
    ws2 = MagicMock()
    ws2.send = AsyncMock(side_effect=_fake_send)

    await registry.subscribe("stream", ws1)
    await registry.subscribe("stream", ws2)

    envelope = {"type": "message", "topic": "stream", "data": "hello", "id": "abc", "ts": "now"}
    delivered = await registry.broadcast("stream", envelope)

    assert delivered == 2
    assert len(received) == 2
    parsed = json.loads(received[0])
    assert parsed["data"] == "hello"


@pytest.mark.asyncio
async def test_broadcast_no_subscribers_returns_zero():
    registry = Registry()
    envelope = {"type": "message", "topic": "empty", "data": "x", "id": "1", "ts": "t"}
    delivered = await registry.broadcast("empty", envelope)
    assert delivered == 0


# ---------------------------------------------------------------------------
# BrokerStats tests
# ---------------------------------------------------------------------------

def test_broker_stats_record_publish():
    stats = BrokerStats()
    stats.record_publish("events")
    stats.record_publish("events")
    stats.record_publish("alerts")

    assert stats.messages_routed == 3
    assert stats.topic_message_counts["events"] == 2
    assert stats.topic_message_counts["alerts"] == 1
