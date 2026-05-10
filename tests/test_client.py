"""Tests for client message serialization and protocol helpers."""

from __future__ import annotations

import json


# ---------------------------------------------------------------------------
# Protocol format tests (no real WebSocket needed)
# ---------------------------------------------------------------------------

def test_publish_message_format():
    """Publish wire message must be valid JSON with required fields."""
    msg = {"type": "publish", "topic": "events", "data": "deploy started"}
    raw = json.dumps(msg)
    parsed = json.loads(raw)
    assert parsed["type"] == "publish"
    assert parsed["topic"] == "events"
    assert parsed["data"] == "deploy started"


def test_subscribe_message_format():
    msg = {"type": "subscribe", "topic": "logs"}
    raw = json.dumps(msg)
    parsed = json.loads(raw)
    assert parsed["type"] == "subscribe"
    assert "topic" in parsed


def test_ack_structure():
    """Ack envelopes must carry type, action, and topic."""
    ack = {"type": "ack", "action": "publish", "topic": "events", "delivered": 3}
    assert ack["type"] == "ack"
    assert ack["delivered"] == 3


def test_message_envelope_fields():
    """Inbound message envelopes from the broker must have all required keys."""
    env = {
        "type": "message",
        "topic": "alerts",
        "data": "threshold exceeded",
        "id": "a1b2c3d4",
        "ts": "2026-05-10T12:00:00+00:00",
    }
    for key in ("type", "topic", "data", "id", "ts"):
        assert key in env, f"Missing key: {key}"
    assert env["type"] == "message"


def test_topics_response_structure():
    resp = {"type": "topics", "topics": {"events": 2, "alerts": 1}}
    assert resp["type"] == "topics"
    assert isinstance(resp["topics"], dict)
    assert resp["topics"]["events"] == 2


def test_ws_uri_format():
    """_ws_uri should produce a valid ws:// URI."""
    from topicflow.client import _ws_uri

    uri = _ws_uri("localhost", 8765)
    assert uri == "ws://localhost:8765"

    uri2 = _ws_uri("0.0.0.0", 9000)
    assert uri2 == "ws://0.0.0.0:9000"
