"""Tests for AI digest prompt construction and summarize_batch interface."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Prompt construction — verify content without calling the real API
# ---------------------------------------------------------------------------

def test_summarize_batch_prompt_includes_topic_and_messages(monkeypatch):
    """summarize_batch must include the topic name and all messages in the prompt."""
    prompts_seen: list[str] = []

    mock_stream = MagicMock()
    mock_stream.__enter__ = MagicMock(return_value=mock_stream)
    mock_stream.__exit__ = MagicMock(return_value=False)
    mock_stream.text_stream = iter(["This is a test summary."])

    mock_client = MagicMock()
    mock_client.messages.stream.return_value = mock_stream

    def _capture_stream(**kwargs):
        prompts_seen.append(kwargs["messages"][0]["content"])
        return mock_stream

    mock_client.messages.stream.side_effect = _capture_stream

    mock_anthropic = MagicMock()
    mock_anthropic.Anthropic.return_value = mock_client

    with patch.dict("sys.modules", {"anthropic": mock_anthropic}):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        from importlib import reload
        import topicflow.digest as digest_mod
        reload(digest_mod)

        digest_mod.summarize_batch(
            messages=["deploy started", "health check OK", "memory spike detected"],
            topic="production-events",
        )

    assert len(prompts_seen) == 1
    prompt = prompts_seen[0]
    assert "production-events" in prompt
    assert "deploy started" in prompt
    assert "health check OK" in prompt
    assert "memory spike detected" in prompt


def test_summarize_batch_caps_at_50_messages(monkeypatch):
    """summarize_batch should only include the first 50 messages in the prompt."""
    prompts_seen: list[str] = []

    mock_stream = MagicMock()
    mock_stream.__enter__ = MagicMock(return_value=mock_stream)
    mock_stream.__exit__ = MagicMock(return_value=False)
    mock_stream.text_stream = iter(["Summary."])

    mock_client = MagicMock()

    def _capture(**kwargs):
        prompts_seen.append(kwargs["messages"][0]["content"])
        return mock_stream

    mock_client.messages.stream.side_effect = _capture

    mock_anthropic = MagicMock()
    mock_anthropic.Anthropic.return_value = mock_client

    with patch.dict("sys.modules", {"anthropic": mock_anthropic}):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        from importlib import reload
        import topicflow.digest as digest_mod
        reload(digest_mod)

        messages = [f"msg-{i}" for i in range(80)]
        digest_mod.summarize_batch(messages=messages, topic="flood-test")

    assert len(prompts_seen) == 1
    prompt = prompts_seen[0]
    assert "msg-49" in prompt
    assert "msg-50" not in prompt
    assert "30 more messages" in prompt


def test_summarize_batch_missing_api_key(monkeypatch, capsys):
    """summarize_batch should exit gracefully when ANTHROPIC_API_KEY is unset."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    mock_anthropic = MagicMock()
    with patch.dict("sys.modules", {"anthropic": mock_anthropic}):
        from importlib import reload
        import topicflow.digest as digest_mod
        reload(digest_mod)

        with pytest.raises(SystemExit):
            digest_mod.summarize_batch(["msg"], "topic")


import pytest
