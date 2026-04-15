"""
Tests for LLM backend implementations.

Covers NoOpBackend, AnthropicBackend, OpenAIBackend, and the factory function.
Uses standard library + unittest.mock to avoid hard dependencies on httpx or pytest.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, Mock, patch

# Mock httpx before importing llm_backends to handle absence in test environment
sys.modules["httpx"] = MagicMock()

from rosteriq.llm_backends import (
    AnthropicBackend,
    LLMBackend,
    NoOpBackend,
    OpenAIBackend,
    get_llm_backend,
)


# ============================================================================
# Test NoOpBackend
# ============================================================================

class TestNoOpBackend(unittest.TestCase):
    """Test NoOpBackend always returns empty response."""

    def test_noop_returns_empty(self):
        """NoOpBackend.complete() always returns empty text."""
        backend = NoOpBackend()
        result = asyncio.run(
            backend.complete(
                prompt="What's the forecast?",
                tools=None,
                system=None,
            )
        )

        assert result["text"] == ""
        assert result["tool_calls"] is None
        assert result["raw"] is None

    def test_noop_ignores_tools_and_system(self):
        """NoOpBackend ignores tools and system prompts."""
        backend = NoOpBackend()
        result = asyncio.run(
            backend.complete(
                prompt="Test prompt",
                tools=[{"name": "test_tool", "description": "A test tool"}],
                system="You are helpful.",
            )
        )

        assert result["text"] == ""
        assert result["tool_calls"] is None


# ============================================================================
# Test get_llm_backend Factory
# ============================================================================

class TestGetLLMBackend(unittest.TestCase):
    """Test get_llm_backend factory function."""

    def test_defaults_to_noop(self):
        """get_llm_backend() defaults to NoOpBackend when unset."""
        with patch.dict(os.environ, {}, clear=True):
            backend = get_llm_backend()
            assert isinstance(backend, NoOpBackend)

    def test_explicit_none_backend(self):
        """get_llm_backend() returns NoOp when ROSTERIQ_LLM_BACKEND='none'."""
        with patch.dict(os.environ, {"ROSTERIQ_LLM_BACKEND": "none"}, clear=True):
            backend = get_llm_backend()
            assert isinstance(backend, NoOpBackend)

    def test_anthropic_with_key(self):
        """get_llm_backend() returns AnthropicBackend when key is set."""
        with patch.dict(
            os.environ,
            {"ROSTERIQ_LLM_BACKEND": "anthropic", "ANTHROPIC_API_KEY": "test-key-123"},
            clear=True,
        ):
            backend = get_llm_backend()
            assert isinstance(backend, AnthropicBackend)
            assert backend.api_key == "test-key-123"

    def test_anthropic_without_key_falls_back(self):
        """get_llm_backend() falls back to NoOp if Anthropic key missing."""
        with patch.dict(
            os.environ,
            {"ROSTERIQ_LLM_BACKEND": "anthropic"},
            clear=True,
        ):
            backend = get_llm_backend()
            assert isinstance(backend, NoOpBackend)

    def test_openai_with_key(self):
        """get_llm_backend() returns OpenAIBackend when key is set."""
        with patch.dict(
            os.environ,
            {"ROSTERIQ_LLM_BACKEND": "openai", "OPENAI_API_KEY": "sk-test-key-456"},
            clear=True,
        ):
            backend = get_llm_backend()
            assert isinstance(backend, OpenAIBackend)
            assert backend.api_key == "sk-test-key-456"

    def test_openai_without_key_falls_back(self):
        """get_llm_backend() falls back to NoOp if OpenAI key missing."""
        with patch.dict(
            os.environ,
            {"ROSTERIQ_LLM_BACKEND": "openai"},
            clear=True,
        ):
            backend = get_llm_backend()
            assert isinstance(backend, NoOpBackend)

    def test_unknown_backend_falls_back(self):
        """get_llm_backend() falls back to NoOp for unknown backend names."""
        with patch.dict(
            os.environ,
            {"ROSTERIQ_LLM_BACKEND": "unknown_backend"},
            clear=True,
        ):
            backend = get_llm_backend()
            assert isinstance(backend, NoOpBackend)


# ============================================================================
# Test AnthropicBackend
# ============================================================================

class TestAnthropicBackend(unittest.TestCase):
    """Test AnthropicBackend API calls and response parsing."""

    def setUp(self):
        """Set up a backend instance for testing."""
        self.backend = AnthropicBackend(api_key="test-api-key")

    def test_anthropic_model_and_key(self):
        """AnthropicBackend stores model and API key."""
        assert self.backend.model == "claude-haiku-4-5-20251001"
        assert self.backend.api_key == "test-api-key"
        assert self.backend.timeout == 15.0

    def test_anthropic_constructs_correct_payload(self):
        """AnthropicBackend constructs correct HTTP request."""
        # Create a mock AsyncClient
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "content": [{"type": "text", "text": "Hello, world!"}],
            "id": "msg-123",
        }

        mock_client = MagicMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        with patch.object(
            sys.modules["httpx"], "AsyncClient", return_value=mock_client
        ):
            result = asyncio.run(
                self.backend.complete(
                    prompt="What's the forecast?",
                    system="You are helpful",
                    tools=None,
                )
            )

            # Verify result
            assert result["text"] == "Hello, world!"
            assert mock_client.post.called

    def test_anthropic_parses_tool_use(self):
        """AnthropicBackend correctly parses tool_use blocks."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "content": [
                {"type": "text", "text": "I'll check the forecast."},
                {
                    "type": "tool_use",
                    "id": "tool-call-1",
                    "name": "get_forecast",
                    "input": {"date": "2024-04-15", "venue_id": "demo-venue"},
                },
            ],
        }

        mock_client = MagicMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        with patch.object(
            sys.modules["httpx"], "AsyncClient", return_value=mock_client
        ):
            result = asyncio.run(
                self.backend.complete(
                    prompt="What's the forecast?",
                    tools=[
                        {
                            "name": "get_forecast",
                            "description": "Get forecast",
                            "input_schema": {},
                        }
                    ],
                )
            )

            assert result["text"] == "I'll check the forecast."
            assert result["tool_calls"] is not None
            assert len(result["tool_calls"]) == 1
            assert result["tool_calls"][0]["name"] == "get_forecast"
            assert result["tool_calls"][0]["input"]["date"] == "2024-04-15"

    def test_anthropic_handles_http_error(self):
        """AnthropicBackend raises on HTTP error."""
        mock_error = Exception("API error")

        mock_client = MagicMock()
        mock_client.post = AsyncMock(side_effect=mock_error)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        with patch.object(
            sys.modules["httpx"], "AsyncClient", return_value=mock_client
        ):
            with self.assertRaises(Exception):
                asyncio.run(
                    self.backend.complete(prompt="Test", tools=None, system=None)
                )


# ============================================================================
# Test OpenAIBackend
# ============================================================================

class TestOpenAIBackend(unittest.TestCase):
    """Test OpenAIBackend API calls and response parsing."""

    def setUp(self):
        """Set up a backend instance for testing."""
        self.backend = OpenAIBackend(api_key="sk-test-key")

    def test_openai_model_and_key(self):
        """OpenAIBackend stores model and API key."""
        assert self.backend.model == "gpt-4o-mini"
        assert self.backend.api_key == "sk-test-key"
        assert self.backend.timeout == 15.0

    def test_openai_constructs_correct_payload(self):
        """OpenAIBackend constructs correct HTTP request."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": "The forecast shows...",
                        "tool_calls": None,
                    }
                }
            ],
        }

        mock_client = MagicMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        with patch.object(
            sys.modules["httpx"], "AsyncClient", return_value=mock_client
        ):
            result = asyncio.run(
                self.backend.complete(
                    prompt="What's the forecast?",
                    system="You are helpful",
                    tools=None,
                )
            )

            # Check result
            assert result["text"] == "The forecast shows..."
            assert mock_client.post.called

    def test_openai_parses_function_calls(self):
        """OpenAIBackend correctly parses function_call in tool_calls."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": "I'll check the data.",
                        "tool_calls": [
                            {
                                "id": "call-abc123",
                                "type": "function",
                                "function": {
                                    "name": "get_labour_cost",
                                    "arguments": '{"start_date": "2024-04-08", "end_date": "2024-04-15", "venue_id": "demo-venue"}',
                                },
                            }
                        ],
                    }
                }
            ],
        }

        mock_client = MagicMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        with patch.object(
            sys.modules["httpx"], "AsyncClient", return_value=mock_client
        ):
            result = asyncio.run(
                self.backend.complete(
                    prompt="Labour cost last week?",
                    tools=[
                        {
                            "name": "get_labour_cost",
                            "description": "Get labour cost",
                            "input_schema": {},
                        }
                    ],
                )
            )

            assert result["text"] == "I'll check the data."
            assert result["tool_calls"] is not None
            assert len(result["tool_calls"]) == 1
            assert result["tool_calls"][0]["name"] == "get_labour_cost"
            assert result["tool_calls"][0]["input"]["start_date"] == "2024-04-08"

    def test_openai_handles_http_error(self):
        """OpenAIBackend raises on HTTP error."""
        mock_error = Exception("Rate limited")

        mock_client = MagicMock()
        mock_client.post = AsyncMock(side_effect=mock_error)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        with patch.object(
            sys.modules["httpx"], "AsyncClient", return_value=mock_client
        ):
            with self.assertRaises(Exception):
                asyncio.run(
                    self.backend.complete(prompt="Test", tools=None, system=None)
                )


# ============================================================================
# Test Abstract Base
# ============================================================================

class TestLLMBackendAbstract(unittest.TestCase):
    """Test that LLMBackend is abstract."""

    def test_cannot_instantiate_abstract(self):
        """LLMBackend cannot be instantiated directly."""
        with self.assertRaises(TypeError):
            LLMBackend()


# ============================================================================
# Run tests
# ============================================================================

if __name__ == "__main__":
    unittest.main()
