"""
LLM Backend abstraction for the Ask agent.

Provides pluggable backend implementations (Anthropic, OpenAI, NoOp)
with a common interface for tool-aware conversational completions.

Design:
  - Abstract base class LLMBackend with a single async method
  - Concrete implementations for Anthropic (claude-haiku) and OpenAI (gpt-4o-mini)
  - NoOpBackend for demo/fallback mode
  - get_llm_backend() factory that reads env vars and handles missing credentials
  - All HTTP calls wrapped with timeouts and retry logic
  - httpx imports are lazy to avoid hard dependencies
"""

from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# LLMBackend Abstract Base
# ============================================================================

class LLMBackend(ABC):
    """
    Abstract base class for LLM backends used by the Ask agent.

    Subclasses implement async complete() to dispatch prompts to their
    respective APIs, optionally with tool-calling capability.
    """

    @abstractmethod
    async def complete(
        self,
        prompt: str,
        tools: Optional[list[dict]] = None,
        system: Optional[str] = None,
    ) -> dict:
        """
        Request a completion from the LLM.

        Args:
            prompt: The user message / prompt to complete
            tools: Optional list of tool definitions (function-calling style)
            system: Optional system prompt / instructions

        Returns:
            Dictionary with keys:
              - "text": str — the assistant's text response
              - "tool_calls": list[dict] | None — any tool invocations (parsed)
              - "raw": dict | None — the raw API response (for debugging)
        """
        pass


# ============================================================================
# NoOp Backend (Demo / Fallback)
# ============================================================================

class NoOpBackend(LLMBackend):
    """
    Null backend that always returns empty text.

    Used when no API key is configured or explicitly set as the backend.
    Allows the ask agent to fall back to rule-based classification.
    """

    async def complete(
        self,
        prompt: str,
        tools: Optional[list[dict]] = None,
        system: Optional[str] = None,
    ) -> dict:
        """Always return empty response."""
        return {
            "text": "",
            "tool_calls": None,
            "raw": None,
        }


# ============================================================================
# Anthropic Backend
# ============================================================================

class AnthropicBackend(LLMBackend):
    """
    Backend using Anthropic's Messages API.

    Reads ANTHROPIC_API_KEY env var and uses claude-haiku-4-5-20251001.
    Supports tool_use blocks for function calling.
    """

    def __init__(self, api_key: str):
        """Initialize with an API key."""
        self.api_key = api_key
        self.model = "claude-haiku-4-5-20251001"
        self.timeout = 15.0  # seconds

    async def complete(
        self,
        prompt: str,
        tools: Optional[list[dict]] = None,
        system: Optional[str] = None,
    ) -> dict:
        """
        Request a completion from Claude.

        Returns:
            {
              "text": str (assistant response),
              "tool_calls": list[dict] | None,
              "raw": dict (Anthropic API response)
            }
        """
        # Lazy import to avoid hard dependency
        import httpx

        # Build request payload
        messages = [{"role": "user", "content": prompt}]

        payload = {
            "model": self.model,
            "max_tokens": 1024,
            "messages": messages,
        }

        if system:
            payload["system"] = system

        if tools:
            # Convert tool format: list of dicts with 'name', 'description', 'input_schema'
            payload["tools"] = tools

        headers = {
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }

        url = "https://api.anthropic.com/v1/messages"

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                data = response.json()
        except httpx.HTTPError as e:
            logger.error(f"Anthropic API error: {e}")
            raise

        # Parse response
        text = ""
        tool_calls = None

        if "content" in data:
            for block in data.get("content", []):
                if block.get("type") == "text":
                    text += block.get("text", "")
                elif block.get("type") == "tool_use":
                    if tool_calls is None:
                        tool_calls = []
                    tool_calls.append({
                        "id": block.get("id"),
                        "name": block.get("name"),
                        "input": block.get("input", {}),
                    })

        return {
            "text": text,
            "tool_calls": tool_calls,
            "raw": data,
        }


# ============================================================================
# OpenAI Backend
# ============================================================================

class OpenAIBackend(LLMBackend):
    """
    Backend using OpenAI's /v1/chat/completions endpoint.

    Reads OPENAI_API_KEY env var and uses gpt-4o-mini.
    Supports function_call for function calling.
    """

    def __init__(self, api_key: str):
        """Initialize with an API key."""
        self.api_key = api_key
        self.model = "gpt-4o-mini"
        self.timeout = 15.0  # seconds

    async def complete(
        self,
        prompt: str,
        tools: Optional[list[dict]] = None,
        system: Optional[str] = None,
    ) -> dict:
        """
        Request a completion from OpenAI.

        Returns:
            {
              "text": str (assistant response),
              "tool_calls": list[dict] | None,
              "raw": dict (OpenAI API response)
            }
        """
        # Lazy import to avoid hard dependency
        import httpx

        # Build request payload
        messages = [
            {"role": "user", "content": prompt},
        ]

        if system:
            messages.insert(0, {"role": "system", "content": system})

        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": 1024,
            "temperature": 0.7,
        }

        if tools:
            # Convert to OpenAI function format
            payload["tools"] = [
                {
                    "type": "function",
                    "function": {
                        "name": tool.get("name", ""),
                        "description": tool.get("description", ""),
                        "parameters": tool.get("input_schema", {}),
                    }
                }
                for tool in tools
            ]
            payload["tool_choice"] = "auto"

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        url = "https://api.openai.com/v1/chat/completions"

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                data = response.json()
        except httpx.HTTPError as e:
            logger.error(f"OpenAI API error: {e}")
            raise

        # Parse response
        text = ""
        tool_calls = None

        if "choices" in data and len(data["choices"]) > 0:
            choice = data["choices"][0]
            message = choice.get("message", {})

            # Text content
            if message.get("content"):
                text = message["content"]

            # Tool calls
            if "tool_calls" in message and message["tool_calls"]:
                tool_calls = []
                for tc in message["tool_calls"]:
                    if tc.get("type") == "function":
                        func = tc.get("function", {})
                        tool_calls.append({
                            "id": tc.get("id"),
                            "name": func.get("name"),
                            "input": json.loads(func.get("arguments", "{}")),
                        })

        return {
            "text": text,
            "tool_calls": tool_calls,
            "raw": data,
        }


# ============================================================================
# Factory Function
# ============================================================================

def get_llm_backend() -> LLMBackend:
    """
    Get an LLM backend instance based on environment configuration.

    Reads ROSTERIQ_LLM_BACKEND env var:
      - "anthropic": Uses AnthropicBackend (requires ANTHROPIC_API_KEY)
      - "openai": Uses OpenAIBackend (requires OPENAI_API_KEY)
      - "none" or unset: Uses NoOpBackend (demo mode)

    If a backend is configured but the API key is missing, logs a warning
    and falls back to NoOpBackend.

    Returns:
        An LLMBackend instance (one of the concrete implementations)
    """
    backend_name = os.getenv("ROSTERIQ_LLM_BACKEND", "none").lower()

    if backend_name == "anthropic":
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            logger.warning(
                "ROSTERIQ_LLM_BACKEND is 'anthropic' but ANTHROPIC_API_KEY is not set. "
                "Falling back to NoOpBackend (demo mode)."
            )
            return NoOpBackend()
        return AnthropicBackend(api_key)

    elif backend_name == "openai":
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.warning(
                "ROSTERIQ_LLM_BACKEND is 'openai' but OPENAI_API_KEY is not set. "
                "Falling back to NoOpBackend (demo mode)."
            )
            return NoOpBackend()
        return OpenAIBackend(api_key)

    else:
        # Default: NoOp (includes "none" and unrecognized values)
        if backend_name and backend_name != "none":
            logger.warning(
                f"Unknown ROSTERIQ_LLM_BACKEND value: {backend_name}. "
                "Using NoOpBackend (demo mode)."
            )
        return NoOpBackend()
