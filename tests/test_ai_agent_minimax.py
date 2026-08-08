"""
MiniMax (OpenAI-compatible) provider for the AI agent.

The agent was Gemini-only. These tests cover the new pluggable provider:
the Gemini tool schema converts to OpenAI's tools schema, the agent selects
MiniMax via LLM_PROVIDER, and the OpenAI-style tool_calls loop drives the same
13 tools and returns the model's final text. No real network — respx-mocked.
"""

import asyncio
import json

import httpx
import respx

from rosteriq import ai_agent
from rosteriq.ai_agent import RosterIQAgent, _gemini_tools_to_openai, GEMINI_TOOLS


# --- tool-schema conversion ------------------------------------------------

def test_gemini_tools_convert_to_openai_schema():
    tools = _gemini_tools_to_openai(GEMINI_TOOLS)
    # One OpenAI tool per Gemini function declaration; grows as tools are added.
    declared = sum(len(g.get("functionDeclarations", [])) for g in GEMINI_TOOLS)
    assert len(tools) == declared >= 13
    blob = json.dumps(tools)
    # uppercase Gemini types must be gone
    assert "OBJECT" not in blob and "STRING" not in blob and "BOOLEAN" not in blob
    for t in tools:
        assert t["type"] == "function"
        fn = t["function"]
        assert fn["name"] and "parameters" in fn
        assert fn["parameters"]["type"] == "object"
    # a tool with a required arg keeps it
    gen = next(t for t in tools if t["function"]["name"] == "generate_roster")
    assert gen["function"]["parameters"]["required"] == ["start_date", "end_date"]


# --- provider selection / config ------------------------------------------

def test_minimax_requires_its_own_key(monkeypatch):
    monkeypatch.setattr(ai_agent, "LLM_PROVIDER", "minimax")
    monkeypatch.setattr(ai_agent, "MINIMAX_API_KEY", "")
    try:
        RosterIQAgent("venue-1")
        assert False, "expected ValueError when MINIMAX_API_KEY missing"
    except ValueError as e:
        assert "MINIMAX_API_KEY" in str(e)


# --- the OpenAI-compatible tool-calling loop ------------------------------

_TOOL_ROUND = httpx.Response(200, json={
    "choices": [{
        "message": {
            "role": "assistant",
            "content": "",
            "tool_calls": [{
                "id": "call_1",
                "type": "function",
                "function": {"name": "get_employees", "arguments": "{}"},
            }],
        }
    }]
})
_FINAL_ROUND = httpx.Response(200, json={
    "choices": [{
        "message": {"role": "assistant", "content": "You have no staff on file yet."}
    }]
})


@respx.mock
def test_minimax_chat_executes_tool_then_answers(monkeypatch):
    monkeypatch.setattr(ai_agent, "LLM_PROVIDER", "minimax")
    monkeypatch.setattr(ai_agent, "MINIMAX_API_KEY", "test-key")
    route = respx.post(ai_agent.MINIMAX_API_URL).mock(side_effect=[_TOOL_ROUND, _FINAL_ROUND])

    agent = RosterIQAgent("venue-1")
    result = asyncio.run(agent.chat([{"role": "user", "content": "How many staff do I have?"}]))

    # final answer comes back, and the tool was actually invoked
    assert result["response"] == "You have no staff on file yet."
    assert any(c["tool"] == "get_employees" for c in result["tool_calls"])

    # first request: MiniMax endpoint, Bearer auth, OpenAI tools, our model
    req1 = json.loads(route.calls[0].request.content)
    assert route.calls[0].request.headers["authorization"] == "Bearer test-key"
    assert req1["model"] == "MiniMax-M3"
    assert req1["tool_choice"] == "auto"
    assert any(t["function"]["name"] == "get_employees" for t in req1["tools"])
    assert req1["messages"][0]["role"] == "system"  # system prompt injected

    # second request: tool result fed back as a role:tool message
    req2 = json.loads(route.calls[1].request.content)
    roles = [m["role"] for m in req2["messages"]]
    assert "assistant" in roles and "tool" in roles


@respx.mock
def test_minimax_api_error_is_handled_gracefully(monkeypatch):
    monkeypatch.setattr(ai_agent, "LLM_PROVIDER", "minimax")
    monkeypatch.setattr(ai_agent, "MINIMAX_API_KEY", "test-key")
    respx.post(ai_agent.MINIMAX_API_URL).mock(
        return_value=httpx.Response(400, json={"error": {"message": "invalid model"}})
    )
    agent = RosterIQAgent("venue-1")
    result = asyncio.run(agent.chat([{"role": "user", "content": "hi"}]))
    assert "trouble connecting" in result["response"].lower()
    assert "invalid model" in result["response"]


@respx.mock
def test_minimax_plain_answer_no_tools(monkeypatch):
    monkeypatch.setattr(ai_agent, "LLM_PROVIDER", "minimax")
    monkeypatch.setattr(ai_agent, "MINIMAX_API_KEY", "test-key")
    respx.post(ai_agent.MINIMAX_API_URL).mock(return_value=httpx.Response(200, json={
        "choices": [{"message": {"role": "assistant", "content": "Hello! How can I help?"}}]
    }))
    agent = RosterIQAgent("venue-1")
    result = asyncio.run(agent.chat([{"role": "user", "content": "hi"}]))
    assert result["response"] == "Hello! How can I help?"
    assert result["tool_calls"] == []
