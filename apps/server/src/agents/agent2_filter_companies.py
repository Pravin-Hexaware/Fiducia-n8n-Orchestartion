import json
from typing import Annotated, TypedDict
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage, SystemMessage
from langgraph.graph import StateGraph, START, END
from langgraph.prebuilt import ToolNode
from langgraph.checkpoint.memory import MemorySaver
import operator
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
from functools import lru_cache

load_dotenv()

# YOUR EXACT TOOL ✅ (No modification needed)
from utils.tools import load_and_filter_companies
from utils.llm_testing import get_azure_chat_openai  # Your LLM

LLM = get_azure_chat_openai()


class AgentState(TypedDict):
    messages: Annotated[list, operator.add]


# ================== PROPER TOOL BINDING (Your tool) ==================
tools = [load_and_filter_companies]  # YOUR TOOL HERE ✅
llm_with_tools = LLM.bind_tools(tools, strict=True)


def call_model(state, config=None):
    messages = state["messages"]

    # Check if we already have tool results in history

    has_tool_results = any(isinstance(m, ToolMessage) for m in messages)

    # SYSTEM PROMPT MODIFICATION:

    # We add a rule that if results exist, the agent should summarize.

    system_prompt = """You are the SECTOR AND INDUSTRY RESEARCH AGENT.This agent is responsible for the Sector & Industry Research sub-process within the Research and Idea Generation process of the Fund Mandate capability. It specializes in top-down analysis, identifying broader market trends, competitive landscapes, and macroeconomic tailwinds or headwinds affecting specific industries. Trigger this agent when the user needs high-level thematic insights or comparative sector data to inform investment mandates.

    TOOL INPUT FORMAT (CRITICAL):
The user provides JSON with mandate_id and filters. You MUST pass them EXACTLY as provided:
{
  "mandate_id": <number from input>,
  "additionalProp1": {<filters from input>}

  ⚠️ CRITICAL - YOU MUST EMIT THINKING TEXT FIRST - EVERY LLM CALL BEFORE TOOL
    ═════════════════════════════════════════════════════════════════════════════

    Your role: Research companies by filters. IMPORTANT: Only think about your approach. Do NOT process or think about company data in your reasoning.

    MANDATORY FORMAT - EVERY RESPONSE STARTS WITH:

    Thought: I am a Sector and Industry Research Agent responsible for the Sector & Industry Research sub-process within the Research and Idea Generation process of the Fund Mandate capability. [Your reasoning...]
    Analysis: [Your strategy for filtering companies...]
    Action: load_and_filter_companies

    THEN the tool executes.

    ═════════════════════════════════════════════════════════════════════════════
    NON-NEGOTIABLE RULES - NO EXCEPTIONS:
    ═════════════════════════════════════════════════════════════════════════════

    1️⃣ FIRST LINE MUST BE "Thought: I am a Sector and Industry Research Agent responsible for the Sector & Industry Research sub-process within the Research and Idea Generation process of the Fund Mandate capability."
    2️⃣ SECOND LINE MUST BE "Analysis: [your approach]"
    3️⃣ THIRD LINE MUST BE "Action: load_and_filter_companies"
    4️⃣ EMIT THINKING TEXT COMPLETELY BEFORE TOOL INVOCATION
    6️⃣ Pass user filters AS-IS to the tool
    7️⃣ Return tool output without modification

    NEW RULE: If the conversation history already contains a ToolMessage,

    DO NOT call the tool again. Instead, provide a final summary of the

    companies found and then STOP.
    NOW START WITH "Thought: I am a Sector and Industry Research Agent.":
"""

    messages_with_system = [SystemMessage(content=system_prompt)] + messages

    # STEP 1: Always get the Thinking text

    llm_no_tools = LLM

    thinking_response = llm_no_tools.invoke(messages_with_system, config=config)

    # STEP 2: Conditional Tool Call

    if not has_tool_results:

        # First pass: Get tool calls

        llm_with_tools = LLM.bind_tools(tools, strict=True)

        tool_call_response = llm_with_tools.invoke(messages_with_system, config=config)

        # We return BOTH messages to keep the history clean

        # The thinking_response is just text, tool_call_response has the tool_calls list

        return {"messages": [thinking_response, tool_call_response]}

    else:

        # Second pass: We already have data, just return the thinking/summary

        return {"messages": [thinking_response]}


def should_continue(state):
    messages = state["messages"]

    last_message = messages[-1]

    # Check if the LLM actually requested a tool

    if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
        return "tools"

    # If no tool calls (e.g., it just provided a summary), we end

    return END


# ================== YOUR FACTORY (Exact signature) ==================
def create_sector_and_industry_research_agent():
    """Fast agent using YOUR load_and_filter_companies tool"""
    workflow = StateGraph(AgentState)

    # Agent node (thinks)
    workflow.add_node("agent", call_model)

    # YOUR TOOL via ToolNode ✅
    tool_node = ToolNode(tools)  # Executes load_and_filter_companies
    workflow.add_node("tools", tool_node)

    # Edges: Agent → Tool → Agent (loop for multi-turn)
    workflow.add_edge(START, "agent")
    workflow.add_conditional_edges(
        "agent",
        should_continue,
        {"tools": "tools", END: END}
    )
    workflow.add_edge("tools", "agent")  # Loop back to agent for continued thinking

    # Compile
    return workflow.compile()  # NO checkpointer - fresh state on each invoke


if __name__ == "__main__":
    agent = create_sector_and_industry_research_agent()

    # YOUR EXACT INPUT
    user_input = '{"additionalProp1": {"geography": "us", "sector": "technology", "industry": "software & IT services"}}'


    config = {"configurable": {"thread_id": "test-1"}}
    result = agent.invoke({"messages": [HumanMessage(content=user_input)]}, config)

    print("✅ AGENT EXECUTED SUCCESSFULLY!")
    print("📊 Messages generated:", len(result["messages"]))

    # Print ALL messages properly
    tool_called = False
    for i, msg in enumerate(result["messages"][-4:], 1):  # Last 4 messages
        print(f"\nStep {i}:", end=" ")

        if hasattr(msg, 'tool_calls') and msg.tool_calls:
            tool_called = True
            tc = msg.tool_calls[0]
            print(f"🛠️  TOOL CALL: {tc['name']}({tc['args']})")
        elif isinstance(msg, ToolMessage):
            tool_called = True
            print(f"📈 YOUR TOOL OUTPUT: {msg.content[:400]}...")
        elif hasattr(msg, 'content'):
            print(f"💭 {msg.content[:300]}...")
        else:
            print(f"[Message type: {type(msg)}]")

    if tool_called:
        print("\n🎉 SUCCESS: Your load_and_filter_companies tool was called!")
        tool_msgs = [m for m in result["messages"] if isinstance(m, ToolMessage)]
        if tool_msgs:
            companies_json = json.loads(tool_msgs[-1].content)
            print(f"📊 Companies found: {len(companies_json.get('qualified', []))}")
            print(f"🏢 First company: {companies_json['qualified'][0].get('Company', 'N/A')}")
    else:
        print("\n No tool call detected")
