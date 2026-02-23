from typing_extensions import TypedDict, Annotated
import operator
from langgraph.graph import StateGraph, START, END
from langgraph.prebuilt import ToolNode, tools_condition
from langgraph.checkpoint.memory import MemorySaver

from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import BaseMessage, HumanMessage
from dotenv import load_dotenv

from utils.tools import scan_mandate_folder_and_parse, extract_dynamic_criteria  # Fixed above
from utils.llm_testing import get_azure_chat_openai

load_dotenv()

LLM = get_azure_chat_openai()
if LLM is None:
    raise RuntimeError("LLM initialization failed: LLM is None")

tools = [scan_mandate_folder_and_parse, extract_dynamic_criteria]
LLM_WITH_TOOLS = LLM.bind_tools(tools)

REACT_PROMPT = """You are the MANDATE PARSING AGENT.

Available tools: {tools}
Tool names: {tool_names}

═════════════════════════════════════════════════════════════════════════════
⚠️ CRITICAL - YOU MUST EMIT THINKING TEXT IN EVERY LLM CALL BEFORE TOOLS ⚠️
═════════════════════════════════════════════════════════════════════════════

Question: {input}

MANDATORY FORMAT - EVERY RESPONSE MUST START WITH THIS EXACT STRUCTURE:

Thought: [REQUIRED -"Im a Mandate Parsing Agent" + Your reasoning. What do you need to do?]
Analysis: [REQUIRED - Your strategy and approach]
Action: [REQUIRED - The tool name to call]

THEN the tool executes.

═════════════════════════════════════════════════════════════════════════════
NON-NEGOTIABLE RULES:
═════════════════════════════════════════════════════════════════════════════

1️⃣ YOUR FIRST LINE MUST START WITH "Thought:" - NO EXCEPTIONS
2️⃣ YOU MUST include "Analysis:" line explaining your approach
3️⃣ YOU MUST include "Action:" with tool name
4️⃣ ONLY THEN will tools be called
5️⃣ Use tools in order: scan_mandate_folder_and_parse → extract_dynamic_criteria
6️⃣ Output thinking BEFORE any tool invocation
7️⃣ Return extract_dynamic_criteria output EXACTLY as-is

EXAMPLE OF CORRECT FORMAT:
Thought:Im a Mandate Parsing Agent. I need to scan the mandate PDF first, then extract criteria from it.
Analysis: I will call scan_mandate_folder_and_parse to read the PDF, then extract_dynamic_criteria to parse the data.
Action: scan_mandate_folder_and_parse

NOW EMIT THINKING FIRST (start with "Thought:"):"""

agent_prompt = ChatPromptTemplate.from_messages([
    ("system", REACT_PROMPT),
    MessagesPlaceholder(variable_name="messages"),
])


class AgentState(TypedDict):
    messages: Annotated[list[BaseMessage], operator.add]
    pdf_name: str
    query: str
    capability_params: dict


def agent_node(state: AgentState, config=None) -> dict:
    """LLM follows REACT format with full message history.

    Two-step process:
    1. LLM WITHOUT tools → outputs thinking text (Thought/Analysis/Action)
    2. LLM WITH tools → outputs tool calls
    """
    # Use query from state with fallback
    input_text = state.get("query") or "Scan input_fund_mandate and extract criteria"

    # Include capability_params in the context if provided
    capability_info = ""
    if state.get("capability_params"):
        capability_info = f"\n\nCapability Parameters: {state['capability_params']}"

    # STEP 1: Call LLM WITHOUT tools to get thinking text
    llm_no_tools = LLM  # No bind_tools - allows plain text output
    result_no_tools = agent_prompt | llm_no_tools

    thinking_response = result_no_tools.invoke(
        {
            "tools": "\n".join([f"{t.name}: {t.description}" for t in tools]),
            "tool_names": ", ".join(t.name for t in tools),
            "input": input_text + capability_info,
            "messages": state["messages"],
        },
        config=config,
    )

    # print(f"[AGENT1] Thinking response type: {type(thinking_response)}")
    # if hasattr(thinking_response, 'content'):
    #     print(f"[AGENT1] Thinking content: {str(thinking_response.content)[:200]}")

    # STEP 2: Call LLM WITH tools to get tool calls
    # print("\n[AGENT1] STEP 2: Getting tool calls (with tools)...")
    llm_with_tools = LLM.bind_tools(tools)
    result_with_tools = agent_prompt | llm_with_tools

    tool_response = result_with_tools.invoke(
        {
            "tools": "\n".join([f"{t.name}: {t.description}" for t in tools]),
            "tool_names": ", ".join(t.name for t in tools),
            "input": input_text + capability_info,
            "messages": state["messages"],
        },
        config=config,
    )

    print(f"[AGENT1] Tool response type: {type(tool_response)}")
    if hasattr(tool_response, 'tool_calls'):
        print(f"[AGENT1] Tool calls: {len(tool_response.tool_calls) if tool_response.tool_calls else 0}")

    # Return the tool response (which triggers tool execution in the graph)
    return {"messages": [tool_response]}


# Build graph (unchanged)
workflow = StateGraph(AgentState)
workflow.add_node("agent", agent_node)
tool_node = ToolNode(tools)
workflow.add_node("tools", tool_node)
workflow.add_edge(START, "agent")
workflow.add_conditional_edges("agent", tools_condition)
workflow.add_edge("tools", "agent")
graph = workflow.compile()  # NO checkpointer - fresh state on each invoke

if __name__ == "__main__":
    # Test with pdf_name, query, and capability_params
    user_input = {
        "messages": [HumanMessage(content="Scan input_fund_mandate and extract criteria")],
        "pdf_name": "fund_mandate.pdf",
        "query": "Scan input_fund_mandate and extract criteria",
        "capability_params": {}
    }
    result = graph.invoke(user_input)

    # Final JSON in last message
    final_msg = result["messages"][-1]
    print("MANDATE JSON:", final_msg.content)