import config # Ensures env vars are loaded
import os
from google.adk.agents import Agent
from google.adk.models import Gemini
# --- Import the MCP tool classes ---
from google.adk.tools.mcp_tool.mcp_toolset import MCPToolset
from google.adk.tools.mcp_tool.mcp_session_manager import SseServerParams

# --- Configure the Model for Google ---
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY not found in .env file. Please add it.")

# --- Define the connection to your MCP server ---
mcp_tools = MCPToolset(
    connection_params=SseServerParams(
        url="http://localhost:8001/sse"
    )
)

# --- Define the Main Agent ---
root_agent = Agent(
    name="upi_agent",
    model=Gemini(
        model_name="gemini-1.5-pro-latest",
        api_key=GOOGLE_API_KEY
    ),
    instruction=(
        "You are a friendly and helpful assistant with access to two tools: "
        "1. `ask_upi_document`: For UPI process, features, security, or history questions. "
        "2. `query_customer_database`: For customer transactions, accounts, or calculations. "
        "\n\n"
        "CRITICAL: Always use FULL chat history for context. When a user asks follow-ups (e.g., 'average amount' after 'show Tony Toy's transactions'), "
        "formulate a *standalone, contextualized query* for the tool (e.g., 'average transaction amount for Tony Toy'). "
        "Do NOT send bare queries—include names/dates/filters from prior messages to avoid wrong results. "
        "If unclear, ask for clarification before tool call."
        "\n\n"
        "When presenting data results, be clear and conversational. For tables, present them in a readable format. "
        "For single values or simple answers, provide context and explanation."
    ),
    tools=[
        mcp_tools
    ]
)

# --- Main execution block ---
if __name__ == "__main__":
    print("=" * 60)
    print("Assistant Ready")
    print("=" * 60)
    print("\nType 'quit' or 'exit' to stop.\n")
    
    # Interactive loop
    while True:
        try:
            user_input = input("You: ").strip()
            
            if user_input.lower() in ['quit', 'exit', 'q']:
                print("\nGoodbye!")
                break
            
            if not user_input:
                continue
            
            print("\nAssistant: ", end="", flush=True)
            
            # Run the agent with the user's query
            response = root_agent.run(user_input)
            
            # Print the response
            print(response)
            print()  # Empty line for better readability
            
        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break
        except Exception as e:
            print(f"\nError: {str(e)}")
            print("Please try again or type 'quit' to exit.\n")