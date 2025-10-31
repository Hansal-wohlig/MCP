import config
import os
from google.adk.agents import Agent
from google.adk.models import Gemini
from google.adk.tools.mcp_tool.mcp_toolset import MCPToolset
from google.adk.tools.mcp_tool.mcp_session_manager import SseServerParams
from auth import get_authenticated_user

# --- Configure the Model for Google ---
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY not found in .env file. Please add it.")

# --- Authenticate user at startup ---
CURRENT_USER = get_authenticated_user()

if not CURRENT_USER:
    print("\n❌ Exiting due to authentication failure.")
    exit(1)

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
        model_name="gemini-2.5-pro",
        api_key=GOOGLE_API_KEY,
    ),
    instruction=(
        f"You are a friendly and helpful assistant with access to two tools: "
        f"1. `ask_upi_document`: For UPI process, features, security, or history questions. "
        f"2. `query_customer_database`: For customer transactions, accounts, or calculations. "
        f"\n\n"
        f"IMPORTANT SECURITY CONTEXT:"
        f"- Current authenticated user: '{CURRENT_USER}'"
        f"- This user can ONLY access their own data"
        f"- You MUST enforce row-level security for all database queries"
        f"\n\n"
        f"WHEN CALLING query_customer_database:"
        f"- ALWAYS pass current_user='{CURRENT_USER}' as a parameter"
        f"- Convert user questions like 'my transactions' to 'transactions for {CURRENT_USER}'"
        f"- If user asks about 'all customers' or other user names, politely explain they can only see their own data"
        f"- For any database query, ensure it's scoped to '{CURRENT_USER}'"
        f"\n\n"
        f"EXAMPLES OF CORRECT TOOL CALLS:"
        f"- User asks 'show my transactions' → query_customer_database('show transactions for {CURRENT_USER}', current_user='{CURRENT_USER}')"
        f"- User asks 'what is my average transaction amount' → query_customer_database('average transaction amount for {CURRENT_USER}', current_user='{CURRENT_USER}')"
        f"- User asks 'my account details' → query_customer_database('account details for {CURRENT_USER}', current_user='{CURRENT_USER}')"
        f"\n\n"
        f"Always use FULL chat history for context. When a user asks follow-ups, formulate standalone queries."
        f"\n\n"
        f"When presenting data results, be clear and conversational."
    ),
    tools=[
        mcp_tools
    ]
)

# --- Main execution block ---
if __name__ == "__main__":
    print("\n" + "=" * 60)
    print(f"✓ Assistant Ready")
    print("=" * 60)
    print(f"👤 Logged in as: {CURRENT_USER}")
    print(f"🔒 Security: You can only access your own data")
    print("=" * 60)
    print("\nType 'quit' or 'exit' to stop.\n")
    
    # Interactive loop
    while True:
        try:
            user_input = input("You: ").strip()
            
            if user_input.lower() in ['quit', 'exit', 'q']:
                print("\n👋 Goodbye!")
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
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {str(e)}")
            print("Please try again or type 'quit' to exit.\n")