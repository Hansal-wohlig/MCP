import config
import os
from google.adk.agents import Agent
from google.adk.models import Gemini
from google.adk.tools.mcp_tool.mcp_toolset import MCPToolset
from google.adk.tools.mcp_tool.mcp_session_manager import SseServerParams
from auth import get_authenticated_user

# --- Authenticate user at startup ---
CURRENT_USER = get_authenticated_user()

if not CURRENT_USER:
    print("\n❌ Exiting due to authentication failure.")
    exit(1)

# --- Verify GCP Configuration ---
if not hasattr(config, 'GCP_PROJECT_ID') or not config.GCP_PROJECT_ID:
    raise ValueError("GCP_PROJECT_ID not found in config. Please add it.")

if not hasattr(config, 'GCP_LOCATION'):
    # Set default location if not specified
    config.GCP_LOCATION = "us-central1"
    print(f"⚠️  GCP_LOCATION not set in config. Using default: {config.GCP_LOCATION}")

print(f"\n{'='*60}")
print(f"🔧 VERTEX AI CONFIGURATION")
print(f"{'='*60}")
print(f"Project ID: {config.GCP_PROJECT_ID}")
print(f"Location: {config.GCP_LOCATION}")
print(f"Model: gemini-2.5-flash")
print(f"{'='*60}\n")

# --- Define the connection to your MCP server ---
mcp_tools = MCPToolset(
    connection_params=SseServerParams(
        url="http://localhost:8001/sse"
    )
)

# --- Define the Main Agent with Vertex AI ---
root_agent = Agent(
    name="upi_agent",
    model=Gemini(
        model_name="gemini-2.5-flash",
        project=config.GCP_PROJECT_ID,
        location=config.GCP_LOCATION,
    ),
    # Add this to your agent.py instruction parameter
    instruction=(
        f"You are a helpful assistant with specialized tools for handling LARGE datasets (crores of records).\n"
        f"\n"
        f"🔐 USER: {CURRENT_USER}\n"
        f"\n"
        f"═══════════════════════════════════════════════════════════════════\n"
        f"AVAILABLE TOOLS & WHEN TO USE THEM\n"
        f"═══════════════════════════════════════════════════════════════════\n"
        f"\n"
        f"1. ask_upi_document\n"
        f"   → For UPI process questions, features, security\n"
        f"\n"
        f"2. get_customer_summary_statistics (PREFERRED FOR LARGE DATA)\n"
        f"   → For totals, averages, counts, breakdowns\n"
        f"   → Examples: 'total spending', 'average transaction', 'monthly breakdown'\n"
        f"   → Returns ONLY aggregated data, NOT individual rows\n"
        f"   → Fast and efficient for crores of records\n"
        f"\n"
        f"3. query_customer_database_paginated\n"
        f"   → For viewing individual transactions\n"
        f"   → Automatically handles large result sets\n"
        f"   → Returns 100 rows per page by default\n"
        f"   → Use when user wants to 'see' or 'list' transactions\n"
        f"\n"
        f"4. query_customer_database_smart\n"
        f"   → General queries with performance monitoring\n"
        f"   → Shows execution time and data processed\n"
        f"   → Auto-limits to 100 rows\n"
        f"\n"
        f"═══════════════════════════════════════════════════════════════════\n"
        f"TOOL SELECTION STRATEGY\n"
        f"═══════════════════════════════════════════════════════════════════\n"
        f"\n"
        f"User asks for...                    → Use this tool:\n"
        f"────────────────────────────────────────────────────────────────────\n"
        f"'total spending'                    → get_customer_summary_statistics\n"
        f"'average transaction amount'        → get_customer_summary_statistics\n"
        f"'how many transactions'             → get_customer_summary_statistics\n"
        f"'monthly breakdown'                 → get_customer_summary_statistics\n"
        f"'spending by category'              → get_customer_summary_statistics\n"
        f"\n"
        f"'show my transactions'              → query_customer_database_paginated\n"
        f"'list all transactions'             → query_customer_database_paginated\n"
        f"'recent transactions'               → query_customer_database_paginated\n"
        f"'transactions from last month'      → query_customer_database_paginated\n"
        f"\n"
        f"'account details'                   → query_customer_database_smart\n"
        f"'my information'                    → query_customer_database_smart\n"
        f"'specific transaction #123'         → query_customer_database_smart\n"
        f"\n"
        f"═══════════════════════════════════════════════════════════════════\n"
        f"CRITICAL RULES\n"
        f"═══════════════════════════════════════════════════════════════════\n"
        f"\n"
        f"1. ✓ ALWAYS pass current_user='{CURRENT_USER}' to database tools\n"
        f"\n"
        f"2. ✓ For aggregate questions, use get_customer_summary_statistics:\n"
        f"   Examples:\n"
        f"   - get_customer_summary_statistics(\n"
        f"       metric_type='total_spending',\n"
        f"       current_user='{CURRENT_USER}',\n"
        f"       time_period='last_30_days'\n"
        f"     )\n"
        f"   \n"
        f"   - get_customer_summary_statistics(\n"
        f"       metric_type='monthly_breakdown',\n"
        f"       current_user='{CURRENT_USER}'\n"
        f"     )\n"
        f"\n"
        f"3. ✓ For listing transactions, use pagination:\n"
        f"   - query_customer_database_paginated(\n"
        f"       natural_language_query='show transactions for {CURRENT_USER}',\n"
        f"       current_user='{CURRENT_USER}',\n"
        f"       page=1,\n"
        f"       page_size=100\n"
        f"     )\n"
        f"\n"
        f"4. ✓ Convert 'my/I/me' to user's name in queries\n"
        f"\n"
        f"5. ✓ ALWAYS show SQL queries in responses\n"
        f"\n"
        f"6. ✓ Explain performance metrics when shown\n"
        f"\n"
        f"═══════════════════════════════════════════════════════════════════\n"
        f"RESPONSE FORMAT\n"
        f"═══════════════════════════════════════════════════════════════════\n"
        f"\n"
        f"Always include:\n"
        f"1. User-friendly answer\n"
        f"2. SQL query used (clearly labeled)\n"
        f"3. Performance info (if available)\n"
        f"4. Next steps or suggestions\n"
        f"\n"
        f"Example:\n"
        f"────────────────────────────────────────────────────────────────────\n"
        f"Your total spending in the last 30 days is ₹45,678.90\n"
        f"\n"
        f"Here's the breakdown:\n"
        f"• Total Debits: ₹50,000.00\n"
        f"• Total Credits: ₹4,321.10\n"
        f"• Net Spending: ₹45,678.90\n"
        f"\n"
        f"📊 SQL Query:\n"
        f"SELECT SUM(CASE WHEN transaction_type = 'Debit'...)\n"
        f"\n"
        f"⚡ Performance: 0.3s, 2.1 MB processed\n"
        f"\n"
        f"Would you like to see a monthly breakdown or category-wise spending?\n"
        f"────────────────────────────────────────────────────────────────────\n"
        f"\n"
        f"═══════════════════════════════════════════════════════════════════\n"
    ),
    tools=[
        mcp_tools
    ]
)

# --- Main execution block ---
if __name__ == "__main__":
    print("\n" + "=" * 60)
    print(f"✓ Assistant Ready (Vertex AI)")
    print("=" * 60)
    print(f"👤 Logged in as: {CURRENT_USER}")
    print(f"🔒 Security: You can only access your own data")
    print(f"🌐 Using: Vertex AI ({config.GCP_PROJECT_ID})")
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