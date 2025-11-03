import config
import uvicorn
import pandas as pd
import re
from google.cloud import bigquery
from fastmcp import FastMCP
from langchain_google_vertexai import ChatVertexAI, VertexAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_core.prompts import ChatPromptTemplate
from typing import Optional, Tuple

# Import utilities
from schema_utils import (
    fetch_bigquery_schema, 
    format_schema_for_llm,
    generate_all_table_contexts
)

from table_context import ACCESS_CONTROL

# --- 1. Initialize All Shared Resources ---
print("\n" + "="*60)
print("🚀 INITIALIZING MCP TOOLBOX SERVER (VERTEX AI)")
print("="*60)

# Verify GCP Configuration
if not hasattr(config, 'GCP_PROJECT_ID') or not config.GCP_PROJECT_ID:
    raise ValueError("GCP_PROJECT_ID not found in config. Please add it.")

if not hasattr(config, 'GCP_LOCATION'):
    config.GCP_LOCATION = "us-central1"
    print(f"⚠️  GCP_LOCATION not set in config. Using default: {config.GCP_LOCATION}")

print(f"\n📍 Vertex AI Configuration:")
print(f"   Project: {config.GCP_PROJECT_ID}")
print(f"   Location: {config.GCP_LOCATION}")

print("\n[1/5] Initializing AI models with Vertex AI...")
llm = ChatVertexAI(
    model_name="gemini-1.5-pro",  # Available models:
                                   # - gemini-2.0-flash-exp (fast, cost-effective)
                                   # - gemini-1.5-pro (balanced)
                                   # - gemini-1.5-flash (fastest)
    project=config.GCP_PROJECT_ID,
    location=config.GCP_LOCATION,
    temperature=0,
)

embeddings = VertexAIEmbeddings(
    model_name="text-embedding-004",  # Vertex AI embedding model
    project=config.GCP_PROJECT_ID,
    location=config.GCP_LOCATION,
)
print("✓ LLM and Embeddings initialized (Vertex AI)")

print("\n[2/5] Connecting to BigQuery...")
bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
print(f"✓ Connected to project: {config.GCP_PROJECT_ID}")

# --- Fetch Dynamic Schema ---
print("\n[3/5] Fetching database schema from BigQuery...")
try:
    schema_info = fetch_bigquery_schema(bq_client, config.BIGQUERY_DATASET)
    print(f"✓ Schema fetched for {len(schema_info)} tables")
except Exception as e:
    print(f"❌ ERROR: Could not fetch schema: {str(e)}")
    raise

# --- Generate Table Contexts with Gemini via Vertex AI ---
print("\n[4/5] Generating intelligent table contexts using Gemini (Vertex AI)...")
try:
    table_contexts = generate_all_table_contexts(schema_info, llm)
    formatted_schema = format_schema_for_llm(schema_info, table_contexts)
    
    print("\n" + "="*60)
    print("📚 GENERATED SCHEMA WITH CONTEXTS:")
    print("="*60)
    print(formatted_schema)
    print("="*60 + "\n")
    
except Exception as e:
    print(f"⚠️  Warning: Could not generate contexts with Gemini: {str(e)}")
    print("Falling back to basic schema...")
    table_contexts = {}
    formatted_schema = format_schema_for_llm(schema_info, table_contexts)

# --- Load Vector Store ---
print("[5/5] Loading vector store for PDF queries...")
try:
    vector_store = FAISS.load_local(
        config.VECTOR_STORE_PATH,
        embeddings,
        allow_dangerous_deserialization=True
    )
    print(f"✓ Vector store loaded from {config.VECTOR_STORE_PATH}")
except Exception as e:
    print(f"❌ FATAL: Could not load vector store from {config.VECTOR_STORE_PATH}")
    print(f"Error: {str(e)}")
    print("Please run 'python -m agent.pdf_indexer' first to create the vector store.")
    raise

print("\n" + "="*60)
print("✅ ALL RESOURCES INITIALIZED SUCCESSFULLY (VERTEX AI)")
print("="*60 + "\n")

# --- 2. CREATE FastMCP INSTANCE ---
mcp = FastMCP("MyToolboxServer")

# --- 3. Define PDF Tool Logic ---
pdf_prompt = ChatPromptTemplate.from_messages([
    ("system",
    """
    You are a helpful assistant that answers questions about UPI (Unified Payments Interface).
    Use the provided context to answer the user's question. 
    Your answer should be based SOLELY on the context provided.
    If the context does not contain the answer, say that you cannot find the information in the document.
    
    Context:
    {context}
    """),
    ("human", "{question}")
])
pdf_generation_chain = pdf_prompt | llm

@mcp.tool
def ask_upi_document(question: str) -> str:
    """
    Answers questions about the UPI (Unified Payments Interface) process
    by searching a dedicated PDF document. Use this for questions about
    how UPI works, its features, security, limits, or history.
    """
    print(f"[PDF Tool] Received query: {question}")
    
    docs = vector_store.similarity_search(question, k=3)
    if not docs:
        return "I couldn't find any relevant information in the document to answer that question."
        
    context = "\n\n".join([f"Context {i+1}:\n{doc.page_content}" for i, doc in enumerate(docs)])
    
    response = pdf_generation_chain.invoke({
        "context": context,
        "question": question
    })
    
    return response.content

# --- 4. Security Validation Functions ---

def extract_customer_names_from_sql(sql_query: str) -> list[str]:
    """Extract customer names from SQL WHERE clauses."""
    pattern = r"customer_name\s*=\s*'([^']+)'"
    matches = re.findall(pattern, sql_query, re.IGNORECASE)
    return matches

def validate_sql_access(sql_query: str, current_user: str) -> Tuple[bool, str]:
    """
    Validate that the SQL query only accesses the current user's data.
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not current_user:
        return False, "🚫 Authentication required to access database."
    
    # Extract customer names from the query
    referenced_customers = extract_customer_names_from_sql(sql_query)
    
    # Check if query references other customers
    for customer in referenced_customers:
        if customer != current_user:
            return False, (
                f"🚫 SECURITY VIOLATION DETECTED!\n"
                f"   Attempted to access: '{customer}'\n"
                f"   You are authenticated as: '{current_user}'\n"
                f"   You can only access your own data.\n"
                f"   This incident has been logged."
            )
    
    sql_upper = sql_query.upper()
    
    # Check for queries without WHERE clause on restricted tables
    if ("FROM CUSTOMERS" in sql_upper or "FROM TRANSACTIONS" in sql_upper or 
        f"FROM {config.BIGQUERY_DATASET.upper()}.CUSTOMERS" in sql_upper or
        f"FROM {config.BIGQUERY_DATASET.upper()}.TRANSACTIONS" in sql_upper):
        if "WHERE" not in sql_upper:
            return False, (
                f"🚫 SECURITY VIOLATION: Query attempts to access all records without filtering.\n"
                f"   You can only access your own data (authenticated as: '{current_user}')."
            )
    
    return True, ""

def _check_access_permission(natural_language_query: str, current_user: str) -> Tuple[bool, str]:
    """
    Check if the natural language query is trying to access unauthorized data.
    Focus only on obvious 'all users' patterns. Let SQL-level validation catch specific user attempts.
    """
    if not current_user:
        return False, "🚫 Authentication required to access database."
    
    query_lower = natural_language_query.lower()
    
    # Only block obvious attempts to get ALL users' data
    if any(pattern in query_lower for pattern in [
        "all customers",
        "all users",
        "every customer", 
        "list all customers",
        "show all customers",
        "every user"
    ]):
        return False, (
            f"🚫 Access Denied: You can only access your own data.\n"
            f"   You are authenticated as '{current_user}'.\n"
            f"   Try asking: 'my transactions' or 'my spending'"
        )
    
    # Allow everything else - SQL validation will catch unauthorized access
    return True, ""

# --- 5. Define BigQuery Tool Logic with Security ---

sql_prompt = ChatPromptTemplate.from_messages([
    ("system",
    f"""
    You are a BigQuery SQL expert. Given a user question and the database schema, 
    write a valid **BigQuery** SQL query to answer it.
    Only output the SQL query and nothing else - no explanations, no markdown formatting.
    Ensure tables are referenced as `{config.BIGQUERY_DATASET}.table_name`.
    
    **CRITICAL SECURITY RULES**:
    - Current user context: {{current_user}}
    - You MUST add row-level security filters to ALL queries
    - ALWAYS include: WHERE customer_name = {{current_user}}
    - For transactions, JOIN with customers and filter: WHERE c.customer_name = {{current_user}}
    - NEVER generate queries that access all customers or other users' data
    - If query asks for data about another user, respond with: "ACCESS_DENIED"
    
    **SQL Rules**:
    - Always use single quotes for string literals, never double quotes
    - BigQuery is case-sensitive for string comparisons
    - Handle NULL values appropriately using IS NULL or IS NOT NULL
    
    **Query Patterns with Security**:
    - "my transactions" → 
      SELECT t.* FROM {config.BIGQUERY_DATASET}.transactions t 
      JOIN {config.BIGQUERY_DATASET}.customers c ON t.customer_id = c.customer_id 
      WHERE c.customer_name = {{current_user}}
    
    - "my account details" → 
      SELECT * FROM {config.BIGQUERY_DATASET}.customers 
      WHERE customer_name = {{current_user}}
    
    - "my average transaction amount" → 
      SELECT AVG(t.transaction_amount) FROM {config.BIGQUERY_DATASET}.transactions t 
      JOIN {config.BIGQUERY_DATASET}.customers c ON t.customer_id = c.customer_id 
      WHERE c.customer_name = {{current_user}}
    
    Database schema:
    {formatted_schema}
    """),
    ("human", "{question}")
])
sql_generation_chain = sql_prompt | llm

summary_prompt = ChatPromptTemplate.from_messages([
    ("system", """
    You are a helpful data assistant. Provide a concise, conversational answer 
    based on the user's question and the summary of the query result.
    1. If the result is a single value, answer directly with context.
    2. If there are no results or an error, state that clearly.
    """),
    ("human", "User Question: {question}\nQuery Result Summary: {result_summary}")
])
summary_chain = summary_prompt | llm

def _execute_query(sql_query: str, current_user: Optional[str] = None) -> Tuple[str, pd.DataFrame | None]:
    """Execute SQL query with security validation."""
    
    # Validate SQL access
    if current_user:
        is_valid, error_msg = validate_sql_access(sql_query, current_user)
        if not is_valid:
            print(f"[SECURITY BLOCKED] User: {current_user} | Query: {sql_query[:100]}")
            return error_msg, None
    
    try:
        clean_sql = sql_query.strip().replace("```sql", "").replace("```", "")
        print(f"--- Executing SQL for user '{current_user}': ---\n{clean_sql}\n" + "-"*50)
        
        query_job = bq_client.query(clean_sql)
        results = query_job.to_dataframe()
        
        # Post-execution validation
        if current_user and not results.empty:
            if 'customer_name' in results.columns:
                unauthorized_names = results[results['customer_name'] != current_user]['customer_name'].unique()
                if len(unauthorized_names) > 0:
                    print(f"[SECURITY] Post-execution check FAILED: Found data for {unauthorized_names}")
                    return (
                        f"🚫 Security validation failed: Query returned unauthorized data.\n"
                        f"   This incident has been logged."
                    ), None
        
        if results.empty:
            return "The query executed successfully but returned no results.", None
        
        return results.to_string(index=False), results
        
    except Exception as e:
        print(f"ERROR DETAILS: {str(e)}")
        return f"An error occurred while executing the BigQuery query: {e}", None

@mcp.tool
def query_customer_database(natural_language_query: str, current_user: str = None) -> str:
    """
    Answers questions about customer data, transactions, accounts, or
    financial calculations from a BigQuery database.
    
    SECURITY: Enforces row-level security - users can only access their own data.
    
    Args:
        natural_language_query: The user's question in natural language
        current_user: The authenticated user's name (REQUIRED for security)
    
    Returns:
        Query results with SQL query included
    """
    print(f"\n{'='*60}")
    print(f"[BQ Tool] Query: {natural_language_query}")
    print(f"[BQ Tool] Authenticated User: {current_user or 'NONE - DENIED'}")
    print(f"{'='*60}")
    
    # Require authentication
    if not current_user:
        return "🚫 Access Denied: Authentication required to access customer data."
    
    # Check natural language for unauthorized access attempts
    is_allowed, error_msg = _check_access_permission(natural_language_query, current_user)
    if not is_allowed:
        print(f"[SECURITY] Blocked at NL level: {natural_language_query}")
        return error_msg
    
    # Generate SQL with user context
    sql_response = sql_generation_chain.invoke({
        "question": natural_language_query,
        "current_user": f"'{current_user}'"
    })
    sql_query = sql_response.content.strip()
    
    print(f"[BQ Tool] Generated SQL: {sql_query[:150]}...")
    
    # Check for access denied in SQL generation
    if "ACCESS_DENIED" in sql_query:
        return (
            f"🚫 Access Denied: You can only query your own data.\n"
            f"   You are authenticated as '{current_user}'."
        )
    
    # Validate SQL query
    if "cannot answer" in sql_query.lower():
        return "I'm sorry, but I cannot answer that question with the available database schema."
    
    sql_upper = sql_query.upper().replace("```SQL", "").replace("```", "").strip()
    if not (sql_upper.startswith('SELECT') or sql_upper.startswith('WITH')):
        return "I encountered an issue generating a SQL query. Please try rephrasing your question."
    
    # Execute SQL with security validation
    text_result, df_result = _execute_query(sql_query, current_user)
    
    # Clean SQL for display
    clean_sql = sql_query.strip().replace("```sql", "").replace("```", "")
    
    # Execute SQL with security validation
    text_result, df_result = _execute_query(sql_query, current_user)
    
    # If execution was blocked, text_result contains the error
    if df_result is None:
        return f"[SQL QUERY]\n{clean_sql}\n\n[ERROR]\n{text_result}"
    
    # Build response with SQL at the top
    response_parts = []
    
    # SQL Section - Always at the top with clear marker
    response_parts.append("[SQL QUERY]")
    response_parts.append(clean_sql)
    response_parts.append("")
    response_parts.append("[DATA RESULTS]")
    response_parts.append("")
    
    # Data Section
    if not df_result.empty:
        if df_result.shape == (1, 1):
            # Single value
            value = df_result.iloc[0, 0]
            if isinstance(value, float):
                response_parts.append(f"Result: {value:,.2f}")
            else:
                response_parts.append(f"Result: {value}")
        else:
            # Table
            response_parts.append(f"Rows: {len(df_result)}")
            response_parts.append("")
            table_str = df_result.to_string(
                index=False,
                max_colwidth=25,
                justify='left'
            )
            response_parts.append(table_str)
    else:
        response_parts.append("No results found.")
    
    return "\n".join(response_parts)

# --- 6. Run the Server ---
if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("🚀 Starting Combined MCP Toolbox Server (VERTEX AI)")
    print("=" * 60)
    print(f"🌐 Project: {config.GCP_PROJECT_ID}")
    print(f"📍 Location: {config.GCP_LOCATION}")
    print(f"🔒 Row-Level Security: {'ENABLED' if ACCESS_CONTROL['enabled'] else 'DISABLED'}")
    print("=" * 60)
    
    mcp.run(
        transport="sse",     
        host="0.0.0.0",       
        port=8001,
        path="/sse"           
    )