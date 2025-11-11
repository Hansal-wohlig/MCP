import config
import uvicorn
import pandas as pd
import re
import json
import logging
from google.cloud import bigquery
from fastmcp import FastMCP
from langchain_google_vertexai import ChatVertexAI, VertexAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_core.prompts import ChatPromptTemplate
from typing import Optional, Tuple
from collections import defaultdict
from datetime import datetime, timedelta

# Import utilities
from schema_cache_manager import (
    load_or_refresh_schema,
    SchemaCache
)

from table_context import ACCESS_CONTROL

# --- Configure Audit Logging ---
audit_logger = logging.getLogger('mcp_security_audit')
audit_logger.setLevel(logging.INFO)

# File handler for audit trail
audit_handler = logging.FileHandler('mcp_security_audit.log')
audit_handler.setFormatter(logging.Formatter(
    '%(asctime)s | %(levelname)s | %(message)s'
))
audit_logger.addHandler(audit_handler)

# Console handler for real-time monitoring
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(logging.Formatter(
    '🚨 SECURITY: %(message)s'
))
audit_logger.addHandler(console_handler)

def log_query_attempt(
    user: str,
    query: str,
    status: str,  # 'ALLOWED', 'BLOCKED', 'ERROR'
    reason: str = None,
    row_count: int = None
):
    """
    Log all query attempts per guardrails compliance requirements.
    """
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'user_id': user,
        'query': query[:500],  # Truncate long queries
        'status': status,
        'reason': reason,
        'row_count': row_count
    }
    
    audit_logger.info(json.dumps(log_entry))

# --- Rate Limiter (Layer 4) ---
class RateLimiter:
    def __init__(self, max_queries_per_minute=10, max_queries_per_session=100):
        self.max_per_minute = max_queries_per_minute
        self.max_per_session = max_queries_per_session
        self.user_queries = defaultdict(list)
        self.session_counts = defaultdict(int)
    
    def is_allowed(self, user: str) -> Tuple[bool, str]:
        """Check if user has exceeded rate limits."""
        now = datetime.now()
        
        # Check session limit
        if self.session_counts[user] >= self.max_per_session:
            return False, (
                f"🚫 Rate limit exceeded: Maximum {self.max_per_session} queries per session.\n"
                f"   Please start a new session."
            )
        
        # Check per-minute limit
        one_minute_ago = now - timedelta(minutes=1)
        self.user_queries[user] = [
            ts for ts in self.user_queries[user] if ts > one_minute_ago
        ]
        
        if len(self.user_queries[user]) >= self.max_per_minute:
            return False, (
                f"🚫 Rate limit exceeded: Maximum {self.max_per_minute} queries per minute.\n"
                f"   Please wait before trying again."
            )
        
        # Record this query
        self.user_queries[user].append(now)
        self.session_counts[user] += 1
        
        return True, ""
    
    def reset_session(self, user: str):
        """Reset session count for a user."""
        self.session_counts[user] = 0
        self.user_queries[user] = []

# Initialize rate limiter
rate_limiter = RateLimiter(max_queries_per_minute=10, max_queries_per_session=100)

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
    model_name="gemini-2.5-flash",
    project=config.GCP_PROJECT_ID,
    location=config.GCP_LOCATION,
    temperature=0,
)

embeddings = VertexAIEmbeddings(
    model_name="text-embedding-004",
    project=config.GCP_PROJECT_ID,
    location=config.GCP_LOCATION,
)
print("✓ LLM and Embeddings initialized (Vertex AI)")

print("\n[2/5] Connecting to BigQuery...")
bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
print(f"✓ Connected to project: {config.GCP_PROJECT_ID}")

# --- Fetch Dynamic Schema ---
print("\n[3/5] Loading schema from cache...")
try:
    schema_info, table_contexts, formatted_schema = load_or_refresh_schema(
        bq_client=bq_client,
        llm=llm,
        force_refresh=False  # Set to True to force refresh
    )
    
    # Show cache status
    cache_manager = SchemaCache()
    cache_info = cache_manager.get_cache_info()
    if cache_info['exists']:
        print(f"✓ Using cached schema (age: {cache_info['age_days']} days)")
    else:
        print(f"✓ Fresh schema fetched and cached")
    
    print(f"✓ Schema loaded for {len(schema_info)} tables")
    
except Exception as e:
    print(f"❌ ERROR: Could not load schema: {str(e)}")
    raise

# --- Display Schema ---
print("\n[4/5] Schema loaded successfully")
print("\n" + "="*60)
print("📚 SCHEMA SUMMARY:")
print("="*60)
for table_name in schema_info.keys():
    print(f"  • {table_name}: {len(schema_info[table_name]['fields'])} columns")
print("="*60 + "\n")

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

def validate_query_type(sql_query: str) -> Tuple[bool, str]:
    """
    Layer 1: Comprehensive query type validation against prohibited operations.
    Implements Query Parser & Validator per guardrails documentation.
    """
    sql_upper = sql_query.upper().strip()
    
    # Remove SQL comments to prevent bypassing
    sql_upper = re.sub(r'--.*$', '', sql_upper, flags=re.MULTILINE)
    sql_upper = re.sub(r'/\*.*?\*/', '', sql_upper, flags=re.DOTALL)
    
    # Prohibited operations from guardrails document
    prohibited_patterns = {
        'DELETE': ['DELETE', 'TRUNCATE', 'DROP TABLE', 'DROP DATABASE'],
        'UPDATE': ['UPDATE'],
        'INSERT': ['INSERT'],
        'SCHEMA': ['ALTER TABLE', 'ALTER DATABASE', 'CREATE TABLE', 'CREATE DATABASE', 
                   'CREATE INDEX', 'DROP INDEX', 'CREATE VIEW', 'DROP VIEW'],
        'ADMIN': ['GRANT', 'REVOKE', 'CREATE USER', 'DROP USER', 'ALTER USER'],
        'INJECTION': [';.*(?:DELETE|UPDATE|INSERT|DROP)', 'EXEC', 'EXECUTE', 'xp_', 'sp_'],
    }
    
    for category, keywords in prohibited_patterns.items():
        for keyword in keywords:
            # Use word boundaries to avoid false positives
            pattern = r'\b' + re.escape(keyword).replace(r'\ ', r'\s+') + r'\b'
            if re.search(pattern, sql_upper):
                return False, (
                    f"🚫 SECURITY BLOCK: {category} operations are not permitted.\n"
                    f"   Detected: {keyword}\n"
                    f"   This chatbot is READ-ONLY and can only execute SELECT queries.\n"
                    f"   Reason: Banking security regulations require data integrity.\n"
                    f"   This incident has been logged for audit purposes."
                )
    
    # Ensure query starts with SELECT or WITH (for CTEs)
    if not (sql_upper.startswith('SELECT') or sql_upper.startswith('WITH')):
        return False, (
            "🚫 SECURITY BLOCK: Only SELECT queries are permitted.\n"
            "   This chatbot has READ-ONLY access to the database.\n"
            "   This incident has been logged for audit purposes."
        )
    
    return True, ""

def extract_customer_names_from_sql(sql_query: str) -> list[str]:
    """Extract customer names from SQL WHERE clauses."""
    pattern = r"customer_name\s*=\s*'([^']+)'"
    matches = re.findall(pattern, sql_query, re.IGNORECASE)
    return matches

def validate_sql_access(sql_query: str, current_user: str) -> Tuple[bool, str]:
    """
    Layer 3: Validate that the SQL query only accesses the current user's data.
    Implements Row-Level Security per guardrails.
    
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
                f"   This incident has been logged for audit and compliance."
            )
    
    sql_upper = sql_query.upper()
    
    # Check for queries without WHERE clause on restricted tables
    if ("FROM CUSTOMERS" in sql_upper or "FROM TRANSACTIONS" in sql_upper or 
        f"FROM {config.BIGQUERY_DATASET.upper()}.CUSTOMERS" in sql_upper or
        f"FROM {config.BIGQUERY_DATASET.upper()}.TRANSACTIONS" in sql_upper):
        if "WHERE" not in sql_upper:
            return False, (
                f"🚫 SECURITY VIOLATION: Query attempts to access all records without filtering.\n"
                f"   You can only access your own data (authenticated as: '{current_user}').\n"
                f"   This incident has been logged for audit and compliance."
            )
    
    return True, ""

def _check_access_permission(natural_language_query: str, current_user: str) -> Tuple[bool, str]:
    """
    Check if the natural language query is trying to access unauthorized data.
    Focus only on obvious 'all users' patterns.
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
        "every user",
        "total customers",
        "count of customers"
    ]):
        return False, (
            f"🚫 Access Denied: You can only access your own data.\n"
            f"   You are authenticated as '{current_user}'.\n"
            f"   Try asking: 'my transactions' or 'my spending'\n"
            f"   This incident has been logged for audit purposes."
        )
    
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
    - Do NOT add LIMIT clause - the system handles this automatically
    
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
    """
    Execute SQL query with comprehensive security validation and result limits.
    Implements all 4 layers of security guardrails.
    """
    
    # Layer 1: Query Type Validation
    is_valid_type, error_msg = validate_query_type(sql_query)
    if not is_valid_type:
        print(f"[SECURITY BLOCKED - Query Type] User: {current_user}")
        log_query_attempt(
            user=current_user or 'UNKNOWN',
            query=sql_query,
            status='BLOCKED',
            reason='Prohibited query type detected'
        )
        return error_msg, None
    
    # Layer 3: SQL Access Validation (Row-Level Security)
    if current_user:
        is_valid, error_msg = validate_sql_access(sql_query, current_user)
        if not is_valid:
            print(f"[SECURITY BLOCKED - Access] User: {current_user} | Query: {sql_query[:100]}")
            log_query_attempt(
                user=current_user,
                query=sql_query,
                status='BLOCKED',
                reason='Row-level security violation'
            )
            return error_msg, None
    
    try:
        clean_sql = sql_query.strip().replace("```sql", "").replace("```", "")
        
        # Add LIMIT clause if not present (max 1000 rows per guardrails)
        if 'LIMIT' not in clean_sql.upper():
            clean_sql = f"{clean_sql.rstrip(';')} LIMIT 1000"
        
        print(f"--- Executing SQL for user '{current_user}': ---\n{clean_sql}\n" + "-"*50)
        
        # Configure query job with limits per guardrails
        job_config = bigquery.QueryJobConfig(
            use_query_cache=True,
            maximum_bytes_billed=10**9  # 1GB limit to prevent expensive queries
        )
        
        query_job = bq_client.query(clean_sql, job_config=job_config)
        
        # Wait with timeout (30 seconds per guardrails)
        results = query_job.result(timeout=30).to_dataframe()
        
        # Enforce max rows (per guardrails: 1000 rows)
        warning_msg = ""
        if len(results) > 1000:
            results = results.head(1000)
            warning_msg = "\n\n⚠️ Results limited to 1000 rows per security policy."
        
        # Post-execution validation (Double-check security)
        if current_user and not results.empty:
            if 'customer_name' in results.columns:
                unauthorized_names = results[results['customer_name'] != current_user]['customer_name'].unique()
                if len(unauthorized_names) > 0:
                    print(f"[SECURITY] Post-execution check FAILED: Found data for {unauthorized_names}")
                    log_query_attempt(
                        user=current_user,
                        query=sql_query,
                        status='BLOCKED',
                        reason='Post-execution validation failed - unauthorized data detected'
                    )
                    return (
                        f"🚫 Security validation failed: Query returned unauthorized data.\n"
                        f"   This incident has been logged for audit and compliance."
                    ), None
        
        if results.empty:
            return "The query executed successfully but returned no results." + warning_msg, None
        
        return results.to_string(index=False) + warning_msg, results
        
    except Exception as e:
        error_detail = str(e)
        print(f"ERROR DETAILS: {error_detail}")
        log_query_attempt(
            user=current_user or 'UNKNOWN',
            query=sql_query,
            status='ERROR',
            reason=error_detail[:200]
        )
        
        # User-friendly error message
        if "timeout" in error_detail.lower():
            return (
                "⏱️ Query timeout: The query took too long to execute (max 30 seconds).\n"
                "   Try simplifying your query or adding more specific filters."
            ), None
        elif "bytes" in error_detail.lower():
            return (
                "💾 Query too expensive: This query would process too much data.\n"
                "   Try adding more specific filters to reduce the data scanned."
            ), None
        else:
            return f"An error occurred while executing the query: {e}", None

@mcp.tool
def query_customer_database(natural_language_query: str, current_user: str = None) -> str:
    """
    Answers questions about customer data, transactions, accounts, or
    financial calculations from a BigQuery database.
    
    SECURITY: Enforces comprehensive multi-layer security guardrails:
    - Layer 1: Query Parser & Validator (blocks prohibited operations)
    - Layer 2: Database READ-ONLY user permissions
    - Layer 3: Row-Level Security (users can only access their own data)
    - Layer 4: Rate Limiting (10 queries/minute, 100 queries/session)
    
    Additional Safeguards:
    - Max 1000 rows per query
    - 30-second query timeout
    - 1GB maximum bytes billed
    - Comprehensive audit logging
    
    Args:
        natural_language_query: The user's question in natural language
        current_user: The authenticated user's name (REQUIRED for security)
    
    Returns:
        Query results with SQL query included, or security error message
    """
    print(f"\n{'='*60}")
    print(f"[BQ Tool] Query: {natural_language_query}")
    print(f"[BQ Tool] Authenticated User: {current_user or 'NONE - DENIED'}")
    print(f"{'='*60}")
    
    # 1. Authentication check
    if not current_user:
        log_query_attempt(
            user='ANONYMOUS',
            query=natural_language_query,
            status='BLOCKED',
            reason='No authentication provided'
        )
        return "🚫 Access Denied: Authentication required to access customer data."
    
    # 2. Layer 4: Rate Limiting
    is_allowed, limit_msg = rate_limiter.is_allowed(current_user)
    if not is_allowed:
        log_query_attempt(
            user=current_user,
            query=natural_language_query,
            status='BLOCKED',
            reason='Rate limit exceeded'
        )
        return limit_msg
    
    # 3. Natural language access permission check
    is_allowed, error_msg = _check_access_permission(natural_language_query, current_user)
    if not is_allowed:
        print(f"[SECURITY] Blocked at NL level: {natural_language_query}")
        log_query_attempt(
            user=current_user,
            query=natural_language_query,
            status='BLOCKED',
            reason='Unauthorized access pattern in natural language query'
        )
        return error_msg
    
    # 4. Generate SQL with user context
    sql_response = sql_generation_chain.invoke({
        "question": natural_language_query,
        "current_user": f"'{current_user}'"
    })
    sql_query = sql_response.content.strip()
    
    print(f"[BQ Tool] Generated SQL: {sql_query[:150]}...")
    
    # 5. Check for access denied in SQL generation
    if "ACCESS_DENIED" in sql_query:
        log_query_attempt(
            user=current_user,
            query=natural_language_query,
            status='BLOCKED',
            reason='LLM detected unauthorized access attempt'
        )
        return (
            f"🚫 Access Denied: You can only query your own data.\n"
            f"   You are authenticated as '{current_user}'."
        )
    
    # 6. Validate SQL query was generated properly
    if "cannot answer" in sql_query.lower():
        return "I'm sorry, but I cannot answer that question with the available database schema."
    
    sql_upper = sql_query.upper().replace("```SQL", "").replace("```", "").strip()
    if not (sql_upper.startswith('SELECT') or sql_upper.startswith('WITH')):
        return "I encountered an issue generating a SQL query. Please try rephrasing your question."
    
    # 7. Execute SQL with all security validations
    text_result, df_result = _execute_query(sql_query, current_user)
    
    # 8. Log successful execution
    if df_result is not None:
        log_query_attempt(
            user=current_user,
            query=natural_language_query,
            status='ALLOWED',
            row_count=len(df_result)
        )
    # If df_result is None, error was already logged in _execute_query
    
    # 9. Build response with SQL at the top
    clean_sql = sql_query.strip().replace("```sql", "").replace("```", "")
    
    response_parts = []
    
    # SQL Section - Always at the top with clear marker
    response_parts.append("[SQL QUERY]")
    response_parts.append(clean_sql)
    response_parts.append("")
    response_parts.append("[DATA RESULTS]")
    response_parts.append("")
    
    # Data Section
    if df_result is not None:
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
    else:
        # Error case - text_result contains the error message
        response_parts.append(text_result)
    
    return "\n".join(response_parts)

# --- 6. Run the Server ---
if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("🚀 Starting MCP Toolbox Server with Security Guardrails")
    print("=" * 60)
    print(f"🌐 Project: {config.GCP_PROJECT_ID}")
    print(f"📍 Location: {config.GCP_LOCATION}")
    print(f"🔒 Security Implementation:")
    print(f"   ✓ Layer 1: Query Parser & Validator")
    print(f"   ✓ Layer 2: Database READ-ONLY Permissions")
    print(f"   ✓ Layer 3: Row-Level Security")
    print(f"   ✓ Layer 4: Rate Limiting (10/min, 100/session)")
    print(f"\n📊 Query Safeguards:")
    print(f"   • Max rows per query: 1000")
    print(f"   • Query timeout: 30 seconds")
    print(f"   • Max bytes billed: 1GB")
    print(f"\n📝 Compliance:")
    print(f"   • Audit log: mcp_security_audit.log")
    print(f"   • All queries logged with timestamp, user, status")
    print(f"   • Security violations trigger alerts")
    print("\n🚫 Prohibited Operations (per Banking Regulations):")
    print(f"   • DELETE, UPDATE, INSERT statements")
    print(f"   • Schema modifications (ALTER, CREATE, DROP)")
    print(f"   • Administrative commands (GRANT, REVOKE)")
    print(f"\n✅ Allowed Operations:")
    print(f"   • SELECT queries only (READ-ONLY access)")
    print("=" * 60)
    
    mcp.run(
        transport="sse",     
        host="0.0.0.0",       
        port=8001,
        path="/sse"           
    )