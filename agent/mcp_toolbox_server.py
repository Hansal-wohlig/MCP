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
    model_name="gemini-2.5-flash",
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


@mcp.tool
def get_customer_summary_statistics(
    metric_type: str,
    current_user: str = None,
    time_period: str = None,
    group_by: str = None
) -> str:
    """
    Get aggregated summary statistics without returning individual rows.
    Optimized for large datasets (crores of records).
    
    Args:
        metric_type: Type of summary - 'total_spending', 'transaction_count', 
                    'average_amount', 'monthly_breakdown', 'category_summary'
        current_user: Authenticated user name (REQUIRED)
        time_period: Optional filter like 'last_30_days', 'this_year', 'last_month'
        group_by: Optional grouping like 'month', 'category', 'transaction_type'
    
    Returns:
        Summary statistics without individual transaction rows
    """
    if not current_user:
        return "🚫 Access Denied: Authentication required."
    
    print(f"[Summary Stats] User: {current_user}, Metric: {metric_type}")
    
    # Build WHERE clause with time filter
    time_filter = ""
    if time_period == 'last_30_days':
        time_filter = "AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"
    elif time_period == 'this_year':
        time_filter = "AND EXTRACT(YEAR FROM t.transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE())"
    elif time_period == 'last_month':
        time_filter = "AND t.transaction_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AND t.transaction_date < DATE_TRUNC(CURRENT_DATE(), MONTH)"
    
    base_query = f"""
    FROM {config.BIGQUERY_DATASET}.transactions t
    JOIN {config.BIGQUERY_DATASET}.customers c ON t.customer_id = c.customer_id
    WHERE c.customer_name = '{current_user}'
    {time_filter}
    """
    
    # Build aggregation query based on metric type
    if metric_type == 'total_spending':
        sql = f"""
        SELECT 
            SUM(CASE WHEN t.transaction_type = 'Debit' THEN t.transaction_amount ELSE 0 END) as total_debits,
            SUM(CASE WHEN t.transaction_type = 'Credit' THEN t.transaction_amount ELSE 0 END) as total_credits,
            SUM(CASE WHEN t.transaction_type = 'Debit' THEN t.transaction_amount ELSE -t.transaction_amount END) as net_spending
        {base_query}
        """
    
    elif metric_type == 'transaction_count':
        sql = f"""
        SELECT 
            COUNT(*) as total_transactions,
            COUNT(CASE WHEN t.transaction_type = 'Debit' THEN 1 END) as debit_count,
            COUNT(CASE WHEN t.transaction_type = 'Credit' THEN 1 END) as credit_count
        {base_query}
        """
    
    elif metric_type == 'average_amount':
        sql = f"""
        SELECT 
            AVG(t.transaction_amount) as avg_amount,
            MIN(t.transaction_amount) as min_amount,
            MAX(t.transaction_amount) as max_amount,
            STDDEV(t.transaction_amount) as std_deviation
        {base_query}
        """
    
    elif metric_type == 'monthly_breakdown' or group_by == 'month':
        sql = f"""
        SELECT 
            FORMAT_DATE('%Y-%m', t.transaction_date) as month,
            COUNT(*) as transaction_count,
            SUM(t.transaction_amount) as total_amount,
            AVG(t.transaction_amount) as avg_amount
        {base_query}
        GROUP BY month
        ORDER BY month DESC
        LIMIT 12
        """
    
    elif metric_type == 'category_summary' or group_by == 'category':
        sql = f"""
        SELECT 
            t.transaction_category,
            COUNT(*) as transaction_count,
            SUM(t.transaction_amount) as total_amount,
            AVG(t.transaction_amount) as avg_amount
        {base_query}
        GROUP BY t.transaction_category
        ORDER BY total_amount DESC
        LIMIT 20
        """
    
    else:
        return f"❌ Unknown metric_type: {metric_type}. Use: total_spending, transaction_count, average_amount, monthly_breakdown, category_summary"
    
    # Execute query
    text_result, df_result = _execute_query(sql, current_user)
    
    if df_result is None:
        return f"[SQL QUERY]\n{sql}\n\n[ERROR]\n{text_result}"
    
    # Format response
    response_parts = [
        "[SQL QUERY]",
        sql,
        "",
        "[SUMMARY RESULTS]",
        ""
    ]
    
    if not df_result.empty:
        response_parts.append(df_result.to_string(index=False))
    else:
        response_parts.append("No data found for this period.")
    
    return "\n".join(response_parts)



@mcp.tool
def query_customer_database_paginated(
    natural_language_query: str, 
    current_user: str = None,
    page: int = 1,
    page_size: int = 100,
    order_by: str = None
) -> str:
    """
    Query customer database with pagination for large result sets.
    
    Args:
        natural_language_query: The user's question in natural language
        current_user: The authenticated user's name (REQUIRED)
        page: Page number (starts at 1)
        page_size: Number of rows per page (default 100, max 1000)
        order_by: Column to sort by (optional, e.g., 'transaction_date DESC')
    
    Returns:
        Paginated query results with metadata
    """
    print(f"\n{'='*60}")
    print(f"[BQ Paginated] Query: {natural_language_query}")
    print(f"[BQ Paginated] User: {current_user}, Page: {page}, Size: {page_size}")
    print(f"{'='*60}")
    
    if not current_user:
        return "🚫 Access Denied: Authentication required."
    
    # Validate page_size
    page_size = min(max(1, page_size), 1000)  # Limit between 1-1000
    offset = (page - 1) * page_size
    
    # Check access permission
    is_allowed, error_msg = _check_access_permission(natural_language_query, current_user)
    if not is_allowed:
        return error_msg
    
    # Generate base SQL
    sql_response = sql_generation_chain.invoke({
        "question": natural_language_query,
        "current_user": f"'{current_user}'"
    })
    base_sql = sql_response.content.strip().replace("```sql", "").replace("```", "")
    
    if "ACCESS_DENIED" in base_sql:
        return f"🚫 Access Denied: You can only query your own data."
    
    # Get total count first (for pagination metadata)
    count_sql = f"SELECT COUNT(*) as total FROM ({base_sql}) as subquery"
    
    try:
        count_result = bq_client.query(count_sql).to_dataframe()
        total_rows = int(count_result.iloc[0]['total'])
        total_pages = (total_rows + page_size - 1) // page_size
    except Exception as e:
        total_rows = 0
        total_pages = 0
        print(f"[Warning] Could not get count: {e}")
    
    # Add pagination to SQL
    if order_by:
        paginated_sql = f"{base_sql} ORDER BY {order_by} LIMIT {page_size} OFFSET {offset}"
    else:
        paginated_sql = f"{base_sql} LIMIT {page_size} OFFSET {offset}"
    
    # Execute with security validation
    text_result, df_result = _execute_query(paginated_sql, current_user)
    
    if df_result is None:
        return f"[SQL QUERY]\n{paginated_sql}\n\n[ERROR]\n{text_result}"
    
    # Build response
    response_parts = [
        "[PAGINATION INFO]",
        f"Page: {page}/{total_pages}",
        f"Showing rows: {offset + 1}-{min(offset + page_size, total_rows)}",
        f"Total rows: {total_rows:,}",
        "",
        "[SQL QUERY]",
        paginated_sql,
        "",
        "[DATA RESULTS]"
    ]
    
    if not df_result.empty:
        response_parts.append(f"Rows on this page: {len(df_result)}")
        response_parts.append("")
        response_parts.append(df_result.to_string(index=False, max_colwidth=25))
        
        if page < total_pages:
            response_parts.append("")
            response_parts.append(f"📄 To see next page, use: page={page + 1}")
    else:
        response_parts.append("No results on this page.")
    
    return "\n".join(response_parts)

@mcp.tool
def query_customer_database_smart(
    natural_language_query: str, 
    current_user: str = None,
    result_limit: int = 100
) -> str:
    """
    Smart query tool with PERFORMANCE MONITORING and automatic optimization.
    Shows execution time, data processed, and cost metrics.
    
    Args:
        natural_language_query: User's question in natural language
        current_user: Authenticated user name (REQUIRED)
        result_limit: Maximum rows to return (default 100, max 1000)
    
    Returns:
        Query results with performance metrics
    
    Examples:
        query_customer_database_smart('my account details', 'John Doe')
        query_customer_database_smart('transaction #12345', 'John Doe')
    """
    print(f"\n{'='*60}")
    print(f"[BQ Smart] Query: {natural_language_query}")
    print(f"[BQ Smart] User: {current_user}, Limit: {result_limit}")
    print(f"{'='*60}")
    
    if not current_user:
        return "🚫 Access Denied: Authentication required."
    
    # Validate limit
    result_limit = min(max(1, result_limit), 1000)
    
    # Check access
    is_allowed, error_msg = _check_access_permission(natural_language_query, current_user)
    if not is_allowed:
        return error_msg
    
    # Generate SQL
    sql_response = sql_generation_chain.invoke({
        "question": natural_language_query,
        "current_user": f"'{current_user}'"
    })
    sql_query = sql_response.content.strip().replace("```sql", "").replace("```", "")
    
    if "ACCESS_DENIED" in sql_query:
        return f"🚫 Access Denied: You can only query your own data."
    
    # Add LIMIT if not present
    if 'LIMIT' not in sql_query.upper():
        sql_query += f" LIMIT {result_limit}"
    
    # Execute with performance tracking
    import time
    from google.cloud import bigquery
    
    try:
        job_config = bigquery.QueryJobConfig(
            use_query_cache=True,
            use_legacy_sql=False,
            labels={'user': current_user.replace(' ', '_').lower()}
        )
        
        start_time = time.time()
        query_job = bq_client.query(sql_query, job_config=job_config)
        
        # Validate SQL access
        is_valid, error_msg = validate_sql_access(sql_query, current_user)
        if not is_valid:
            return error_msg
        
        results = query_job.result().to_dataframe()
        execution_time = time.time() - start_time
        
        # Collect performance metrics
        metadata = {
            'bytes_processed': query_job.total_bytes_processed or 0,
            'cache_hit': query_job.cache_hit,
            'execution_time': round(execution_time, 2),
            'rows_returned': len(results)
        }
        
        print(f"⚡ Performance: {metadata['execution_time']}s, "
              f"{metadata['bytes_processed'] / (1024**3):.3f} GB, "
              f"Cache: {metadata['cache_hit']}")
        
    except Exception as e:
        return f"[SQL QUERY]\n{sql_query}\n\n[ERROR]\n{str(e)}"
    
    # Build response
    response_parts = [
        "[SQL QUERY]",
        sql_query,
        "",
        "[PERFORMANCE METRICS]",
        f"⚡ Execution Time: {metadata['execution_time']}s",
        f"📦 Data Processed: {metadata['bytes_processed'] / (1024**3):.4f} GB",
        f"💾 Cache Hit: {'Yes ✓' if metadata['cache_hit'] else 'No'}",
        f"📊 Rows Returned: {metadata['rows_returned']:,}",
        "",
        "[DATA RESULTS]",
        ""
    ]
    
    if not results.empty:
        response_parts.append(results.to_string(index=False, max_colwidth=25))
        
        if len(results) >= result_limit:
            response_parts.append("")
            response_parts.append(f"⚠️  Results limited to {result_limit} rows. Use pagination for more.")
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