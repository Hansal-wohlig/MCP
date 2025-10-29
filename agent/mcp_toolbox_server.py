import config # Ensures env vars are loaded
import uvicorn
import pandas as pd
from google.cloud import bigquery
from fastmcp import FastMCP
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_core.prompts import ChatPromptTemplate

# --- 1. Initialize All Shared Resources ---
print("Initializing shared resources (LLM, Embeddings, BQ, Vector Store)...")
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=config.OPENAI_API_KEY)
embeddings = OpenAIEmbeddings(api_key=config.OPENAI_API_KEY)
bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)

try:
    vector_store = FAISS.load_local(
        config.VECTOR_STORE_PATH_OPENAI, 
        embeddings, 
        allow_dangerous_deserialization=True
    )
    print(f"Successfully loaded vector store from {config.VECTOR_STORE_PATH_OPENAI}")
except Exception as e:
    print(f"FATAL: Could not load vector store from {config.VECTOR_STORE_PATH_OPENAI}")
    print(f"Error: {str(e)}")
    print("Please run 'python pdf_indexer.py' first to create the vector store.")
    raise

# --- 2. CREATE FastMCP INSTANCE ---
mcp = FastMCP("MyToolboxServer")

# --- 3. Define PDF Tool Logic (Stateless) ---
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
    
    # 1. Retrieve context
    docs = vector_store.similarity_search(question, k=3)
    if not docs:
        return "I couldn't find any relevant information in the document to answer that question."
        
    context = "\n\n".join([f"Context {i+1}:\n{doc.page_content}" for i, doc in enumerate(docs)])
    
    # 2. Generate response
    response = pdf_generation_chain.invoke({
        "context": context,
        "question": question
    })
    
    return response.content

# --- 4. Define BigQuery Tool Logic (Stateless) ---
schema = f"""
1. `customers` table: customer_id (INTEGER), customer_name (STRING), email (STRING), phone_number (STRING), address (STRING), customer_since (DATE).
2. `transactions` table: transaction_id (INTEGER), customer_id (INTEGER), account_number (STRING), account_type (STRING), transaction_timestamp (TIMESTAMP), transaction_amount (FLOAT), transaction_type (STRING, 'Credit'/'Debit'), counterparty_name (STRING), counterparty_account (STRING).
"""

sql_prompt = ChatPromptTemplate.from_messages([
    ("system",
    f"""
    You are a BigQuery SQL expert. Given a user question and the database schema, 
    write a valid **BigQuery** SQL query to answer it.
    Only output the SQL query and nothing else - no explanations, no markdown formatting.
    Ensure tables are referenced as `{config.BIGQUERY_DATASET}.table_name`.
    
    **CRITICAL SQL Rules**:
    - Always use single quotes for string literals, never double quotes
    - For customer names, use exact matching: WHERE customer_name = 'Tony Toy'
    - When asked about "transactions by [customer name]", JOIN transactions with customers table
    - Use this pattern for customer transactions:
      SELECT t.* FROM {config.BIGQUERY_DATASET}.transactions t
      JOIN {config.BIGQUERY_DATASET}.customers c ON t.customer_id = c.customer_id
      WHERE c.customer_name = '[customer name]'
    - BigQuery is case-sensitive for string comparisons
    - Handle NULL values appropriately using IS NULL or IS NOT NULL
    
    **Common Query Patterns**:
    - "transactions by [name]" → JOIN transactions with customers, filter by customer_name
    - "all customers" → SELECT * FROM customers
    - "average transaction amount" → SELECT AVG(transaction_amount) FROM transactions
    - "most frequent type" → GROUP BY transaction_type, ORDER BY COUNT(*) DESC, LIMIT 1
    
    **Key Tips**:
    - For "most frequent" or "main" transaction_type: Use COUNT(*) with GROUP BY and ORDER BY count DESC
    - For "maximum amount": Use MAX() or ORDER BY amount DESC LIMIT 1
    - When combining conditions (e.g., "most frequent type AND max amount for that type"):
      * Use CTEs to break down the logic
      * First identify the most frequent type using COUNT
      * Then find the max amount for THAT specific type
      * Join with customers table to get customer name
    - Add meaningful sorting: Use ORDER BY to sort results logically
    - Use appropriate joins: Prefer INNER JOIN unless including entities without matches is necessary
    - Use CTEs (Common Table Expressions) for complex multi-step queries to improve readability
    
    **IMPORTANT**: 
    - If the question CAN be answered with the schema, generate a SQL query
    - Only output 'Cannot answer with available data.' if the question asks for data that doesn't exist in the schema
    - Questions about customers, transactions, amounts, types, accounts ARE answerable
    
    Database schema:
    {schema}
    """),
    ("human", "{question}")
])
sql_generation_chain = sql_prompt | llm

summary_prompt = ChatPromptTemplate.from_messages([
    ("system", """
    You are a helpful data assistant. Provide a concise, conversational answer 
    based on the user's question and the summary of the query result.
    1. If the result is a single value, answer directly. (e.g., "The average amount is 1520.75.")
    2. If there are no results or an error, state that clearly.
    """),
    ("human", "User Question: {question}\nQuery Result Summary: {result_summary}")
])
summary_chain = summary_prompt | llm

def _execute_query(sql_query: str) -> tuple[str, pd.DataFrame | None]:
    try:
        clean_sql = sql_query.strip().replace("```sql", "").replace("```", "")
        print(f"--- Executing SQL: ---\n{clean_sql}\n--------------------")
        
        query_job = bq_client.query(clean_sql)
        results = query_job.to_dataframe()
        if results.empty:
            return "The query executed successfully but returned no results.", None
        return results.to_string(index=False), results
    except Exception as e:
        print(f"ERROR DETAILS: {str(e)}")
        return f"An error occurred while executing the BigQuery query: {e}", None
    
@mcp.tool
def query_customer_database(natural_language_query: str) -> str:
    """
    Answers questions about customer data, transactions, accounts, or
    financial calculations from a BigQuery database.
    """
    print(f"[BQ Tool] Received query: {natural_language_query}")
    
    # 1. Generate SQL
    sql_response = sql_generation_chain.invoke({"question": natural_language_query})
    sql_query = sql_response.content.strip()
    
    print(f"[BQ Tool] Generated SQL: {sql_query[:100]}...")  # Log first 100 chars
    
    # 2. Validate SQL query
    if "cannot answer" in sql_query.lower():
        return "I'm sorry, but I cannot answer that question with the available database schema. The database contains information about customers and their transactions. Please rephrase your question."
    
    # Check if it's actually SQL
    sql_upper = sql_query.upper().replace("```SQL", "").replace("```", "").strip()
    if not (sql_upper.startswith('SELECT') or sql_upper.startswith('WITH')):
        return f"I encountered an issue generating a SQL query for your question. Please try rephrasing it or ask about customers, transactions, or transaction amounts."
    
    # 3. Execute SQL
    text_result, df_result = _execute_query(sql_query)
    
    # 4. Decide how to respond
    if df_result is not None and not df_result.empty:
        # Case 1: Single value (e.g. COUNT, AVG) → use LLM for nice phrasing
        if df_result.shape == (1, 1):
            result_summary_for_llm = f"The query returned a single value: {df_result.iloc[0, 0]}"
            final_summary = summary_chain.invoke({
                "question": natural_language_query,
                "result_summary": result_summary_for_llm
            }).content
            return final_summary
        
        # Case 2: Full table (e.g. SELECT * FROM customers) → return as Markdown table
        markdown_table = df_result.to_markdown(index=False)
        return f"Here is the query result as a table:\n\n{markdown_table}"
    
    else:
        # Case 3: Error or no results → use LLM for polite message
        result_summary_for_llm = text_result
        final_summary = summary_chain.invoke({
            "question": natural_language_query,
            "result_summary": result_summary_for_llm
        }).content
        return final_summary
# --- 5. Run the SINGLE Server ---
if __name__ == "__main__":
    print("--- Starting Combined MCP Toolbox Server (PDF + BigQuery) ---")
    
    mcp.run(
        transport="sse",     
        host="0.0.0.0",       
        port=8001,
        path="/sse"           
    )