import config
from google.cloud import bigquery
from typing import Dict, List, Optional
from datetime import datetime
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate

def fetch_bigquery_schema(client: bigquery.Client, dataset_name: str) -> Dict[str, List[Dict]]:
    """
    Fetch the current schema from BigQuery for all tables in a dataset.
    
    Returns:
        Dict mapping table names to their schema information
    """
    print(f"\n{'='*60}")
    print(f"📊 FETCHING SCHEMA FROM BIGQUERY")
    print(f"{'='*60}")
    print(f"Project: {config.GCP_PROJECT_ID}")
    print(f"Dataset: {dataset_name}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    schema_info = {}
    
    try:
        dataset_ref = f"{config.GCP_PROJECT_ID}.{dataset_name}"
        
        # List all tables in the dataset
        tables = list(client.list_tables(dataset_ref))
        print(f"Found {len(tables)} table(s) in dataset:\n")
        
        for table_item in tables:
            table_name = table_item.table_id
            table_ref = f"{dataset_ref}.{table_name}"
            
            print(f"  📋 Fetching schema for: {table_name}")
            
            # Get table schema
            table = client.get_table(table_ref)
            
            schema_fields = []
            for field in table.schema:
                schema_fields.append({
                    "name": field.name,
                    "type": field.field_type,
                    "mode": field.mode,
                    "description": field.description or ""
                })
            
            schema_info[table_name] = {
                "fields": schema_fields,
                "num_rows": table.num_rows,
                "created": table.created,
                "modified": table.modified
            }
            
            print(f"     ✓ {len(schema_fields)} columns, {table.num_rows} rows")
        
        print(f"\n{'='*60}")
        print(f"✓ Schema fetch completed successfully!")
        print(f"{'='*60}\n")
        
        return schema_info
        
    except Exception as e:
        print(f"\n{'='*60}")
        print(f"❌ ERROR FETCHING SCHEMA")
        print(f"{'='*60}")
        print(f"Error: {str(e)}")
        print(f"{'='*60}\n")
        raise

def generate_table_context_with_gemini(
    table_name: str, 
    schema_fields: List[Dict], 
    num_rows: int,
    llm: ChatGoogleGenerativeAI
) -> Dict[str, str]:
    """
    Use Gemini to generate intelligent context for a table based on its schema.
    
    Args:
        table_name: Name of the table
        schema_fields: List of field dictionaries with name, type, mode
        num_rows: Number of rows in the table
        llm: Initialized Gemini LLM instance
    
    Returns:
        Dict with 'description', 'usage', and other context
    """
    # Format schema for Gemini
    fields_str = "\n".join([
        f"  - {field['name']} ({field['type']}) {'[REQUIRED]' if field['mode'] == 'REQUIRED' else ''}"
        for field in schema_fields
    ])
    
    # Create prompt for Gemini - NOTE: Double curly braces {{ }} to escape them
    context_prompt = ChatPromptTemplate.from_messages([
        ("system", """
        You are a database documentation expert. Given a table name and its schema,
        generate helpful context that will be used by an AI assistant to understand
        when and how to use this table.
        
        Your response must be a JSON object with exactly these fields (use double curly braces for JSON):
        {{
            "description": "A 1-2 sentence description of what this table contains",
            "usage": "When should this table be used? What questions can it answer?",
            "business_context": "What business purpose does this table serve?",
            "common_queries": "Examples of common query patterns for this table",
            "sensitive": "true or false - does this table contain sensitive user data?"
        }}
        
        Base your analysis on the table name and column names to infer the purpose.
        Be specific and practical. Output ONLY valid JSON, nothing else.
        """),
        ("human", """
        Table Name: {table_name}
        Number of Rows: {num_rows}
        
        Columns:
        {fields}
        
        Generate context for this table in JSON format.
        """)
    ])
    
    try:
        response = (context_prompt | llm).invoke({
            "table_name": table_name,
            "num_rows": num_rows,
            "fields": fields_str
        })
        
        # Parse JSON response
        import json
        # Extract JSON from response (in case there's extra text)
        content = response.content.strip()
        
        # Try to find JSON in the response
        start_idx = content.find('{')
        end_idx = content.rfind('}') + 1
        
        if start_idx != -1 and end_idx > start_idx:
            json_str = content[start_idx:end_idx]
            context_json = json.loads(json_str)
        else:
            context_json = json.loads(content)
        
        return {
            "description": context_json.get("description", ""),
            "usage": context_json.get("usage", ""),
            "business_context": context_json.get("business_context", ""),
            "common_queries": context_json.get("common_queries", ""),
            "sensitive": context_json.get("sensitive", "true").lower() == "true",
            "row_level_security": context_json.get("sensitive", "true").lower() == "true"
        }
        
    except Exception as e:
        print(f"     ⚠️  Warning: Could not generate context with Gemini: {str(e)}")
        # Fallback to basic context
        return {
            "description": f"Table containing {table_name} data",
            "usage": f"Use this table to query {table_name} information",
            "business_context": "General business data",
            "common_queries": "Standard SELECT queries",
            "sensitive": True,
            "row_level_security": True
        }

def generate_all_table_contexts(
    schema_info: Dict, 
    llm: ChatGoogleGenerativeAI
) -> Dict[str, Dict]:
    """
    Generate context for all tables using Gemini.
    
    Args:
        schema_info: Schema information from fetch_bigquery_schema
        llm: Initialized Gemini LLM instance
    
    Returns:
        Dict mapping table names to their generated contexts
    """
    print(f"\n{'='*60}")
    print(f"🤖 GENERATING TABLE CONTEXTS WITH GEMINI")
    print(f"{'='*60}\n")
    
    table_contexts = {}
    
    for table_name, info in schema_info.items():
        print(f"  🧠 Generating context for: {table_name}")
        
        context = generate_table_context_with_gemini(
            table_name=table_name,
            schema_fields=info["fields"],
            num_rows=info["num_rows"],
            llm=llm
        )
        
        table_contexts[table_name] = context
        
        print(f"     ✓ Context generated")
        print(f"     Description: {context['description'][:60]}...")
    
    print(f"\n{'='*60}")
    print(f"✓ All table contexts generated!")
    print(f"{'='*60}\n")
    
    return table_contexts

def format_schema_for_llm(schema_info: Dict, table_contexts: Dict) -> str:
    """
    Format the schema information into a readable string for the LLM.
    
    Args:
        schema_info: Schema information from fetch_bigquery_schema
        table_contexts: Table context (can be from Gemini or manual)
    
    Returns:
        Formatted schema string
    """
    schema_parts = []
    
    schema_parts.append("=" * 60)
    schema_parts.append("DATABASE SCHEMA (Dynamically Fetched)")
    schema_parts.append("=" * 60)
    
    for table_name, info in schema_info.items():
        # Get context
        context = table_contexts.get(table_name, {})
        
        schema_parts.append(f"\n📊 TABLE: {table_name.upper()}")
        schema_parts.append("-" * 60)
        
        if context.get("description"):
            schema_parts.append(f"📝 Description: {context['description']}")
        
        if context.get("business_context"):
            schema_parts.append(f"💼 Business Context: {context['business_context']}")
        
        if context.get("usage"):
            schema_parts.append(f"🎯 Usage: {context['usage']}")
        
        if context.get("common_queries"):
            schema_parts.append(f"📋 Common Queries: {context['common_queries']}")
        
        schema_parts.append(f"📊 Rows: {info['num_rows']:,}")
        
        if context.get("sensitive"):
            schema_parts.append(f"🔒 Sensitive Data: Yes - Row-level security enforced")
        
        schema_parts.append("\n📋 Columns:")
        
        for field in info["fields"]:
            field_desc = f"  • {field['name']} ({field['type']})"
            if field['mode'] == 'REQUIRED':
                field_desc += " [REQUIRED]"
            if field['description']:
                field_desc += f" - {field['description']}"
            schema_parts.append(field_desc)
        
        schema_parts.append("")  # Empty line between tables
    
    schema_parts.append("=" * 60)
    
    return "\n".join(schema_parts)