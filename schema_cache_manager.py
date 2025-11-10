"""
Schema Cache Manager
Manages cached schema that refreshes every 7 days via cron job
"""

import config
import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from google.cloud import bigquery
from typing import Dict, List, Optional
from langchain_google_vertexai import ChatVertexAI
from langchain_core.prompts import ChatPromptTemplate

# Cache configuration
CACHE_DIR = Path("./schema_cache")
CACHE_FILE = CACHE_DIR / "schema_cache.json"
CACHE_VALIDITY_DAYS = 7


class SchemaCache:
    """Manages schema caching with automatic refresh"""
    
    def __init__(self, cache_file: Path = CACHE_FILE):
        self.cache_file = cache_file
        self.cache_dir = cache_file.parent
        self._ensure_cache_dir()
    
    def _ensure_cache_dir(self):
        """Create cache directory if it doesn't exist"""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def save_cache(self, schema_info: Dict, table_contexts: Dict):
        """
        Save schema and contexts to cache file with timestamp
        
        Args:
            schema_info: Schema information from BigQuery
            table_contexts: Generated table contexts
        """
        cache_data = {
            "timestamp": datetime.now().isoformat(),
            "schema_info": schema_info,
            "table_contexts": table_contexts,
            "project_id": config.GCP_PROJECT_ID,
            "dataset": config.BIGQUERY_DATASET
        }
        
        with open(self.cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2, default=str)
        
        print(f"✓ Schema cache saved to: {self.cache_file}")
        print(f"  Timestamp: {cache_data['timestamp']}")
        print(f"  Tables: {len(schema_info)}")
    
    def load_cache(self) -> Optional[Dict]:
        """
        Load cached schema if it exists and is valid
        
        Returns:
            Cached data dict or None if cache is invalid/missing
        """
        if not self.cache_file.exists():
            print(f"⚠️  No cache file found at: {self.cache_file}")
            return None
        
        try:
            with open(self.cache_file, 'r') as f:
                cache_data = json.load(f)
            
            # Check cache age
            cache_timestamp = datetime.fromisoformat(cache_data['timestamp'])
            age_days = (datetime.now() - cache_timestamp).days
            
            print(f"\n{'='*60}")
            print(f"📦 LOADING CACHED SCHEMA")
            print(f"{'='*60}")
            print(f"Cache file: {self.cache_file}")
            print(f"Cache age: {age_days} days")
            print(f"Created: {cache_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Tables: {len(cache_data['schema_info'])}")
            
            if age_days > CACHE_VALIDITY_DAYS:
                print(f"⚠️  Cache is stale (>{CACHE_VALIDITY_DAYS} days old)")
                print(f"{'='*60}\n")
                return None
            
            print(f"✓ Cache is valid")
            print(f"{'='*60}\n")
            
            return cache_data
            
        except Exception as e:
            print(f"❌ Error loading cache: {str(e)}")
            return None
    
    def is_cache_valid(self) -> bool:
        """Check if cache exists and is still valid"""
        if not self.cache_file.exists():
            return False
        
        try:
            with open(self.cache_file, 'r') as f:
                cache_data = json.load(f)
            
            cache_timestamp = datetime.fromisoformat(cache_data['timestamp'])
            age_days = (datetime.now() - cache_timestamp).days
            
            return age_days <= CACHE_VALIDITY_DAYS
        except:
            return False
    
    def get_cache_info(self) -> Dict:
        """Get information about the current cache"""
        if not self.cache_file.exists():
            return {
                "exists": False,
                "valid": False
            }
        
        try:
            with open(self.cache_file, 'r') as f:
                cache_data = json.load(f)
            
            cache_timestamp = datetime.fromisoformat(cache_data['timestamp'])
            age_days = (datetime.now() - cache_timestamp).days
            valid = age_days <= CACHE_VALIDITY_DAYS
            
            return {
                "exists": True,
                "valid": valid,
                "timestamp": cache_data['timestamp'],
                "age_days": age_days,
                "tables_count": len(cache_data['schema_info']),
                "project_id": cache_data.get('project_id'),
                "dataset": cache_data.get('dataset')
            }
        except Exception as e:
            return {
                "exists": True,
                "valid": False,
                "error": str(e)
            }


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
                "created": str(table.created),
                "modified": str(table.modified)
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
    llm: ChatVertexAI
) -> Dict[str, str]:
    """
    Use Gemini via Vertex AI to generate intelligent context for a table.
    
    Args:
        table_name: Name of the table
        schema_fields: List of field dictionaries
        num_rows: Number of rows in the table
        llm: Initialized Vertex AI Gemini LLM instance
    
    Returns:
        Dict with table context information
    """
    fields_str = "\n".join([
        f"  - {field['name']} ({field['type']}) {'[REQUIRED]' if field['mode'] == 'REQUIRED' else ''}"
        for field in schema_fields
    ])
    
    context_prompt = ChatPromptTemplate.from_messages([
        ("system", """
        You are a database documentation expert. Given a table name and its schema,
        generate helpful context that will be used by an AI assistant to understand
        when and how to use this table.
        
        Your response must be a JSON object with exactly these fields:
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
        
        import json
        content = response.content.strip()
        
        # Extract JSON from response
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
    llm: ChatVertexAI
) -> Dict[str, Dict]:
    """
    Generate context for all tables using Gemini.
    
    Args:
        schema_info: Schema information from fetch_bigquery_schema
        llm: Initialized Vertex AI Gemini LLM instance
    
    Returns:
        Dict mapping table names to their generated contexts
    """
    print(f"\n{'='*60}")
    print(f"🤖 GENERATING TABLE CONTEXTS WITH GEMINI (Vertex AI)")
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
        table_contexts: Table context information
    
    Returns:
        Formatted schema string
    """
    schema_parts = []
    
    schema_parts.append("=" * 60)
    schema_parts.append("DATABASE SCHEMA (Cached)")
    schema_parts.append("=" * 60)
    
    for table_name, info in schema_info.items():
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
        
        schema_parts.append("")
    
    schema_parts.append("=" * 60)
    
    return "\n".join(schema_parts)


def load_or_refresh_schema(
    bq_client: bigquery.Client,
    llm: ChatVertexAI,
    force_refresh: bool = False
) -> tuple[Dict, Dict, str]:
    """
    Load schema from cache or refresh if needed.
    
    Args:
        bq_client: BigQuery client
        llm: Vertex AI LLM instance
        force_refresh: Force refresh even if cache is valid
    
    Returns:
        Tuple of (schema_info, table_contexts, formatted_schema)
    """
    cache_manager = SchemaCache()
    
    # Check if we should use cache
    if not force_refresh and cache_manager.is_cache_valid():
        cache_data = cache_manager.load_cache()
        if cache_data:
            return (
                cache_data['schema_info'],
                cache_data['table_contexts'],
                format_schema_for_llm(
                    cache_data['schema_info'],
                    cache_data['table_contexts']
                )
            )
    
    # Cache invalid or force refresh - fetch fresh data
    print("🔄 Refreshing schema from BigQuery...")
    schema_info = fetch_bigquery_schema(bq_client, config.BIGQUERY_DATASET)
    table_contexts = generate_all_table_contexts(schema_info, llm)
    
    # Save to cache
    cache_manager.save_cache(schema_info, table_contexts)
    
    formatted_schema = format_schema_for_llm(schema_info, table_contexts)
    
    return schema_info, table_contexts, formatted_schema