#!/usr/bin/env python3
"""
Schema Refresh Job
Run this script via cron every 7 days to refresh the schema cache

Cron setup:
0 2 * * 0 /mindgate/bin/python schema_refresh_job.py >> /var/log/schema_refresh.log 2>&1

This runs every Sunday at 2 AM
"""

import sys
import config
from pathlib import Path
from datetime import datetime
from google.cloud import bigquery
from langchain_google_vertexai import ChatVertexAI

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from schema_cache_manager import (
    fetch_bigquery_schema,
    generate_all_table_contexts,
    SchemaCache,
    format_schema_for_llm
)


def main():
    """Main function to refresh schema cache"""
    print("\n" + "=" * 60)
    print("🔄 SCHEMA REFRESH JOB")
    print("=" * 60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Project: {config.GCP_PROJECT_ID}")
    print(f"Dataset: {config.BIGQUERY_DATASET}")
    print("=" * 60 + "\n")
    
    try:
        # Initialize clients
        print("1. Initializing Google Cloud clients...")
        bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
        print("   ✓ BigQuery client initialized")
        
        llm = ChatVertexAI(
            model_name="gemini-2.0-flash-exp",
            project=config.GCP_PROJECT_ID,
            location=config.GCP_LOCATION,
            temperature=0,
        )
        print("   ✓ Vertex AI LLM initialized")
        print(f"   Model: gemini-2.0-flash-exp")
        
        # Test Gemini connection
        print("\n1.5. Testing Gemini connection...")
        try:
            test_response = llm.invoke("Say 'Connection OK'")
            print(f"   ✓ Gemini test: {test_response.content[:50]}")
        except Exception as test_error:
            print(f"   ⚠️ Gemini test failed: {str(test_error)}")
            print("   Continuing with fallback context generation...")
        
        # Fetch fresh schema
        print("\n2. Fetching schema from BigQuery...")
        schema_info = fetch_bigquery_schema(bq_client, config.BIGQUERY_DATASET)
        print(f"   ✓ Fetched schema for {len(schema_info)} tables")
        
        # Generate contexts
        print("\n3. Generating table contexts with Gemini...")
        table_contexts = generate_all_table_contexts(schema_info, llm)
        print(f"   ✓ Generated contexts for {len(table_contexts)} tables")
        
        # Save to cache
        print("\n4. Saving to cache...")
        cache_manager = SchemaCache()
        cache_manager.save_cache(schema_info, table_contexts)
        print("   ✓ Cache saved successfully")
        
        # Display summary
        print("\n" + "=" * 60)
        print("✅ SCHEMA REFRESH COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Tables refreshed: {len(schema_info)}")
        print(f"Cache location: {cache_manager.cache_file}")
        print("=" * 60 + "\n")
        
        # Show cache info
        cache_info = cache_manager.get_cache_info()
        print("📊 Cache Information:")
        print(f"   • Valid: {cache_info['valid']}")
        print(f"   • Age: {cache_info['age_days']} days")
        print(f"   • Tables: {cache_info['tables_count']}")
        print(f"   • Next refresh: {datetime.now().strftime('%Y-%m-%d')} + 7 days")
        print("\n")
        
        return 0
        
    except Exception as e:
        print("\n" + "=" * 60)
        print("❌ SCHEMA REFRESH FAILED")
        print("=" * 60)
        print(f"Error: {str(e)}")
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60 + "\n")
        
        import traceback
        traceback.print_exc()
        
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)