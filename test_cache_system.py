#!/usr/bin/env python3
"""
Test script for schema cache system
Run this to verify the caching system works correctly
"""

import config
from google.cloud import bigquery
from langchain_google_vertexai import ChatVertexAI
from schema_cache_manager import SchemaCache, load_or_refresh_schema
from datetime import datetime
import json

def print_section(title):
    """Print a formatted section header"""
    print(f"\n{'='*60}")
    print(f"{title}")
    print(f"{'='*60}\n")

def test_cache_info():
    """Test 1: Check cache information"""
    print_section("TEST 1: Cache Information")
    
    cache = SchemaCache()
    info = cache.get_cache_info()
    
    print("Cache Status:")
    print(f"  • Exists: {info.get('exists', False)}")
    print(f"  • Valid: {info.get('valid', False)}")
    
    if info.get('exists'):
        print(f"  • Timestamp: {info.get('timestamp', 'N/A')}")
        print(f"  • Age: {info.get('age_days', 'N/A')} days")
        print(f"  • Tables: {info.get('tables_count', 'N/A')}")
        print(f"  • Project: {info.get('project_id', 'N/A')}")
        print(f"  • Dataset: {info.get('dataset', 'N/A')}")
    
    return info.get('exists', False)

def test_load_cache():
    """Test 2: Load cached schema"""
    print_section("TEST 2: Load Cached Schema")
    
    cache = SchemaCache()
    cached_data = cache.load_cache()
    
    if cached_data:
        print("✓ Cache loaded successfully!")
        print(f"  • Tables in cache: {len(cached_data['schema_info'])}")
        print(f"  • Tables: {', '.join(cached_data['schema_info'].keys())}")
        return True
    else:
        print("⚠️  No valid cache found")
        return False

def test_fresh_fetch():
    """Test 3: Fetch fresh schema from BigQuery"""
    print_section("TEST 3: Fresh Schema Fetch")
    
    try:
        print("Initializing clients...")
        bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
        llm = ChatVertexAI(
            model_name="gemini-2.5-flash",
            project=config.GCP_PROJECT_ID,
            location=config.GCP_LOCATION,
            temperature=0,
        )
        print("✓ Clients initialized\n")
        
        print("Fetching schema (this may take a minute)...")
        schema_info, table_contexts, formatted_schema = load_or_refresh_schema(
            bq_client=bq_client,
            llm=llm,
            force_refresh=True  # Force refresh for testing
        )
        
        print("\n✓ Schema fetched successfully!")
        print(f"  • Tables fetched: {len(schema_info)}")
        print(f"  • Contexts generated: {len(table_contexts)}")
        
        # Display sample
        print("\nSample Table Info:")
        for i, (table_name, info) in enumerate(schema_info.items()):
            if i >= 2:  # Show only first 2 tables
                break
            print(f"\n  📊 {table_name}:")
            print(f"     • Columns: {len(info['fields'])}")
            print(f"     • Rows: {info['num_rows']:,}")
            context = table_contexts.get(table_name, {})
            if context.get('description'):
                print(f"     • Description: {context['description'][:60]}...")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        return False

def test_cache_performance():
    """Test 4: Compare cache vs fresh fetch performance"""
    print_section("TEST 4: Performance Comparison")
    
    try:
        bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
        llm = ChatVertexAI(
            model_name="gemini-2.5-flash",
            project=config.GCP_PROJECT_ID,
            location=config.GCP_LOCATION,
            temperature=0,
        )
        
        # Test with cache
        print("Loading with cache...")
        start_time = datetime.now()
        schema_info_cached, _, _ = load_or_refresh_schema(
            bq_client=bq_client,
            llm=llm,
            force_refresh=False
        )
        cache_time = (datetime.now() - start_time).total_seconds()
        
        print(f"✓ Cache load time: {cache_time:.2f} seconds")
        print(f"  • Tables loaded: {len(schema_info_cached)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

def test_cache_content():
    """Test 5: Verify cache file content"""
    print_section("TEST 5: Cache File Content")
    
    cache = SchemaCache()
    
    if not cache.cache_file.exists():
        print("⚠️  Cache file does not exist")
        return False
    
    try:
        with open(cache.cache_file, 'r') as f:
            data = json.load(f)
        
        print("✓ Cache file is valid JSON")
        print(f"  • File size: {cache.cache_file.stat().st_size / 1024:.2f} KB")
        print(f"  • Keys present: {', '.join(data.keys())}")
        
        # Validate structure
        required_keys = ['timestamp', 'schema_info', 'table_contexts', 'project_id', 'dataset']
        missing_keys = [key for key in required_keys if key not in data]
        
        if missing_keys:
            print(f"⚠️  Missing keys: {', '.join(missing_keys)}")
            return False
        
        print("✓ All required keys present")
        
        # Show some stats
        print("\nCache Statistics:")
        print(f"  • Tables: {len(data['schema_info'])}")
        print(f"  • Total columns: {sum(len(t['fields']) for t in data['schema_info'].values())}")
        print(f"  • Total rows: {sum(t['num_rows'] for t in data['schema_info'].values()):,}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error reading cache: {str(e)}")
        return False

def main():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("🧪 SCHEMA CACHE SYSTEM TEST SUITE")
    print("=" * 60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Project: {config.GCP_PROJECT_ID}")
    print(f"Dataset: {config.BIGQUERY_DATASET}")
    
    results = {
        "cache_info": False,
        "load_cache": False,
        "fresh_fetch": False,
        "performance": False,
        "content_validation": False
    }
    
    # Run tests
    results["cache_info"] = test_cache_info()
    results["load_cache"] = test_load_cache()
    
    print("\n🔄 Starting fresh fetch test (this will take a few minutes)...")
    print("Press Ctrl+C to skip this test\n")
    
    try:
        results["fresh_fetch"] = test_fresh_fetch()
        results["performance"] = test_cache_performance()
    except KeyboardInterrupt:
        print("\n\n⚠️  Fresh fetch test skipped by user")
    
    results["content_validation"] = test_cache_content()
    
    # Print summary
    print_section("TEST SUMMARY")
    
    total_tests = len(results)
    passed_tests = sum(1 for v in results.values() if v)
    
    print("Results:")
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {status} - {test_name.replace('_', ' ').title()}")
    
    print(f"\nTotal: {passed_tests}/{total_tests} tests passed")
    
    if passed_tests == total_tests:
        print("\n🎉 All tests passed! Cache system is working correctly.")
    else:
        print("\n⚠️  Some tests failed. Check the output above for details.")
    
    print("\n" + "=" * 60 + "\n")
    
    return 0 if passed_tests == total_tests else 1

if __name__ == "__main__":
    import sys
    sys.exit(main())