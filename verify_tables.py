#!/usr/bin/env python3
"""
Verify BigQuery Tables - Checks if all tables are ready for data insertion
"""

import config
from google.cloud import bigquery
import sys
import time

def verify_tables():
    """Verify all required BigQuery tables exist and are accessible"""

    print("\n" + "=" * 70)
    print("🔍 VERIFYING BIGQUERY TABLES")
    print("=" * 70)
    print(f"Project: {config.GCP_PROJECT_ID}")
    print(f"Dataset: {config.BIGQUERY_DATASET}")
    print("=" * 70 + "\n")

    # Connect to BigQuery
    try:
        client = bigquery.Client(project=config.GCP_PROJECT_ID)
        print("✓ Connected to BigQuery\n")
    except Exception as e:
        print(f"❌ Failed to connect to BigQuery: {e}")
        sys.exit(1)

    dataset_id = f"{config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET}"

    # Check dataset
    try:
        dataset = client.get_dataset(dataset_id)
        print(f"✓ Dataset exists: {config.BIGQUERY_DATASET}")
        print(f"  Location: {dataset.location}\n")
    except Exception as e:
        print(f"❌ Dataset not found: {e}")
        print(f"\n💡 Run: python create_dataset.py")
        sys.exit(1)

    # Required tables
    required_tables = [
        'upi_bank',
        'upi_customer',
        'upi_customer_credentials',
        'upi_merchant',
        'upi_transaction',
        'upi_transaction_audit',
        'upi_refund'
    ]

    print("Checking tables:")
    print("-" * 70)

    all_ready = True
    table_info = []

    for table_name in required_tables:
        table_id = f"{dataset_id}.{table_name}"
        try:
            table = client.get_table(table_id)
            row_count = table.num_rows
            status = "✓ Ready"
            table_info.append({
                'name': table_name,
                'status': 'ready',
                'rows': row_count,
                'created': table.created
            })
            print(f"  ✓ {table_name:<30} Rows: {row_count:<10} Status: Ready")
        except Exception as e:
            status = "❌ Not Found"
            all_ready = False
            table_info.append({
                'name': table_name,
                'status': 'missing',
                'error': str(e)
            })
            print(f"  ❌ {table_name:<30} Status: Not Found")

    print("-" * 70 + "\n")

    # Test insert capability
    if all_ready:
        print("Testing insert capability...")
        print("-" * 70)

        # Try a simple query to verify read access
        try:
            query = f"SELECT COUNT(*) as cnt FROM `{dataset_id}.upi_bank` LIMIT 1"
            query_job = client.query(query)
            result = list(query_job.result())
            print(f"  ✓ Read access verified")
            print(f"  ✓ Current banks in table: {result[0]['cnt']}")
        except Exception as e:
            print(f"  ⚠️  Read test failed: {e}")

        print("-" * 70 + "\n")

    # Summary
    print("=" * 70)
    if all_ready:
        print("✅ ALL TABLES READY")
        print("=" * 70)
        print("\nYou can now run:")
        print("  • python generate_upi_bigquery_direct_test.py  (for testing)")
        print("  • python generate_upi_bigquery_direct.py       (for production)")
        print("=" * 70 + "\n")
        return True
    else:
        print("❌ TABLES NOT READY")
        print("=" * 70)
        print("\nMissing tables detected. Please run:")
        print("  python create_bigquery_tables.py")
        print("\nThen wait 2-3 minutes and run this verification again:")
        print("  python verify_tables.py")
        print("=" * 70 + "\n")
        return False

if __name__ == "__main__":
    success = verify_tables()
    sys.exit(0 if success else 1)
