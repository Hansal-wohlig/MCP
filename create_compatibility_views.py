#!/usr/bin/env python3
"""
Create BigQuery Views for MCP Server Compatibility
Maps UPI table structure to what mcp_toolbox_server expects
"""

import config
from google.cloud import bigquery
import sys

def create_compatibility_views():
    """Create views that map UPI tables to MCP server expected structure"""

    print("\n" + "=" * 70)
    print("🔧 CREATING BIGQUERY COMPATIBILITY VIEWS")
    print("=" * 70)
    print(f"Project: {config.GCP_PROJECT_ID}")
    print(f"Dataset: {config.BIGQUERY_DATASET}")
    print("=" * 70 + "\n")

    client = bigquery.Client(project=config.GCP_PROJECT_ID)
    dataset_id = f"{config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET}"

    views = {
        'customers': f"""
        CREATE OR REPLACE VIEW `{dataset_id}.customers` AS
        SELECT
            customer_id,
            name as customer_name,  -- Map 'name' to 'customer_name'
            mobile_number,
            email,
            primary_vpa as vpa,
            bank_account_no as account_number,
            ifsc_code,
            created_at as created_date
        FROM `{dataset_id}.upi_customer`
        """,

        'transactions': f"""
        CREATE OR REPLACE VIEW `{dataset_id}.transactions` AS
        SELECT
            t.transaction_id,
            t.upi_txn_ref as transaction_reference,
            c.customer_id,
            c.name as customer_name,  -- Add customer_name for easier filtering
            t.payer_vpa,
            t.payee_vpa,
            t.amount as transaction_amount,
            t.currency,
            t.transaction_type,
            t.status as transaction_status,
            t.initiated_at as transaction_date,
            t.completed_at,
            t.failure_reason,
            t.merchant_id,
            t.remarks as description
        FROM `{dataset_id}.upi_transaction` t
        LEFT JOIN `{dataset_id}.upi_customer` c
            ON t.payer_vpa = c.primary_vpa
        """,

        'merchants': f"""
        CREATE OR REPLACE VIEW `{dataset_id}.merchants` AS
        SELECT
            merchant_id,
            merchant_name as name,
            merchant_vpa as vpa,
            category,
            settlement_account_no as account_number,
            ifsc_code,
            created_at as created_date
        FROM `{dataset_id}.upi_merchant`
        """
    }

    for view_name, view_sql in views.items():
        try:
            print(f"Creating view: {view_name}...")
            query_job = client.query(view_sql)
            query_job.result()  # Wait for completion
            print(f"✓ Created view: {view_name}")
        except Exception as e:
            print(f"❌ Error creating view {view_name}: {e}")
            sys.exit(1)

    print("\n" + "=" * 70)
    print("✅ COMPATIBILITY VIEWS CREATED SUCCESSFULLY")
    print("=" * 70)
    print("\nViews created:")
    print("  • customers → maps upi_customer (name → customer_name)")
    print("  • transactions → maps upi_transaction")
    print("  • merchants → maps upi_merchant")
    print("\n" + "=" * 70)
    print("🔍 TEST QUERIES")
    print("=" * 70)
    print(f"\n-- Test customer view")
    print(f"SELECT customer_name, vpa FROM `{dataset_id}.customers` LIMIT 5;")
    print(f"\n-- Test transaction view")
    print(f"SELECT transaction_reference, transaction_amount, transaction_status")
    print(f"FROM `{dataset_id}.transactions` LIMIT 5;")
    print("=" * 70 + "\n")

    # Test the views
    test_views(client, dataset_id)

def test_views(client, dataset_id):
    """Test that views work correctly"""
    print("🧪 TESTING VIEWS")
    print("-" * 70)

    test_queries = {
        'customers': f"SELECT COUNT(*) as count FROM `{dataset_id}.customers`",
        'transactions': f"SELECT COUNT(*) as count FROM `{dataset_id}.transactions`",
        'merchants': f"SELECT COUNT(*) as count FROM `{dataset_id}.merchants`"
    }

    for view_name, query in test_queries.items():
        try:
            query_job = client.query(query)
            results = list(query_job.result())
            count = results[0]['count']
            print(f"  ✓ {view_name}: {count:,} records")
        except Exception as e:
            print(f"  ❌ {view_name}: Error - {e}")

    print("-" * 70 + "\n")

def main():
    create_compatibility_views()

if __name__ == "__main__":
    main()
