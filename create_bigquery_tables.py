#!/usr/bin/env python3
"""
Create BigQuery Tables for UPI Banking System
"""

import os
from google.cloud import bigquery
from google.cloud.exceptions import Conflict

def create_tables():
    """Create all required BigQuery tables"""
    
    project_id = os.environ.get('GCP_PROJECT_ID')
    dataset_id = os.environ.get('BIGQUERY_DATASET', 'upi_banking')
    
    if not project_id:
        print("❌ GCP_PROJECT_ID environment variable must be set")
        return False
    
    client = bigquery.Client(project=project_id)
    dataset_ref = f"{project_id}.{dataset_id}"
    
    print("\n" + "=" * 70)
    print("🏗️  CREATING BIGQUERY TABLES")
    print("=" * 70)
    print(f"Project: {project_id}")
    print(f"Dataset: {dataset_id}")
    print("=" * 70 + "\n")
    
    # Create dataset if it doesn't exist
    try:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Change if needed
        client.create_dataset(dataset, timeout=30)
        print(f"✅ Created dataset: {dataset_id}")
    except Conflict:
        print(f"✅ Dataset already exists: {dataset_id}")
    except Exception as e:
        print(f"⚠️  Dataset check: {e}")
    
    print()
    
    # Table schemas
    tables = {
        'upi_bank': [
            bigquery.SchemaField("bank_code", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("bank_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("iin", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("is_active", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
        ],
        'upi_customer': [
            bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("mobile_number", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("primary_vpa", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("bank_account_no", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ifsc_code", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
        ],
        'upi_customer_credentials': [
            bigquery.SchemaField("credential_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("pin_hash", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("is_active", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
        ],
        'upi_merchant': [
            bigquery.SchemaField("merchant_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("merchant_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("merchant_vpa", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("settlement_account_no", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ifsc_code", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
        ],
        'upi_transaction': [
            bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("upi_txn_ref", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("payer_vpa", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("payee_vpa", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("payer_bank_code", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("payee_bank_code", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("amount", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("transaction_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("initiated_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("completed_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("failure_reason", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("merchant_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("remarks", "STRING", mode="NULLABLE"),
        ],
        'upi_transaction_audit': [
            bigquery.SchemaField("audit_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("old_status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("new_status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("changed_by", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("change_timestamp", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("comments", "STRING", mode="NULLABLE"),
        ],
        'upi_refund': [
            bigquery.SchemaField("refund_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("original_txn_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("refund_amount", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("refund_reason", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("processed_at", "TIMESTAMP", mode="NULLABLE"),
        ],
    }
    
    # Create tables
    created_count = 0
    existing_count = 0
    
    for table_name, schema in tables.items():
        table_id = f"{dataset_ref}.{table_name}"
        table = bigquery.Table(table_id, schema=schema)
        
        try:
            client.create_table(table)
            print(f"✅ Created table: {table_name}")
            created_count += 1
        except Conflict:
            print(f"✅ Table already exists: {table_name}")
            existing_count += 1
        except Exception as e:
            print(f"❌ Failed to create table {table_name}: {e}")
            return False
    
    print("\n" + "=" * 70)
    print("✅ TABLE CREATION COMPLETE")
    print("=" * 70)
    print(f"Created: {created_count}")
    print(f"Already existed: {existing_count}")
    print(f"Total tables: {len(tables)}")
    print("=" * 70 + "\n")
    
    return True


if __name__ == "__main__":
    success = create_tables()
    exit(0 if success else 1)