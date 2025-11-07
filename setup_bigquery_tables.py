"""
Script to create BigQuery tables and insert sample data for the MindGate project.
This creates two tables: customers and transactions.
"""

import config
from google.cloud import bigquery
from datetime import datetime, timedelta
import random

def create_dataset_if_not_exists(client, dataset_id):
    """Create the dataset if it doesn't exist."""
    dataset_ref = f"{config.GCP_PROJECT_ID}.{dataset_id}"

    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_ref} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset, timeout=30)
        print(f"Created dataset {dataset_ref}")

def create_customers_table(client):
    """Create the customers table."""
    table_id = f"{config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET}.customers"

    schema = [
        bigquery.SchemaField("customer_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("customer_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("email", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("phone_number", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("customer_since", "DATE", mode="REQUIRED"),
    ]

    table = bigquery.Table(table_id, schema=schema)

    try:
        client.delete_table(table_id)
        print(f"Deleted existing table {table_id}")
    except Exception:
        pass

    table = client.create_table(table)
    print(f"Created table {table_id}")

    # Insert sample customer data
    rows_to_insert = [
        {
            "customer_id": 1,
            "customer_name": "Tony Toy",
            "email": "tony.toy@example.com",
            "phone_number": "+1-555-0101",
            "address": "123 Main St, New York, NY 10001",
            "customer_since": "2020-01-15"
        },
        {
            "customer_id": 2,
            "customer_name": "Sarah Smith",
            "email": "sarah.smith@example.com",
            "phone_number": "+1-555-0102",
            "address": "456 Oak Ave, Los Angeles, CA 90001",
            "customer_since": "2020-03-20"
        },
        {
            "customer_id": 3,
            "customer_name": "Michael Johnson",
            "email": "michael.j@example.com",
            "phone_number": "+1-555-0103",
            "address": "789 Pine Rd, Chicago, IL 60601",
            "customer_since": "2020-05-10"
        },
        {
            "customer_id": 4,
            "customer_name": "Emily Davis",
            "email": "emily.davis@example.com",
            "phone_number": "+1-555-0104",
            "address": "321 Elm St, Houston, TX 77001",
            "customer_since": "2020-07-25"
        },
        {
            "customer_id": 5,
            "customer_name": "Robert Wilson",
            "email": "robert.w@example.com",
            "phone_number": "+1-555-0105",
            "address": "654 Maple Dr, Phoenix, AZ 85001",
            "customer_since": "2020-09-12"
        }
    ]

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print(f"Errors inserting customers: {errors}")
    else:
        print(f"Successfully inserted {len(rows_to_insert)} customer records")

def create_transactions_table(client):
    """Create the transactions table."""
    table_id = f"{config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET}.transactions"

    schema = [
        bigquery.SchemaField("transaction_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("customer_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("account_number", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("account_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("transaction_timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("transaction_amount", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("transaction_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("counterparty_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("counterparty_account", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id, schema=schema)

    try:
        client.delete_table(table_id)
        print(f"Deleted existing table {table_id}")
    except Exception:
        pass

    table = client.create_table(table)
    print(f"Created table {table_id}")

    # Generate sample transaction data
    transaction_types = ["Credit", "Debit"]
    account_types = ["Savings", "Checking"]

    counterparties = [
        "Amazon", "Walmart", "Starbucks", "Target", "Netflix",
        "Uber", "DoorDash", "Electric Company", "Water Utility",
        "John Doe", "Jane Smith", "ABC Corp", "XYZ Ltd"
    ]

    rows_to_insert = []
    transaction_id = 1

    # Generate transactions for each customer
    for customer_id in range(1, 6):
        num_transactions = random.randint(8, 15)
        account_num = f"ACC{customer_id:04d}{random.randint(1000, 9999)}"
        account_type = random.choice(account_types)

        for _ in range(num_transactions):
            trans_type = random.choice(transaction_types)
            amount = round(random.uniform(10.0, 5000.0), 2)

            # Random date in the last 6 months
            days_ago = random.randint(0, 180)
            trans_time = datetime.now() - timedelta(days=days_ago, hours=random.randint(0, 23))

            rows_to_insert.append({
                "transaction_id": transaction_id,
                "customer_id": customer_id,
                "account_number": account_num,
                "account_type": account_type,
                "transaction_timestamp": trans_time.isoformat(),
                "transaction_amount": amount,
                "transaction_type": trans_type,
                "counterparty_name": random.choice(counterparties),
                "counterparty_account": f"EXT{random.randint(100000, 999999)}"
            })
            transaction_id += 1

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print(f"Errors inserting transactions: {errors}")
    else:
        print(f"Successfully inserted {len(rows_to_insert)} transaction records")

def main():
    """Main function to set up BigQuery tables."""
    print("=" * 60)
    print("BigQuery Table Setup for MindGate")
    print("=" * 60)

    print(f"\nProject ID: {config.GCP_PROJECT_ID}")
    print(f"Dataset: {config.BIGQUERY_DATASET}")
    print(f"Service Account: {config.SERVICE_ACCOUNT_KEY_FILE}\n")

    # Initialize BigQuery client
    client = bigquery.Client(project=config.GCP_PROJECT_ID)

    # Create dataset
    print("Step 1: Creating dataset...")
    create_dataset_if_not_exists(client, config.BIGQUERY_DATASET)

    # Create customers table
    print("\nStep 2: Creating customers table...")
    create_customers_table(client)

    # Create transactions table
    print("\nStep 3: Creating transactions table...")
    create_transactions_table(client)

    print("\n" + "=" * 60)
    print("Setup completed successfully!")
    print("=" * 60)

    # Query to verify
    print("\nVerifying data...")

    query = f"""
    SELECT
        COUNT(*) as customer_count
    FROM `{config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET}.customers`
    """
    result = client.query(query).to_dataframe()
    print(f"Total customers: {result['customer_count'].iloc[0]}")

    query = f"""
    SELECT
        COUNT(*) as transaction_count
    FROM `{config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET}.transactions`
    """
    result = client.query(query).to_dataframe()
    print(f"Total transactions: {result['transaction_count'].iloc[0]}")

    print("\nYou can now run your MCP server with these tables!")

if __name__ == "__main__":
    main()
