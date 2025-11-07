"""
Script to populate BigQuery tables with 500 customers and 1000 transactions.
Schema matches exactly what's needed for the MCP server.
"""

import config
from google.cloud import bigquery
from datetime import datetime, timedelta
import random
import time

def populate_customers(client, num_customers=500):
    """Populate customers table with sample data."""
    table_id = f"{config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET}.customers"

    # Drop and recreate table
    schema = [
        bigquery.SchemaField("customer_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("customer_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("email", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("phone_number", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("customer_since", "DATE", mode="REQUIRED"),
    ]

    try:
        client.delete_table(table_id)
        print(f"Deleted existing customers table")
    except Exception:
        pass

    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table)
    print(f"Created customers table")

    # Wait for table to be ready
    time.sleep(2)

    # Generate customer data
    first_names = [
        "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
        "William", "Barbara", "David", "Elizabeth", "Richard", "Susan", "Joseph", "Jessica",
        "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
        "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
        "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
        "Kenneth", "Dorothy", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa",
        "Timothy", "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon",
        "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy",
        "Nicholas", "Shirley", "Eric", "Angela", "Jonathan", "Helen", "Stephen", "Anna",
        "Larry", "Brenda", "Justin", "Pamela", "Scott", "Nicole", "Brandon", "Emma",
        "Benjamin", "Samantha", "Samuel", "Katherine", "Raymond", "Christine", "Gregory", "Debra",
        "Frank", "Rachel", "Alexander", "Catherine", "Patrick", "Carolyn", "Jack", "Janet",
        "Dennis", "Ruth", "Jerry", "Maria", "Tyler", "Heather", "Aaron", "Diane"
    ]

    last_names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
        "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas",
        "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
        "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young",
        "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
        "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
        "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker",
        "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales", "Murphy",
        "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson", "Bailey",
        "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson",
        "Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray", "Mendoza",
        "Ruiz", "Hughes", "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers"
    ]

    cities_states = [
        ("New York", "NY", "10001"), ("Los Angeles", "CA", "90001"), ("Chicago", "IL", "60601"),
        ("Houston", "TX", "77001"), ("Phoenix", "AZ", "85001"), ("Philadelphia", "PA", "19019"),
        ("San Antonio", "TX", "78201"), ("San Diego", "CA", "92101"), ("Dallas", "TX", "75201"),
        ("San Jose", "CA", "95101"), ("Austin", "TX", "73301"), ("Jacksonville", "FL", "32099"),
        ("Fort Worth", "TX", "76101"), ("Columbus", "OH", "43004"), ("San Francisco", "CA", "94102"),
        ("Charlotte", "NC", "28201"), ("Indianapolis", "IN", "46201"), ("Seattle", "WA", "98101"),
        ("Denver", "CO", "80201"), ("Washington", "DC", "20001"), ("Boston", "MA", "02101"),
        ("El Paso", "TX", "79901"), ("Detroit", "MI", "48201"), ("Nashville", "TN", "37201"),
        ("Portland", "OR", "97201"), ("Memphis", "TN", "37501"), ("Oklahoma City", "OK", "73101"),
        ("Las Vegas", "NV", "89101"), ("Louisville", "KY", "40201"), ("Baltimore", "MD", "21201")
    ]

    streets = [
        "Main St", "Oak Ave", "Pine Rd", "Elm St", "Maple Dr", "Cedar Ln", "Washington Blvd",
        "Park Ave", "Broadway", "Lincoln St", "Jefferson Dr", "Madison Ave", "Adams St",
        "First St", "Second Ave", "Third Rd", "Market St", "Church St", "Spring St", "Hill Rd"
    ]

    rows_to_insert = []
    print(f"Generating {num_customers} customer records...")

    for i in range(1, num_customers + 1):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        customer_name = f"{first_name} {last_name}"

        city, state, zip_code = random.choice(cities_states)
        street_num = random.randint(100, 9999)
        street = random.choice(streets)
        address = f"{street_num} {street}, {city}, {state} {zip_code}"

        # Random customer_since date between 2018 and 2024
        days_since = random.randint(365, 365*7)
        customer_since = (datetime.now() - timedelta(days=days_since)).date().isoformat()

        rows_to_insert.append({
            "customer_id": i,
            "customer_name": customer_name,
            "email": f"{first_name.lower()}.{last_name.lower()}{i}@example.com",
            "phone_number": f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
            "address": address,
            "customer_since": customer_since
        })

    # Insert using load job
    print("Inserting customer records...")
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = client.load_table_from_json(rows_to_insert, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f"✓ Successfully inserted {len(rows_to_insert)} customers")


def populate_transactions(client, num_transactions=1000, num_customers=500):
    """Populate transactions table with sample data."""
    table_id = f"{config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET}.transactions"

    # Drop and recreate table
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

    try:
        client.delete_table(table_id)
        print(f"Deleted existing transactions table")
    except Exception:
        pass

    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table)
    print(f"Created transactions table")

    # Wait for table to be ready
    time.sleep(2)

    # Generate transaction data
    transaction_types = ["Credit", "Debit"]
    account_types = ["Savings", "Checking"]

    counterparties = [
        "Amazon", "Walmart", "Starbucks", "Target", "Netflix", "Spotify", "Apple",
        "Uber", "Lyft", "DoorDash", "GrubHub", "Electric Company", "Water Utility",
        "Gas Company", "Internet Provider", "Verizon", "AT&T", "T-Mobile",
        "John Doe", "Jane Smith", "Bob Johnson", "Alice Williams", "ABC Corp",
        "XYZ Ltd", "Global Inc", "Tech Solutions", "Best Buy", "Home Depot",
        "CVS Pharmacy", "Walgreens", "Whole Foods", "Costco", "Sam's Club"
    ]

    # Create account mapping for customers (each customer has 1-2 accounts)
    customer_accounts = {}
    for customer_id in range(1, num_customers + 1):
        num_accounts = random.randint(1, 2)
        accounts = []
        for _ in range(num_accounts):
            account_num = f"ACC{customer_id:05d}{random.randint(1000, 9999)}"
            account_type = random.choice(account_types)
            accounts.append((account_num, account_type))
        customer_accounts[customer_id] = accounts

    rows_to_insert = []
    print(f"Generating {num_transactions} transaction records...")

    for transaction_id in range(1, num_transactions + 1):
        # Random customer
        customer_id = random.randint(1, num_customers)

        # Get one of their accounts
        account_num, account_type = random.choice(customer_accounts[customer_id])

        # Transaction details
        trans_type = random.choice(transaction_types)
        amount = round(random.uniform(5.0, 5000.0), 2)

        # Random timestamp in the last 12 months
        days_ago = random.randint(0, 365)
        hours_ago = random.randint(0, 23)
        minutes_ago = random.randint(0, 59)
        trans_time = datetime.now() - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)

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

    # Insert using load job
    print("Inserting transaction records...")
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = client.load_table_from_json(rows_to_insert, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f"✓ Successfully inserted {len(rows_to_insert)} transactions")


def main():
    """Main function to populate BigQuery tables."""
    print("=" * 60)
    print("BigQuery Table Population")
    print("=" * 60)

    print(f"\nProject ID: {config.GCP_PROJECT_ID}")
    print(f"Dataset: {config.BIGQUERY_DATASET}")
    print(f"Target: 500 customers, 1000 transactions\n")

    # Initialize BigQuery client
    client = bigquery.Client(project=config.GCP_PROJECT_ID)

    # Populate customers
    print("Step 1: Populating customers table...")
    populate_customers(client, num_customers=500)

    # Populate transactions
    print("\nStep 2: Populating transactions table...")
    populate_transactions(client, num_transactions=1000, num_customers=500)

    print("\n" + "=" * 60)
    print("Population completed successfully!")
    print("=" * 60)

    # Verify counts
    print("\nVerifying counts...")

    query = f"SELECT COUNT(*) as count FROM `{config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET}.customers`"
    result = client.query(query).to_dataframe()
    print(f"  Customers: {result['count'].iloc[0]}")

    query = f"SELECT COUNT(*) as count FROM `{config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET}.transactions`"
    result = client.query(query).to_dataframe()
    print(f"  Transactions: {result['count'].iloc[0]}")

    print("\nReady to use!")


if __name__ == "__main__":
    main()
