#!/usr/bin/env python3
"""
Create Sample Table in BigQuery
Creates a sample table with 500 realistic entries for testing
"""

import config
from google.cloud import bigquery
from datetime import datetime, timedelta
import random
from faker import Faker

# Initialize Faker for realistic data generation
fake = Faker()

def create_sample_table():
    """Create a sample table with 500 entries in BigQuery"""
    
    print("\n" + "=" * 60)
    print("📊 CREATING SAMPLE TABLE IN BIGQUERY")
    print("=" * 60)
    print(f"Project: {config.GCP_PROJECT_ID}")
    print(f"Dataset: {config.BIGQUERY_DATASET}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60 + "\n")
    
    # Initialize BigQuery client
    client = bigquery.Client(project=config.GCP_PROJECT_ID)
    
    # Define table name
    table_name = "employees"
    table_id = f"{config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET}.{table_name}"
    
    # Define table schema
    schema = [
        bigquery.SchemaField("employee_id", "STRING", mode="REQUIRED", description="Unique employee identifier"),
        bigquery.SchemaField("employee_name", "STRING", mode="REQUIRED", description="Full name of the employee"),
        bigquery.SchemaField("email", "STRING", mode="REQUIRED", description="Employee email address"),
        bigquery.SchemaField("phone_number", "STRING", mode="NULLABLE", description="Employee phone number"),
        bigquery.SchemaField("department", "STRING", mode="REQUIRED", description="Department name"),
        bigquery.SchemaField("position", "STRING", mode="REQUIRED", description="Job title/position"),
        bigquery.SchemaField("salary", "FLOAT64", mode="REQUIRED", description="Annual salary in USD"),
        bigquery.SchemaField("hire_date", "DATE", mode="REQUIRED", description="Date when employee was hired"),
        bigquery.SchemaField("manager_id", "STRING", mode="NULLABLE", description="Employee ID of direct manager"),
        bigquery.SchemaField("office_location", "STRING", mode="REQUIRED", description="Office city location"),
        bigquery.SchemaField("employment_status", "STRING", mode="REQUIRED", description="Current employment status"),
        bigquery.SchemaField("years_of_experience", "INTEGER", mode="REQUIRED", description="Total years of work experience"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", description="Record creation timestamp"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED", description="Record last update timestamp"),
    ]
    
    # Check if table exists and delete it
    try:
        client.get_table(table_id)
        print(f"⚠️  Table {table_name} already exists. Deleting...")
        client.delete_table(table_id)
        print(f"✓ Table deleted successfully\n")
    except Exception:
        print(f"✓ Table {table_name} does not exist. Creating new...\n")
    
    # Create the table
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(f"✓ Created table {table.project}.{table.dataset_id}.{table.table_id}\n")
    
    # Generate 500 sample records
    print("📝 Generating 500 sample records...")
    
    departments = [
        "Engineering", "Product", "Sales", "Marketing", 
        "Human Resources", "Finance", "Operations", "Customer Success",
        "Data Science", "Design", "Legal", "Security"
    ]
    
    positions = {
        "Engineering": ["Software Engineer", "Senior Engineer", "Engineering Manager", "Tech Lead", "DevOps Engineer"],
        "Product": ["Product Manager", "Senior PM", "Product Owner", "VP Product"],
        "Sales": ["Sales Rep", "Account Executive", "Sales Manager", "VP Sales"],
        "Marketing": ["Marketing Manager", "Content Writer", "SEO Specialist", "CMO"],
        "Human Resources": ["HR Manager", "Recruiter", "HR Business Partner", "VP HR"],
        "Finance": ["Accountant", "Financial Analyst", "Finance Manager", "CFO"],
        "Operations": ["Operations Manager", "Program Manager", "Operations Analyst"],
        "Customer Success": ["Customer Success Manager", "Support Engineer", "VP Customer Success"],
        "Data Science": ["Data Scientist", "ML Engineer", "Data Analyst", "Head of Data"],
        "Design": ["UI Designer", "UX Designer", "Design Lead", "Creative Director"],
        "Legal": ["Legal Counsel", "Compliance Officer", "General Counsel"],
        "Security": ["Security Engineer", "Security Analyst", "CISO"]
    }
    
    office_locations = [
        "San Francisco", "New York", "Austin", "Seattle", 
        "Boston", "Chicago", "Los Angeles", "Denver",
        "Portland", "Miami", "Remote"
    ]
    
    employment_statuses = ["Active", "Active", "Active", "Active", "Active", "On Leave", "Active"]
    
    rows_to_insert = []
    
    # Generate manager IDs pool (first 50 employees will be potential managers)
    manager_ids = [f"EMP{str(i).zfill(5)}" for i in range(1, 51)]
    
    for i in range(1, 501):
        employee_id = f"EMP{str(i).zfill(5)}"
        department = random.choice(departments)
        position = random.choice(positions[department])
        
        # First 50 employees are more likely to be managers (no manager_id)
        if i <= 50:
            manager_id = None if random.random() < 0.7 else random.choice(manager_ids[:i] if i > 1 else [None])
        else:
            manager_id = random.choice(manager_ids)
        
        # Salary based on position level
        base_salary = 60000
        if "Senior" in position or "Lead" in position:
            base_salary = 120000
        elif "Manager" in position:
            base_salary = 100000
        elif "VP" in position or "Head" in position:
            base_salary = 180000
        elif any(title in position for title in ["CFO", "CMO", "CTO", "CISO", "General Counsel"]):
            base_salary = 250000
        
        salary = base_salary + random.uniform(-10000, 30000)
        
        # Hire date between 6 months and 10 years ago
        days_ago = random.randint(180, 3650)
        hire_date = (datetime.now() - timedelta(days=days_ago)).date()
        
        # Years of experience
        years_of_experience = random.randint(1, 25)
        
        # Timestamps
        created_at = datetime.now() - timedelta(days=random.randint(1, 365))
        updated_at = created_at + timedelta(days=random.randint(0, 30))
        
        row = {
            "employee_id": employee_id,
            "employee_name": fake.name(),
            "email": f"{fake.user_name()}@company.com",
            "phone_number": fake.phone_number(),
            "department": department,
            "position": position,
            "salary": round(salary, 2),
            "hire_date": hire_date.isoformat(),
            "manager_id": manager_id,
            "office_location": random.choice(office_locations),
            "employment_status": random.choice(employment_statuses),
            "years_of_experience": years_of_experience,
            "created_at": created_at.isoformat(),
            "updated_at": updated_at.isoformat(),
        }
        
        rows_to_insert.append(row)
    
    print(f"✓ Generated {len(rows_to_insert)} records\n")
    
    # Insert data in batches
    print("💾 Inserting data into BigQuery...")
    batch_size = 100
    total_inserted = 0
    
    for i in range(0, len(rows_to_insert), batch_size):
        batch = rows_to_insert[i:i + batch_size]
        errors = client.insert_rows_json(table_id, batch)
        
        if errors:
            print(f"❌ Errors inserting batch {i//batch_size + 1}: {errors}")
        else:
            total_inserted += len(batch)
            print(f"   ✓ Batch {i//batch_size + 1}: Inserted {len(batch)} rows (Total: {total_inserted})")
    
    print(f"\n{'='*60}")
    print(f"✅ TABLE CREATION COMPLETED")
    print(f"{'='*60}")
    print(f"Table: {table_name}")
    print(f"Records inserted: {total_inserted}")
    print(f"Location: {table.project}.{table.dataset_id}.{table.table_id}")
    print(f"{'='*60}\n")
    
    # Display sample query
    print("📋 Sample queries to test:")
    print(f"   SELECT * FROM `{table_id}` LIMIT 10;")
    print(f"   SELECT department, COUNT(*) as count FROM `{table_id}` GROUP BY department;")
    print(f"   SELECT AVG(salary) as avg_salary FROM `{table_id}`;")
    print()


def main():
    """Main function"""
    try:
        create_sample_table()
        return 0
    except Exception as e:
        print("\n" + "=" * 60)
        print("❌ TABLE CREATION FAILED")
        print("=" * 60)
        print(f"Error: {str(e)}")
        print("=" * 60 + "\n")
        
        import traceback
        traceback.print_exc()
        
        return 1


if __name__ == "__main__":
    import sys
    exit_code = main()
    sys.exit(exit_code)