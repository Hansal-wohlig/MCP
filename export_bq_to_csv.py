#!/usr/bin/env python3
"""
Export UPI Banking Tables from BigQuery to CSV
"""

import os
from google.cloud import bigquery
import csv

def export_table_to_csv_formatted(client, dataset_ref, table_name, output_file, order_by_column):
    """Export a BigQuery table to a formatted CSV file with quotes."""
    table_id = f"{dataset_ref}.{table_name}"
    
    print(f"\nExporting {table_name}...")
    
    # Query with ORDER BY to ensure consistent ordering
    query = f"SELECT * FROM `{table_id}` ORDER BY {order_by_column}"
    
    try:
        # Run query and get results
        query_job = client.query(query)
        results = query_job.result()
        
        # Create data directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Get column names
        columns = [field.name for field in results.schema]
        
        # Write to CSV with quotes around all fields
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            
            # Write header
            writer.writerow(columns)
            
            # Write data rows
            row_count = 0
            for row in results:
                # Convert row to list, handling different data types
                row_data = []
                for value in row.values():
                    if value is None:
                        row_data.append('')
                    elif hasattr(value, 'isoformat'):  # Date/Datetime/Timestamp
                        row_data.append(value.isoformat())
                    else:
                        row_data.append(str(value))
                
                writer.writerow(row_data)
                row_count += 1
        
        print(f"✓ Exported {row_count:,} rows to {output_file}")
        
        # Show preview (only if file has data)
        if row_count > 0:
            print(f"\nPreview of {os.path.basename(output_file)} (first 3 rows):")
            with open(output_file, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i < 4:  # Header + 3 rows
                        # Truncate long lines for display
                        display_line = line.rstrip()
                        if len(display_line) > 120:
                            display_line = display_line[:117] + "..."
                        print(f"  {display_line}")
                    else:
                        break
        
        return True
        
    except Exception as e:
        print(f"✗ Error exporting {table_name}: {str(e)}")
        return False

def main():
    print("=" * 70)
    print("📊 UPI Banking System - BigQuery to CSV Export")
    print("=" * 70)
    
    # Get configuration from environment
    project_id = os.environ.get('GCP_PROJECT_ID')
    dataset_id = os.environ.get('BIGQUERY_DATASET', 'upi_banking')
    
    if not project_id:
        print("\n❌ Error: GCP_PROJECT_ID environment variable must be set")
        return
    
    print(f"\nProject: {project_id}")
    print(f"Dataset: {dataset_id}")
    
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    dataset_ref = f"{project_id}.{dataset_id}"
    
    # Define tables to export with their ordering column
    tables_config = [
        {
            'name': 'upi_bank',
            'output': 'data1/upi_banks.csv',
            'order_by': 'bank_code',
            'description': 'Bank master data'
        },
        {
            'name': 'upi_customer',
            'output': 'data1/upi_customers.csv',
            'order_by': 'created_at',
            'description': 'Customer information'
        },
        {
            'name': 'upi_customer_credentials',
            'output': 'data1/upi_customer_credentials.csv',
            'order_by': 'customer_id',
            'description': 'Customer authentication credentials'
        },
        {
            'name': 'upi_merchant',
            'output': 'data1/upi_merchants.csv',
            'order_by': 'created_at',
            'description': 'Merchant information'
        },
        {
            'name': 'upi_transaction',
            'output': 'data1/upi_transactions.csv',
            'order_by': 'initiated_at',
            'description': 'UPI transaction records'
        },
        {
            'name': 'upi_transaction_audit',
            'output': 'data1/upi_transaction_audit.csv',
            'order_by': 'change_timestamp',
            'description': 'Transaction audit trail'
        },
        {
            'name': 'upi_refund',
            'output': 'data1/upi_refunds.csv',
            'order_by': 'processed_at',
            'description': 'Refund transactions'
        }
    ]
    
    print(f"\nExporting {len(tables_config)} tables with formatted output...\n")
    print("-" * 70)
    
    # Export each table
    success_count = 0
    failed_tables = []
    
    for table_config in tables_config:
        print(f"\n📋 {table_config['description']}")
        if export_table_to_csv_formatted(
            client, 
            dataset_ref,
            table_config['name'], 
            table_config['output'],
            table_config['order_by']
        ):
            success_count += 1
        else:
            failed_tables.append(table_config['name'])
        print("-" * 70)
    
    print("\n" + "=" * 70)
    print(f"✅ Export completed! ({success_count}/{len(tables_config)} tables)")
    print("=" * 70)
    
    # Show file locations and statistics
    print("\n📁 Exported files:\n")
    total_size = 0
    total_rows = 0
    
    for table_config in tables_config:
        output_file = table_config['output']
        if os.path.exists(output_file):
            size = os.path.getsize(output_file)
            total_size += size
            
            with open(output_file, 'r') as f:
                line_count = sum(1 for _ in f) - 1  # Exclude header
            total_rows += line_count
            
            # Format size nicely
            if size < 1024:
                size_str = f"{size} B"
            elif size < 1024 * 1024:
                size_str = f"{size / 1024:.1f} KB"
            else:
                size_str = f"{size / (1024 * 1024):.1f} MB"
            
            print(f"  ✓ {output_file}")
            print(f"    Size: {size_str} | Rows: {line_count:,}")
            print()
    
    # Summary
    print("=" * 70)
    print("📊 Export Summary")
    print("=" * 70)
    print(f"Total files: {success_count}")
    print(f"Total rows: {total_rows:,}")
    
    if total_size < 1024 * 1024:
        print(f"Total size: {total_size / 1024:.1f} KB")
    else:
        print(f"Total size: {total_size / (1024 * 1024):.1f} MB")
    
    if failed_tables:
        print(f"\n⚠️  Failed tables: {', '.join(failed_tables)}")
    
    print("=" * 70)

if __name__ == "__main__":
    main()