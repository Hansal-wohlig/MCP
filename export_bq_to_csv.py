import config
import os
from google.cloud import bigquery
import csv

def export_table_to_csv_formatted(client, table_name, output_file, order_by_column):
    """Export a BigQuery table to a formatted CSV file with quotes."""
    dataset_id = f"{config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET}"
    table_id = f"{dataset_id}.{table_name}"
    
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
        
        print(f"✓ Exported {row_count} rows to {output_file}")
        
        # Show preview
        print(f"\nPreview of {os.path.basename(output_file)} (first 3 rows):")
        with open(output_file, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i < 4:  # Header + 3 rows
                    print(f"  {line.rstrip()}")
                else:
                    break
        
        return True
        
    except Exception as e:
        print(f"✗ Error exporting {table_name}: {str(e)}")
        return False

def main():
    print("=" * 70)
    print("BigQuery to Formatted CSV Export")
    print("=" * 70)
    
    # Initialize BigQuery client
    client = bigquery.Client(project=config.GCP_PROJECT_ID)
    
    # Define tables to export with their ordering column
    tables_config = [
        {
            'name': 'customers',
            'output': 'data2/customers.csv',
            'order_by': 'customer_id'
        },
        {
            'name': 'transactions',
            'output': 'data2/transactions.csv',
            'order_by': 'transaction_id'
        },
        {
            'name': 'employees',
            'output': 'data2/emplyees.csv',
            'order_by': 'employee_id'
        } 
    ]
    
    print(f"\nExporting {len(tables_config)} tables with formatted output...\n")
    
    # Export each table
    success_count = 0
    for table_config in tables_config:
        if export_table_to_csv_formatted(
            client, 
            table_config['name'], 
            table_config['output'],
            table_config['order_by']
        ):
            success_count += 1
    
    print("\n" + "=" * 70)
    print(f"✓ Export completed! ({success_count}/{len(tables_config)} tables)")
    print("=" * 70)
    
    # Show file locations
    print("\nExported files:")
    for table_config in tables_config:
        output_file = table_config['output']
        if os.path.exists(output_file):
            size = os.path.getsize(output_file)
            with open(output_file, 'r') as f:
                line_count = sum(1 for _ in f)
            print(f"  - {output_file}")
            print(f"    Size: {size:,} bytes | Rows: {line_count - 1} (excluding header)")

if __name__ == "__main__":
    main()