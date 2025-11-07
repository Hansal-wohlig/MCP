"""
Script to create BigQuery dataset before populating tables.
"""

import config
from google.cloud import bigquery

def create_dataset():
    """Create BigQuery dataset if it doesn't exist."""
    print("=" * 60)
    print("BigQuery Dataset Creation")
    print("=" * 60)
    
    # Set default location if not in config
    if not hasattr(config, 'GCP_LOCATION'):
        config.GCP_LOCATION = "us-central1"
        print(f"⚠️  GCP_LOCATION not set in config. Using default: {config.GCP_LOCATION}")
    
    print(f"\nProject ID: {config.GCP_PROJECT_ID}")
    print(f"Dataset: {config.BIGQUERY_DATASET}")
    print(f"Location: {config.GCP_LOCATION}\n")
    
    # Initialize BigQuery client
    client = bigquery.Client(project=config.GCP_PROJECT_ID)
    
    # Construct dataset ID
    dataset_id = f"{config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET}"
    
    # Check if dataset exists
    try:
        client.get_dataset(dataset_id)
        print(f"✓ Dataset '{config.BIGQUERY_DATASET}' already exists")
        
        # Ask user if they want to proceed
        response = input("\nDataset exists. Do you want to continue anyway? (y/n): ")
        if response.lower() != 'y':
            print("Exiting...")
            return
            
    except Exception:
        print(f"Dataset '{config.BIGQUERY_DATASET}' does not exist. Creating...")
        
        # Create dataset
        dataset = bigquery.Dataset(dataset_id)
        
        # Set dataset location
        dataset.location = config.GCP_LOCATION
        
        # Optional: Set dataset description
        dataset.description = "Dataset for MCP server with customer and transaction data"
        
        # Optional: Set default table expiration (None = no expiration)
        # dataset.default_table_expiration_ms = 365 * 24 * 60 * 60 * 1000  # 1 year
        
        # Create the dataset
        dataset = client.create_dataset(dataset, timeout=30)
        
        print(f"✓ Created dataset '{dataset.project}.{dataset.dataset_id}'")
        print(f"  Location: {dataset.location}")
    
    print("\n" + "=" * 60)
    print("Dataset setup completed!")
    print("=" * 60)
    print("\nYou can now run: python populate_tables.py")


if __name__ == "__main__":
    create_dataset()