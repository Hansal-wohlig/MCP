
#!/usr/bin/env python3
"""
UPI Data Generation - Direct to BigQuery - TEST MODE
Uses smaller dataset for testing
"""

# Import the main generator class
from generate_upi_bigquery_direct import UPIBigQueryGenerator

# Import test configuration
import upi_data_gen_config_bq_test as config_module

def main():
    print("\n" + "=" * 70)
    print("⚡ RUNNING IN TEST MODE - BIGQUERY DIRECT")
    print("=" * 70)
    print(f"Transactions: {config_module.DATA_CONFIG['num_transactions']:,}")
    print(f"Customers: {config_module.DATA_CONFIG['num_customers']:,}")
    print(f"Merchants: {config_module.DATA_CONFIG['num_merchants']:,}")
    print(f"Banks: {config_module.DATA_CONFIG['num_banks']:,}")
    print("=" * 70 + "\n")

    # Pass test configuration to the generator
    generator = UPIBigQueryGenerator(data_config=config_module.DATA_CONFIG)
    generator.run()


if __name__ == "__main__":
    main()
