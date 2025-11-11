#!/usr/bin/env python3
"""
UPI Data Generation Script - TEST MODE
Uses smaller dataset for testing
"""

# Import the main generator class
from generate_upi_data import UPIDataGenerator

# Override the config import
import sys
import upi_data_gen_config_test as config_module

# Monkey patch the config in the main module
sys.modules['upi_data_gen_config'].DATA_CONFIG = config_module.DATA_CONFIG
sys.modules['upi_data_gen_config'].OUTPUT_CONFIG = config_module.OUTPUT_CONFIG

def main():
    print("\n" + "=" * 70)
    print("⚡ RUNNING IN TEST MODE")
    print("=" * 70)
    print(f"Transactions: {config_module.DATA_CONFIG['num_transactions']:,}")
    print(f"Customers: {config_module.DATA_CONFIG['num_customers']:,}")
    print(f"Merchants: {config_module.DATA_CONFIG['num_merchants']:,}")
    print(f"Banks: {config_module.DATA_CONFIG['num_banks']:,}")
    print("=" * 70 + "\n")

    generator = UPIDataGenerator()
    generator.run()


if __name__ == "__main__":
    main()
