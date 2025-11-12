"""
Configuration for UPI Data Generation - BigQuery Direct Mode - TEST VERSION
Uses smaller dataset for testing
"""

# Import the main config for reusing constants
from upi_data_gen_config_bq import (
    INDIAN_BANKS,
    FIRST_NAMES,
    LAST_NAMES,
    MERCHANT_CATEGORIES,
    FAILURE_REASONS,
    REFUND_REASONS
)

# ===== DATA GENERATION CONFIG - TEST MODE =====
DATA_CONFIG = {
    # Number of records for each table (REDUCED for testing)
    'num_banks': 10,
    'num_customers': 100,  # 100 customers for testing
    'num_merchants': 20,   # 20 merchants for testing
    'num_transactions': 500,  # 500 transactions for testing

    # Transaction distribution
    'merchant_transaction_percentage': 70,  # 70% transactions to merchants, 30% P2P

    # Transaction type distribution (percentages)
    'transaction_type_distribution': {
        'DEBIT': 50,    # Customer paying
        'CREDIT': 30,   # Customer receiving
        'REFUND': 10,   # Refunds
        'TRANSFER': 10  # P2P transfers
    },

    # Transaction status distribution (percentages)
    'transaction_status_distribution': {
        'SUCCESS': 85,
        'FAILED': 10,
        'PENDING': 3,
        'REVERSED': 2
    },

    # Audit and refund percentages
    'audit_percentage': 15,     # 15% of transactions have audit records
    'refund_percentage': 5,     # 5% of transactions have refunds

    # Batch size for bulk inserts (BigQuery streaming inserts)
    'batch_size': 100,  # Smaller batch size for testing
}
