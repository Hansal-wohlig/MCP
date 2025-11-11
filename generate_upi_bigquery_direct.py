#!/usr/bin/env python3
"""
UPI Data Generation - Direct to BigQuery
Generates realistic UPI transaction data and inserts directly into BigQuery
No PostgreSQL required!
"""

import config
from google.cloud import bigquery
import uuid
import random
from datetime import datetime, timedelta
import sys
import hashlib
import time
import upi_data_gen_config_bq

class UPIBigQueryGenerator:
    def __init__(self, data_config=None):
        self.bq_client = None
        self.dataset_id = f"{config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET}"
        self.banks = []
        self.customer_ids = []
        self.customer_vpas = []
        self.merchant_ids = []
        self.merchant_vpas = []
        self.transaction_ids = []

        # Use provided config or default from module
        self.DATA_CONFIG = data_config if data_config is not None else upi_data_gen_config_bq.DATA_CONFIG
        self.INDIAN_BANKS = upi_data_gen_config_bq.INDIAN_BANKS
        self.FIRST_NAMES = upi_data_gen_config_bq.FIRST_NAMES
        self.LAST_NAMES = upi_data_gen_config_bq.LAST_NAMES
        self.MERCHANT_CATEGORIES = upi_data_gen_config_bq.MERCHANT_CATEGORIES
        self.FAILURE_REASONS = upi_data_gen_config_bq.FAILURE_REASONS
        self.REFUND_REASONS = upi_data_gen_config_bq.REFUND_REASONS

    def connect_bigquery(self):
        """Initialize BigQuery client and verify dataset"""
        print("\n" + "=" * 70)
        print("☁️  CONNECTING TO BIGQUERY")
        print("=" * 70)
        try:
            self.bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
            print(f"✓ Connected to BigQuery project: {config.GCP_PROJECT_ID}")

            # Verify dataset exists and is not deleted
            try:
                dataset = self.bq_client.get_dataset(self.dataset_id)
                print(f"✓ Dataset: {config.BIGQUERY_DATASET} (Location: {dataset.location})")
            except Exception as e:
                print(f"⚠️  Dataset issue: {e}")
                print(f"🔧 Recreating dataset: {config.BIGQUERY_DATASET}")

                # Set default location if not in config
                location = getattr(config, 'GCP_LOCATION', 'us-central1')

                # Create dataset
                dataset = bigquery.Dataset(self.dataset_id)
                dataset.location = location
                dataset.description = "Dataset for MCP server with customer and transaction data"

                dataset = self.bq_client.create_dataset(dataset, timeout=30)
                print(f"✓ Created dataset: {dataset.dataset_id} (Location: {dataset.location})")

                # Wait a bit for dataset to be fully ready
                time.sleep(3)

            print("=" * 70 + "\n")
        except Exception as e:
            print(f"❌ Error connecting to BigQuery: {e}")
            sys.exit(1)

    def create_bigquery_tables(self):
        """Create BigQuery tables with appropriate schemas"""
        print("📋 CREATING BIGQUERY TABLES")
        print("-" * 70)

        # First, verify dataset is accessible by listing tables
        try:
            list(self.bq_client.list_tables(self.dataset_id, max_results=1))
            print(f"  ✓ Dataset is accessible")
        except Exception as e:
            print(f"  ⚠️  Dataset accessibility issue: {e}")
            print(f"  🔧 Attempting to fix dataset...")

            # Try to delete and recreate dataset
            try:
                self.bq_client.delete_dataset(
                    self.dataset_id,
                    delete_contents=True,
                    not_found_ok=True
                )
                print(f"  ✓ Deleted old dataset")
                time.sleep(5)  # Wait for deletion to complete

                # Recreate dataset
                location = getattr(config, 'GCP_LOCATION', 'us-central1')
                dataset = bigquery.Dataset(self.dataset_id)
                dataset.location = location
                dataset.description = "Dataset for MCP server with customer and transaction data"
                dataset = self.bq_client.create_dataset(dataset, timeout=30)
                print(f"  ✓ Recreated dataset: {dataset.dataset_id}")
                time.sleep(3)  # Wait for dataset to be ready
            except Exception as recreate_error:
                print(f"  ❌ Failed to recreate dataset: {recreate_error}")
                sys.exit(1)

        tables_schemas = {
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
                bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("pin_hash", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("last_login", "TIMESTAMP", mode="NULLABLE"),
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
                bigquery.SchemaField("amount", "FLOAT64", mode="REQUIRED"),
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
                bigquery.SchemaField("refund_amount", "FLOAT64", mode="REQUIRED"),
                bigquery.SchemaField("refund_reason", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("processed_at", "TIMESTAMP", mode="NULLABLE"),
            ]
        }

        for table_name, schema in tables_schemas.items():
            table_id = f"{self.dataset_id}.{table_name}"

            # Delete table if it exists
            try:
                self.bq_client.get_table(table_id)
                print(f"  ⚠️  Table {table_name} exists. Deleting...")
                self.bq_client.delete_table(table_id)
                print(f"  ✓ Deleted existing table")
            except Exception:
                print(f"  ℹ️  Table {table_name} does not exist. Creating new...")

            # Create new table
            try:
                table = bigquery.Table(table_id, schema=schema)
                table = self.bq_client.create_table(table)
                print(f"  ✓ Created table: {table_name}")
            except Exception as e:
                print(f"  ❌ Error creating table {table_name}: {e}")
                sys.exit(1)

        print("-" * 70 + "\n")

    def wait_for_tables(self):
        """Wait for tables to be fully available in BigQuery"""
        print("⏳ WAITING FOR TABLES TO BE READY")
        print("-" * 70)

        tables_to_check = [
            'upi_bank',
            'upi_customer',
            'upi_customer_credentials',
            'upi_merchant',
            'upi_transaction',
            'upi_transaction_audit',
            'upi_refund'
        ]

        max_retries = 10
        retry_delay = 2  # seconds

        for table_name in tables_to_check:
            table_id = f"{self.dataset_id}.{table_name}"

            for attempt in range(max_retries):
                try:
                    # Try to get the table
                    table = self.bq_client.get_table(table_id)
                    print(f"  ✓ Table {table_name} is ready")
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"  ⏳ Waiting for {table_name} (attempt {attempt + 1}/{max_retries})...")
                        time.sleep(retry_delay)
                    else:
                        print(f"  ❌ Table {table_name} not ready after {max_retries} attempts: {e}")
                        sys.exit(1)

        print("-" * 70 + "\n")

    def weighted_choice(self, choices_dict):
        """Make a weighted random choice based on percentage distribution"""
        choices = list(choices_dict.keys())
        weights = list(choices_dict.values())
        return random.choices(choices, weights=weights, k=1)[0]

    def generate_banks(self):
        """Generate and insert bank records"""
        print("🏦 GENERATING BANK DATA")
        print("-" * 70)

        table_id = f"{self.dataset_id}.upi_bank"
        banks_data = []

        for i, (code, name, iin) in enumerate(self.INDIAN_BANKS):
            if i >= self.DATA_CONFIG['num_banks']:
                break

            self.banks.append(code)

            banks_data.append({
                'bank_code': code,
                'bank_name': name,
                'iin': iin,
                'is_active': True,
                'created_at': datetime.now().isoformat()
            })

        # Insert into BigQuery
        try:
            errors = self.bq_client.insert_rows_json(table_id, banks_data)
            if errors:
                print(f"❌ Errors inserting banks: {errors}")
                sys.exit(1)
            else:
                print(f"✓ Inserted {len(banks_data)} banks")
                print(f"  Banks: {', '.join(self.banks[:5])}{'...' if len(self.banks) > 5 else ''}")
                print("-" * 70 + "\n")
        except Exception as e:
            print(f"❌ Error inserting banks: {e}")
            sys.exit(1)

    def generate_customers(self):
        """Generate customer records in batches and insert into BigQuery"""
        print("👥 GENERATING CUSTOMER DATA")
        print("-" * 70)

        table_id = f"{self.dataset_id}.upi_customer"
        num_customers = self.DATA_CONFIG['num_customers']
        batch_size = self.DATA_CONFIG['batch_size']
        total_inserted = 0

        for batch_start in range(0, num_customers, batch_size):
            batch_end = min(batch_start + batch_size, num_customers)
            batch_count = batch_end - batch_start

            customers_data = []
            for i in range(batch_count):
                customer_id = str(uuid.uuid4())
                self.customer_ids.append(customer_id)

                first_name = random.choice(self.FIRST_NAMES)
                last_name = random.choice(self.LAST_NAMES)
                name = f"{first_name} {last_name}"

                # Generate unique mobile number (10 digits starting with 6-9)
                mobile = f"{random.randint(6, 9)}{random.randint(100000000, 999999999)}"

                # Generate VPA
                vpa_prefix = name.lower().replace(' ', '.')
                vpa = f"{vpa_prefix}{random.randint(1, 9999)}@{random.choice(self.banks).lower()}"
                self.customer_vpas.append(vpa)

                # Generate email
                email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 9999)}@gmail.com"

                # Bank account details
                account_no = f"{random.randint(10000000000, 99999999999)}"
                bank_code = random.choice(self.banks)
                ifsc = f"{bank_code}0{random.randint(100000, 999999)}"

                customers_data.append({
                    'customer_id': customer_id,
                    'name': name,
                    'mobile_number': mobile,
                    'email': email,
                    'primary_vpa': vpa,
                    'bank_account_no': account_no,
                    'ifsc_code': ifsc,
                    'created_at': datetime.now().isoformat()
                })

            # Insert batch into BigQuery
            try:
                errors = self.bq_client.insert_rows_json(table_id, customers_data)
                if errors:
                    print(f"  ⚠️  Some errors in batch {batch_start // batch_size + 1}: {len(errors)} errors")
                    # Continue anyway

                total_inserted += batch_count
                progress = (total_inserted / num_customers) * 100
                print(f"  ✓ Batch {batch_start // batch_size + 1}: {batch_count:,} customers | "
                      f"Total: {total_inserted:,}/{num_customers:,} ({progress:.1f}%)")

            except Exception as e:
                print(f"❌ Error inserting customers batch {batch_start}: {e}")
                # Continue with next batch
                continue

        print(f"✓ Total customers inserted: {total_inserted:,}")
        print("-" * 70 + "\n")

    def generate_customer_credentials(self):
        """Generate customer credentials (default PIN for all customers for testing)"""
        print("🔑 GENERATING CUSTOMER CREDENTIALS")
        print("-" * 70)

        table_id = f"{self.dataset_id}.upi_customer_credentials"
        batch_size = self.DATA_CONFIG['batch_size']
        total_inserted = 0

        # Default PIN for testing: "1234"
        default_pin = "1234"
        pin_hash = hashlib.sha256(default_pin.encode()).hexdigest()

        print(f"  ℹ️  Default PIN for all customers: {default_pin}")

        for batch_start in range(0, len(self.customer_ids), batch_size):
            batch_end = min(batch_start + batch_size, len(self.customer_ids))
            batch_customer_ids = self.customer_ids[batch_start:batch_end]

            credentials_data = []
            for customer_id in batch_customer_ids:
                credentials_data.append({
                    'customer_id': customer_id,
                    'pin_hash': pin_hash,
                    'created_at': datetime.now().isoformat(),
                    'last_login': None
                })

            # Insert batch into BigQuery
            try:
                errors = self.bq_client.insert_rows_json(table_id, credentials_data)
                if errors:
                    print(f"  ⚠️  Some errors in batch {batch_start // batch_size + 1}: {len(errors)} errors")

                total_inserted += len(credentials_data)
                progress = (total_inserted / len(self.customer_ids)) * 100
                print(f"  ✓ Batch {batch_start // batch_size + 1}: {len(credentials_data):,} credentials | "
                      f"Total: {total_inserted:,}/{len(self.customer_ids):,} ({progress:.1f}%)")

            except Exception as e:
                print(f"❌ Error inserting credentials batch {batch_start}: {e}")
                continue

        print(f"✓ Total credentials inserted: {total_inserted:,}")
        print(f"  ℹ️  All customers can login with PIN: {default_pin}")
        print("-" * 70 + "\n")

    def generate_merchants(self):
        """Generate merchant records and insert into BigQuery"""
        print("🏪 GENERATING MERCHANT DATA")
        print("-" * 70)

        table_id = f"{self.dataset_id}.upi_merchant"
        num_merchants = self.DATA_CONFIG['num_merchants']
        batch_size = min(self.DATA_CONFIG['batch_size'], num_merchants)
        total_inserted = 0

        for batch_start in range(0, num_merchants, batch_size):
            batch_end = min(batch_start + batch_size, num_merchants)
            batch_count = batch_end - batch_start

            merchants_data = []
            for i in range(batch_count):
                merchant_id = str(uuid.uuid4())
                self.merchant_ids.append(merchant_id)

                category = random.choice(self.MERCHANT_CATEGORIES)
                merchant_name = f"{category} Store {random.randint(1, 9999)}"
                merchant_vpa = f"merchant{batch_start + i}@{random.choice(self.banks).lower()}"
                self.merchant_vpas.append(merchant_vpa)

                # Settlement account
                settlement_account = f"{random.randint(10000000000, 99999999999)}"
                bank_code = random.choice(self.banks)
                ifsc = f"{bank_code}0{random.randint(100000, 999999)}"

                merchants_data.append({
                    'merchant_id': merchant_id,
                    'merchant_name': merchant_name,
                    'merchant_vpa': merchant_vpa,
                    'category': category,
                    'settlement_account_no': settlement_account,
                    'ifsc_code': ifsc,
                    'created_at': datetime.now().isoformat()
                })

            # Insert batch
            try:
                errors = self.bq_client.insert_rows_json(table_id, merchants_data)
                if errors:
                    print(f"  ⚠️  Some errors in batch {batch_start // batch_size + 1}: {len(errors)} errors")

                total_inserted += batch_count
                progress = (total_inserted / num_merchants) * 100
                print(f"  ✓ Batch {batch_start // batch_size + 1}: {batch_count:,} merchants | "
                      f"Total: {total_inserted:,}/{num_merchants:,} ({progress:.1f}%)")

            except Exception as e:
                print(f"❌ Error inserting merchants batch {batch_start}: {e}")
                continue

        print(f"✓ Total merchants inserted: {total_inserted:,}")
        print("-" * 70 + "\n")

    def generate_transactions(self):
        """Generate transaction records in batches and insert into BigQuery"""
        print("💳 GENERATING TRANSACTION DATA")
        print("-" * 70)

        table_id = f"{self.dataset_id}.upi_transaction"
        num_transactions = self.DATA_CONFIG['num_transactions']
        batch_size = self.DATA_CONFIG['batch_size']
        total_inserted = 0

        # Time range: last 365 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)

        for batch_start in range(0, num_transactions, batch_size):
            batch_end = min(batch_start + batch_size, num_transactions)
            batch_count = batch_end - batch_start

            transactions_data = []
            for i in range(batch_count):
                transaction_id = str(uuid.uuid4())
                self.transaction_ids.append(transaction_id)

                # Generate UPI transaction reference
                upi_txn_ref = f"UPI{random.randint(100000000000, 999999999999)}"

                # Select random payer and payee
                payer_vpa = random.choice(self.customer_vpas)

                # Determine if this is a merchant transaction
                is_merchant_txn = random.random() < (self.DATA_CONFIG['merchant_transaction_percentage'] / 100)

                if is_merchant_txn and self.merchant_vpas:
                    payee_vpa = random.choice(self.merchant_vpas)
                    merchant_id = random.choice(self.merchant_ids)
                else:
                    payee_vpa = random.choice(self.customer_vpas)
                    # Ensure different from payer
                    while payee_vpa == payer_vpa and len(self.customer_vpas) > 1:
                        payee_vpa = random.choice(self.customer_vpas)
                    merchant_id = None

                # Banks
                payer_bank = random.choice(self.banks)
                payee_bank = random.choice(self.banks)

                # Amount: Random between 10 and 50,000
                amount = round(random.uniform(10, 50000), 2)

                # Transaction type and status
                txn_type = self.weighted_choice(self.DATA_CONFIG['transaction_type_distribution'])
                status = self.weighted_choice(self.DATA_CONFIG['transaction_status_distribution'])

                # Timestamps
                initiated_at = start_date + timedelta(
                    seconds=random.randint(0, int((end_date - start_date).total_seconds()))
                )

                if status in ['SUCCESS', 'FAILED', 'REVERSED']:
                    completed_at = initiated_at + timedelta(seconds=random.randint(1, 300))
                else:
                    completed_at = None

                # Failure reason
                failure_reason = None
                if status in ['FAILED', 'REVERSED']:
                    failure_reason = random.choice(self.FAILURE_REASONS)

                # Remarks
                remarks = f"{txn_type} transaction" if random.random() > 0.7 else None

                txn_data = {
                    'transaction_id': transaction_id,
                    'upi_txn_ref': upi_txn_ref,
                    'payer_vpa': payer_vpa,
                    'payee_vpa': payee_vpa,
                    'payer_bank_code': payer_bank,
                    'payee_bank_code': payee_bank,
                    'amount': amount,
                    'currency': 'INR',
                    'transaction_type': txn_type,
                    'status': status,
                    'initiated_at': initiated_at.isoformat(),
                    'completed_at': completed_at.isoformat() if completed_at else None,
                    'failure_reason': failure_reason,
                    'merchant_id': merchant_id,
                    'remarks': remarks
                }

                transactions_data.append(txn_data)

            # Insert batch into BigQuery
            try:
                errors = self.bq_client.insert_rows_json(table_id, transactions_data)
                if errors:
                    print(f"  ⚠️  Some errors in batch {batch_start // batch_size + 1}: {len(errors)} errors")

                total_inserted += batch_count
                progress = (total_inserted / num_transactions) * 100
                print(f"  ✓ Batch {batch_start // batch_size + 1}: {batch_count:,} transactions | "
                      f"Total: {total_inserted:,}/{num_transactions:,} ({progress:.1f}%)")

            except Exception as e:
                print(f"❌ Error inserting transactions batch {batch_start}: {e}")
                continue

        print(f"✓ Total transactions inserted: {total_inserted:,}")
        print("-" * 70 + "\n")

    def generate_audit_records(self):
        """Generate audit records for transactions"""
        print("📋 GENERATING AUDIT RECORDS")
        print("-" * 70)

        table_id = f"{self.dataset_id}.upi_transaction_audit"
        audit_percentage = self.DATA_CONFIG['audit_percentage']
        num_audits = int(len(self.transaction_ids) * (audit_percentage / 100))

        # Sample random transactions to audit
        audited_txns = random.sample(self.transaction_ids, min(num_audits, len(self.transaction_ids)))

        batch_size = self.DATA_CONFIG['batch_size']
        total_inserted = 0
        statuses = ['PENDING', 'SUCCESS', 'FAILED', 'REVERSED']

        all_audit_data = []
        for txn_id in audited_txns:
            # Create 1-3 audit entries per transaction
            num_audits_per_txn = random.randint(1, 3)
            old_status = 'PENDING'

            for _ in range(num_audits_per_txn):
                audit_id = str(uuid.uuid4())
                new_status = random.choice(statuses)
                changed_by = f"SYSTEM_{random.randint(1, 10)}"
                change_timestamp = datetime.now() - timedelta(days=random.randint(0, 365))
                comments = f"Status changed from {old_status} to {new_status}"

                all_audit_data.append({
                    'audit_id': audit_id,
                    'transaction_id': txn_id,
                    'old_status': old_status,
                    'new_status': new_status,
                    'changed_by': changed_by,
                    'change_timestamp': change_timestamp.isoformat(),
                    'comments': comments
                })

                old_status = new_status

        # Insert in batches
        for batch_start in range(0, len(all_audit_data), batch_size):
            batch_end = min(batch_start + batch_size, len(all_audit_data))
            batch = all_audit_data[batch_start:batch_end]

            try:
                errors = self.bq_client.insert_rows_json(table_id, batch)
                if errors:
                    print(f"  ⚠️  Some errors in batch: {len(errors)} errors")

                total_inserted += len(batch)
                progress = (batch_end / len(all_audit_data)) * 100
                print(f"  ✓ Batch {batch_start // batch_size + 1}: {len(batch):,} audit records | "
                      f"Progress: {batch_end:,}/{len(all_audit_data):,} ({progress:.1f}%)")

            except Exception as e:
                print(f"❌ Error inserting audit records: {e}")
                continue

        print(f"✓ Total audit records inserted: {total_inserted:,}")
        print("-" * 70 + "\n")

    def generate_refunds(self):
        """Generate refund records"""
        print("💰 GENERATING REFUND RECORDS")
        print("-" * 70)

        table_id = f"{self.dataset_id}.upi_refund"
        refund_percentage = self.DATA_CONFIG['refund_percentage']
        num_refunds = int(len(self.transaction_ids) * (refund_percentage / 100))

        refundable_txns = random.sample(self.transaction_ids, min(num_refunds, len(self.transaction_ids)))

        batch_size = self.DATA_CONFIG['batch_size']
        total_inserted = 0

        all_refund_data = []
        for txn_id in refundable_txns:
            refund_id = str(uuid.uuid4())
            refund_amount = round(random.uniform(10, 50000), 2)
            refund_reason = random.choice(self.REFUND_REASONS)
            status = random.choice(['INITIATED', 'SUCCESS', 'FAILED'])
            processed_at = datetime.now() - timedelta(days=random.randint(0, 365))

            all_refund_data.append({
                'refund_id': refund_id,
                'original_txn_id': txn_id,
                'refund_amount': refund_amount,
                'refund_reason': refund_reason,
                'status': status,
                'processed_at': processed_at.isoformat()
            })

        # Insert in batches
        for batch_start in range(0, len(all_refund_data), batch_size):
            batch_end = min(batch_start + batch_size, len(all_refund_data))
            batch = all_refund_data[batch_start:batch_end]

            try:
                errors = self.bq_client.insert_rows_json(table_id, batch)
                if errors:
                    print(f"  ⚠️  Some errors in batch: {len(errors)} errors")

                total_inserted += len(batch)
                progress = (batch_end / len(all_refund_data)) * 100
                print(f"  ✓ Batch {batch_start // batch_size + 1}: {len(batch):,} refunds | "
                      f"Progress: {batch_end:,}/{len(all_refund_data):,} ({progress:.1f}%)")

            except Exception as e:
                print(f"❌ Error inserting refunds: {e}")
                continue

        print(f"✓ Total refunds inserted: {total_inserted:,}")
        print("-" * 70 + "\n")

    def verify_data(self):
        """Verify data in BigQuery tables"""
        print("✅ VERIFYING BIGQUERY DATA")
        print("-" * 70)

        tables = [
            'upi_bank',
            'upi_customer',
            'upi_customer_credentials',
            'upi_merchant',
            'upi_transaction',
            'upi_transaction_audit',
            'upi_refund'
        ]

        for table_name in tables:
            table_id = f"{self.dataset_id}.{table_name}"
            query = f"SELECT COUNT(*) as count FROM `{table_id}`"

            try:
                query_job = self.bq_client.query(query)
                results = query_job.result()
                count = list(results)[0]['count']
                print(f"  {table_name}: {count:,} records")
            except Exception as e:
                print(f"  {table_name}: ⚠️  Error - {e}")

        print("-" * 70 + "\n")

    def run(self):
        """Run the complete data generation process"""
        start_time = datetime.now()

        print("\n" + "=" * 70)
        print("🚀 UPI DATA GENERATION TO BIGQUERY - DIRECT MODE")
        print("=" * 70)
        print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Target: {self.DATA_CONFIG['num_transactions']:,} transactions")
        print(f"Dataset: {config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET}")
        print("=" * 70 + "\n")

        try:
            self.connect_bigquery()
            self.create_bigquery_tables()
            self.wait_for_tables()
            self.generate_banks()
            self.generate_customers()
            self.generate_customer_credentials()
            self.generate_merchants()
            self.generate_transactions()
            self.generate_audit_records()
            self.generate_refunds()
            self.verify_data()

            end_time = datetime.now()
            duration = end_time - start_time

            print("=" * 70)
            print("✅ DATA GENERATION COMPLETED SUCCESSFULLY")
            print("=" * 70)
            print(f"Finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Total duration: {duration}")
            print(f"BigQuery Dataset: {config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET}")
            print("=" * 70 + "\n")

            # Print sample queries
            print("📋 Sample BigQuery Queries:")
            print("-" * 70)
            print(f"-- View transactions")
            print(f"SELECT * FROM `{self.dataset_id}.upi_transaction` LIMIT 10;\n")
            print(f"-- Transaction statistics")
            print(f"SELECT status, COUNT(*) as count, SUM(amount) as total_amount")
            print(f"FROM `{self.dataset_id}.upi_transaction`")
            print(f"GROUP BY status;")
            print("-" * 70 + "\n")

        except KeyboardInterrupt:
            print("\n\n⚠️  Process interrupted by user")
        except Exception as e:
            print(f"\n\n❌ Error during data generation: {e}")
            import traceback
            traceback.print_exc()


def main():
    generator = UPIBigQueryGenerator()
    generator.run()


if __name__ == "__main__":
    main()
