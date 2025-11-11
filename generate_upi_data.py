#!/usr/bin/env python3
"""
UPI Data Generation Script
Generates realistic UPI transaction data and inserts into PostgreSQL
Handles 10M+ transactions with efficient batching and memory management
"""

import psycopg2
import uuid
import random
from datetime import datetime, timedelta
import io
import sys
from upi_data_gen_config import *

class UPIDataGenerator:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.banks = []
        self.customer_ids = []
        self.merchant_ids = []
        self.transaction_ids = []

    def connect_db(self):
        """Connect to PostgreSQL database"""
        print("\n" + "=" * 70)
        print("🔌 CONNECTING TO DATABASE")
        print("=" * 70)
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor()
            print(f"✓ Connected to {DB_CONFIG['database']} on {DB_CONFIG['host']}:{DB_CONFIG['port']}")
            print("=" * 70 + "\n")
        except Exception as e:
            print(f"❌ Error connecting to database: {e}")
            sys.exit(1)

    def close_db(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("\n✓ Database connection closed\n")

    def truncate_tables(self):
        """Truncate all tables before inserting new data"""
        print("🗑️  TRUNCATING EXISTING TABLES")
        print("-" * 70)
        tables = [
            'upi_refund',
            'upi_transaction_audit',
            'upi_transaction',
            'upi_merchant',
            'upi_customer',
            'upi_bank'
        ]

        try:
            for table in tables:
                self.cursor.execute(f"TRUNCATE TABLE {table} CASCADE;")
                print(f"✓ Truncated {table}")
            self.conn.commit()
            print("-" * 70 + "\n")
        except Exception as e:
            print(f"❌ Error truncating tables: {e}")
            self.conn.rollback()
            sys.exit(1)

    def generate_banks(self):
        """Generate bank records"""
        print("🏦 GENERATING BANK DATA")
        print("-" * 70)

        banks_data = []
        for i, (code, name, iin) in enumerate(INDIAN_BANKS):
            if i >= DATA_CONFIG['num_banks']:
                break
            banks_data.append((code, name, iin, True, datetime.now()))

        # Insert banks using COPY for efficiency
        try:
            output = io.StringIO()
            for bank in banks_data:
                output.write('\t'.join(map(str, bank)) + '\n')
            output.seek(0)

            self.cursor.copy_from(
                output,
                'upi_bank',
                columns=['bank_code', 'bank_name', 'iin', 'is_active', 'created_at'],
                sep='\t'
            )
            self.conn.commit()

            # Store bank codes for later use
            self.banks = [b[0] for b in banks_data]

            print(f"✓ Inserted {len(banks_data)} banks")
            print(f"  Banks: {', '.join(self.banks[:5])}{'...' if len(self.banks) > 5 else ''}")
            print("-" * 70 + "\n")
        except Exception as e:
            print(f"❌ Error inserting banks: {e}")
            self.conn.rollback()
            sys.exit(1)

    def generate_customers(self):
        """Generate customer records in batches"""
        print("👥 GENERATING CUSTOMER DATA")
        print("-" * 70)

        num_customers = DATA_CONFIG['num_customers']
        batch_size = DATA_CONFIG['batch_size']
        total_inserted = 0

        for batch_start in range(0, num_customers, batch_size):
            batch_end = min(batch_start + batch_size, num_customers)
            batch_count = batch_end - batch_start

            customers_data = []
            for i in range(batch_count):
                customer_id = str(uuid.uuid4())
                self.customer_ids.append(customer_id)

                first_name = random.choice(FIRST_NAMES)
                last_name = random.choice(LAST_NAMES)
                name = f"{first_name} {last_name}"

                # Generate unique mobile number (10 digits starting with 6-9)
                mobile = f"{random.randint(6, 9)}{random.randint(100000000, 999999999)}"

                # Generate VPA
                vpa_prefix = name.lower().replace(' ', '.')
                vpa = f"{vpa_prefix}{random.randint(1, 9999)}@{random.choice(self.banks).lower()}"

                # Generate email
                email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 9999)}@gmail.com"

                # Bank account details
                account_no = f"{random.randint(10000000000, 99999999999)}"
                bank_code = random.choice(self.banks)
                ifsc = f"{bank_code}0{random.randint(100000, 999999)}"

                customers_data.append((
                    customer_id, name, mobile, email, vpa,
                    account_no, ifsc, datetime.now()
                ))

            # Insert batch using COPY
            try:
                output = io.StringIO()
                for customer in customers_data:
                    output.write('\t'.join(map(str, customer)) + '\n')
                output.seek(0)

                self.cursor.copy_from(
                    output,
                    'upi_customer',
                    columns=['customer_id', 'name', 'mobile_number', 'email',
                            'primary_vpa', 'bank_account_no', 'ifsc_code', 'created_at'],
                    sep='\t'
                )
                self.conn.commit()

                total_inserted += batch_count
                progress = (total_inserted / num_customers) * 100
                print(f"  ✓ Batch {batch_start // batch_size + 1}: {batch_count:,} customers | "
                      f"Total: {total_inserted:,}/{num_customers:,} ({progress:.1f}%)")

            except Exception as e:
                print(f"❌ Error inserting customers batch {batch_start}: {e}")
                self.conn.rollback()
                sys.exit(1)

        print(f"✓ Total customers inserted: {total_inserted:,}")
        print("-" * 70 + "\n")

    def generate_merchants(self):
        """Generate merchant records"""
        print("🏪 GENERATING MERCHANT DATA")
        print("-" * 70)

        num_merchants = DATA_CONFIG['num_merchants']
        batch_size = min(DATA_CONFIG['batch_size'], num_merchants)
        total_inserted = 0

        for batch_start in range(0, num_merchants, batch_size):
            batch_end = min(batch_start + batch_size, num_merchants)
            batch_count = batch_end - batch_start

            merchants_data = []
            for i in range(batch_count):
                merchant_id = str(uuid.uuid4())
                self.merchant_ids.append(merchant_id)

                category = random.choice(MERCHANT_CATEGORIES)
                merchant_name = f"{category} Store {random.randint(1, 9999)}"
                merchant_vpa = f"merchant{batch_start + i}@{random.choice(self.banks).lower()}"

                # Settlement account
                settlement_account = f"{random.randint(10000000000, 99999999999)}"
                bank_code = random.choice(self.banks)
                ifsc = f"{bank_code}0{random.randint(100000, 999999)}"

                merchants_data.append((
                    merchant_id, merchant_name, merchant_vpa, category,
                    settlement_account, ifsc, datetime.now()
                ))

            # Insert batch
            try:
                output = io.StringIO()
                for merchant in merchants_data:
                    output.write('\t'.join(map(str, merchant)) + '\n')
                output.seek(0)

                self.cursor.copy_from(
                    output,
                    'upi_merchant',
                    columns=['merchant_id', 'merchant_name', 'merchant_vpa', 'category',
                            'settlement_account_no', 'ifsc_code', 'created_at'],
                    sep='\t'
                )
                self.conn.commit()

                total_inserted += batch_count
                progress = (total_inserted / num_merchants) * 100
                print(f"  ✓ Batch {batch_start // batch_size + 1}: {batch_count:,} merchants | "
                      f"Total: {total_inserted:,}/{num_merchants:,} ({progress:.1f}%)")

            except Exception as e:
                print(f"❌ Error inserting merchants batch {batch_start}: {e}")
                self.conn.rollback()
                sys.exit(1)

        print(f"✓ Total merchants inserted: {total_inserted:,}")
        print("-" * 70 + "\n")

    def weighted_choice(self, choices_dict):
        """Make a weighted random choice based on percentage distribution"""
        choices = list(choices_dict.keys())
        weights = list(choices_dict.values())
        return random.choices(choices, weights=weights, k=1)[0]

    def generate_transactions(self):
        """Generate transaction records in batches"""
        print("💳 GENERATING TRANSACTION DATA")
        print("-" * 70)

        num_transactions = DATA_CONFIG['num_transactions']
        batch_size = DATA_CONFIG['batch_size']
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

                # Generate UPI transaction reference (12 alphanumeric characters)
                upi_txn_ref = f"UPI{random.randint(100000000000, 999999999999)}"

                # Select random payer and payee
                payer_idx = random.randint(0, len(self.customer_ids) - 1)
                payee_idx = random.randint(0, len(self.customer_ids) - 1)

                # Ensure payer and payee are different
                while payee_idx == payer_idx:
                    payee_idx = random.randint(0, len(self.customer_ids) - 1)

                # Get VPAs (we'll construct them)
                payer_id = self.customer_ids[payer_idx]
                payee_id = self.customer_ids[payee_idx]

                # Construct VPAs
                payer_vpa = f"user{payer_idx}@{random.choice(self.banks).lower()}"

                # Determine if this is a merchant transaction
                is_merchant_txn = random.random() < (DATA_CONFIG['merchant_transaction_percentage'] / 100)

                if is_merchant_txn and self.merchant_ids:
                    merchant_id = random.choice(self.merchant_ids)
                    payee_vpa = f"merchant{random.randint(0, len(self.merchant_ids)-1)}@{random.choice(self.banks).lower()}"
                else:
                    merchant_id = None
                    payee_vpa = f"user{payee_idx}@{random.choice(self.banks).lower()}"

                # Banks
                payer_bank = random.choice(self.banks)
                payee_bank = random.choice(self.banks)

                # Amount: Random between 10 and 50,000
                amount = round(random.uniform(10, 50000), 2)

                # Transaction type and status
                txn_type = self.weighted_choice(DATA_CONFIG['transaction_type_distribution'])
                status = self.weighted_choice(DATA_CONFIG['transaction_status_distribution'])

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
                    failure_reason = random.choice(FAILURE_REASONS)

                # Remarks
                remarks = f"{txn_type} transaction" if random.random() > 0.7 else None

                transactions_data.append((
                    transaction_id, upi_txn_ref, payer_vpa, payee_vpa,
                    payer_bank, payee_bank, amount, 'INR', txn_type, status,
                    initiated_at, completed_at, failure_reason,
                    merchant_id if merchant_id else '\\N',  # NULL for PostgreSQL COPY
                    remarks if remarks else '\\N'
                ))

            # Insert batch using COPY
            try:
                output = io.StringIO()
                for txn in transactions_data:
                    # Handle NULL values properly for COPY
                    row = []
                    for val in txn:
                        if val == '\\N':
                            row.append('\\N')
                        elif isinstance(val, datetime):
                            row.append(val.isoformat())
                        else:
                            row.append(str(val))
                    output.write('\t'.join(row) + '\n')
                output.seek(0)

                self.cursor.copy_from(
                    output,
                    'upi_transaction',
                    columns=['transaction_id', 'upi_txn_ref', 'payer_vpa', 'payee_vpa',
                            'payer_bank_code', 'payee_bank_code', 'amount', 'currency',
                            'transaction_type', 'status', 'initiated_at', 'completed_at',
                            'failure_reason', 'merchant_id', 'remarks'],
                    sep='\t',
                    null='\\N'
                )
                self.conn.commit()

                total_inserted += batch_count
                progress = (total_inserted / num_transactions) * 100
                print(f"  ✓ Batch {batch_start // batch_size + 1}: {batch_count:,} transactions | "
                      f"Total: {total_inserted:,}/{num_transactions:,} ({progress:.1f}%)")

            except Exception as e:
                print(f"❌ Error inserting transactions batch {batch_start}: {e}")
                self.conn.rollback()
                sys.exit(1)

        print(f"✓ Total transactions inserted: {total_inserted:,}")
        print("-" * 70 + "\n")

    def generate_audit_records(self):
        """Generate audit records for a percentage of transactions"""
        print("📋 GENERATING AUDIT RECORDS")
        print("-" * 70)

        # Calculate how many transactions to audit
        audit_percentage = DATA_CONFIG['audit_percentage']
        num_audits = int(len(self.transaction_ids) * (audit_percentage / 100))

        # Sample random transactions to audit
        audited_txns = random.sample(self.transaction_ids, min(num_audits, len(self.transaction_ids)))

        batch_size = DATA_CONFIG['batch_size']
        total_inserted = 0

        statuses = ['PENDING', 'SUCCESS', 'FAILED', 'REVERSED']

        for batch_start in range(0, len(audited_txns), batch_size):
            batch_end = min(batch_start + batch_size, len(audited_txns))
            batch_txns = audited_txns[batch_start:batch_end]

            audit_data = []
            for txn_id in batch_txns:
                # Create 1-3 audit entries per transaction
                num_audits_per_txn = random.randint(1, 3)
                old_status = 'PENDING'

                for j in range(num_audits_per_txn):
                    audit_id = str(uuid.uuid4())
                    new_status = random.choice(statuses)
                    changed_by = f"SYSTEM_{random.randint(1, 10)}"
                    change_timestamp = datetime.now() - timedelta(days=random.randint(0, 365))
                    comments = f"Status changed from {old_status} to {new_status}"

                    audit_data.append((
                        audit_id, txn_id, old_status, new_status,
                        changed_by, change_timestamp, comments
                    ))

                    old_status = new_status

            # Insert batch
            try:
                output = io.StringIO()
                for audit in audit_data:
                    row = []
                    for val in audit:
                        if isinstance(val, datetime):
                            row.append(val.isoformat())
                        else:
                            row.append(str(val))
                    output.write('\t'.join(row) + '\n')
                output.seek(0)

                self.cursor.copy_from(
                    output,
                    'upi_transaction_audit',
                    columns=['audit_id', 'transaction_id', 'old_status', 'new_status',
                            'changed_by', 'change_timestamp', 'comments'],
                    sep='\t'
                )
                self.conn.commit()

                total_inserted += len(audit_data)
                progress = (batch_end / len(audited_txns)) * 100
                print(f"  ✓ Batch {batch_start // batch_size + 1}: {len(audit_data):,} audit records | "
                      f"Progress: {batch_end:,}/{len(audited_txns):,} ({progress:.1f}%)")

            except Exception as e:
                print(f"❌ Error inserting audit records batch {batch_start}: {e}")
                self.conn.rollback()
                sys.exit(1)

        print(f"✓ Total audit records inserted: {total_inserted:,}")
        print("-" * 70 + "\n")

    def generate_refunds(self):
        """Generate refund records for failed/reversed transactions"""
        print("💰 GENERATING REFUND RECORDS")
        print("-" * 70)

        # Get failed/reversed transaction IDs
        # For simulation, we'll randomly select from transaction_ids
        refund_percentage = DATA_CONFIG['refund_percentage']
        num_refunds = int(len(self.transaction_ids) * (refund_percentage / 100))

        refundable_txns = random.sample(self.transaction_ids, min(num_refunds, len(self.transaction_ids)))

        batch_size = DATA_CONFIG['batch_size']
        total_inserted = 0

        for batch_start in range(0, len(refundable_txns), batch_size):
            batch_end = min(batch_start + batch_size, len(refundable_txns))
            batch_txns = refundable_txns[batch_start:batch_end]

            refund_data = []
            for txn_id in batch_txns:
                refund_id = str(uuid.uuid4())
                refund_amount = round(random.uniform(10, 50000), 2)
                refund_reason = random.choice(REFUND_REASONS)
                status = random.choice(['INITIATED', 'SUCCESS', 'FAILED'])
                processed_at = datetime.now() - timedelta(days=random.randint(0, 365))

                refund_data.append((
                    refund_id, txn_id, refund_amount, refund_reason,
                    status, processed_at
                ))

            # Insert batch
            try:
                output = io.StringIO()
                for refund in refund_data:
                    row = []
                    for val in refund:
                        if isinstance(val, datetime):
                            row.append(val.isoformat())
                        else:
                            row.append(str(val))
                    output.write('\t'.join(row) + '\n')
                output.seek(0)

                self.cursor.copy_from(
                    output,
                    'upi_refund',
                    columns=['refund_id', 'original_txn_id', 'refund_amount',
                            'refund_reason', 'status', 'processed_at'],
                    sep='\t'
                )
                self.conn.commit()

                total_inserted += len(refund_data)
                progress = (batch_end / len(refundable_txns)) * 100
                print(f"  ✓ Batch {batch_start // batch_size + 1}: {len(refund_data):,} refunds | "
                      f"Progress: {batch_end:,}/{len(refundable_txns):,} ({progress:.1f}%)")

            except Exception as e:
                print(f"❌ Error inserting refunds batch {batch_start}: {e}")
                self.conn.rollback()
                sys.exit(1)

        print(f"✓ Total refunds inserted: {total_inserted:,}")
        print("-" * 70 + "\n")

    def verify_data(self):
        """Verify the inserted data"""
        print("✅ VERIFYING DATA")
        print("-" * 70)

        tables = [
            'upi_bank',
            'upi_customer',
            'upi_merchant',
            'upi_transaction',
            'upi_transaction_audit',
            'upi_refund'
        ]

        for table in tables:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table};")
            count = self.cursor.fetchone()[0]
            print(f"  {table}: {count:,} records")

        print("-" * 70 + "\n")

    def run(self):
        """Run the complete data generation process"""
        start_time = datetime.now()

        print("\n" + "=" * 70)
        print("🚀 UPI DATA GENERATION STARTED")
        print("=" * 70)
        print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Target: {DATA_CONFIG['num_transactions']:,} transactions")
        print("=" * 70 + "\n")

        try:
            self.connect_db()
            self.truncate_tables()
            self.generate_banks()
            self.generate_customers()
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
            print(f"Database: {DB_CONFIG['database']}")
            print("=" * 70 + "\n")

        except KeyboardInterrupt:
            print("\n\n⚠️  Process interrupted by user")
            self.conn.rollback()
        except Exception as e:
            print(f"\n\n❌ Error during data generation: {e}")
            import traceback
            traceback.print_exc()
            self.conn.rollback()
        finally:
            self.close_db()


def main():
    generator = UPIDataGenerator()
    generator.run()


if __name__ == "__main__":
    main()
