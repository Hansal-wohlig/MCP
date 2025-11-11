#!/usr/bin/env python3
"""
UPI Data Generation - Direct to BigQuery
Generates realistic UPI transaction data and uploads directly to BigQuery
"""

import os
import sys
import uuid
import hashlib
import random
from datetime import datetime, timedelta
from typing import List, Dict
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from faker import Faker
import time

# Initialize Faker
fake = Faker('en_IN')  # Indian locale for realistic Indian data


class UPIBigQueryGenerator:
    def __init__(self, data_config: Dict = None):
        """
        Initialize the UPI BigQuery Generator
        
        Args:
            data_config: Dictionary containing configuration like num_transactions, num_customers, etc.
        """
        self.config = data_config or {}
        self.num_transactions = self.config.get('num_transactions', 10000000)
        self.num_customers = self.config.get('num_customers', 100000)
        self.num_merchants = self.config.get('num_merchants', 5000)
        self.num_banks = self.config.get('num_banks', 50)
        self.batch_size = self.config.get('batch_size', 10000)
        
        # BigQuery setup
        self.project_id = os.environ.get('GCP_PROJECT_ID')
        self.dataset_id = os.environ.get('BIGQUERY_DATASET', 'upi_banking')
        
        if not self.project_id:
            print("\n" + "="*70)
            print("❌ ERROR: GCP_PROJECT_ID environment variable not set!")
            print("="*70)
            print("\nPlease set it using one of these methods:\n")
            print("Method 1 - Terminal:")
            print("  export GCP_PROJECT_ID='your-project-id'")
            print("  python insert_data.py\n")
            print("Method 2 - Create .env file:")
            print("  echo 'GCP_PROJECT_ID=your-project-id' > .env")
            print("  pip install python-dotenv")
            print("  python insert_data.py\n")
            print("Method 3 - Inline (macOS/Linux):")
            print("  GCP_PROJECT_ID='your-project-id' python insert_data.py\n")
            print("="*70 + "\n")
            raise ValueError("GCP_PROJECT_ID environment variable must be set")
        
        self.client = bigquery.Client(project=self.project_id)
        self.dataset_ref = f"{self.project_id}.{self.dataset_id}"
        
        # Cache for generated data
        self.banks = []
        self.customers = []
        self.merchants = []
        self.bank_codes = []
        
        print(f"✅ Initialized UPI BigQuery Generator")
        print(f"   Project: {self.project_id}")
        print(f"   Dataset: {self.dataset_id}")

    def hash_pin(self, pin: str) -> str:
        """Hash PIN for secure storage"""
        return hashlib.sha256(pin.encode()).hexdigest()

    def generate_vpa(self, name: str, bank_code: str) -> str:
        """Generate realistic VPA (Virtual Payment Address)"""
        username = name.lower().replace(' ', '.')
        # Add some randomness
        if random.random() > 0.7:
            username += str(random.randint(1, 999))
        return f"{username}@{bank_code.lower()}"

    def generate_banks(self) -> List[Dict]:
        """Generate bank data"""
        print(f"\n📊 Generating {self.num_banks} banks...")
        
        indian_banks = [
            ('SBI', 'State Bank of India', '607'),
            ('HDFC', 'HDFC Bank', '608'),
            ('ICICI', 'ICICI Bank', '609'),
            ('AXIS', 'Axis Bank', '610'),
            ('KOTAK', 'Kotak Mahindra Bank', '611'),
            ('PNB', 'Punjab National Bank', '612'),
            ('BOB', 'Bank of Baroda', '613'),
            ('CANARA', 'Canara Bank', '614'),
            ('UNION', 'Union Bank of India', '615'),
            ('IDBI', 'IDBI Bank', '616'),
            ('BOI', 'Bank of India', '617'),
            ('INDIAN', 'Indian Bank', '618'),
            ('CENTRAL', 'Central Bank of India', '619'),
            ('IOB', 'Indian Overseas Bank', '620'),
            ('FEDERAL', 'Federal Bank', '621'),
            ('YES', 'Yes Bank', '622'),
            ('INDUSIND', 'IndusInd Bank', '623'),
            ('IDFC', 'IDFC First Bank', '624'),
            ('BANDHAN', 'Bandhan Bank', '625'),
            ('RBL', 'RBL Bank', '626'),
        ]
        
        banks = []
        for i in range(self.num_banks):
            if i < len(indian_banks):
                code, name, iin = indian_banks[i]
            else:
                code = f"BANK{i+1:03d}"
                name = f"{fake.company()} Bank"
                iin = f"{600 + i}"
            
            banks.append({
                'bank_code': code,
                'bank_name': name,
                'iin': iin,
                'is_active': True,
                'created_at': datetime.now()
            })
            self.bank_codes.append(code)
        
        self.banks = banks
        print(f"   ✅ Generated {len(banks)} banks")
        return banks

    def generate_customers(self) -> List[Dict]:
        """Generate customer data"""
        print(f"\n👥 Generating {self.num_customers} customers...")
        
        customers = []
        customer_credentials = []
        
        for i in range(self.num_customers):
            customer_id = str(uuid.uuid4())
            name = fake.name()
            mobile = fake.phone_number()[:15]
            bank_code = random.choice(self.bank_codes)
            vpa = self.generate_vpa(name, bank_code)
            
            customer = {
                'customer_id': customer_id,
                'name': name,
                'mobile_number': mobile,
                'email': fake.email(),
                'primary_vpa': vpa,
                'bank_account_no': str(fake.random_number(digits=16)),
                'ifsc_code': f"{bank_code}0{fake.random_number(digits=6)}",
                'created_at': datetime.now() - timedelta(days=random.randint(30, 365))
            }
            customers.append(customer)
            
            # Generate credentials for authentication
            pin = f"{random.randint(1000, 9999)}"
            credential = {
                'credential_id': str(uuid.uuid4()),
                'customer_id': customer_id,
                'pin_hash': self.hash_pin(pin),
                'created_at': customer['created_at']
            }
            customer_credentials.append(credential)
            
            if (i + 1) % 10000 == 0:
                print(f"   Generated {i + 1:,} customers...")
        
        self.customers = customers
        self.customer_credentials = customer_credentials
        print(f"   ✅ Generated {len(customers)} customers")
        return customers

    def generate_merchants(self) -> List[Dict]:
        """Generate merchant data"""
        print(f"\n🏪 Generating {self.num_merchants} merchants...")
        
        merchant_categories = [
            'Grocery', 'Restaurant', 'Retail', 'Electronics', 
            'Fuel', 'Healthcare', 'Education', 'Entertainment',
            'Travel', 'E-commerce', 'Utilities', 'Insurance'
        ]
        
        merchants = []
        for i in range(self.num_merchants):
            merchant_name = fake.company()
            bank_code = random.choice(self.bank_codes)
            
            merchant = {
                'merchant_id': str(uuid.uuid4()),
                'merchant_name': merchant_name,
                'merchant_vpa': f"{merchant_name.lower().replace(' ', '')}@{bank_code.lower()}",
                'category': random.choice(merchant_categories),
                'settlement_account_no': str(fake.random_number(digits=16)),
                'ifsc_code': f"{bank_code}0{fake.random_number(digits=6)}",
                'created_at': datetime.now() - timedelta(days=random.randint(30, 730))
            }
            merchants.append(merchant)
            
            if (i + 1) % 1000 == 0:
                print(f"   Generated {i + 1:,} merchants...")
        
        self.merchants = merchants
        print(f"   ✅ Generated {len(merchants)} merchants")
        return merchants

    def generate_transactions_batch(self, start_idx: int, batch_size: int) -> List[Dict]:
        """Generate a batch of transaction data"""
        transactions = []
        
        # Time range for transactions (last 180 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=180)
        
        for i in range(batch_size):
            # Random transaction type distribution
            txn_type = random.choices(
                ['PAY', 'COLLECT', 'REFUND'],
                weights=[85, 10, 5]
            )[0]
            
            # Random status distribution
            status = random.choices(
                ['SUCCESS', 'FAILED', 'PENDING'],
                weights=[90, 8, 2]
            )[0]
            
            # Get random customer and determine payee
            customer = random.choice(self.customers)
            
            # 70% merchant transactions, 30% peer-to-peer
            if random.random() < 0.7:
                merchant = random.choice(self.merchants)
                payee_vpa = merchant['merchant_vpa']
                merchant_id = merchant['merchant_id']
            else:
                peer = random.choice(self.customers)
                payee_vpa = peer['primary_vpa']
                merchant_id = None
            
            payer_vpa = customer['primary_vpa']
            
            # Amount generation (realistic distribution)
            if merchant_id:  # Merchant transaction
                amount = round(random.choices(
                    [random.uniform(10, 100), random.uniform(100, 500), 
                     random.uniform(500, 2000), random.uniform(2000, 10000)],
                    weights=[40, 35, 20, 5]
                )[0], 2)
            else:  # P2P transaction
                amount = round(random.uniform(10, 5000), 2)
            
            # Random bank codes
            payer_bank = random.choice(self.bank_codes)
            payee_bank = random.choice(self.bank_codes)
            
            # Transaction timestamp
            initiated_at = fake.date_time_between(start_date=start_date, end_date=end_date)
            
            if status == 'SUCCESS':
                completed_at = initiated_at + timedelta(seconds=random.randint(1, 30))
                failure_reason = None
            elif status == 'FAILED':
                completed_at = initiated_at + timedelta(seconds=random.randint(5, 60))
                failure_reasons = [
                    'Insufficient funds', 'Invalid VPA', 'Transaction declined by bank',
                    'Technical error', 'Daily limit exceeded', 'UPI PIN incorrect'
                ]
                failure_reason = random.choice(failure_reasons)
            else:  # PENDING
                completed_at = None
                failure_reason = None
            
            transaction = {
                'transaction_id': str(uuid.uuid4()),
                'upi_txn_ref': f"UPI{fake.random_number(digits=16)}",
                'payer_vpa': payer_vpa,
                'payee_vpa': payee_vpa,
                'payer_bank_code': payer_bank,
                'payee_bank_code': payee_bank,
                'amount': amount,
                'currency': 'INR',
                'transaction_type': txn_type,
                'status': status,
                'initiated_at': initiated_at,
                'completed_at': completed_at,
                'failure_reason': failure_reason,
                'merchant_id': merchant_id,
                'remarks': fake.sentence(nb_words=6) if random.random() > 0.5 else None
            }
            transactions.append(transaction)
        
        return transactions

    def serialize_datetime(self, obj):
        """Convert datetime objects to ISO format strings"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        return obj

    def prepare_rows_for_bigquery(self, rows: List[Dict]) -> List[Dict]:
        """Prepare rows for BigQuery by converting datetime objects to strings"""
        prepared_rows = []
        for row in rows:
            prepared_row = {}
            for key, value in row.items():
                if isinstance(value, datetime):
                    prepared_row[key] = value.isoformat()
                else:
                    prepared_row[key] = value
            prepared_rows.append(prepared_row)
        return prepared_rows

    def insert_to_bigquery(self, table_name: str, rows: List[Dict]):
        """Insert data into BigQuery table"""
        table_id = f"{self.dataset_ref}.{table_name}"
        
        try:
            # Convert datetime objects to ISO format strings
            prepared_rows = self.prepare_rows_for_bigquery(rows)
            
            errors = self.client.insert_rows_json(table_id, prepared_rows)
            if errors:
                print(f"   ⚠️  Errors inserting into {table_name}: {errors}")
            return len(errors) == 0
        except GoogleCloudError as e:
            print(f"   ❌ Failed to insert into {table_name}: {e}")
            return False

    def insert_banks(self):
        """Insert banks into BigQuery"""
        print(f"\n📤 Inserting {len(self.banks)} banks into BigQuery...")
        success = self.insert_to_bigquery('upi_bank', self.banks)
        if success:
            print(f"   ✅ Successfully inserted banks")
        return success

    def insert_customers(self):
        """Insert customers into BigQuery in batches"""
        print(f"\n📤 Inserting {len(self.customers)} customers into BigQuery...")
        
        total_batches = (len(self.customers) + self.batch_size - 1) // self.batch_size
        
        for i in range(0, len(self.customers), self.batch_size):
            batch = self.customers[i:i + self.batch_size]
            batch_num = i // self.batch_size + 1
            
            success = self.insert_to_bigquery('upi_customer', batch)
            if success:
                print(f"   ✅ Batch {batch_num}/{total_batches} inserted ({len(batch)} customers)")
            else:
                print(f"   ❌ Batch {batch_num}/{total_batches} failed")
                return False
            
            time.sleep(0.5)  # Rate limiting
        
        print(f"   ✅ Successfully inserted all customers")
        return True

    def insert_customer_credentials(self):
        """Insert customer credentials into BigQuery"""
        print(f"\n📤 Inserting {len(self.customer_credentials)} credentials into BigQuery...")
        
        total_batches = (len(self.customer_credentials) + self.batch_size - 1) // self.batch_size
        
        for i in range(0, len(self.customer_credentials), self.batch_size):
            batch = self.customer_credentials[i:i + self.batch_size]
            batch_num = i // self.batch_size + 1
            
            success = self.insert_to_bigquery('upi_customer_credentials', batch)
            if success:
                print(f"   ✅ Batch {batch_num}/{total_batches} inserted ({len(batch)} credentials)")
            else:
                print(f"   ❌ Batch {batch_num}/{total_batches} failed")
                return False
            
            time.sleep(0.5)
        
        print(f"   ✅ Successfully inserted all credentials")
        return True

    def insert_merchants(self):
        """Insert merchants into BigQuery in batches"""
        print(f"\n📤 Inserting {len(self.merchants)} merchants into BigQuery...")
        
        total_batches = (len(self.merchants) + self.batch_size - 1) // self.batch_size
        
        for i in range(0, len(self.merchants), self.batch_size):
            batch = self.merchants[i:i + self.batch_size]
            batch_num = i // self.batch_size + 1
            
            success = self.insert_to_bigquery('upi_merchant', batch)
            if success:
                print(f"   ✅ Batch {batch_num}/{total_batches} inserted ({len(batch)} merchants)")
            else:
                print(f"   ❌ Batch {batch_num}/{total_batches} failed")
                return False
            
            time.sleep(0.5)
        
        print(f"   ✅ Successfully inserted all merchants")
        return True

    def insert_transactions(self):
        """Generate and insert transactions into BigQuery in batches"""
        print(f"\n💳 Generating and inserting {self.num_transactions:,} transactions...")
        print(f"   Batch size: {self.batch_size:,}")
        
        total_batches = (self.num_transactions + self.batch_size - 1) // self.batch_size
        start_time = time.time()
        
        for batch_num in range(total_batches):
            batch_start_idx = batch_num * self.batch_size
            current_batch_size = min(self.batch_size, self.num_transactions - batch_start_idx)
            
            # Generate batch
            transactions = self.generate_transactions_batch(batch_start_idx, current_batch_size)
            
            # Insert batch
            success = self.insert_to_bigquery('upi_transaction', transactions)
            
            if success:
                elapsed = time.time() - start_time
                rate = (batch_num + 1) * self.batch_size / elapsed
                remaining = (total_batches - batch_num - 1) * self.batch_size / rate if rate > 0 else 0
                
                print(f"   ✅ Batch {batch_num + 1}/{total_batches} inserted "
                      f"({current_batch_size:,} txns) | "
                      f"Rate: {rate:.0f} txns/sec | "
                      f"ETA: {remaining/60:.1f} min")
            else:
                print(f"   ❌ Batch {batch_num + 1}/{total_batches} failed")
                return False
            
            time.sleep(0.3)  # Rate limiting
        
        total_time = time.time() - start_time
        print(f"\n   ✅ Successfully inserted {self.num_transactions:,} transactions")
        print(f"   ⏱️  Total time: {total_time/60:.2f} minutes")
        print(f"   📊 Average rate: {self.num_transactions/total_time:.0f} txns/sec")
        return True

    def run(self):
        """Run the complete data generation and upload process"""
        print("\n" + "=" * 70)
        print("🚀 STARTING UPI DATA GENERATION TO BIGQUERY")
        print("=" * 70)
        print(f"Target Transactions: {self.num_transactions:,}")
        print(f"Customers: {self.num_customers:,}")
        print(f"Merchants: {self.num_merchants:,}")
        print(f"Banks: {self.num_banks}")
        print(f"Batch Size: {self.batch_size:,}")
        print("=" * 70)
        
        overall_start = time.time()
        
        try:
            # Step 1: Generate and insert banks
            self.generate_banks()
            if not self.insert_banks():
                print("❌ Failed at banks insertion")
                return False
            
            # Step 2: Generate and insert customers
            self.generate_customers()
            if not self.insert_customers():
                print("❌ Failed at customers insertion")
                return False
            
            # Step 3: Insert customer credentials
            if not self.insert_customer_credentials():
                print("❌ Failed at credentials insertion")
                return False
            
            # Step 4: Generate and insert merchants
            self.generate_merchants()
            if not self.insert_merchants():
                print("❌ Failed at merchants insertion")
                return False
            
            # Step 5: Generate and insert transactions
            if not self.insert_transactions():
                print("❌ Failed at transactions insertion")
                return False
            
            # Success!
            total_time = time.time() - overall_start
            print("\n" + "=" * 70)
            print("✅ DATA GENERATION COMPLETE!")
            print("=" * 70)
            print(f"Total time: {total_time/60:.2f} minutes")
            print(f"Banks: {len(self.banks)}")
            print(f"Customers: {len(self.customers)}")
            print(f"Merchants: {len(self.merchants)}")
            print(f"Transactions: {self.num_transactions:,}")
            print("=" * 70 + "\n")
            
            return True
            
        except Exception as e:
            print(f"\n❌ Error during data generation: {e}")
            import traceback
            traceback.print_exc()
            return False


if __name__ == "__main__":
    # Default configuration
    default_config = {
        'num_transactions': 10000000,
        'num_customers': 100000,
        'num_merchants': 5000,
        'num_banks': 50,
        'batch_size': 10000
    }
    
    generator = UPIBigQueryGenerator(data_config=default_config)
    generator.run()