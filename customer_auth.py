"""
Authentication System for UPI
Authenticates both customers (VPA/Mobile + PIN) and merchants (VPA + password)
"""

import hashlib
from typing import Optional, Dict, Tuple
from google.cloud import bigquery
import config

class CustomerAuthenticator:
    def __init__(self):
        self.bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
        self.dataset_id = f"{config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET}"
        self.authenticated_user = None
        self.user_type = None  # 'customer' or 'merchant'

    def hash_pin(self, pin: str) -> str:
        """Hash the PIN for secure comparison"""
        return hashlib.sha256(pin.encode()).hexdigest()

    def authenticate_by_vpa_pin(self, vpa: str, pin: str) -> Optional[Dict]:
        """
        Authenticate customer using VPA and PIN
        Returns customer data if successful, None otherwise
        """
        # Query BigQuery for customer credentials
        query = f"""
        SELECT
            c.customer_id,
            c.name,
            c.mobile_number,
            c.email,
            c.primary_vpa,
            c.bank_account_no,
            cred.pin_hash
        FROM `{self.dataset_id}.upi_customer` c
        JOIN `{self.dataset_id}.upi_customer_credentials` cred
            ON c.customer_id = cred.customer_id
        WHERE c.primary_vpa = @vpa
        LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("vpa", "STRING", vpa)
            ]
        )

        try:
            query_job = self.bq_client.query(query, job_config=job_config)
            results = list(query_job.result())

            if not results:
                return None

            customer = results[0]
            pin_hash = self.hash_pin(pin)

            # Verify PIN
            if customer['pin_hash'] == pin_hash:
                self.authenticated_user = {
                    'customer_id': customer['customer_id'],
                    'name': customer['name'],
                    'mobile_number': customer['mobile_number'],
                    'email': customer['email'],
                    'primary_vpa': customer['primary_vpa'],
                    'bank_account_no': customer['bank_account_no']
                }
                self.user_type = 'customer'
                return self.authenticated_user
            else:
                return None

        except Exception as e:
            print(f"Authentication error: {e}")
            return None

    def authenticate_by_mobile_pin(self, mobile_number: str, pin: str) -> Optional[Dict]:
        """
        Authenticate customer using mobile number and PIN
        Returns customer data if successful, None otherwise
        """
        query = f"""
        SELECT
            c.customer_id,
            c.name,
            c.mobile_number,
            c.email,
            c.primary_vpa,
            c.bank_account_no,
            cred.pin_hash
        FROM `{self.dataset_id}.upi_customer` c
        JOIN `{self.dataset_id}.upi_customer_credentials` cred
            ON c.customer_id = cred.customer_id
        WHERE c.mobile_number = @mobile
        LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("mobile", "STRING", mobile_number)
            ]
        )

        try:
            query_job = self.bq_client.query(query, job_config=job_config)
            results = list(query_job.result())

            if not results:
                return None

            customer = results[0]
            pin_hash = self.hash_pin(pin)

            # Verify PIN
            if customer['pin_hash'] == pin_hash:
                self.authenticated_user = {
                    'customer_id': customer['customer_id'],
                    'name': customer['name'],
                    'mobile_number': customer['mobile_number'],
                    'email': customer['email'],
                    'primary_vpa': customer['primary_vpa'],
                    'bank_account_no': customer['bank_account_no']
                }
                self.user_type = 'customer'
                return self.authenticated_user
            else:
                return None

        except Exception as e:
            print(f"Authentication error: {e}")
            return None

    def authenticate_merchant_by_vpa_password(self, vpa: str, password: str) -> Optional[Dict]:
        """
        Authenticate merchant using VPA and password from BigQuery
        Returns merchant data if successful, None otherwise
        """
        # Query BigQuery for merchant with password verification
        query = f"""
        SELECT
            m.merchant_id,
            m.merchant_name,
            m.merchant_vpa,
            m.category,
            m.settlement_account_no,
            m.ifsc_code,
            m.password_hash
        FROM `{self.dataset_id}.upi_merchant` m
        WHERE m.merchant_vpa = @vpa
        LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("vpa", "STRING", vpa)
            ]
        )

        try:
            query_job = self.bq_client.query(query, job_config=job_config)
            results = list(query_job.result())

            if not results:
                return None

            merchant = results[0]
            password_hash = hashlib.sha256(password.encode()).hexdigest()

            # Verify password hash
            if merchant['password_hash'] == password_hash:
                self.authenticated_user = {
                    'merchant_id': merchant['merchant_id'],
                    'merchant_name': merchant['merchant_name'],
                    'merchant_vpa': merchant['merchant_vpa'],
                    'category': merchant['category'],
                    'settlement_account_no': merchant['settlement_account_no'],
                    'ifsc_code': merchant['ifsc_code']
                }
                self.user_type = 'merchant'
                return self.authenticated_user
            else:
                return None

        except Exception as e:
            print(f"Authentication error: {e}")
            return None

    def get_sample_merchants(self, limit: int = 5) -> list:
        """Get sample merchants for display"""
        query = f"""
        SELECT merchant_vpa, merchant_name, category, password
        FROM `{self.dataset_id}.upi_merchant`
        WHERE password IS NOT NULL
        LIMIT @limit
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("limit", "INT64", limit)
            ]
        )

        try:
            query_job = self.bq_client.query(query, job_config=job_config)
            results = list(query_job.result())
            return results
        except Exception as e:
            print(f"Error fetching sample merchants: {e}")
            return []

    def get_authenticated_user(self) -> Optional[Tuple[Dict, str]]:
        """
        Prompt user for credentials and authenticate (customer or merchant)
        Returns (user_data, user_type) if successful, (None, None) otherwise
        """
        print("\n" + "=" * 70)
        print("🔐 UPI AUTHENTICATION SYSTEM")
        print("=" * 70)
        print("\nSelect User Type:")
        print("  1. Customer (Login with VPA/Mobile + PIN)")
        print("  2. Merchant (Login with VPA + Password)")
        print("-" * 70)

        user_type_choice = input("Choose user type (1=Customer, 2=Merchant): ").strip()

        if user_type_choice == "1":
            # Customer authentication
            return self._authenticate_customer()
        elif user_type_choice == "2":
            # Merchant authentication
            return self._authenticate_merchant()
        else:
            print("❌ Invalid choice. Please select 1 or 2.")
            return None, None

    def _authenticate_customer(self) -> Optional[Tuple[Dict, str]]:
        """Internal method to authenticate customers"""
        print("\n" + "=" * 70)
        print("👤 CUSTOMER LOGIN")
        print("=" * 70)
        print("Login using your VPA (e.g., user@bank) or Mobile Number")
        print("-" * 70)

        max_attempts = 3
        for attempt in range(max_attempts):
            print(f"\nAttempt {attempt + 1}/{max_attempts}")
            print("-" * 70)

            auth_method = input("Choose login method (1=VPA, 2=Mobile): ").strip()

            if auth_method == "1":
                vpa = input("Enter your VPA: ").strip()
                pin = input("Enter your 4-digit PIN: ").strip()
                user = self.authenticate_by_vpa_pin(vpa, pin)

            elif auth_method == "2":
                mobile = input("Enter your mobile number: ").strip()
                pin = input("Enter your 4-digit PIN: ").strip()
                user = self.authenticate_by_mobile_pin(mobile, pin)

            else:
                print("❌ Invalid choice. Please select 1 or 2.")
                continue

            if user:
                print("\n" + "=" * 70)
                print("✅ CUSTOMER AUTHENTICATION SUCCESSFUL")
                print("=" * 70)
                print(f"Welcome, {user['name']}!")
                print(f"VPA: {user['primary_vpa']}")
                print(f"Mobile: {user['mobile_number']}")
                print(f"User Type: CUSTOMER")
                print("=" * 70 + "\n")
                return user, 'customer'
            else:
                remaining = max_attempts - attempt - 1
                if remaining > 0:
                    print(f"\n❌ Authentication failed. {remaining} attempt(s) remaining.")
                else:
                    print("\n❌ Authentication failed. Maximum attempts exceeded.")
                    return None, None

        return None, None

    def _authenticate_merchant(self) -> Optional[Tuple[Dict, str]]:
        """Internal method to authenticate merchants"""
        print("\n" + "=" * 70)
        print("🏪 MERCHANT LOGIN")
        print("=" * 70)
        
        # Fetch and display sample merchants
        print("Fetching sample merchants...")
        sample_merchants = self.get_sample_merchants(limit=5)
        
        if sample_merchants:
            print("\nSample merchants (for testing):")
            print("-" * 70)
            for merchant in sample_merchants:
                vpa = merchant['merchant_vpa'][:35]
                name = merchant['merchant_name'][:25]
                category = merchant['category'] if merchant['category'] else 'Other'
                password = merchant['password']
                print(f"  • {vpa:<35} ({category})")
                print(f"    Password: {password}")
        else:
            print("\nPassword format: Based on merchant category")
            print("  • Grocery → grocery123")
            print("  • Electronics → electronics123")
            print("  • Restaurant → restaurant123")
            print("  • Healthcare → healthcare123")
            print("  • Entertainment → entertainment123")
            print("  • E-commerce → ecommerce123")
            print("  • Education → education123")
            print("  • Fuel → fuel123")
            print("  • Insurance → insurance123")
            print("  • Retail → retail123")
            print("  • Travel → travel123")
            print("  • Utilities → utilities123")
            print("  • Other → merchant123")
        
        print("-" * 70)

        max_attempts = 3
        for attempt in range(max_attempts):
            print(f"\nAttempt {attempt + 1}/{max_attempts}")
            print("-" * 70)

            vpa = input("Enter merchant VPA: ").strip()
            password = input("Enter merchant password: ").strip()

            merchant = self.authenticate_merchant_by_vpa_password(vpa, password)

            if merchant:
                print("\n" + "=" * 70)
                print("✅ MERCHANT AUTHENTICATION SUCCESSFUL")
                print("=" * 70)
                print(f"Welcome, {merchant['merchant_name']}!")
                print(f"VPA: {merchant['merchant_vpa']}")
                print(f"Category: {merchant['category']}")
                print(f"User Type: MERCHANT")
                print("=" * 70 + "\n")
                return merchant, 'merchant'
            else:
                remaining = max_attempts - attempt - 1
                if remaining > 0:
                    print(f"\n❌ Authentication failed. {remaining} attempt(s) remaining.")
                else:
                    print("\n❌ Authentication failed. Maximum attempts exceeded.")
                    return None, None

        return None, None

    def get_authenticated_customer(self) -> Optional[Dict]:
        """
        Legacy method for backward compatibility
        Returns customer data if successful (customers only)
        """
        user_data, user_type = self.get_authenticated_user()
        if user_type == 'customer':
            return user_data
        return None

    def is_authenticated(self) -> bool:
        """Check if a user is currently authenticated"""
        return self.authenticated_user is not None

    def get_user_type(self) -> Optional[str]:
        """Get the authenticated user's type ('customer' or 'merchant')"""
        return self.user_type

    def get_user_identifier(self) -> Optional[str]:
        """Get the authenticated user's identifier (name for customers, VPA for merchants)"""
        if not self.authenticated_user or not self.user_type:
            return None

        if self.user_type == 'customer':
            return self.authenticated_user.get('name')
        elif self.user_type == 'merchant':
            return self.authenticated_user.get('merchant_vpa')

        return None

    def get_customer_id(self) -> Optional[str]:
        """Get the authenticated customer's ID (customers only)"""
        if self.authenticated_user and self.user_type == 'customer':
            return self.authenticated_user['customer_id']
        return None

    def get_customer_vpa(self) -> Optional[str]:
        """Get the authenticated customer's VPA (customers only)"""
        if self.authenticated_user and self.user_type == 'customer':
            return self.authenticated_user['primary_vpa']
        return None

    def get_merchant_vpa(self) -> Optional[str]:
        """Get the authenticated merchant's VPA (merchants only)"""
        if self.authenticated_user and self.user_type == 'merchant':
            return self.authenticated_user['merchant_vpa']
        return None

    def logout(self):
        """Logout the current user"""
        self.authenticated_user = None
        self.user_type = None