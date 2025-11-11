"""
Customer Authentication System
Authenticates UPI customers using VPA and PIN
"""

import hashlib
from typing import Optional, Dict
from google.cloud import bigquery
import config

class CustomerAuthenticator:
    def __init__(self):
        self.bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
        self.dataset_id = f"{config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET}"
        self.authenticated_customer = None

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
                self.authenticated_customer = {
                    'customer_id': customer['customer_id'],
                    'name': customer['name'],
                    'mobile_number': customer['mobile_number'],
                    'email': customer['email'],
                    'primary_vpa': customer['primary_vpa'],
                    'bank_account_no': customer['bank_account_no']
                }
                return self.authenticated_customer
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
                self.authenticated_customer = {
                    'customer_id': customer['customer_id'],
                    'name': customer['name'],
                    'mobile_number': customer['mobile_number'],
                    'email': customer['email'],
                    'primary_vpa': customer['primary_vpa'],
                    'bank_account_no': customer['bank_account_no']
                }
                return self.authenticated_customer
            else:
                return None

        except Exception as e:
            print(f"Authentication error: {e}")
            return None

    def get_authenticated_customer(self) -> Optional[Dict]:
        """
        Prompt user for credentials and authenticate
        Returns customer data if successful
        """
        print("\n" + "=" * 70)
        print("🔐 UPI CUSTOMER AUTHENTICATION")
        print("=" * 70)
        print("\nLogin using your VPA (e.g., user@bank) or Mobile Number")
        print("-" * 70)

        max_attempts = 3
        for attempt in range(max_attempts):
            print(f"\nAttempt {attempt + 1}/{max_attempts}")
            print("-" * 70)

            auth_method = input("Choose login method (1=VPA, 2=Mobile): ").strip()

            if auth_method == "1":
                vpa = input("Enter your VPA: ").strip()
                pin = input("Enter your 4-digit PIN: ").strip()

                customer = self.authenticate_by_vpa_pin(vpa, pin)

            elif auth_method == "2":
                mobile = input("Enter your mobile number: ").strip()
                pin = input("Enter your 4-digit PIN: ").strip()

                customer = self.authenticate_by_mobile_pin(mobile, pin)

            else:
                print("❌ Invalid choice. Please select 1 or 2.")
                continue

            if customer:
                print("\n" + "=" * 70)
                print("✅ AUTHENTICATION SUCCESSFUL")
                print("=" * 70)
                print(f"Welcome, {customer['name']}!")
                print(f"VPA: {customer['primary_vpa']}")
                print(f"Mobile: {customer['mobile_number']}")
                print("=" * 70 + "\n")
                return customer
            else:
                remaining = max_attempts - attempt - 1
                if remaining > 0:
                    print(f"\n❌ Authentication failed. {remaining} attempt(s) remaining.")
                else:
                    print("\n❌ Authentication failed. Maximum attempts exceeded.")
                    return None

        return None

    def is_authenticated(self) -> bool:
        """Check if a customer is currently authenticated"""
        return self.authenticated_customer is not None

    def get_customer_id(self) -> Optional[str]:
        """Get the authenticated customer's ID"""
        if self.authenticated_customer:
            return self.authenticated_customer['customer_id']
        return None

    def get_customer_vpa(self) -> Optional[str]:
        """Get the authenticated customer's VPA"""
        if self.authenticated_customer:
            return self.authenticated_customer['primary_vpa']
        return None

    def logout(self):
        """Logout the current customer"""
        self.authenticated_customer = None
