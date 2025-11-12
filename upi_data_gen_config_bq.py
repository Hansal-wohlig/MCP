"""
Configuration for UPI Data Generation - BigQuery Direct Mode
"""

# ===== DATA GENERATION CONFIG =====
DATA_CONFIG = {
    # Number of records for each table
    'num_banks': 50,
    'num_customers': 10000,  # 10k customers
    'num_merchants': 1000,   # 1k merchants
    'num_transactions': 100000,  # 100k transactions

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
    'batch_size': 5000,
}

# ===== INDIAN BANKS DATA =====
INDIAN_BANKS = [
    ('SBIN', 'State Bank of India', '607152'),
    ('HDFC', 'HDFC Bank', '607153'),
    ('ICIC', 'ICICI Bank', '607154'),
    ('AXIS', 'Axis Bank', '607155'),
    ('PUNB', 'Punjab National Bank', '607156'),
    ('BOB', 'Bank of Baroda', '607157'),
    ('CNRB', 'Canara Bank', '607158'),
    ('UBIN', 'Union Bank of India', '607159'),
    ('IOBA', 'Indian Overseas Bank', '607160'),
    ('CBIN', 'Central Bank of India', '607161'),
    ('IOB', 'Indian Bank', '607162'),
    ('UCBA', 'UCO Bank', '607163'),
    ('BKID', 'Bank of India', '607164'),
    ('MAHB', 'Bank of Maharashtra', '607165'),
    ('PSIB', 'Punjab & Sind Bank', '607166'),
    ('IDIB', 'Indian Bank', '607167'),
    ('YESB', 'Yes Bank', '607168'),
    ('KVBL', 'Karur Vysya Bank', '607169'),
    ('SRCB', 'Saraswat Co-operative Bank', '607170'),
    ('SCBL', 'Standard Chartered Bank', '607171'),
    ('HSBC', 'HSBC Bank', '607172'),
    ('DEUT', 'Deutsche Bank', '607173'),
    ('CITI', 'Citibank', '607174'),
    ('BARB', 'Bank of America', '607175'),
    ('ABNA', 'ABN AMRO Bank', '607176'),
    ('KKBK', 'Kotak Mahindra Bank', '607177'),
    ('INDB', 'IndusInd Bank', '607178'),
    ('FDRL', 'Federal Bank', '607179'),
    ('SYNB', 'Syndicate Bank', '607180'),
    ('ALLA', 'Allahabad Bank', '607181'),
    ('ANDB', 'Andhra Bank', '607182'),
    ('CORP', 'Corporation Bank', '607183'),
    ('VIJB', 'Vijaya Bank', '607184'),
    ('DENA', 'Dena Bank', '607185'),
    ('OBC', 'Oriental Bank of Commerce', '607186'),
    ('UTIB', 'Axis Bank', '607187'),
    ('CHAS', 'JP Morgan Chase Bank', '607188'),
    ('BNP', 'BNP Paribas', '607189'),
    ('RATN', 'RBL Bank', '607190'),
    ('DBSS', 'DBS Bank India', '607191'),
    ('IDFC', 'IDFC FIRST Bank', '607192'),
    ('LAVB', 'Lakshmi Vilas Bank', '607193'),
    ('JSFB', 'Jana Small Finance Bank', '607194'),
    ('ESAF', 'ESAF Small Finance Bank', '607195'),
    ('ESFB', 'Equitas Small Finance Bank', '607196'),
    ('UTKS', 'Utkarsh Small Finance Bank', '607197'),
    ('FINO', 'Fino Payments Bank', '607198'),
    ('AIRP', 'Airtel Payments Bank', '607199'),
    ('PAYT', 'Paytm Payments Bank', '607200'),
    ('JAKA', 'Jio Payments Bank', '607201'),
]

# ===== NAME DATA =====
FIRST_NAMES = [
    # Male names
    'Aarav', 'Vivaan', 'Aditya', 'Vihaan', 'Arjun', 'Sai', 'Arnav', 'Ayaan', 'Krishna', 'Ishaan',
    'Shaurya', 'Atharv', 'Advik', 'Pranav', 'Reyansh', 'Muhammad', 'Syed', 'Yusuf', 'Kabir', 'Advaith',
    'Raj', 'Rohan', 'Aryan', 'Rahul', 'Dev', 'Rishi', 'Vikram', 'Arun', 'Karan', 'Nikhil',
    # Female names
    'Saanvi', 'Aadhya', 'Kiara', 'Diya', 'Pihu', 'Ananya', 'Anika', 'Navya', 'Angel', 'Pari',
    'Aarohi', 'Myra', 'Sara', 'Jhanvi', 'Siya', 'Avni', 'Riya', 'Shanaya', 'Ira', 'Mira',
    'Priya', 'Nisha', 'Kavya', 'Sneha', 'Pooja', 'Anjali', 'Divya', 'Neha', 'Simran', 'Meera',
    # Popular Indian names
    'Amit', 'Suresh', 'Rajesh', 'Ramesh', 'Mahesh', 'Dinesh', 'Ganesh', 'Harish', 'Satish', 'Prakash',
    'Sunita', 'Geeta', 'Sita', 'Rita', 'Anita', 'Kavita', 'Savita', 'Lalita', 'Smita', 'Nita',
    'Tony', 'Linda', 'Rahul', 'Anjali', 'Vikram'
]

LAST_NAMES = [
    # Common Indian surnames
    'Kumar', 'Singh', 'Sharma', 'Verma', 'Patel', 'Shah', 'Jain', 'Gupta', 'Reddy', 'Rao',
    'Nair', 'Menon', 'Iyer', 'Iyengar', 'Kulkarni', 'Deshmukh', 'Patil', 'Joshi', 'Mehta', 'Agarwal',
    'Bansal', 'Chopra', 'Malhotra', 'Kapoor', 'Bhatia', 'Sethi', 'Arora', 'Khanna', 'Sinha', 'Mishra',
    'Pandey', 'Tiwari', 'Dubey', 'Chaudhary', 'Das', 'Ghosh', 'Bose', 'Sen', 'Roy', 'Mukherjee',
    'Khan', 'Ali', 'Ahmed', 'Hussain', 'Rizvi', 'Siddiqui', 'Ansari', 'Qureshi', 'Sheikh', 'Malik',
    'Toy', 'James', 'Verma', 'Patel', 'Singh'
]

# ===== MERCHANT CATEGORIES =====
MERCHANT_CATEGORIES = [
    'Grocery', 'Electronics', 'Fashion', 'Food & Dining', 'Healthcare',
    'Education', 'Travel', 'Entertainment', 'Utilities', 'Fuel',
    'Books & Stationery', 'Sports & Fitness', 'Home & Furniture',
    'Jewelry', 'Pharmacy', 'Mobile Recharge', 'Insurance', 'Investment'
]

# ===== TRANSACTION REASONS =====
FAILURE_REASONS = [
    'Insufficient Balance',
    'Bank Server Down',
    'Transaction Timeout',
    'Invalid PIN',
    'Account Blocked',
    'Limit Exceeded',
    'Technical Error',
    'Invalid Beneficiary'
]

REFUND_REASONS = [
    'Duplicate Transaction',
    'Merchant Request',
    'Customer Complaint',
    'Service Not Delivered',
    'Wrong Amount Debited',
    'Accidental Payment',
    'Defective Product'
]
