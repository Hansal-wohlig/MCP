import hashlib
import os
import sys
from typing import Optional, Tuple

# Password to User mapping for CUSTOMERS
# In production, store this in a secure database with proper hashing
CUSTOMER_PASSWORD_TO_USER = {
    # Password hash : Username
    hashlib.sha256("tony123".encode()).hexdigest(): "Tony Toy",
    hashlib.sha256("linda123".encode()).hexdigest(): "Linda James",
    hashlib.sha256("rahul123".encode()).hexdigest(): "Rahul Verma",
    hashlib.sha256("anjali123".encode()).hexdigest(): "Anjali Patel",
    hashlib.sha256("vikram123".encode()).hexdigest(): "Vikram Singh",
}

# Password to Merchant mapping for MERCHANTS
# In production, store this in a secure database with proper hashing
MERCHANT_PASSWORD_TO_USER = {
    # Password hash : Merchant VPA
    hashlib.sha256("grocery123".encode()).hexdigest(): "merchant0@sbin",
    hashlib.sha256("electronics123".encode()).hexdigest(): "merchant1@hdfc",
    hashlib.sha256("fashion123".encode()).hexdigest(): "merchant2@icic",
    hashlib.sha256("food123".encode()).hexdigest(): "merchant3@axis",
    hashlib.sha256("healthcare123".encode()).hexdigest(): "merchant4@punb",
}

def authenticate_user_by_password(password: str) -> Optional[Tuple[str, str]]:
    """
    Authenticate user by password only.
    Returns (username, user_type) if authentication successful, (None, None) otherwise.
    user_type is either 'customer' or 'merchant'
    """
    password_hash = hashlib.sha256(password.encode()).hexdigest()

    # Check customer passwords first
    if password_hash in CUSTOMER_PASSWORD_TO_USER:
        return CUSTOMER_PASSWORD_TO_USER[password_hash], 'customer'

    # Check merchant passwords
    if password_hash in MERCHANT_PASSWORD_TO_USER:
        return MERCHANT_PASSWORD_TO_USER[password_hash], 'merchant'

    return None, None

def get_authenticated_user(password_override: Optional[str] = None) -> Optional[Tuple[str, str]]:
    """
    Authenticate user (customer or merchant).

    Authentication methods (in order of priority):
    1. password_override parameter (for programmatic use)
    2. UPI_AUTH_PASSWORD environment variable (for background jobs)
    3. Interactive prompt (for interactive use)

    Args:
        password_override: Optional password to use instead of prompting

    Returns:
        (username, user_type) if authentication successful, (None, None) otherwise
        user_type is either 'customer' or 'merchant'
    """
    # Try password override first (passed as argument)
    if password_override:
        username, user_type = authenticate_user_by_password(password_override)
        if username:
            print("\n" + "=" * 60)
            print("🔐 AUTHENTICATION")
            print("=" * 60)
            print(f"✓ Authentication successful!")
            print(f"✓ Logged in as: {username} ({user_type.upper()})")
            print("=" * 60 + "\n")
            return username, user_type
        else:
            print("\n❌ Invalid password provided")
            return None, None

    # Try environment variable
    env_password = os.environ.get('UPI_AUTH_PASSWORD')
    if env_password:
        username, user_type = authenticate_user_by_password(env_password)
        if username:
            print("\n" + "=" * 60)
            print("🔐 AUTHENTICATION")
            print("=" * 60)
            print(f"✓ Authentication successful (via environment variable)")
            print(f"✓ Logged in as: {username} ({user_type.upper()})")
            print("=" * 60 + "\n")
            return username, user_type
        else:
            print("\n❌ Invalid password in UPI_AUTH_PASSWORD environment variable")
            return None, None

    # Interactive prompt (only if stdin is available)
    if not sys.stdin.isatty():
        print("\n❌ No password provided and running in non-interactive mode")
        print("   Set UPI_AUTH_PASSWORD environment variable or pass password as argument")
        return None, None

    print("\n" + "=" * 60)
    print("🔐 AUTHENTICATION REQUIRED")
    print("=" * 60)
    print("\nAvailable test passwords:")
    print("  CUSTOMERS:")
    print("    - tony123 (Tony Toy)")
    print("    - linda123 (Linda James)")
    print("    - rahul123 (Rahul Verma)")
    print("    - anjali123 (Anjali Patel)")
    print("    - vikram123 (Vikram Singh)")
    print("\n  MERCHANTS:")
    print("    - grocery123 (Grocery Store - merchant0@sbin)")
    print("    - electronics123 (Electronics Store - merchant1@hdfc)")
    print("    - fashion123 (Fashion Store - merchant2@icic)")
    print("    - food123 (Food Store - merchant3@axis)")
    print("    - healthcare123 (Healthcare Store - merchant4@punb)")
    print()

    max_attempts = 3
    for attempt in range(max_attempts):
        password = input("Enter password: ").strip()

        username, user_type = authenticate_user_by_password(password)

        if username:
            print(f"✓ Authentication successful!")
            print(f"✓ Logged in as: {username} ({user_type.upper()})")
            return username, user_type
        else:
            remaining = max_attempts - attempt - 1
            if remaining > 0:
                print(f"✗ Invalid password. {remaining} attempt(s) remaining.\n")
            else:
                print("✗ Authentication failed. Maximum attempts exceeded.")
                return None, None

    return None, None