import hashlib
import os
import sys
from typing import Optional

# Password to User mapping
# In production, store this in a secure database with proper hashing
PASSWORD_TO_USER = {
    # Password hash : Username
    hashlib.sha256("tony123".encode()).hexdigest(): "Tony Toy",
    hashlib.sha256("linda123".encode()).hexdigest(): "Linda James",
    hashlib.sha256("rahul123".encode()).hexdigest(): "Rahul Verma",
    hashlib.sha256("anjali123".encode()).hexdigest(): "Anjali Patel",
    hashlib.sha256("vikram123".encode()).hexdigest(): "Vikram Singh",
}


def authenticate_user_by_password(password: str) -> Optional[str]:
    """
    Authenticate user by password only.
    Returns username if authentication successful, None otherwise.
    """
    password_hash = hashlib.sha256(password.encode()).hexdigest()

    return PASSWORD_TO_USER.get(password_hash)

def get_authenticated_user(password_override: Optional[str] = None) -> Optional[str]:
    """
    Authenticate user.

    Authentication methods (in order of priority):
    1. password_override parameter (for programmatic use)
    2. UPI_AUTH_PASSWORD environment variable (for background jobs)
    3. Interactive prompt (for interactive use)

    Args:
        password_override: Optional password to use instead of prompting

    Returns:
        Username if authentication successful, None otherwise
    """
    # Try password override first (passed as argument)
    if password_override:
        username = authenticate_user_by_password(password_override)
        if username:
            print("\n" + "=" * 60)
            print("🔐 AUTHENTICATION")
            print("=" * 60)
            print(f"✓ Authentication successful!")
            print(f"✓ Logged in as: {username}")
            print("=" * 60 + "\n")
            return username
        else:
            print("\n❌ Invalid password provided")
            return None

    # Try environment variable
    env_password = os.environ.get('UPI_AUTH_PASSWORD')
    if env_password:
        username = authenticate_user_by_password(env_password)
        if username:
            print("\n" + "=" * 60)
            print("🔐 AUTHENTICATION")
            print("=" * 60)
            print(f"✓ Authentication successful (via environment variable)")
            print(f"✓ Logged in as: {username}")
            print("=" * 60 + "\n")
            return username
        else:
            print("\n❌ Invalid password in UPI_AUTH_PASSWORD environment variable")
            return None

    # Interactive prompt (only if stdin is available)
    if not sys.stdin.isatty():
        print("\n❌ No password provided and running in non-interactive mode")
        print("   Set UPI_AUTH_PASSWORD environment variable or pass password as argument")
        return None

    print("\n" + "=" * 60)
    print("🔐 AUTHENTICATION REQUIRED")
    print("=" * 60)
    print("\nAvailable test passwords:")
    print("  - tony123 (Tony Toy)")
    print("  - linda123 (Linda James)")
    print("  - rahul123 (Rahul Verma)")
    print("  - anjali123 (Anjali Patel)")
    print("  - vikram123 (Vikram Singh)")
    print()

    max_attempts = 3
    for attempt in range(max_attempts):
        password = input("Enter password: ").strip()

        username = authenticate_user_by_password(password)

        if username:
            print(f"✓ Authentication successful!")
            print(f"✓ Logged in as: {username}")
            return username
        else:
            remaining = max_attempts - attempt - 1
            if remaining > 0:
                print(f"✗ Invalid password. {remaining} attempt(s) remaining.\n")
            else:
                print("✗ Authentication failed. Maximum attempts exceeded.")
                return None

    return None