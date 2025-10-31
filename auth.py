import hashlib
from typing import Optional

# Password to User mapping
# In production, store this in a secure database with proper hashing
PASSWORD_TO_USER = {
    # Password hash : Username
    hashlib.sha256("tony123".encode()).hexdigest(): "Tony Toy",
    hashlib.sha256("priya123".encode()).hexdigest(): "Priya Sharma",
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

def get_authenticated_user() -> Optional[str]:
    """Prompt user for password and authenticate."""
    print("\n" + "=" * 60)
    print("🔐 AUTHENTICATION REQUIRED")
    print("=" * 60)
    print("\nAvailable test passwords:")
    print("  - tony123 (Tony Toy)")
    print("  - priya123 (Priya Sharma)")
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