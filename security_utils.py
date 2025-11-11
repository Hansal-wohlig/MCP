import re
from typing import Tuple, List
from datetime import datetime

def validate_query_type(sql_query: str) -> Tuple[bool, str]:
    """
    Layer 1: Comprehensive query type validation against prohibited operations.
    Implements Query Parser & Validator per guardrails documentation.
    
    Args:
        sql_query: The SQL query to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    sql_upper = sql_query.upper().strip()
    
    # Remove SQL comments to prevent bypassing
    sql_upper = re.sub(r'--.*$', '', sql_upper, flags=re.MULTILINE)
    sql_upper = re.sub(r'/\*.*?\*/', '', sql_upper, flags=re.DOTALL)
    
    # Prohibited operations from guardrails document
    prohibited_patterns = {
        'DELETE': [
            'DELETE', 'TRUNCATE', 'DROP TABLE', 'DROP DATABASE'
        ],
        'UPDATE': [
            'UPDATE'
        ],
        'INSERT': [
            'INSERT'
        ],
        'SCHEMA': [
            'ALTER TABLE', 'ALTER DATABASE', 'CREATE TABLE', 'CREATE DATABASE',
            'CREATE INDEX', 'DROP INDEX', 'CREATE VIEW', 'DROP VIEW',
            'ALTER COLUMN', 'ADD COLUMN', 'DROP COLUMN'
        ],
        'ADMIN': [
            'GRANT', 'REVOKE', 'CREATE USER', 'DROP USER', 'ALTER USER',
            'CREATE ROLE', 'DROP ROLE'
        ],
        'INJECTION': [
            ';.*(?:DELETE|UPDATE|INSERT|DROP)', 'EXEC', 'EXECUTE',
            'xp_', 'sp_executesql'
        ],
    }
    
    for category, keywords in prohibited_patterns.items():
        for keyword in keywords:
            # Use word boundaries to avoid false positives
            # Replace spaces in keywords with flexible whitespace pattern
            pattern = r'\b' + re.escape(keyword).replace(r'\ ', r'\s+') + r'\b'
            if re.search(pattern, sql_upper):
                return False, (
                    f"🚫 SECURITY BLOCK: {category} operations are not permitted.\n"
                    f"   Detected: {keyword}\n"
                    f"   This chatbot is READ-ONLY and can only execute SELECT queries.\n"
                    f"   Reason: Banking security regulations require data integrity.\n"
                    f"   This incident has been logged for audit purposes."
                )
    
    # Ensure query starts with SELECT or WITH (for CTEs)
    if not (sql_upper.startswith('SELECT') or sql_upper.startswith('WITH')):
        return False, (
            "🚫 SECURITY BLOCK: Only SELECT queries are permitted.\n"
            "   This chatbot has READ-ONLY access to the database.\n"
            "   This incident has been logged for audit purposes."
        )
    
    return True, ""


def extract_customer_names_from_sql(sql_query: str) -> List[str]:
    """
    Extract customer names from SQL WHERE clauses.
    
    Args:
        sql_query: The SQL query to parse
        
    Returns:
        List of customer names found in the query
    """
    # Pattern for customer_name = 'value'
    pattern = r"customer_name\s*=\s*'([^']+)'"
    matches = re.findall(pattern, sql_query, re.IGNORECASE)
    return matches


def validate_row_level_security(sql_query: str, current_user: str) -> Tuple[bool, str]:
    """
    Layer 3: Validate that the SQL query only accesses the current user's data.
    Implements Row-Level Security per guardrails.
    
    Args:
        sql_query: The SQL query to validate
        current_user: The authenticated user's name
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not current_user:
        return False, "🚫 Authentication required to access database."
    
    # Extract customer names from the query
    referenced_customers = extract_customer_names_from_sql(sql_query)
    
    # Check if query references other customers
    for customer in referenced_customers:
        if customer != current_user:
            return False, (
                f"🚫 SECURITY VIOLATION DETECTED!\n"
                f"   Attempted to access: '{customer}'\n"
                f"   You are authenticated as: '{current_user}'\n"
                f"   You can only access your own data.\n"
                f"   This incident has been logged for audit and compliance."
            )
    
    sql_upper = sql_query.upper()
    
    # Check for queries without WHERE clause on restricted tables
    # This catches attempts to query all data
    restricted_tables = ['CUSTOMERS', 'TRANSACTIONS', 'ACCOUNTS']
    
    for table in restricted_tables:
        # Check if table is referenced
        if f"FROM {table}" in sql_upper or f"JOIN {table}" in sql_upper:
            # Ensure WHERE clause exists
            if "WHERE" not in sql_upper:
                return False, (
                    f"🚫 SECURITY VIOLATION: Query attempts to access all records without filtering.\n"
                    f"   Table: {table}\n"
                    f"   You can only access your own data (authenticated as: '{current_user}').\n"
                    f"   This incident has been logged for audit and compliance."
                )
    
    return True, ""


def validate_natural_language_query(query: str, current_user: str) -> Tuple[bool, str]:
    """
    Validate natural language query for unauthorized access patterns.
    
    Args:
        query: The natural language query from the user
        current_user: The authenticated user's name
        
    Returns:
        Tuple of (is_allowed, error_message)
    """
    if not current_user:
        return False, "🚫 Authentication required to access database."
    
    query_lower = query.lower()
    
    # Patterns that indicate attempt to access all users' data
    prohibited_patterns = [
        "all customers",
        "all users",
        "every customer",
        "every user",
        "list all customers",
        "show all customers",
        "total customers",
        "count of customers",
        "list customers",
        "show customers",
        "all accounts",
        "every account"
    ]
    
    if any(pattern in query_lower for pattern in prohibited_patterns):
        return False, (
            f"🚫 Access Denied: You can only access your own data.\n"
            f"   You are authenticated as '{current_user}'.\n"
            f"   Try asking: 'my transactions' or 'my account details'\n"
            f"   This incident has been logged for audit purposes."
        )
    
    return True, ""


def sanitize_error_message(error: str) -> str:
    """
    Sanitize error messages to prevent information leakage.
    
    Args:
        error: The raw error message
        
    Returns:
        Sanitized, user-friendly error message
    """
    error_lower = str(error).lower()
    
    # Map technical errors to user-friendly messages
    if "timeout" in error_lower:
        return (
            "⏱️ Query timeout: The query took too long to execute (max 30 seconds).\n"
            "   Try simplifying your query or adding more specific filters."
        )
    elif "bytes" in error_lower or "quota" in error_lower:
        return (
            "💾 Query too expensive: This query would process too much data.\n"
            "   Try adding more specific filters to reduce the data scanned."
        )
    elif "permission" in error_lower or "denied" in error_lower:
        return (
            "🚫 Access denied: You don't have permission to access this resource.\n"
            "   Please contact your administrator."
        )
    elif "not found" in error_lower:
        return (
            "❓ Resource not found: The requested table or column doesn't exist.\n"
            "   Please check your query and try again."
        )
    else:
        # Generic error message that doesn't leak details
        return (
            "⚠️ An error occurred while processing your request.\n"
            "   Please try again or contact support if the issue persists."
        )


def format_query_for_logging(query: str, max_length: int = 500) -> str:
    """
    Format query for logging by truncating and sanitizing.
    
    Args:
        query: The query to format
        max_length: Maximum length of the logged query
        
    Returns:
        Formatted query string
    """
    if len(query) > max_length:
        return query[:max_length] + "... [TRUNCATED]"
    return query


def is_sensitive_data(column_name: str) -> bool:
    """
    Check if a column contains sensitive data that should be masked.
    
    Args:
        column_name: Name of the database column
        
    Returns:
        True if column contains sensitive data
    """
    sensitive_patterns = [
        'ssn', 'social_security',
        'password', 'pwd',
        'credit_card', 'card_number', 'cvv',
        'pin', 'secret',
        'tax_id', 'ein'
    ]
    
    column_lower = column_name.lower()
    return any(pattern in column_lower for pattern in sensitive_patterns)


def mask_sensitive_value(value: str, mask_char: str = '*', visible_chars: int = 4) -> str:
    """
    Mask sensitive values, showing only last few characters.
    
    Args:
        value: The value to mask
        mask_char: Character to use for masking
        visible_chars: Number of characters to leave visible
        
    Returns:
        Masked value
    """
    if not value or len(value) <= visible_chars:
        return mask_char * len(value) if value else ""
    
    return mask_char * (len(value) - visible_chars) + value[-visible_chars:]


def generate_security_report(
    user: str,
    allowed_queries: int,
    blocked_queries: int,
    error_queries: int
) -> str:
    """
    Generate a security report for a user session.
    
    Args:
        user: Username
        allowed_queries: Number of allowed queries
        blocked_queries: Number of blocked queries
        error_queries: Number of queries with errors
        
    Returns:
        Formatted security report
    """
    total = allowed_queries + blocked_queries + error_queries
    
    report = f"""
╔═══════════════════════════════════════════════════════════╗
║           SESSION SECURITY REPORT                         ║
╠═══════════════════════════════════════════════════════════╣
║ User: {user:<50} ║
║ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<44} ║
╠═══════════════════════════════════════════════════════════╣
║ Total Queries: {total:<44} ║
║ ✓ Allowed:     {allowed_queries:<44} ║
║ 🚫 Blocked:    {blocked_queries:<44} ║
║ ⚠️  Errors:     {error_queries:<44} ║
╠═══════════════════════════════════════════════════════════╣
"""
    
    if blocked_queries > 0:
        report += f"║ ⚠️  SECURITY ALERT: {blocked_queries} unauthorized access attempts     ║\n"
        report += "║    Review audit log for details                           ║\n"
        report += "╠═══════════════════════════════════════════════════════════╣\n"
    
    report += "║ All queries logged for compliance and audit purposes     ║\n"
    report += "╚═══════════════════════════════════════════════════════════╝\n"
    
    return report