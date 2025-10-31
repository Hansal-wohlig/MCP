"""
Table context configuration.
This file is now used as a FALLBACK if Gemini context generation fails.
Gemini will generate contexts dynamically at server startup.
"""

# Fallback contexts (used only if Gemini generation fails)
FALLBACK_TABLE_CONTEXTS = {
    "customers": {
        "description": "Contains customer profile information including personal details and account creation dates.",
        "usage": "Use this table to get customer names, contact information, addresses, and when they joined.",
        "sensitive": True,
        "row_level_security": True
    },
    "transactions": {
        "description": "Contains all financial transactions including credits, debits, and transfer details.",
        "usage": "Use this table to analyze transaction history, amounts, types, and counterparty information.",
        "sensitive": True,
        "row_level_security": True
    }
}

# Access control configuration
ACCESS_CONTROL = {
    "enabled": True,
    "user_identifier_column": "customer_name",
    "restricted_tables": ["transactions", "customers"]
}