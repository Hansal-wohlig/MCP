import os
from dotenv import load_dotenv

# --- 1. Define Paths ---
# Get the project root directory (where this config.py file is)
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# --- 2. Load .env file from Project Root ---
# This looks for .env in the 'MCP' folder
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# --- 3. Google Cloud Configuration ---
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")

# Get the relative path from the .env file
SERVICE_ACCOUNT_KEY_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
if SERVICE_ACCOUNT_KEY_FILE:
    # Build the full, absolute path to the key file
    abs_service_path = os.path.join(PROJECT_ROOT, SERVICE_ACCOUNT_KEY_FILE)
    if os.path.exists(abs_service_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = abs_service_path
    else:
        print(f"Warning: Service account key not found at {abs_service_path}")
else:
    print("Warning: GOOGLE_APPLICATION_CREDENTIALS not set in .env")


# --- 4. API Keys ---
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY") # For agent.py

# --- 5. File Paths (Absolute) ---
# Builds full path to 'MCP/data/...'
PDF_PATH = os.path.join(PROJECT_ROOT, "data", "UPI Transaction Process Explained.pdf")
# Builds full path to 'MCP/vector_store_openai'
VECTOR_STORE_PATH_OPENAI = os.path.join(PROJECT_ROOT, "vector_store_openai")


# --- 6. Validation ---
if not all([GCP_PROJECT_ID, BIGQUERY_DATASET, OPENAI_API_KEY, GOOGLE_API_KEY]):
    raise ValueError(
        "Missing required environment variables from .env file:\n"
        "GCP_PROJECT_ID, BIGQUERY_DATASET, OPENAI_API_KEY, GOOGLE_API_KEY"
    )

if not os.path.exists(PDF_PATH):
     print(f"Warning: PDF_PATH not found at {PDF_PATH}")