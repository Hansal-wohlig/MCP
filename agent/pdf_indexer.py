import os
from config import PDF_PATH, VECTOR_STORE_PATH, GOOGLE_API_KEY
from langchain_community.document_loaders import PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import FAISS
from langchain_google_genai import GoogleGenerativeAIEmbeddings

def create_vector_store():
    """Create and save FAISS vector store from PDF."""
    if not os.path.exists(PDF_PATH):
        print(f"Error: PDF not found at {PDF_PATH}")
        return

    print("Loading PDF...")
    loader = PyPDFLoader(PDF_PATH)
    documents = loader.load()

    print("Splitting into chunks...")
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1500, chunk_overlap=150)
    docs = text_splitter.split_documents(documents)

    print("Creating embeddings...")
    try:
        embeddings = GoogleGenerativeAIEmbeddings(
            model="gemini-embedding-001",
            google_api_key=GOOGLE_API_KEY
        )
    except Exception as e:
        print(f"Error initializing embeddings: {e}")
        return

    print(f"Creating FAISS store at {VECTOR_STORE_PATH}...")
    try:
        db = FAISS.from_documents(docs, embeddings)
        db.save_local(VECTOR_STORE_PATH)
        print("Vector store created successfully!")
    except Exception as e:
        print(f"Error creating vector store: {e}")

if __name__ == "__main__":
    create_vector_store()