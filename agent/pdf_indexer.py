import os
from langchain_community.document_loaders import PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import FAISS
from langchain_google_vertexai import VertexAIEmbeddings
import config

def create_vector_store():
    """Create and save FAISS vector store from PDF using Vertex AI."""
    
    if not os.path.exists(config.PDF_PATH):
        print(f"❌ Error: PDF not found at {config.PDF_PATH}")
        return

    print("\n" + "="*60)
    print("📄 PDF INDEXING WITH VERTEX AI")
    print("="*60)
    
    print(f"\n[1/4] Loading PDF from {config.PDF_PATH}...")
    loader = PyPDFLoader(config.PDF_PATH)
    documents = loader.load()
    print(f"✓ Loaded {len(documents)} pages")

    print("\n[2/4] Splitting into chunks...")
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1500, 
        chunk_overlap=150
    )
    docs = text_splitter.split_documents(documents)
    print(f"✓ Created {len(docs)} text chunks")

    print("\n[3/4] Creating embeddings with Vertex AI...")
    try:
        embeddings = VertexAIEmbeddings(
            model_name="text-embedding-004",
            project=config.GCP_PROJECT_ID,
            location=config.GCP_LOCATION if hasattr(config, 'GCP_LOCATION') else "us-central1"
        )
        print("✓ Vertex AI embeddings initialized")
    except Exception as e:
        print(f"❌ Error initializing Vertex AI embeddings: {e}")
        return

    print(f"\n[4/4] Creating FAISS vector store at {config.VECTOR_STORE_PATH}...")
    try:
        # Create the directory if it doesn't exist
        os.makedirs(os.path.dirname(config.VECTOR_STORE_PATH) if os.path.dirname(config.VECTOR_STORE_PATH) else '.', exist_ok=True)
        
        db = FAISS.from_documents(docs, embeddings)
        db.save_local(config.VECTOR_STORE_PATH)
        
        print("\n" + "="*60)
        print("✅ VECTOR STORE CREATED SUCCESSFULLY!")
        print("="*60)
        print(f"📁 Location: {config.VECTOR_STORE_PATH}")
        print(f"📊 Total chunks: {len(docs)}")
        print(f"🔧 Embedding model: text-embedding-004 (Vertex AI)")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"\n❌ Error creating vector store: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_vector_store()