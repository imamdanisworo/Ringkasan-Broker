import os
from huggingface_hub import upload_file
import streamlit as st  # Required to access secrets in Streamlit Cloud

# Hugging Face config
REPO_ID = "imamdanisworo/broker-storage"
HF_TOKEN = st.secrets["HF_TOKEN"]

# Function to upload all .xlsx files from local folder to HF
def upload_all_excels():
    folder_path = "."
    for filename in os.listdir(folder_path):
        if filename.endswith(".xlsx"):
            file_path = os.path.join(folder_path, filename)
            print(f"Uploading {filename}...")

            upload_file(
                path_or_fileobj=file_path,
                path_in_repo=filename,
                repo_id=REPO_ID,
                repo_type="dataset",
                token=HF_TOKEN
            )

            print(f"✅ Uploaded {filename}")
