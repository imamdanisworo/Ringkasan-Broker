import os
from huggingface_hub import upload_file

REPO_ID = "imamdanisworo/broker-storage"
HF_TOKEN = os.getenv("HF_TOKEN")  # safer way to use token

def upload_all_csvs():
    folder_path = "."  # this means same folder as app.py and your CSV files
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
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

# This allows it to run from Streamlit button too
if __name__ == "__main__":
    upload_all_csvs()
