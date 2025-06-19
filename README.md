 # Financial Broker Summary App
 
 A simple Streamlit app for uploading and viewing broker data from Excel.
 
 ## How to Run
 
 ```bash
 pip install -r requirements.txt
 streamlit run app.py
+```
+
+### Hugging Face Token
+
+This app requires a Hugging Face token to access the summarization model. Store your token in `.streamlit/secrets.toml` under the key `HF_TOKEN` or set it as an environment variable.
