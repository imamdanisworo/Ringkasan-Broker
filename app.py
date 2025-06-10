import streamlit as st
import pandas as pd
import datetime

st.set_page_config(page_title="Financial App", layout="wide")
st.title("📊 Ringkasan Broker (Start Fresh)")

# Upload Excel
uploaded_file = st.file_uploader("Upload Excel File", type=["xlsx"])

if uploaded_file:
    try:
        # Read Excel
        df = pd.read_excel(uploaded_file, sheet_name="Sheet1")
        df.columns = df.columns.str.strip()  # Clean column names

        # Check expected columns
        expected_cols = {"Kode Perusahaan", "Nama Perusahaan", "Volume", "Nilai", "Frekuensi"}
        if not expected_cols.issubset(set(df.columns)):
            st.error("Excel must include these columns: Kode Perusahaan, Nama Perusahaan, Volume, Nilai, Frekuensi")
        else:
            df["Broker"] = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]

            # Select broker
            selected_broker = st.selectbox("Select Broker", df["Broker"])
            selected_fields = st.multiselect("Select Fields", ["Volume", "Nilai", "Frekuensi"], default=["Volume", "Nilai", "Frekuensi"])
            selected_date = st.date_input("Select Date", datetime.date.today())

            if selected_broker and selected_fields:
                row = df[df["Broker"] == selected_broker].iloc[0]
                display_df = pd.DataFrame({
                    "Date": [selected_date.strftime('%d-%b-%y')] * len(selected_fields),
                    "Field": selected_fields,
                    "Broker": [selected_broker] * len(selected_fields),
                    "Value": [row[field] for field in selected_fields]
                })

                st.dataframe(display_df, use_container_width=True)

    except Exception as e:
        st.error(f"Error: {e}")
else:
    st.info("Upload an Excel file with a sheet named 'Sheet1'")
