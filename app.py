import streamlit as st
import pandas as pd

st.set_page_config(page_title="Financial Broker Summary", layout="wide")
st.title("📊 Ringkasan Broker")

# Upload Excel file
uploaded_file = st.file_uploader("Upload Excel File (Sheet1 expected)", type=["xlsx"])

if uploaded_file:
    try:
        df = pd.read_excel(uploaded_file, sheet_name="Sheet1")
        df.columns = df.columns.str.strip()  # Clean column names

        required_cols = {"Kode Perusahaan", "Nama Perusahaan", "Volume", "Nilai", "Frekuensi"}
        if not required_cols.issubset(set(df.columns)):
            st.error(f"Excel must include columns: {', '.join(required_cols)}")
        elif df.empty:
            st.warning("The sheet is empty.")
        else:
            df["Broker"] = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]

            # Input fields with blank defaults
            col1, col2, col3 = st.columns([1, 1, 2])

            with col1:
                selected_brokers = st.multiselect("Select Broker(s)", options=df["Broker"].tolist())

            with col2:
                selected_fields = st.multiselect("Select Fields", options=["Volume", "Nilai", "Frekuensi"])

            with col3:
                st.markdown("**Select Date Range**")
                date_from = st.date_input("From", value=None, key="from_date")
                date_to = st.date_input("To", value=None, key="to_date")

            # Display table only when all required inputs are set
            if selected_brokers and selected_fields and date_from and date_to:
                if date_from > date_to:
                    st.warning("⚠️ 'From' date must be before or equal to 'To' date.")
                else:
                    date_range = pd.date_range(date_from, date_to, freq="D").strftime('%d-%b-%y')

                    records = []
                    for broker in selected_brokers:
                        row = df[df["Broker"] == broker]
                        if not row.empty:
                            row_data = row.iloc[0]
                            for date_str in date_range:
                                for field in selected_fields:
                                    records.append({
                                        "Date": date_str,
                                        "Field": field,
                                        "Broker": broker,
                                        "Value": row_data[field] if pd.notna(row_data[field]) else ""
                                    })

                    display_df = pd.DataFrame(records)
                    st.dataframe(display_df, use_container_width=True)
            else:
                st.info("Please select all required inputs to display the table.")

    except Exception as e:
        st.error(f"❌ Error reading Excel file: {e}")
else:
    st.info("Please upload an Excel file to begin.")
