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

            # Input section (with no defaults)
            col1, col2, col3 = st.columns([1, 1, 2])

            with col1:
                selected_broker = st.selectbox("Select Broker", options=[""] + df["Broker"].tolist())

            with col2:
                selected_fields = st.multiselect("Select Fields", options=["Volume", "Nilai", "Frekuensi"])

            with col3:
                st.markdown("**Select Date Range**")
                date_from = st.date_input("From", value=None, key="from_date")
                date_to = st.date_input("To", value=None, key="to_date")

            # Display table only when all fields are selected
            if selected_broker and selected_fields and date_from and date_to:
                if date_from > date_to:
                    st.warning("⚠️ 'From' date must be before or equal to 'To' date.")
                else:
                    row = df[df["Broker"] == selected_broker].iloc[0]
                    formatted_date_range = pd.date_range(date_from, date_to, freq="D").strftime('%d-%b-%y')

                    # Prepare display table
                    result = pd.DataFrame([
                        {
                            "Date": date_str,
                            "Field": field,
                            "Broker": selected_broker,
                            "Value": f"{row[field]:,.0f}" if pd.notna(row[field]) else ""
                        }
                        for date_str in formatted_date_range
                        for field in selected_fields
                    ])

                    st.dataframe(result, use_container_width=True)
            else:
                st.info("Please select all required inputs to display the table.")

    except Exception as e:
        st.error(f"❌ Error reading Excel file: {e}")
else:
    st.info("Please upload an Excel file to begin.")
