import streamlit as st
import pandas as pd
import re
from datetime import datetime

st.set_page_config(page_title="Financial Broker Summary", layout="wide")
st.title("📊 Ringkasan Broker")

uploaded_file = st.file_uploader("Upload Excel File (Sheet1 expected)", type=["xlsx"])

if uploaded_file:
    try:
        # Extract filename
        filename = uploaded_file.name
        match = re.search(r'(\d{8})', filename)
        if match:
            date_str = match.group(1)  # e.g., "20250601"
            tanggal = datetime.strptime(date_str, "%Y%m%d").date()
        else:
            st.error("❌ Filename must contain a date in 'yyyymmdd' format, e.g., 'Ringkasan Broker-20250601.xlsx'")
            st.stop()

        # Load Excel
        df = pd.read_excel(uploaded_file, sheet_name="Sheet1")
        df.columns = df.columns.str.strip()

        required_cols = {"Kode Perusahaan", "Nama Perusahaan", "Volume", "Nilai", "Frekuensi"}
        if not required_cols.issubset(set(df.columns)):
            st.error(f"❌ Excel must include columns: {', '.join(required_cols)}")
        elif df.empty:
            st.warning("⚠️ The sheet is empty.")
        else:
            # Inject extracted date
            df["Tanggal"] = tanggal
            df["Broker"] = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]

            col1, col2, col3 = st.columns([1, 1, 2])
            with col1:
                selected_brokers = st.multiselect("Select Broker(s)", options=sorted(df["Broker"].unique()))

            with col2:
                selected_fields = st.multiselect("Select Fields", options=["Volume", "Nilai", "Frekuensi"])

            with col3:
                st.markdown("**Select Date Range**")
                date_from = st.date_input("From", value=tanggal)
                date_to = st.date_input("To", value=tanggal)

            if selected_brokers and selected_fields and date_from and date_to:
                if date_from > date_to:
                    st.warning("⚠️ 'From' date must be before or equal to 'To' date.")
                else:
                    filtered_df = df[
                        (df["Tanggal"] >= date_from) &
                        (df["Tanggal"] <= date_to) &
                        (df["Broker"].isin(selected_brokers))
                    ]

                    if filtered_df.empty:
                        st.info("No data matches the selected filters.")
                    else:
                        display_df = (
                            filtered_df[["Tanggal", "Broker"] + selected_fields]
                            .sort_values(["Tanggal", "Broker"])
                        )

                        # Format numbers
                        for field in selected_fields:
                            display_df[field] = display_df[field].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "")

                        display_df["Tanggal"] = display_df["Tanggal"].apply(lambda d: d.strftime('%d-%b-%y'))

                        st.dataframe(display_df, use_container_width=True)
            elif any([selected_brokers, selected_fields]):
                st.info("Please complete all inputs including the date range to show the table.")

    except Exception as e:
        st.error(f"❌ Error reading Excel file: {e}")
