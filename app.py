import streamlit as st
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="Financial Broker Summary", layout="wide")
st.title("📊 Ringkasan Broker")

# Upload Excel file
uploaded_file = st.file_uploader("Upload Excel File (Sheet1 expected)", type=["xlsx"])

if uploaded_file:
    try:
        df = pd.read_excel(uploaded_file, sheet_name="Sheet1")
        df.columns = df.columns.str.strip()

        required_cols = {"Tanggal", "Kode Perusahaan", "Nama Perusahaan", "Volume", "Nilai", "Frekuensi"}
        if not required_cols.issubset(set(df.columns)):
            st.error(f"❌ Excel must include columns: {', '.join(required_cols)}")
        elif df.empty:
            st.warning("⚠️ The sheet is empty.")
        else:
            # Ensure Tanggal column is datetime
            df["Tanggal"] = pd.to_datetime(df["Tanggal"], errors="coerce")
            df = df.dropna(subset=["Tanggal"])

            df["Broker"] = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]

            col1, col2, col3 = st.columns([1, 1, 2])
            with col1:
                selected_brokers = st.multiselect("Select Broker(s)", options=sorted(df["Broker"].unique()))

            with col2:
                selected_fields = st.multiselect("Select Fields", options=["Volume", "Nilai", "Frekuensi"])

            with col3:
                min_date, max_date = df["Tanggal"].min(), df["Tanggal"].max()
                st.markdown("**Select Date Range**")
                date_from = st.date_input("From", min_value=min_date.date(), max_value=max_date.date(), value=min_date.date())
                date_to = st.date_input("To", min_value=min_date.date(), max_value=max_date.date(), value=max_date.date())

            if selected_brokers and selected_fields and date_from and date_to:
                if date_from > date_to:
                    st.warning("⚠️ 'From' date must be before or equal to 'To' date.")
                else:
                    filtered_df = df[
                        (df["Tanggal"].dt.date >= date_from) &
                        (df["Tanggal"].dt.date <= date_to) &
                        (df["Broker"].isin(selected_brokers))
                    ]

                    if filtered_df.empty:
                        st.info("No data matches the selected filters.")
                    else:
                        display_df = (
                            filtered_df[["Tanggal", "Broker"] + selected_fields]
                            .sort_values(["Tanggal", "Broker"])
                        )

                        # Format values
                        for field in selected_fields:
                            display_df[field] = display_df[field].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "")

                        display_df["Tanggal"] = display_df["Tanggal"].dt.strftime('%d-%b-%y')

                        st.dataframe(display_df, use_container_width=True)
            elif any([selected_brokers, selected_fields]):
                st.info("Please complete all inputs including the date range to show the table.")

    except Exception as e:
        st.error(f"❌ Error reading Excel file: {e}")
