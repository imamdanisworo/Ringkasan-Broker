import streamlit as st
import pandas as pd
import re
from datetime import datetime
import plotly.express as px

st.set_page_config(page_title="Financial Broker Summary", layout="wide")
st.title("📊 Ringkasan Broker")

uploaded_files = st.file_uploader("Upload Multiple Excel Files (Sheet1 expected)", type=["xlsx"], accept_multiple_files=True)

if uploaded_files:
    all_data = []

    for file in uploaded_files:
        try:
            match = re.search(r'(\d{8})', file.name)
            if not match:
                st.warning(f"⚠️ Skipped '{file.name}': No date in 'yyyymmdd' format found.")
                continue

            file_date = datetime.strptime(match.group(1), "%Y%m%d").date()

            df = pd.read_excel(file, sheet_name="Sheet1")
            df.columns = df.columns.str.strip()

            required_cols = {"Kode Perusahaan", "Nama Perusahaan", "Volume", "Nilai", "Frekuensi"}
            if not required_cols.issubset(set(df.columns)):
                st.warning(f"⚠️ Skipped '{file.name}': Missing required columns.")
                continue

            df["Tanggal"] = file_date
            df["Broker"] = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]
            all_data.append(df)

        except Exception as e:
            st.error(f"❌ Error processing '{file.name}': {e}")

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df["Tanggal"] = pd.to_datetime(combined_df["Tanggal"])

        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            selected_brokers = st.multiselect("Select Broker(s)", sorted(combined_df["Broker"].unique()))
        with col2:
            selected_fields = st.multiselect("Select Fields", ["Volume", "Nilai", "Frekuensi"])
        with col3:
            min_date, max_date = combined_df["Tanggal"].min().date(), combined_df["Tanggal"].max().date()
            st.markdown("**Select Date Range**")
            date_from = st.date_input("From", min_value=min_date, max_value=max_date, value=min_date)
            date_to = st.date_input("To", min_value=min_date, max_value=max_date, value=max_date)

        if selected_brokers and selected_fields and date_from and date_to:
            if date_from > date_to:
                st.warning("⚠️ 'From' date must be before or equal to 'To' date.")
            else:
                filtered_df = combined_df[
                    (combined_df["Tanggal"].dt.date >= date_from) &
                    (combined_df["Tanggal"].dt.date <= date_to) &
                    (combined_df["Broker"].isin(selected_brokers))
                ]

                if filtered_df.empty:
                    st.info("No data matches the selected filters.")
                else:
                    # Melt to vertical format
                    melted_df = filtered_df.melt(
                        id_vars=["Tanggal", "Broker"],
                        value_vars=selected_fields,
                        var_name="Field",
                        value_name="Value"
                    )

                    # Format values for display
                    display_df = melted_df.copy()
                    display_df["Formatted Value"] = display_df["Value"].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "")
                    display_df["Tanggal"] = display_df["Tanggal"].dt.strftime('%d-%b-%y')
                    display_df = display_df.sort_values(["Tanggal", "Broker", "Field"])

                    st.dataframe(display_df[["Tanggal", "Broker", "Field", "Formatted Value"]], use_container_width=True)

                    # 📊 Chart section
                    st.markdown("---")
                    st.subheader("📈 Chart by Field")

                    for field in selected_fields:
                        chart_data = melted_df[melted_df["Field"] == field].dropna()
                        if not chart_data.empty:
                            fig = px.line(
                                chart_data,
                                x="Tanggal",
                                y="Value",
                                color="Broker",
                                title=f"{field} over Time",
                                markers=True
                            )
                            fig.update_layout(yaxis_title=field, xaxis_title="Tanggal")
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info(f"No data to chart for {field}.")

        elif any([selected_brokers, selected_fields]):
            st.info("Please complete all inputs including the date range to show the table.")
    else:
        st.warning("⚠️ No valid data found from uploaded files.")
