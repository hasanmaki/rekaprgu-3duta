import streamlit as st

from services.data_service import (
    fetch_and_process_data,
    get_dashboard_metrics,
    get_status_counts,
    get_styled_summary_table,
)

st.set_page_config(page_title="Rekap RGU - Report", page_icon="📊", layout="wide")

# Initialize control state
if "active_kode" not in st.session_state:
    st.session_state.active_kode = None
if "tgl_awal" not in st.session_state:
    st.session_state.tgl_awal = None
if "tgl_akhir" not in st.session_state:
    st.session_state.tgl_akhir = None


def render_sidebar():
    with st.sidebar:
        st.subheader("Filter Utama")
        kode_input = st.text_input("Kode Produk", value="mdm")

        st.subheader("Filter Tanggal")
        tgl_awal = st.date_input("Tanggal Awal", value=None, key="tgl_awal_filter")
        tgl_akhir = st.date_input("Tanggal Akhir", value=None, key="tgl_akhir_filter")

        if st.button(
            label="Terapkan Filter",
            type="primary",
            width="stretch",
            key="apply_filter_button",
        ):
            st.session_state.active_kode = kode_input
            st.session_state.tgl_awal = tgl_awal
            st.session_state.tgl_akhir = tgl_akhir

        st.info("Gunakan tab 'Interaktif' untuk filter detail tambahan")


def render_main():
    if st.session_state.active_kode:
        data = fetch_and_process_data(
            st.session_state.active_kode,
            st.session_state.tgl_awal,
            st.session_state.tgl_akhir,
        )

        if data.empty:
            st.warning(
                f"Tidak ada data untuk kode produk: {st.session_state.active_kode}"
            )
            return

        # Three tabs: Dashboard, Interaktif, and Audit
        tab_dashboard, tab_interactive, tab_audit = st.tabs([
            "Dashboard",
            "Interaktif",
            "Audit",
        ])

        with tab_dashboard:
            st.header(f"Dashboard - Produk: {st.session_state.active_kode}")

            # Dashboard KPIs
            metrics = get_dashboard_metrics(data)
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Rows", metrics.get("total", 0))
            c2.metric("Unique Tujuan", metrics.get("unique_tujuan", 0))
            c3.metric("Total Sukses", metrics.get("sukses", 0))
            c4.metric("Total Gagal", metrics.get("gagal", 0))

            st.subheader("📊 Summary Matrix")

            styled_summary = get_styled_summary_table(data)
            st.dataframe(styled_summary, width="stretch", hide_index=True)

            # Status distribution (bar chart with counts, colored consistently)
            status_counts = get_status_counts(data)
            if not status_counts.empty:
                try:
                    import plotly.express as px

                    color_map = {
                        "SUKSES PROFIT": "#1f77b4",
                        "SUKSES LOSS": "#2a9df4",
                        "GAGAL A1": "#d62728",
                    }

                    fig = px.bar(
                        status_counts,
                        x="final_status",
                        y="count",
                        color="final_status",
                        color_discrete_map=color_map,
                        title="Distribusi Final Status (count)",
                    )
                    fig.update_traces(texttemplate="%{y}", textposition="outside")
                    fig.update_layout(
                        yaxis_title="Count", xaxis_title="", margin=dict(t=30, b=20)
                    )
                    st.plotly_chart(fig, width="stretch")
                except Exception:
                    # If plotly not available or fails, skip plotting
                    pass
                st.divider()

        with tab_interactive:
            st.header("Filter & Interaksi Data")
            st.write(
                "Gunakan filter di bawah untuk berinteraksi dengan data (tidak mengubah Dashboard)."
            )

            # Raw data (expandable) - moved to top
            with st.expander("🔽 Raw Data", expanded=False):
                st.dataframe(data, width="stretch", hide_index=True)

            # Defaults for interactive filters
            min_date = data["tgl_status"].min()
            max_date = data["tgl_status"].max()
            min_time = data["jam_status"].min()
            max_time = data["jam_status"].max()

            # Initialize variables
            submitted = False
            start_date, end_date = min_date, max_date
            tujuan_val, kode_val = "", ""
            jam_start, jam_end = min_time, max_time

            with st.form("interactive_filters"):
                col1, col2 = st.columns(2)
                with col1:
                    start_date = st.date_input(
                        "Tanggal Awal", value=min_date, key="int_start_date"
                    )
                    end_date = st.date_input(
                        "Tanggal Akhir", value=max_date, key="int_end_date"
                    )
                with col2:
                    tujuan_val = st.text_input(
                        "Filter Tujuan (partial)", value="", key="int_tujuan"
                    )
                    kode_val = st.text_input(
                        "Filter Kode Produk (partial)", value="", key="int_kode"
                    )

                # Status label filter
                status_options = ["SUKSES PROFIT", "SUKSES LOSS", "GAGAL A1"]
                status_val = st.multiselect(
                    "Filter Status", options=status_options, key="int_status"
                )
                jam_col1, jam_col2 = st.columns(2)
                with jam_col1:
                    jam_start = st.time_input(
                        "Jam mulai", value=min_time, key="int_jam_start"
                    )
                with jam_col2:
                    jam_end = st.time_input(
                        "Jam akhir", value=max_time, key="int_jam_end"
                    )

                # Always create submit button
                submitted = st.form_submit_button("Apply Filters")

            if submitted:
                # Time validation
                if jam_start and jam_end and jam_start > jam_end:
                    st.error("Jam awal tidak boleh lebih besar dari jam akhir!")
                else:
                    filtered_local = data.copy()
                    # Date filter
                    filtered_local = filtered_local[
                        (filtered_local["tgl_status"] >= start_date)
                        & (filtered_local["tgl_status"] <= end_date)
                    ]

                    # Tujuan filter
                    if tujuan_val:
                        filtered_local = filtered_local[
                            filtered_local["tujuan"].str.contains(
                                tujuan_val, case=False, na=False
                            )
                        ]

                    # Kode produk filter
                    if kode_val:
                        filtered_local = filtered_local[
                            filtered_local["kode_produk"].str.contains(
                                kode_val, case=False, na=False
                            )
                        ]

                    # Status filter
                    if status_val:
                        filtered_local = filtered_local[
                            filtered_local["final_status"].isin(status_val)
                        ]

                    # Time filter
                    filtered_local = filtered_local[
                        (filtered_local["jam_status"] >= jam_start)
                        & (filtered_local["jam_status"] <= jam_end)
                    ]

                    st.subheader(f"Hasil Filter: {len(filtered_local)} baris")
                    st.dataframe(filtered_local, width="stretch", hide_index=True)
            else:
                st.info(
                    "Terapkan filter di form dan klik 'Apply Filters' untuk melihat hasilnya."
                )

        with tab_audit:
            st.header("🧮 Audit & Kalkulasi")
            st.write(
                "Halaman ini untuk perhitungan audit dan analisis saldo dengan filter."
            )

            # Check if there's synchronized audit data
            if (
                "audit_sync_data" in st.session_state
                and not st.session_state.audit_sync_data.empty
            ):
                st.subheader("📋 Data Audit Tersinkronisasi")
                audit_data = st.session_state.audit_sync_data

                # Display summary metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Audit", len(audit_data))
                with col2:
                    success_count = len(
                        audit_data[audit_data["final_status"] == "SUKSES PROFIT"]
                    )
                    st.metric("Sukses Audit", success_count)
                with col3:
                    error_count = len(
                        audit_data[audit_data["final_status"] == "GAGAL A1"]
                    )
                    st.metric("Gagal Audit", error_count)
                with col4:
                    st.metric("Sumber", "API Check")

                # Display audit data
                with st.expander("🔽 Lihat Data Audit", expanded=False):
                    st.dataframe(audit_data, width="stretch", hide_index=True)

                st.divider()

            # Initialize variables
            submitted_audit = False
            start_date_audit, end_date_audit = (
                data["tgl_status"].min(),
                data["tgl_status"].max(),
            )
            jam_start_audit, jam_end_audit = (
                data["jam_status"].min(),
                data["jam_status"].max(),
            )
            saldo_awal, saldo_akhir = 0, 0
            harga_produk = 1000

            with st.form("audit_form"):
                col1, col2 = st.columns(2)
                with col1:
                    start_date_audit = st.date_input(
                        "Tanggal Awal", value=start_date_audit, key="audit_start_date"
                    )
                    end_date_audit = st.date_input(
                        "Tanggal Akhir", value=end_date_audit, key="audit_end_date"
                    )
                    jam_start_audit = st.time_input(
                        "Jam Mulai", value=jam_start_audit, key="audit_jam_start"
                    )
                    jam_end_audit = st.time_input(
                        "Jam Akhir", value=jam_end_audit, key="audit_jam_end"
                    )
                with col2:
                    kode_produk_audit = st.text_input(
                        "Kode Produk",
                        value=st.session_state.active_kode or "",
                        key="audit_kode_produk",
                    )
                    saldo_awal = st.number_input(
                        "Saldo Awal", value=0, key="saldo_awal"
                    )
                    saldo_akhir = st.number_input(
                        "Saldo Akhir", value=0, key="saldo_akhir"
                    )
                    harga_produk = st.number_input(
                        "Harga Produk", value=1000, key="harga_produk"
                    )

                submitted_audit = st.form_submit_button("Hitung Audit")

            if submitted_audit:
                # Time validation
                if (
                    jam_start_audit
                    and jam_end_audit
                    and jam_start_audit > jam_end_audit
                ):
                    st.error("Jam awal tidak boleh lebih besar dari jam akhir!")
                else:
                    # Filter data for audit calculations
                    audit_data = data.copy()

                    # Apply kode_produk filter if specified
                    if kode_produk_audit:
                        audit_data = audit_data[
                            audit_data["kode_produk"].str.contains(
                                kode_produk_audit, case=False, na=False
                            )
                        ]

                    # Apply date and time filters
                    audit_data = audit_data[
                        (audit_data["tgl_status"] >= start_date_audit)
                        & (audit_data["tgl_status"] <= end_date_audit)
                        & (audit_data["jam_status"] >= jam_start_audit)
                        & (audit_data["jam_status"] <= jam_end_audit)
                    ]

                    # Calculate totals
                    total_sukses = audit_data[
                        audit_data["final_status"].isin([
                            "SUKSES PROFIT",
                            "SUKSES LOSS",
                        ])
                    ].shape[0]
                    total_gagal = audit_data[
                        audit_data["final_status"] == "GAGAL A1"
                    ].shape[0]

                    # Calculate monetary values
                    nilai_sukses = total_sukses * harga_produk
                    nilai_refund = total_gagal * harga_produk
                    expected_usage = saldo_awal - saldo_akhir
                    selisih = expected_usage - nilai_sukses

                    # Determine status with color
                    if abs(selisih) <= (nilai_sukses * 0.05):  # 5% tolerance
                        status_text = "COCOK"
                        status_color = "green"
                    else:
                        status_text = "SELISIH"
                        status_color = "red"

                    # Display results in matrix format
                    st.subheader("📊 Hasil Audit")

                    # Create matrix data
                    matrix_data = [
                        ["Total Sukses", f"{total_sukses} transaksi"],
                        ["Total Gagal", f"{total_gagal} transaksi"],
                        ["Nilai Sukses", f"Rp {nilai_sukses:,.0f}"],
                        ["Nilai Refund", f"Rp {nilai_refund:,.0f}"],
                        ["Expected Usage", f"Rp {expected_usage:,.0f}"],
                        ["Actual Usage", f"Rp {nilai_sukses:,.0f}"],
                        ["Selisih", f"Rp {selisih:,.0f}"],
                        [
                            "Status",
                            f"<span style='color: {status_color}; font-weight: bold;'>{status_text}</span>",
                        ],
                    ]

                    # Display as matrix
                    for i, (label, value) in enumerate(matrix_data):
                        if i % 2 == 0:
                            col1, col2 = st.columns(2)

                        with col1:
                            st.write(f"**{label}:**")
                        with col2:
                            if "Status" in label and "<span" in value:
                                st.markdown(value, unsafe_allow_html=True)
                            else:
                                st.write(value)

    else:
        st.info("Silahkan masukkan Kode Produk di sidebar dan klik 'Terapkan Filter'")


def main():
    render_sidebar()
    render_main()


if __name__ == "__main__":
    main()
