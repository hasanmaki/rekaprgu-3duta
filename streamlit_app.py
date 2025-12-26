import pandas as pd
import streamlit as st

st.set_page_config(page_title="Rekap RGU", page_icon="📊", layout="wide")


@st.cache_data(ttl=600)
def fetch_and_process_data(kode_produk: str, tgl_awal=None, tgl_akhir=None):
    """
    Lazy loading: Data hanya di-load jika kode_produk berubah.
    Logic labeling menggunakan vektorisasi (jauh lebih cepat dari .apply).
    """
    if not kode_produk.strip():
        return pd.DataFrame()

    conn = st.connection("sql")
    kode_list = [k.strip() for k in kode_produk.split(",") if k.strip()]

    if not kode_list:
        return pd.DataFrame()
    placeholders = ",".join([f":kode_{i}" for i in range(len(kode_list))])

    # Base query
    sql = f"SELECT tujuan, status, sn, tgl_status FROM transaksi WHERE kode_produk IN ({placeholders})"

    # Add date filter based on parameters
    date_conditions = []
    if tgl_awal:
        date_conditions.append("tgl_status >= :tgl_awal")
    if tgl_akhir:
        # Add one day to end date to include the entire day
        date_conditions.append("tgl_status < DATEADD(day, 1, :tgl_akhir)")

    if date_conditions:
        sql += " AND " + " AND ".join(date_conditions)

    params = {f"kode_{i}": kode for i, kode in enumerate(kode_list)}

    # Add date parameters if they exist
    if tgl_awal:
        params["tgl_awal"] = tgl_awal
    if tgl_akhir:
        params["tgl_akhir"] = tgl_akhir

    df = conn.query(sql, params=params)

    if df.empty:
        return df

    # Split datetime into date and time columns
    df_tgl = pd.to_datetime(df["tgl_status"])
    df["tgl_status"] = df_tgl.dt.date
    df["jam_status"] = df_tgl.dt.time
    df["status_label"] = "GAGAL"  # Default status
    is_success = df["status"] == 20
    mask_valid = is_success & df["sn"].str.startswith("SUP", na=False)
    mask_wait = is_success & ~df["sn"].str.startswith("SUP", na=False)

    df.loc[mask_valid, "status_label"] = "SUKSES VALID"
    df.loc[mask_wait, "status_label"] = "SUKSES WAIT"

    # Calculate final status based on business rules
    df["final_status"] = "GAGAL A1"  # Default final status

    # Group by tujuan to apply business rules
    for tujuan, group in df.groupby("tujuan"):
        valid_count = (group["status_label"] == "SUKSES VALID").sum()
        wait_count = (group["status_label"] == "SUKSES WAIT").sum()

        if valid_count == 1:
            # Rule a: exactly 1 SUKSES VALID = SUKSES PROFIT
            df.loc[group.index, "final_status"] = "SUKSES PROFIT"
        elif valid_count > 1:
            # Rule b: more than 1 SUKSES VALID = SUKSES LOSS (double inject)
            df.loc[group.index, "final_status"] = "SUKSES LOSS"
        elif valid_count == 0 and wait_count > 0:
            # Rule c: no SUKSES VALID but has SUKSES WAIT = SUKSES PROFIT
            df.loc[group.index, "final_status"] = "SUKSES PROFIT"
        # else: remains GAGAL A1 (no success at all)

    return df


def get_summary_table(df: pd.DataFrame):
    """Transformasi data untuk tabel ringkasan."""
    if df.empty:
        return pd.DataFrame()
    # Use final_status for summary instead of status_label
    summary = (
        df.groupby(["tujuan", "final_status"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    summary.columns.name = None

    return summary


def get_styled_summary_table(df: pd.DataFrame):
    """Transformasi data untuk tabel ringkasan dengan color coding."""
    if df.empty:
        return pd.DataFrame()

    # Use final_status for summary instead of status_label
    summary = (
        df.groupby(["tujuan", "final_status"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    summary.columns.name = None

    # Apply styling with color coding using the newer methods
    def highlight_cells(styler):
        # Apply different colors to different columns
        if "SUKSES PROFIT" in summary.columns:
            styler.apply(
                lambda x: [
                    "background-color: lightgreen"
                    if v > 0
                    else "background-color: lightgray"
                    for v in x
                ],
                subset=["SUKSES PROFIT"],
            )
        if "SUKSES LOSS" in summary.columns:
            styler.apply(
                lambda x: [
                    "background-color: lightyellow"
                    if v > 0
                    else "background-color: lightgray"
                    for v in x
                ],
                subset=["SUKSES LOSS"],
            )
        if "GAGAL A1" in summary.columns:
            styler.apply(
                lambda x: [
                    "background-color: lightcoral"
                    if v > 0
                    else "background-color: lightgray"
                    for v in x
                ],
                subset=["GAGAL A1"],
            )
        return styler

    # Apply styling to the dataframe
    styled_summary = summary.style.pipe(highlight_cells)

    # Add some additional styling
    styled_summary = styled_summary.set_properties(**{
        "text-align": "center",
        "font-weight": "bold",
        "border": "1px solid black",
    }).set_table_styles([
        {
            "selector": "th",
            "props": [
                ("background-color", "#f0f0f0"),
                ("font-weight", "bold"),
                ("text-align", "center"),
            ],
        },
        {
            "selector": "caption",
            "props": [
                ("caption-side", "bottom"),
                ("font-size", "0.8em"),
                ("color", "gray"),
            ],
        },
    ])

    return styled_summary


def apply_filters(df: pd.DataFrame):
    """Apply filters to the dataframe based on session state"""
    if df.empty:
        return df

    filtered_df = df.copy()

    # Filter by final status
    if st.session_state.get("final_status_filter", []):
        filtered_df = filtered_df[
            filtered_df["final_status"].isin(st.session_state.final_status_filter)
        ]

    # Filter by tujuan (partial match)
    if st.session_state.get("tujuan_filter", ""):
        tujuan_filter = st.session_state.tujuan_filter.lower()
        filtered_df = filtered_df[
            filtered_df["tujuan"].str.lower().str.contains(tujuan_filter, na=False)
        ]

    # Filter by SN (partial match)
    if st.session_state.get("sn_filter", ""):
        sn_filter = st.session_state.sn_filter.lower()
        filtered_df = filtered_df[
            filtered_df["sn"].str.lower().str.contains(sn_filter, na=False)
        ]

    return filtered_df


@st.fragment
def render_additional_filters():
    """Render additional filters with fragment decorator for partial rerun"""
    st.subheader("Filter Tambahan")

    # Filter untuk final status
    if "final_status_filter" not in st.session_state:
        st.session_state.final_status_filter = []

    final_status_options = ["SUKSES PROFIT", "SUKSES LOSS", "GAGAL A1"]
    selected_final_status = st.multiselect(
        "Filter Final Status",
        options=final_status_options,
        default=st.session_state.final_status_filter,
        help="Pilih satu atau lebih final status untuk difilter",
    )
    st.session_state.final_status_filter = selected_final_status

    # Filter untuk tujuan
    if "tujuan_filter" not in st.session_state:
        st.session_state.tujuan_filter = ""

    tujuan_filter = st.text_input(
        "Filter Tujuan (kosongkan untuk semua)",
        value=st.session_state.tujuan_filter,
        help="Masukkan tujuan yang ingin ditampilkan (partial match)",
    )
    st.session_state.tujuan_filter = tujuan_filter

    # Filter untuk SN
    if "sn_filter" not in st.session_state:
        st.session_state.sn_filter = ""

    sn_filter = st.text_input(
        "Filter SN (kosongkan untuk semua)",
        value=st.session_state.sn_filter,
        help="Masukkan awalan SN yang ingin ditampilkan (partial match)",
    )
    st.session_state.sn_filter = sn_filter

    # Reset filter button
    if st.button("Reset Filter", type="secondary"):
        st.session_state.final_status_filter = []
        st.session_state.tujuan_filter = ""
        st.session_state.sn_filter = ""
        st.session_state.tgl_awal = None
        st.session_state.tgl_akhir = None
        st.rerun()


def render_sidebar():
    with st.sidebar:
        st.subheader("Filter Utama")
        kode_input = st.text_input(
            "Kode Produk",
            value="mdm",
            help="Masukkan satu atau beberapa kode produk, dipisahkan dengan koma (contoh: mdm,saka,telkomsel)",
        )

        # Add date filters
        st.subheader("Filter Tanggal")
        col1, col2 = st.columns(2)
        with col1:
            tgl_awal = st.date_input(
                "Tanggal Awal",
                value=None,
                key="tgl_awal_filter",
                help="Pilih tanggal awal untuk filter",
            )
        with col2:
            tgl_akhir = st.date_input(
                label="Tanggal Akhir",
                value=None,
                key="tgl_akhir_filter",
                help="Kosongkan untuk hanya menggunakan filter tanggal awal",
            )

        btn_terapkan = st.button("Terapkan Filter", type="primary", width="stretch")
        if btn_terapkan:
            st.session_state.active_kode = kode_input
            st.session_state.tgl_awal = tgl_awal
            st.session_state.tgl_akhir = tgl_akhir

        st.divider()
        render_additional_filters()

        st.divider()
        st.subheader("Kalkulator Pemakaian")
        st.text_input("Harga Produk", value="10000", key="harga")
        st.text_input("Saldo Awal", value="500000", key="s_awal")
        st.text_input("Saldo Akhir", value="300000", key="s_akhir")

        if st.button("Hitung Pemakaian"):
            # Contoh logic sederhana
            try:
                pemakaian = int(st.session_state.s_awal) - int(st.session_state.s_akhir)
                st.toast(f"Pemakaian: {pemakaian:,}")
            except:
                st.toast(body="Input saldo harus angka", icon="⚠️")


def render_metrics(df: pd.DataFrame):
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Entri Data", border=True, value=len(df))
    with col2:
        st.metric("Total Unik Tujuan", border=True, value=df["tujuan"].nunique())
    with col3:
        total_profit = (df["final_status"] == "SUKSES PROFIT").sum()
        total_loss = (df["final_status"] == "SUKSES LOSS").sum()
        st.metric(
            label="Total Sukses Profit",
            border=True,
            help=f"Sukses Profit: {total_profit}, Sukses Loss: {total_loss} (double inject)",
            value=total_profit,
        )
    with col4:
        st.metric(
            "Total Gagal A1",
            border=True,
            value=(df["final_status"] == "GAGAL A1").sum(),
        )


def render_matrix_and_calculation(df: pd.DataFrame):
    """Render metrics, summary matrix, and usage calculation"""
    render_metrics(df)

    st.subheader("📊 Summary Matrix", divider="gray")
    styled_summary_df = get_styled_summary_table(df)
    st.dataframe(styled_summary_df, width="stretch", hide_index=True)

    # Summary Statistics
    st.subheader("📋 Summary Statistics", divider="gray")

    # Create summary statistics dataframe
    summary_stats = pd.DataFrame({
        "Kategori": [
            "Total Transaksi",
            "Total Tujuan Unik",
            "SUKSES PROFIT",
            "SUKSES LOSS",
            "GAGAL A1",
        ],
        "Jumlah": [
            len(df),
            df["tujuan"].nunique(),
            (df["final_status"] == "SUKSES PROFIT").sum(),
            (df["final_status"] == "SUKSES LOSS").sum(),
            (df["final_status"] == "GAGAL A1").sum(),
        ],
    })

    st.dataframe(summary_stats, width="stretch", hide_index=True)

    st.subheader("� Perhitungan Pemakaian", divider="gray")
    try:
        harga = (
            int(st.session_state.harga)
            if hasattr(st.session_state, "harga") and st.session_state.harga
            else 10000
        )
        saldo_awal = (
            int(st.session_state.s_awal)
            if hasattr(st.session_state, "s_awal") and st.session_state.s_awal
            else 500000
        )
        saldo_akhir = (
            int(st.session_state.s_akhir)
            if hasattr(st.session_state, "s_akhir") and st.session_state.s_akhir
            else 300000
        )
    except:
        harga = 10000
        saldo_awal = 500000
        saldo_akhir = 300000

    # Calculate usage
    asumsi_pemakaian = saldo_awal - saldo_akhir
    # Use final_status for calculation - only count SUKSES PROFIT for actual usage
    total_sukses_profit = (df["final_status"] == "SUKSES PROFIT").sum()
    actual_pemakaian = harga * total_sukses_profit
    selisih = actual_pemakaian - asumsi_pemakaian

    # Display usage metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "Asumsi Pemakaian", f"{asumsi_pemakaian:,}", help="Saldo Awal - Saldo Akhir"
        )
    with col2:
        st.metric(
            "Actual Pemakaian",
            f"{actual_pemakaian:,}",
            help=f"{harga} × {total_sukses_profit} (Harga × Total Sukses Profit)",
        )
    with col3:
        st.metric(
            "Selisih",
            f"{selisih:,}",
            delta=f"{selisih:,}" if selisih != 0 else None,
            delta_color="inverse" if selisih < 0 else "normal",
        )
    with col4:
        status = "✅ Cocok" if selisih == 0 else "⚠️ Tidak Cocok"
        st.metric("Status", status)


def render_raw_data(df: pd.DataFrame):
    """Render raw data table"""
    st.subheader("🔍 Raw Data", divider="gray")
    st.dataframe(df, width="stretch", hide_index=True)


def render_main_content(df: pd.DataFrame):
    if st.session_state.active_kode:
        kode_list = [
            k.strip() for k in st.session_state.active_kode.split(",") if k.strip()
        ]

        # Format tanggal info
        tgl_info = ""
        if st.session_state.get("tgl_awal"):
            tgl_awal_str = st.session_state.tgl_awal.strftime("%d-%m-%Y")
            tgl_info = f" - Tgl: {tgl_awal_str}"
            if st.session_state.get("tgl_akhir"):
                tgl_akhir_str = st.session_state.tgl_akhir.strftime("%d-%m-%Y")
                tgl_info += f" s/d {tgl_akhir_str}"

        if len(kode_list) > 1:
            st.header(
                f"Rekap RGU - {len(kode_list)} Produk: {', '.join(kode_list)}{tgl_info}",
                divider="blue",
            )
        else:
            st.header(f"Rekap RGU - Produk: {kode_list[0]}{tgl_info}", divider="blue")
    else:
        st.header("Rekap RGU", divider="blue")

    # Create main tabs for Matrix/Calculation and Raw Data
    tab1, tab2 = st.tabs(["📊 Matrix & Kalkulasi", "🔍 Raw Data"])

    with tab1:
        render_matrix_and_calculation(df)

    with tab2:
        render_raw_data(df)


# ==========================================
# 3. APP ORCHESTRATOR (Main Flow)
# ==========================================
def main():
    # Inisialisasi control state (bukan data state)
    if "active_kode" not in st.session_state:
        st.session_state.active_kode = None
    if "tgl_awal" not in st.session_state:
        st.session_state.tgl_awal = None
    if "tgl_akhir" not in st.session_state:
        st.session_state.tgl_akhir = None

    render_sidebar()

    if st.session_state.active_kode:
        data = fetch_and_process_data(
            st.session_state.active_kode,
            st.session_state.tgl_awal,
            st.session_state.tgl_akhir,
        )

        if not data.empty:
            # Apply filters to the data
            filtered_data = apply_filters(data)

            # Show filter info if any filters are applied
            has_filters = (
                st.session_state.get("final_status_filter", [])
                or st.session_state.get("tujuan_filter", "")
                or st.session_state.get("sn_filter", "")
                or st.session_state.get("tgl_awal", None)
                or st.session_state.get("tgl_akhir", None)
            )

            if has_filters:
                st.info(
                    f"Menampilkan {len(filtered_data)} dari {len(data)} data (setelah filter)"
                )

            if not filtered_data.empty:
                render_main_content(filtered_data)
            else:
                st.warning("Tidak ada data yang cocok dengan filter yang dipilih")
        else:
            st.warning(
                body=f"Tidak ada data untuk kode produk: {st.session_state.active_kode}"
            )
    else:
        st.info("Silahkan masukkan Kode Produk di sidebar dan klik 'Terapkan Filter'")


if __name__ == "__main__":
    main()
