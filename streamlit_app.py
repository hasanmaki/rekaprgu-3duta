import pandas as pd
import streamlit as st

st.set_page_config(page_title="Rekap RGU", page_icon="📊", layout="wide")


@st.cache_data(ttl=600)
def fetch_and_process_data(kode_produk: str):
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
    sql = f"SELECT tujuan, status, sn FROM transaksi WHERE kode_produk IN ({placeholders})"
    params = {f"kode_{i}": kode for i, kode in enumerate(kode_list)}
    df = conn.query(sql, params=params)

    if df.empty:
        return df
    df["status_label"] = "SUKSES SUSPECT"
    is_success = df["status"] == 20
    mask_gagal = ~is_success
    mask_direct = is_success & df["sn"].str.startswith("SUP", na=False)
    mask_wait = is_success & df["sn"].str.startswith("CHECK", na=False)

    df.loc[mask_gagal, "status_label"] = "GAGAL"
    df.loc[mask_direct, "status_label"] = "SUKSES DIRECT"
    df.loc[mask_wait, "status_label"] = "SUKSES WAIT"

    return df


def get_summary_table(df: pd.DataFrame):
    """Transformasi data untuk tabel ringkasan."""
    if df.empty:
        return pd.DataFrame()
    summary = (
        df.groupby(["tujuan", "status_label"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    summary.columns.name = None

    return summary


def render_sidebar():
    with st.sidebar:
        st.subheader("Filter Utama")
        kode_input = st.text_input(
            "Kode Produk",
            value="mdm",
            help="Masukkan satu atau beberapa kode produk, dipisahkan dengan koma (contoh: mdm,saka,telkomsel)",
        )
        btn_terapkan = st.button("Terapkan Filter", type="primary")
        if btn_terapkan:
            st.session_state.active_kode = kode_input

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
        total_sukses = (
            (df["status_label"] == "SUKSES DIRECT")
            | (df["status_label"] == "SUKSES WAIT")
            | (df["status_label"] == "SUKSES SUSPECT")
        ).sum()
        st.metric(
            label="Total Sukses",
            border=True,
            help=f"detail : Sukses Direct: {(df['status_label'] == 'SUKSES DIRECT').sum()}, Sukses Wait: {(df['status_label'] == 'SUKSES WAIT').sum()}, Sukses Suspect: {(df['status_label'] == 'SUKSES SUSPECT').sum()}",
            value=total_sukses,
        )
    with col4:
        st.metric(
            "Total Gagal", border=True, value=(df["status_label"] == "GAGAL").sum()
        )


def render_main_content(df: pd.DataFrame):
    if st.session_state.active_kode:
        kode_list = [
            k.strip() for k in st.session_state.active_kode.split(",") if k.strip()
        ]
        if len(kode_list) > 1:
            st.header(
                f"Rekap RGU - {len(kode_list)} Produk: {', '.join(kode_list)}",
                divider="blue",
            )
        else:
            st.header(f"Rekap RGU - Produk: {kode_list[0]}", divider="blue")
    else:
        st.header("Rekap RGU", divider="blue")
    render_metrics(df)
    st.subheader("📈 Perhitungan Pemakaian", divider="gray")
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
    total_sukses = (
        (df["status_label"] == "SUKSES DIRECT")
        | (df["status_label"] == "SUKSES WAIT")
        | (df["status_label"] == "SUKSES SUSPECT")
    ).sum()
    actual_pemakaian = harga * total_sukses
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
            help=f"{harga} × {total_sukses} (Harga × Total Sukses)",
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

    # Create tabs for Matrix and Data
    with st.expander("Lihat Tabel Detail dan Matrix"):
        tab1, tab2 = st.tabs(["📊 Matrix", "🔍 Data"])

        with tab1:
            # Summary Table (Matrix)
            summary_df = get_summary_table(df)
            st.dataframe(summary_df, use_container_width=True, hide_index=True)

        with tab2:
            # Detail Data Table
            st.dataframe(df, use_container_width=True, hide_index=True)


# ==========================================
# 3. APP ORCHESTRATOR (Main Flow)
# ==========================================
def main():
    # Inisialisasi control state (bukan data state)
    if "active_kode" not in st.session_state:
        st.session_state.active_kode = None

    render_sidebar()

    if st.session_state.active_kode:
        data = fetch_and_process_data(st.session_state.active_kode)

        if not data.empty:
            render_main_content(data)
        else:
            st.warning(
                body=f"Tidak ada data untuk kode produk: {st.session_state.active_kode}"
            )
    else:
        st.info("Silahkan masukkan Kode Produk di sidebar dan klik 'Terapkan Filter'")


if __name__ == "__main__":
    main()
