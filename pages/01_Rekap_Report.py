import streamlit as st

from services.data_service import (
    apply_filters,
    fetch_and_process_data,
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
    st.sidebar.subheader("Filter Utama")
    kode_input = st.sidebar.text_input("Kode Produk", value="mdm")

    st.sidebar.subheader("Filter Tanggal")
    tgl_awal = st.sidebar.date_input("Tanggal Awal", value=None, key="tgl_awal_filter")
    tgl_akhir = st.sidebar.date_input(
        "Tanggal Akhir", value=None, key="tgl_akhir_filter"
    )

    if st.sidebar.button("Terapkan Filter", type="primary"):
        st.session_state.active_kode = kode_input
        st.session_state.tgl_awal = tgl_awal
        st.session_state.tgl_akhir = tgl_akhir

    st.sidebar.divider()
    st.sidebar.subheader("Filter Tambahan")
    final_status_options = ["SUKSES PROFIT", "SUKSES LOSS", "GAGAL A1"]
    if "final_status_filter" not in st.session_state:
        st.session_state.final_status_filter = []
    st.session_state.final_status_filter = st.sidebar.multiselect(
        "Filter Final Status",
        options=final_status_options,
        default=st.session_state.final_status_filter,
    )

    if "tujuan_filter" not in st.session_state:
        st.session_state.tujuan_filter = ""
    st.session_state.tujuan_filter = st.sidebar.text_input(
        "Filter Tujuan", value=st.session_state.tujuan_filter
    )

    if "sn_filter" not in st.session_state:
        st.session_state.sn_filter = ""
    st.session_state.sn_filter = st.sidebar.text_input(
        "Filter SN", value=st.session_state.sn_filter
    )


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

        filtered = apply_filters(data, st.session_state)

        st.header(f"Rekap RGU - Produk: {st.session_state.active_kode}")

        # Summary matrix
        st.subheader("📊 Summary Matrix")
        styled_summary = get_styled_summary_table(filtered)
        st.dataframe(styled_summary, width="stretch", hide_index=True)

        # Raw data
        st.subheader("🔍 Raw Data")
        st.dataframe(filtered, width="stretch", hide_index=True)

    else:
        st.info("Silahkan masukkan Kode Produk di sidebar dan klik 'Terapkan Filter'")


def main():
    render_sidebar()
    render_main()


if __name__ == "__main__":
    main()
