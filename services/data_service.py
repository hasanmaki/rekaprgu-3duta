import pandas as pd
import streamlit as st


@st.cache_data(ttl=600)
def fetch_and_process_data(
    kode_produk: str, tgl_awal=None, tgl_akhir=None
) -> pd.DataFrame:
    """
    Lazy loading: Data hanya di-load jika kode_produk berubah.
    Logic labeling menggunakan vektorisasi (lebih cepat dari .apply).
    """
    if not kode_produk or not kode_produk.strip():
        return pd.DataFrame()

    conn = st.connection("sql")
    kode_list = [k.strip() for k in kode_produk.split(",") if k.strip()]

    if not kode_list:
        return pd.DataFrame()
    placeholders = ",".join([f":kode_{i}" for i in range(len(kode_list))])

    # Base query - include kode_produk so we can report and filter by it
    sql = f"SELECT kode_produk, tujuan, status, sn, tgl_status FROM transaksi WHERE kode_produk IN ({placeholders})"

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


@st.cache_data(ttl=300)
def get_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """Transformasi data untuk tabel ringkasan.

    Cached to avoid repeated grouping work on reruns (e.g., when switching tabs).
    """
    if df.empty:
        return pd.DataFrame()
    # Use final_status for summary instead of status_label; include kode_produk for multi-product reports
    summary = (
        df.groupby(["kode_produk", "tujuan", "final_status"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    summary.columns.name = None

    return summary


def get_styled_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """Transformasi data untuk tabel ringkasan tanpa color coding.

    Uses cached `get_summary_table` internally to avoid repeated grouping work.
    """
    if df.empty:
        return pd.DataFrame()

    return get_summary_table(df)


def apply_filters(df: pd.DataFrame, session_state) -> pd.DataFrame:
    """Apply filters to the dataframe using provided session_state dict-like."""
    if df.empty:
        return df

    filtered_df = df.copy()

    # Filter by final status
    if session_state.get("final_status_filter", []):
        filtered_df = filtered_df[
            filtered_df["final_status"].isin(session_state["final_status_filter"])
        ]

    # Filter by kode_produk (partial match)
    if session_state.get("kode_produk_filter", ""):
        kode_filter = session_state["kode_produk_filter"].lower()
        filtered_df = filtered_df[
            filtered_df["kode_produk"].str.lower().str.contains(kode_filter, na=False)
        ]

    # Filter by tujuan (partial match)
    if session_state.get("tujuan_filter", ""):
        tujuan_filter = session_state["tujuan_filter"].lower()
        filtered_df = filtered_df[
            filtered_df["tujuan"].str.lower().str.contains(tujuan_filter, na=False)
        ]

    # Filter by SN (partial match)
    if session_state.get("sn_filter", ""):
        sn_filter = session_state["sn_filter"].lower()
        filtered_df = filtered_df[
            filtered_df["sn"].str.lower().str.contains(sn_filter, na=False)
        ]

    return filtered_df


@st.cache_data(ttl=300)
def get_dashboard_metrics(df: pd.DataFrame) -> dict:
    """Compute simple dashboard metrics from the dataframe.

    Returns a dict with keys: total, unique_tujuan, sukses, gagal
    """
    if df.empty:
        return {"total": 0, "unique_tujuan": 0, "sukses": 0, "gagal": 0}

    total = int(len(df))
    unique_tujuan = int(df["tujuan"].nunique())
    sukses = int(df["final_status"].isin(["SUKSES PROFIT", "SUKSES LOSS"]).sum())
    gagal = int((df["final_status"] == "GAGAL A1").sum())

    return {
        "total": total,
        "unique_tujuan": unique_tujuan,
        "sukses": sukses,
        "gagal": gagal,
    }


@st.cache_data(ttl=300)
def get_status_counts(df: pd.DataFrame) -> pd.DataFrame:
    """Return counts of each final_status as a small dataframe for plotting."""
    if df.empty:
        return pd.DataFrame(columns=["final_status", "count"])

    counts = (
        df["final_status"]
        .value_counts()
        .rename_axis("final_status")
        .reset_index(name="count")
    )
    return counts
