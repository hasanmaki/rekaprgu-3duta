import pandas as pd
import streamlit as st


@st.cache_data(ttl=600)
def fetch_and_process_data(kode_produk: str, tgl_awal=None, tgl_akhir=None):
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

    summary = (
        df.groupby(["tujuan", "final_status"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    summary.columns.name = None

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

    styled_summary = summary.style.pipe(highlight_cells)

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


def apply_filters(df: pd.DataFrame, session_state):
    """Apply filters to the dataframe using provided session_state dict-like."""
    if df.empty:
        return df

    filtered_df = df.copy()

    # Filter by final status
    if session_state.get("final_status_filter", []):
        filtered_df = filtered_df[
            filtered_df["final_status"].isin(session_state["final_status_filter"])
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
