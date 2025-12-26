import json
import threading
import time
from datetime import datetime
from queue import Queue

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Rekap RGU", page_icon="📊", layout="wide")


class AuditQueueManager:
    """Queue manager for audit API requests with error handling and rate limiting"""

    def __init__(self, delay_seconds=30, max_queue=10):
        self.delay_seconds = delay_seconds
        self.max_queue = max_queue
        self.queue = Queue(maxsize=max_queue)
        self.results = []
        self.is_running = False
        self.is_paused = False
        self.processed_count = 0
        self.error_count = 0
        self.skip_count = 0
        self.thread = None

    def add_to_queue(self, phone_number):
        """Add phone number to queue"""
        try:
            self.queue.put(phone_number, timeout=1)
            return True
        except:
            return False

    def start_processing(self, api_url, identifier_kartu, identifier_paket, username):
        """Start processing queue"""
        if not self.is_running:
            self.is_running = True
            self.is_paused = False
            self.thread = threading.Thread(
                target=self._process_queue,
                args=(api_url, identifier_kartu, identifier_paket, username),
            )
            self.thread.daemon = True
            self.thread.start()

    def pause_processing(self):
        """Pause processing"""
        self.is_paused = True

    def resume_processing(self):
        """Resume processing"""
        self.is_paused = False

    def stop_processing(self):
        """Stop processing"""
        self.is_running = False
        if self.thread:
            self.thread.join(timeout=2)

    def _process_queue(self, api_url, identifier_kartu, identifier_paket, username):
        """Internal queue processing method with error handling"""
        while self.is_running:
            if not self.is_paused and not self.queue.empty():
                try:
                    phone_number = self.queue.get(timeout=1)
                    result = self._check_single_number(
                        phone_number,
                        api_url,
                        identifier_kartu,
                        identifier_paket,
                        username,
                    )
                    self.results.append(result)

                    if result.get("status") == "success":
                        self.processed_count += 1
                    elif result.get("status") == "skipped":
                        self.skip_count += 1
                    else:
                        self.error_count += 1

                    time.sleep(self.delay_seconds)
                except Exception as e:
                    error_result = {
                        "nomor": phone_number  # type: ignore
                        if "phone_number" in locals()
                        else "unknown",
                        "error": str(e),
                        "status": "queue_error",
                    }
                    self.results.append(error_result)
                    self.error_count += 1
                finally:
                    try:
                        self.queue.task_done()
                    except:
                        pass
            elif self.is_paused:
                time.sleep(1)  # Wait when paused
            elif self.queue.empty():
                time.sleep(1)  # Wait for new items
            else:
                time.sleep(0.1)  # Brief pause

    def _check_single_number(
        self, phone_number, api_url, identifier_kartu, identifier_paket, username
    ):
        """Check single number via API with robust error handling"""
        try:
            params = {"username": username, "to": phone_number}

            response = requests.get(api_url, params=params, timeout=30)

            # Skip invalid responses but continue processing
            if response.status_code != 200:
                return {
                    "nomor": phone_number,
                    "error": f"HTTP {response.status_code}",
                    "status": "skipped",
                    "message": "Invalid HTTP status",
                }

            try:
                response_data = response.json()
            except json.JSONDecodeError:
                return {
                    "nomor": phone_number,
                    "error": "Invalid JSON response",
                    "status": "skipped",
                    "message": "JSON parsing failed",
                }

            # Parse response
            parsed_data = parse_api_response(
                response_data, identifier_kartu, identifier_paket
            )
            parsed_data["status"] = "success"
            parsed_data["raw_response"] = response_data

            return parsed_data

        except requests.exceptions.Timeout:
            return {
                "nomor": phone_number,
                "error": "Request timeout",
                "status": "skipped",
                "message": "Request timed out after 30 seconds",
            }
        except requests.exceptions.ConnectionError:
            return {
                "nomor": phone_number,
                "error": "Connection error",
                "status": "skipped",
                "message": "Failed to connect to API",
            }
        except requests.exceptions.RequestException as e:
            return {
                "nomor": phone_number,
                "error": str(e),
                "status": "skipped",
                "message": "Request failed",
            }
        except Exception as e:
            return {
                "nomor": phone_number,
                "error": str(e),
                "status": "api_error",
                "message": "Unexpected error",
            }


def parse_api_response(response_json, identifier_kartu, identifier_paket):
    """Parse API response and extract required information"""

    # Normalize MSISDN
    msisdn = response_json.get("msisdn", "")
    if msisdn.startswith("62"):
        msisdn = "0" + msisdn[2:]

    # Extract balance
    balance = response_json.get("custbalanceinfo", "0")

    # Initialize default values
    kartu = act_kartu = end_kartu = paket = act_paket = end_paket = None

    # Parse services
    services = response_json.get("Services", [])
    for service in services:
        package_name = service.get("packagename", "")

        if identifier_kartu.lower() in package_name.lower():
            kartu = package_name
            act_kartu = service.get("activationdate", "")
            end_kartu = service.get("enddate", "")

        if identifier_paket.lower() in package_name.lower():
            paket = package_name
            act_paket = service.get("activationdate", "")
            end_paket = service.get("enddate", "")

    return {
        "nomor": msisdn,
        "kartu": kartu,
        "act_kartu": act_kartu,
        "end_kartu": end_kartu,
        "paket": paket,
        "act_paket": act_paket,
        "end_paket": end_paket,
        "balance": balance,
    }


def convert_results_to_dataframe(results):
    """Convert results to DataFrame with error handling"""
    df_data = []

    for result in results:
        if result.get("status") == "success":
            df_data.append({
                "nomor": result.get("nomor", ""),
                "kartu": result.get("kartu", ""),
                "act_kartu": result.get("act_kartu", ""),
                "end_kartu": result.get("end_kartu", ""),
                "paket": result.get("paket", ""),
                "act_paket": result.get("act_paket", ""),
                "end_paket": result.get("end_paket", ""),
                "balance": result.get("balance", "0"),
            })
        else:
            # Add error entries to maintain data integrity
            df_data.append({
                "nomor": result.get("nomor", ""),
                "kartu": "ERROR",
                "act_kartu": "",
                "end_kartu": "",
                "paket": "ERROR",
                "act_paket": "",
                "end_paket": "",
                "balance": result.get("error", "Unknown error"),
            })

    return pd.DataFrame(df_data)


def save_results_to_json(results, filename=None):
    """Save results to JSON file"""
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"audit_results_{timestamp}.json"

    try:
        with open(filename, "w") as f:
            json.dump(results, f, indent=2, default=str)
        return filename
    except Exception as e:
        st.error(f"Failed to save results: {e}")
        return None


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


@st.fragment
def render_audit_tab(df: pd.DataFrame):
    """Render Audit & Status Check tab with fragment decorator"""
    st.header("🔍 Audit & Status Check")

    # Initialize session state for audit
    if "audit_queue_manager" not in st.session_state:
        st.session_state.audit_queue_manager = None
    if "audit_results" not in st.session_state:
        st.session_state.audit_results = []

    # Configuration Section
    with st.expander("⚙️ Konfigurasi API", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            api_url = st.text_input(
                "API Endpoint URL", value="http://localhost:8005/get_package_status"
            )
            identifier_kartu = st.text_input("Identifier Kartu", value="Kartu")
            username = st.text_input("Username", value="xxxxx")
        with col2:
            delay_seconds = st.number_input(
                "Jeda per kirim (detik)", min_value=1, value=30
            )
            max_queue = st.number_input("Max antrian", min_value=1, value=10)

        identifier_paket = st.text_input(
            "Identifier Paket", value="Freedom Internet 1.5GB/1Hari"
        )

    # Data Selection Section
    st.subheader("📋 Data Selection")
    if not df.empty:
        unique_numbers = df["tujuan"].unique()
        st.write(f"Found {len(unique_numbers)} unique numbers")

        # Preview section
        if st.checkbox("Preview Numbers"):
            preview_df = pd.DataFrame({
                "No": range(1, len(unique_numbers) + 1),
                "Nomor": unique_numbers,
            })
            st.dataframe(preview_df.head(20))

        # Batch selection
        col1, col2 = st.columns(2)
        with col1:
            batch_size = st.number_input(
                "Batch Size",
                min_value=1,
                max_value=len(unique_numbers),
                value=min(10, len(unique_numbers)),
            )
        with col2:
            start_from = st.number_input(
                "Start From", min_value=0, max_value=len(unique_numbers) - 1, value=0
            )

        # Add to queue button
        if st.button("Add to Queue", type="primary"):
            if st.session_state.audit_queue_manager is None:
                st.session_state.audit_queue_manager = AuditQueueManager(
                    delay_seconds, max_queue
                )

            end_idx = min(start_from + batch_size, len(unique_numbers))
            selected_numbers = unique_numbers[start_from:end_idx]

            added_count = 0
            for number in selected_numbers:
                if st.session_state.audit_queue_manager.add_to_queue(number):
                    added_count += 1

            st.success(f"Added {added_count} numbers to queue")

    # Execution Control Section
    st.subheader("🚀 Execution")
    if st.session_state.audit_queue_manager:
        queue_manager = st.session_state.audit_queue_manager

        # Control buttons
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("▶️ Start", type="primary", disabled=queue_manager.is_running):
                queue_manager.start_processing(
                    api_url, identifier_kartu, identifier_paket, username
                )
                st.rerun()

        with col2:
            if not queue_manager.is_paused and queue_manager.is_running:
                if st.button("⏸️ Pause"):
                    queue_manager.pause_processing()
                    st.rerun()
            elif queue_manager.is_paused:
                if st.button("▶️ Resume"):
                    queue_manager.resume_processing()
                    st.rerun()

        with col3:
            if st.button("⏹️ Stop", disabled=not queue_manager.is_running):
                queue_manager.stop_processing()
                st.rerun()

        # Progress tracking
        if queue_manager.is_running or queue_manager.processed_count > 0:
            st.subheader("📊 Progress Tracking")
            col1, col2, col3, col4, col5 = st.columns(5)

            with col1:
                st.metric("Queue Size", queue_manager.queue.qsize())
            with col2:
                st.metric("Processed", queue_manager.processed_count)
            with col3:
                st.metric("Skipped", queue_manager.skip_count)
            with col4:
                st.metric("Errors", queue_manager.error_count)
            with col5:
                status = (
                    "⏸️ Paused"
                    if queue_manager.is_paused
                    else "🔄 Running"
                    if queue_manager.is_running
                    else "⏹️ Stopped"
                )
                st.metric("Status", status)

            # Progress bar
            total = (
                queue_manager.processed_count
                + queue_manager.skip_count
                + queue_manager.error_count
                + queue_manager.queue.qsize()
            )
            if total > 0:
                progress = (
                    queue_manager.processed_count
                    + queue_manager.skip_count
                    + queue_manager.error_count
                ) / total
                st.progress(
                    progress,
                    text=f"Progress: {queue_manager.processed_count + queue_manager.skip_count + queue_manager.error_count}/{total}",
                )

    # Results Section
    st.subheader("📊 Results Analysis")
    if (
        st.session_state.audit_queue_manager
        and st.session_state.audit_queue_manager.results
    ):
        results = st.session_state.audit_queue_manager.results

        # Summary Statistics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Checked", len(results))
        with col2:
            success_count = len([r for r in results if r.get("status") == "success"])
            st.metric("Success", success_count)
        with col3:
            skip_count = len([r for r in results if r.get("status") == "skipped"])
            st.metric("Skipped", skip_count)
        with col4:
            error_count = len([
                r for r in results if r.get("status") not in ["success", "skipped"]
            ])
            st.metric("Errors", error_count)

        # Detailed Results Table
        st.subheader("📋 Detailed Results")
        results_df = convert_results_to_dataframe(results)
        st.dataframe(results_df, width="stretch", hide_index=True)

        # Export Options
        st.subheader("💾 Export Options")
        col1, col2 = st.columns(2)

        with col1:
            if st.button("Export to JSON"):
                filename = save_results_to_json(results)
                if filename:
                    st.success(f"Results saved to {filename}")

        with col2:
            if st.button("Export to CSV"):
                csv = results_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"audit_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                )
    else:
        st.info("No results to display yet. Start the audit process to see results.")


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

    # Create main tabs for Matrix/Calculation, Raw Data, and Audit
    tab1, tab2, tab3 = st.tabs([
        "📊 Matrix & Kalkulasi",
        "🔍 Raw Data",
        "🔍 Audit & Status",
    ])

    with tab1:
        render_matrix_and_calculation(df)

    with tab2:
        render_raw_data(df)

    with tab3:
        render_audit_tab(df)


# ==========================================
# APP ORCHESTRATOR (Minimal Landing)
# ==========================================


def main():
    st.title("Rekap RGU")
    st.markdown(
        "Aplikasi telah dipisah menjadi halaman. Gunakan menu **Pages** di kiri untuk membuka:\n\n- Report: Matrix & Kalkulasi\n- Audit & Status: API checker\n\nJika Anda ingin menjalankan halaman secara terpisah, buka file di folder `pages/`."
    )


if __name__ == "__main__":
    main()
