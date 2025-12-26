import json
from datetime import datetime

import pandas as pd
import streamlit as st

from services.audit import (
    AuditQueueManager,
)

st.set_page_config(page_title="Rekap RGU - Audit", page_icon="🔍", layout="wide")

# Session init
if "audit_data_source" not in st.session_state:
    st.session_state.audit_data_source = "manual"
if "audit_results" not in st.session_state:
    st.session_state.audit_results = []
if "selected_dataframe" not in st.session_state:
    st.session_state.selected_dataframe = None
if "available_dataframes" not in st.session_state:
    st.session_state.available_dataframes = []
if "uploaded_data" not in st.session_state:
    st.session_state.uploaded_data = None


def render():
    st.header("🔍 Audit & Status Check")

    # Data source selection in sidebar
    with st.sidebar:
        st.subheader("📊 Sumber Data")
        data_source = st.radio(
            "Pilih Sumber Data",
            options=["Manual Input", "Upload TXT", "Pilih dari DataFrame"],
            key="data_source",
            index=0,
        )

        if data_source == "Pilih dari DataFrame":
            # Get available dataframes from session state
            if "available_dataframes" not in st.session_state:
                st.session_state.available_dataframes = []

            # Add option to select from existing dataframes
            if st.session_state.available_dataframes:
                st.selectbox(
                    "Pilih DataFrame",
                    options=st.session_state.available_dataframes,
                    key="selected_dataframe",
                )
            else:
                st.info("Tidak ada DataFrame yang tersedia. Pilih opsi lain.")

        elif data_source == "Upload TXT":
            uploaded_file = st.file_uploader("Upload file TXT", type=["txt"])
            if uploaded_file is not None:
                # Read and process uploaded file
                stringio = st.text_input("Masukkan delimiter", value=",")
                try:
                    df = pd.read_csv(uploaded_file, sep=stringio, header=None)
                    st.success(f"File berhasil diupload: {len(df)} baris")

                    # Store in session state
                    if "uploaded_data" not in st.session_state:
                        st.session_state.uploaded_data = df
                except Exception as e:
                    st.error(f"Error membaca file: {str(e)}")
            else:
                st.info("Silakan upload file TXT")

        else:  # Manual input
            st.subheader("📝 Input Manual")
            numbers_text = st.text_area("Masukkan nomor (satu per baris)")

            if st.button("Proses Manual"):
                if numbers_text.strip():
                    # Process manual input
                    numbers = [
                        n.strip() for n in numbers_text.splitlines() if n.strip()
                    ]

                    # Create dataframe from manual input
                    df = pd.DataFrame({
                        "nomor": numbers,
                        "status": "manual",
                        "kartu": "",
                        "act_kartu": "",
                        "end_kartu": "",
                        "paket": "",
                        "act_paket": "",
                        "end_paket": "",
                        "balance": 0,
                    })

                    # Store in session state
                    st.session_state.uploaded_data = df
                    st.success(f"Berhasil memproses {len(numbers)} nomor")
                else:
                    st.warning("Masukkan nomor terlebih dahulu")

    # API Configuration
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

        # Process button
        if st.button("Proses Audit"):
            if (
                data_source in ["Manual Input", "Upload TXT"]
                and st.session_state.uploaded_data is not None
            ):
                # Initialize audit queue manager if not exists
                if "audit_queue_manager" not in st.session_state:
                    st.session_state.audit_queue_manager = AuditQueueManager(
                        delay_seconds, max_queue
                    )

                # Add numbers to queue from selected data source
                if data_source == "Manual Input":
                    numbers = st.session_state.uploaded_data["nomor"].tolist()
                elif data_source == "Upload TXT":
                    numbers = st.session_state.uploaded_data["nomor"].tolist()
                else:
                    numbers = []

                added = 0
                for n in numbers:
                    if st.session_state.audit_queue_manager.add_to_queue(n):
                        added += 1
                st.success(f"Added {added} numbers to queue")

                # Start processing
                st.session_state.audit_queue_manager.start_processing(
                    api_url, identifier_kartu, identifier_paket, username
                )
                st.rerun()

    # Progress and Controls
    if "audit_queue_manager" in st.session_state:
        qm = st.session_state.audit_queue_manager

        with st.container():
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                if st.button("▶️ Start", disabled=qm.is_running):
                    qm.start_processing(
                        api_url, identifier_kartu, identifier_paket, username
                    )
                    st.rerun()
            with col2:
                if not qm.is_paused and qm.is_running:
                    if st.button("⏸️ Pause"):
                        qm.pause_processing()
                        st.rerun()
                elif qm.is_paused:
                    if st.button("▶️ Resume"):
                        qm.resume_processing()
                        st.rerun()
            with col3:
                if st.button("⏹️ Stop", disabled=not qm.is_running):
                    qm.stop_processing()
                    st.rerun()

        if qm.is_running or qm.processed_count > 0:
            st.subheader("📊 Progress Tracking")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Queue Size", qm.queue.qsize())
            with col2:
                st.metric("Processed", qm.processed_count)
            with col3:
                st.metric("Skipped", qm.skip_count)
            with col4:
                st.metric("Errors", qm.error_count)

    # Results
    if (
        "audit_queue_manager" in st.session_state
        and st.session_state.audit_queue_manager.results
    ):
        results = st.session_state.audit_queue_manager.results
        st.subheader("📋 Hasil Audit")

        # Filter options
        with st.expander("🔍 Filter Hasil", expanded=True):
            status_filter = st.multiselect(
                "Filter Status",
                options=["Semua", "Success", "Error"],
                key="status_filter",
                default=["Semua"],
            )

        # Apply filters
        if status_filter != ["Semua"]:
            filtered_results = [
                r
                for r in results
                if status_filter == ["Semua"] or r.get("status") in status_filter
            ]
        else:
            filtered_results = results

        # Display results
        if filtered_results:
            results_df = pd.DataFrame(filtered_results)
            st.dataframe(results_df, width="stretch", hide_index=True)

            # Summary metrics
            success_count = len([
                r for r in filtered_results if r.get("status") == "success"
            ])
            error_count = len([
                r
                for r in filtered_results
                if r.get("status") in ["error", "api_error", "skipped"]
            ])

            st.subheader("📊 Ringkasan Audit")
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Sukses", success_count)
                st.metric("Total Error", error_count)
            with col2:
                total_processed = len(results_df)
                st.metric("Total Diproses", total_processed)

        # Export options
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("Export to JSON"):
                filename = (
                    f"audit_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                )
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(filtered_results, f, indent=2, default=str)
                st.success(f"Results saved to {filename}")
        with col2:
            if st.button("Export to CSV"):
                csv = pd.DataFrame(filtered_results).to_csv(index=False)
                st.download_button(
                    "Download CSV",
                    csv,
                    file_name=f"audit_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                )
        with col3:
            if st.button("Sinkronkan ke Report"):
                # Create synchronized dataframe
                qm = st.session_state.audit_queue_manager
                sync_df = qm.create_synchronized_dataframe(filtered_results)

                # Store in session state for use in report page
                if "audit_sync_data" not in st.session_state:
                    st.session_state.audit_sync_data = sync_df
                else:
                    # Append to existing data
                    st.session_state.audit_sync_data = pd.concat(
                        [st.session_state.audit_sync_data, sync_df], ignore_index=True
                    )

                st.success(f"Berhasil menyinkronkan {len(sync_df)} data ke Report")
                st.info("Data tersedia di halaman Report pada tab 'Audit'")
    else:
        st.info("Tidak ada hasil audit untuk ditampilkan")


if __name__ == "__main__":
    render()
