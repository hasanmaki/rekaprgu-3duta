import streamlit as st

from services.audit import (
    AuditQueueManager,
    convert_results_to_dataframe,
    save_results_to_json,
)

st.set_page_config(page_title="Rekap RGU - Audit", page_icon="🔍", layout="wide")

# Session init
if "audit_queue_manager" not in st.session_state:
    st.session_state.audit_queue_manager = None
if "audit_results" not in st.session_state:
    st.session_state.audit_results = []


def render():
    st.header("🔍 Audit & Status Check")

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

    st.subheader("📋 Data Selection (manual input)")
    numbers_text = st.text_area("Masukkan nomor (satu per baris)")

    if st.button("Add to Queue"):
        if st.session_state.audit_queue_manager is None:
            st.session_state.audit_queue_manager = AuditQueueManager(
                delay_seconds, max_queue
            )

        numbers = [n.strip() for n in numbers_text.splitlines() if n.strip()]
        added = 0
        for n in numbers:
            if st.session_state.audit_queue_manager.add_to_queue(n):
                added += 1
        st.success(f"Added {added} numbers to queue")

    # Controls
    if st.session_state.audit_queue_manager:
        qm = st.session_state.audit_queue_manager
        col1, col2, col3 = st.columns(3)
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
        st.session_state.audit_queue_manager
        and st.session_state.audit_queue_manager.results
    ):
        results = st.session_state.audit_queue_manager.results
        st.subheader("📋 Detailed Results")
        results_df = convert_results_to_dataframe(results)
        st.dataframe(results_df, width="stretch", hide_index=True)

        if st.button("Export to JSON"):
            filename = save_results_to_json(results)
            st.success(f"Results saved to {filename}")
    else:
        st.info("No results to display yet. Start the audit process to see results.")


if __name__ == "__main__":
    render()
