Rekap RGU
=========

Quick start
-----------

- Recommended entrypoint: `main.py`

Run locally:

- With uv tool:

  uv run streamlit run main.py

- Or simple:

  streamlit run main.py

Notes
-----

- App UI is split into pages under `pages/`:
  - `pages/01_Rekap_Report.py` — Report (Matrix & Kalkulasi)
  - `pages/02_Audit_Checker.py` — Audit & Status (API checker)
- Core services are in `services/` to make testing and reuse easier.
