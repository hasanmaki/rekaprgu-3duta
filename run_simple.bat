@echo off
uv sync && .venv\Scripts\activate && uv run streamlit run main.py
