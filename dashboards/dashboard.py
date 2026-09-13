from pathlib import Path
import folium
import pandas as pd
import requests
import streamlit as st
from streamlit_folium import st_folium

BASE = f"http://localhost:{5001}"
PAGE_PATH = Path(__file__).resolve().parent / "streamlit" / "salesmap.py"
SUNBURST_PATH = Path(__file__).resolve().parent / "streamlit" / "sunburst.py"

# set_page_config must be the first Streamlit call on the page.
st.set_page_config(page_title="Olist Brazilian E-Commerce", layout="wide")
st.title("Olist Brazilian E-Commerce")           # big heading
#st.subheader("Revenue of Olist Brazilian E-Commerce split by Cities")  # smaller heading
#st.caption(f"Every number on this page came from our own API at {BASE}")

pg = st.navigation(
    [
        st.Page(
            str(PAGE_PATH),
            title="Olist Revenue Geo-Map",
            icon=":material/overview:",
            default=True,
        ),
        st.Page(
            str(SUNBURST_PATH),
            title="Top Product Revenue",
            icon=":material/overview:",
        ),
    ]
)
pg.run()

