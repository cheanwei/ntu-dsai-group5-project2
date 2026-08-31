import pandas as pd
import streamlit as st
import requests
import folium
from streamlit_folium import st_folium

BASE = f"http://localhost:{5001}"

# set_page_config must be the first Streamlit call on the page.
st.set_page_config(page_title="Olist Brazilian E-Commerce", layout="wide")
st.title("Olist Brazilian E-Commerce")           # big heading
st.caption(f"Every number on this page came from our own API at {BASE}")


m = folium.Map(location=(-14.235, -51.925), zoom_start=4, tiles="OpenStreetMap")
st_data = st_folium(m, width=700, height=500)

# Show click info
st.write("Map interaction:", st_data)