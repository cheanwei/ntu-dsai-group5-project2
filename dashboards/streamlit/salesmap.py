import pandas as pd
import streamlit as st
import requests
import folium
from streamlit_folium import st_folium

BASE = f"http://localhost:{5001}"

# set_page_config must be the first Streamlit call on the page.
st.set_page_config(page_title="Olist Brazilian E-Commerce", layout="wide")

st.title(
    ":material/compare_arrows: Top Revenue by Cities")
st.markdown(
    "Which cities generate the most revenue?")


# Cached so reruns and visitors on the public deployment don't each trigger a
# BigQuery query through the API.
@st.cache_data(ttl="1h")
def load_cities() -> pd.DataFrame:
    response = requests.get(f"{BASE}/api/cities", timeout=60)
    response.raise_for_status()
    return pd.DataFrame(response.json().get("cities", []))


try:
    cities = load_cities()
except Exception as exc:
    st.error(f"Failed to load cities from the API: {exc}")
    st.stop()
if cities.empty:
    st.warning("No data loaded.")
    st.stop()
st.dataframe(cities, width="stretch")


m = folium.Map(location=(-23.545, -46.639), zoom_start=4, tiles="OpenStreetMap")

# Brazil_cities = [{"name": "Brasília", "latitude":-15.8267, "longitude":-47.921},
#                        {"name": "Rio de Janeiro", "latitude":-22.9068, "longitude":-43.1729}]
for city in cities.to_dict(orient="records"):
    folium.Marker(
        location=[city["latitude"], city["longitude"]],
        popup=city["name"],
        icon=folium.Icon(color="blue")
    ).add_to(m)

st_data = st_folium(m, width=700, height=500)

# Show click info
#st.write("Map interaction:", st_data)

