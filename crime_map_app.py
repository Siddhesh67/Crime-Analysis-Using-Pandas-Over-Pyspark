import streamlit as st
import pandas as pd
import folium
from folium.plugins import HeatMap, MarkerCluster, Draw
from streamlit_folium import st_folium
from geopy.geocoders import Nominatim

# Set page configuration
st.set_page_config(page_title="Chicago Crime Map", layout="wide")

# Load the dataset
@st.cache_data
def load_data():
    return pd.read_csv('/Users/smitbhabal/Downloads/Chicago_Crime.csv')

df = load_data()

# Display the columns to check their names (for debugging)
st.write(df.columns)

# Streamlit App
st.title("Interactive Chicago Crime Map")

# Sidebar Filters
with st.sidebar:
    st.header("Filters")

    # Filter: Crime Category
    if 'crime_category' in df.columns:
        crime_categories = df['crime_category'].dropna().unique()
        selected_crime_categories = st.multiselect("Select Crime Categories:", crime_categories, default=crime_categories)
    else:
        st.warning("Crime Category column not found. Proceeding without filtering by crime category.")
        selected_crime_categories = []

    # Filter: Hour Range (based on 'Hour')
    if 'Hour' in df.columns:
        min_hour, max_hour = df['Hour'].min(), df['Hour'].max()
        hour_range = st.slider("Select Hour Range:", min_value=int(min_hour), max_value=int(max_hour), value=(int(min_hour), int(max_hour)))
    else:
        st.warning("Hour column not found.")
        hour_range = (0, 23)

    # Filter: Person Name
    if 'person_name' in df.columns:
        person_names = df['person_name'].dropna().unique()
        selected_person_names = st.multiselect("Select Criminal Names:", person_names, default=person_names)
    else:
        st.warning("Person Name column not found.")
        selected_person_names = []

    # Search Bar for Location
    address = st.text_input("Search for a location (address):")

# Filter data based on sidebar inputs
filtered_data = df[(df['crime_category'].isin(selected_crime_categories)) &
                   (df['Hour'] >= hour_range[0]) &
                   (df['Hour'] <= hour_range[1]) &
                   (df['person_name'].isin(selected_person_names))]

# Initialize map
if address:
    geolocator = Nominatim(user_agent="crime_map_app")
    location = geolocator.geocode(address)
    if location:
        map_center = [location.latitude, location.longitude]
    else:
        st.error("Address not found. Showing default map.")
        map_center = [41.8781, -87.6298]  # Default Chicago coordinates
else:
    map_center = [41.8781, -87.6298]

m = folium.Map(location=map_center, zoom_start=12)

# Add marker cluster
marker_cluster = MarkerCluster().add_to(m)
for _, row in filtered_data.iterrows():
    folium.Marker([row['Latitude'], row['Longitude']],
                  popup=f"Crime: {row['crime_category']}<br>Person: {row['person_name']}<br>Hour: {row['Hour']}").add_to(marker_cluster)

# Add heatmap
heatmap_data = filtered_data[['Latitude', 'Longitude']].dropna().values.tolist()
HeatMap(heatmap_data).add_to(m)

# Add draw tools
Draw().add_to(m)

# Display map
st_folium(m, width=1000, height=700)

# Add additional data visualization
st.subheader("Crime Statistics")

# Bar Chart: Crime Distribution by Category
crime_count = filtered_data['crime_category'].value_counts().reset_index()
crime_count.columns = ['Crime Category', 'Count']
st.bar_chart(crime_count.set_index('Crime Category'))

# Export filtered data
csv_data = filtered_data.to_csv(index=False).encode('utf-8')
st.download_button(label="Download Filtered Data", data=csv_data, file_name='filtered_crimes.csv', mime='text/csv')
