import pandas as pd
import folium
from folium.plugins import MarkerCluster
from geopy.geocoders import Nominatim

# Load the dataset
df = pd.read_csv('/users/smitbhabal/Downloads/Cleaned_Crime_Chicago.csv')

# Initialize the map (centered around Chicago)
m = folium.Map(location=[41.8781, -87.6298], zoom_start=12)

# Create a marker cluster to group markers
marker_cluster = MarkerCluster().add_to(m)

# Function to add markers with popup information
def add_markers(df):
    for _, row in df.iterrows():
        folium.Marker(
            location=[row['Latitude'], row['Longitude']],
            popup=(
                f"Crime: {row['crime_category']}<br>"
                f"Person: {row['person_name']}<br>"
                f"Hour: {row['Hour']}<br>"
                f"Month: {row['Month']}"
            )
        ).add_to(marker_cluster)

# Add all markers to the map
add_markers(df)

# Function to filter data based on criminal name
def filter_by_criminal(df, criminal_name):
    return df[df['person_name'].str.contains(criminal_name, case=False, na=False)]

# Search functionality (you can replace this with a simple input method or GUI in a more advanced version)
criminal_name = input("Enter the criminal name to search: ")

# Filter the data based on the criminal name
if criminal_name:
    filtered_df = filter_by_criminal(df, criminal_name)
    # Clear the existing markers and add only the filtered ones
    marker_cluster = MarkerCluster().add_to(m)
    add_markers(filtered_df)

# Save the map as an HTML file
m.save('crime_map.html')

print("Map has been saved as 'crime_map.html'. Open this file in your browser to view the map.")
