from pyspark.sql import SparkSession
import pandas as pd
import folium

# Initialize SparkSession
spark = SparkSession.builder.appName("CrimeAnalysis").getOrCreate()

# Load dataset with PySpark
file_path = '/Users/smitbhabal/Downloads/Cleaned_Crime_Chicago.csv'

df = spark.read.csv(file_path, header=True, inferSchema=True)

# Search for a specific criminal
criminal_name = input("Enter the criminal name to search: ")
filtered_df = df.filter(df.person_name == criminal_name)

# Convert to Pandas DataFrame for map visualization
filtered_pandas_df = filtered_df.toPandas()

# Generate a map with Folium
m = folium.Map(location=[41.8781, -87.6298], zoom_start=12)
for _, row in filtered_pandas_df.iterrows():
    folium.Marker(
        location=[row['Latitude'], row['Longitude']],
        popup=f"Crime Category: {row['crime_category']}<br>Hour: {row['Hour']}<br>Month: {row['Month']}<br>Criminal: {row['person_name']}"
    ).add_to(m)

# Save map to HTML
m.save("crime_map.html")
print("Map has been saved as 'crime_map.html'. Open this file in your browser to view the map.")
