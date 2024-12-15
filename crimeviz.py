from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import os

# Stop any previous Spark session
os.system("pkill -f 'spark-submit'")

# Initialize Spark session
spark = SparkSession.builder.appName("CrimeAnalysis").getOrCreate()

# file path
file_path = "/Users/smitbhabal/Downloads/Cleaned_Crime_Chicago.csv"

# Load dataset
print("File exists and ready to load.")
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Show schema and sample data
df.printSchema()
df.show(5)

# 1. Count of crimes by month
print("Count of crimes by month:")
crimes_by_month = df.groupBy("Month").count().orderBy("Month")
crimes_by_month.show()

# Convert to Pandas for plotting
pandas_crimes_by_month = crimes_by_month.toPandas()

# Bar chart for crimes by month
plt.figure(figsize=(10, 6))
plt.bar(pandas_crimes_by_month["Month"], pandas_crimes_by_month["count"], color='skyblue')
plt.xlabel("Month")
plt.ylabel("Number of Crimes")
plt.title("Number of Crimes by Month")
plt.xticks(range(1, 13))
plt.savefig("crimes_by_month.png")
plt.show()

# 2. Count of crimes by hour of the day
print("Count of crimes by hour of the day:")
crimes_by_hour = df.groupBy("Hour").count().orderBy("Hour")
crimes_by_hour.show()

# Convert to Pandas for plotting
pandas_crimes_by_hour = crimes_by_hour.toPandas()

# Bar chart for crimes by hour
plt.figure(figsize=(10, 6))
plt.bar(pandas_crimes_by_hour["Hour"], pandas_crimes_by_hour["count"], color='lightgreen')
plt.xlabel("Hour of the Day")
plt.ylabel("Number of Crimes")
plt.title("Number of Crimes by Hour of the Day")
plt.xticks(range(0, 24))
plt.savefig("crimes_by_hour.png")
plt.show()

# 3. Count of crimes by category
print("Count of crimes by category:")
crimes_by_category = df.groupBy("crime_category").count().orderBy("count", ascending=False)
crimes_by_category.show()

# Convert to Pandas for plotting
pandas_crimes_by_category = crimes_by_category.toPandas()

# Pie chart for crimes by category
plt.figure(figsize=(10, 6))
plt.pie(pandas_crimes_by_category["count"], labels=pandas_crimes_by_category["crime_category"], autopct='%1.1f%%', colors=plt.cm.Paired.colors)
plt.title("Distribution of Crimes by Category")
plt.axis("equal")  # Equal aspect ratio ensures that pie chart is drawn as a circle.
plt.savefig("crimes_by_category.png")
plt.show()

# Stop Spark session
spark.stop()
