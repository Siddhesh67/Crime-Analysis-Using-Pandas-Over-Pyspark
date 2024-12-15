import pandas as pd
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

# Create a Spark session
spark = SparkSession.builder \
    .appName("Crime Analysis") \
    .getOrCreate()

# Set the log level to reduce unnecessary warnings
spark.sparkContext.setLogLevel("ERROR")

# Load the dataset
def load_data():
    # Read the dataset with pandas first
    df_pandas = pd.read_csv("/users/smitbhabal/Downloads/Chicago_Crime.csv")

    # Print the first 100 rows for debugging (use head() to show only the first 100)
    print(f"First 100 rows of the dataset:\n{df_pandas.head(100)}")

    # Sample 10% of the data for testing (optional)
    df_pandas = df_pandas.sample(frac=0.1, random_state=1)

    # Print the columns to check for any inconsistencies in the column names
    print("Dataset columns:", df_pandas.columns)

    # Strip any leading/trailing spaces from column names
    df_pandas.columns = df_pandas.columns.str.strip()

    # Create a Spark DataFrame from the pandas DataFrame
    df_spark = spark.createDataFrame(df_pandas)
    
    return df_pandas, df_spark

# Preprocess the dataset
def preprocess_data(df):
    # Check and clean column names if needed
    print("Column names before processing:", df.columns)
    
    # If the column name for date is different (e.g., 'Date' instead of 'date'), change accordingly
    if 'Date' not in df.columns:
        print("The 'Date' column was not found. Here are the actual columns:", df.columns)
        return df

    # Convert the 'Date' column to datetime format
    df['date'] = pd.to_datetime(df['Date'], errors='coerce')
    
    # Check for missing values in the 'date' column and drop rows with missing dates
    df = df.dropna(subset=['date'])
    
    # Return the cleaned dataset
    return df

# Data analysis function
def analyze_data(df):
    # Example analysis: Count the number of crimes by year
    df['year'] = df['date'].dt.year
    crimes_by_year = df['year'].value_counts().sort_index()

    # Plot the analysis
    crimes_by_year.plot(kind='bar', figsize=(10, 6))
    plt.title('Crimes by Year')
    plt.xlabel('Year')
    plt.ylabel('Number of Crimes')
    plt.show()

def main():
    # Load data
    df_pandas, df_spark = load_data()

    # Preprocess data
    df_cleaned = preprocess_data(df_pandas)

    # Perform data analysis
    analyze_data(df_cleaned)

# Run the main function
if __name__ == "__main__":
    main()
