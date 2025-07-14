import pandas as pd

# URL of the raw CSV file on GitHub
# Replace with the actual raw GitHub URL of your CSV file
url = "https://github.com/jforbes24/apple/blob/main/D%26T%20Data%20Test%20No%201.xlsx"

try:
    # Read the CSV file directly into a DataFrame
    df = pd.read_csv(url)
    print("DataFrame loaded successfully!")
    print("\nFirst few rows of the DataFrame:")
    print(df.head())
except Exception as e:
    print(f"Error loading the CSV file: {e}")

# Optional: Display basic info about the DataFrame
print("\nDataFrame Info:")
print(df.info())