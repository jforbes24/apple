import pandas as pd
import requests
from io import BytesIO
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

# Set pandas option to display all columns
pd.set_option('display.max_columns', None)

# Load environment variables from .env file
load_dotenv()

# Retrieve the GitHub token
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    print("Error: GITHUB_TOKEN not found in .env file")
    exit(1)

# GitHub API URL for the file
url = "https://raw.githubusercontent.com/jforbes24/apple/main/APPLE%20Task%201/D%26T%20Data%20Test%20No%201.xlsx"
headers = {"Authorization": f"token {GITHUB_TOKEN}"}

try:
    # Download the file
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    # Read the Excel file from the response content
    df = pd.read_excel(BytesIO(response.content), engine='openpyxl')
    print("DataFrame loaded successfully!")
except Exception as e:
    print(f"Error loading the Excel file: {e}")
    exit(1)

# Function to parse fiscal quarter and year
def parse_fiscal_quarter(fiscal_qtr):
    try:
        # Extract year and quarter (e.g., FY23Q4 -> year=2023, quarter=4)
        year = int(fiscal_qtr[2:4]) + 2000  # Convert FY23 to 2023
        quarter = int(fiscal_qtr[-1])  # Extract quarter number
        # Map quarter to start month (Q1: Oct, Q2: Jan, Q3: Apr, Q4: Jul)
        month = {1: 10, 2: 1, 3: 4, 4: 7}[quarter]
        # Adjust year for Q1 (starts in previous calendar year)
        if quarter == 1:
            year -= 1
        return datetime(year, month, 1)
    except Exception as e:
        print(f"Error parsing fiscal quarter {fiscal_qtr}: {e}")
        return pd.NaT

# Function to parse fiscal week and year
def parse_fiscal_week(fiscal_week):
    try:
        # Extract year and week (e.g., FY23W53 -> year=2023, week=53)
        year = int(fiscal_week[2:4]) + 2000  # Convert FY23 to 2023
        week = int(fiscal_week[5:])  # Extract week number
        # Fiscal year starts on October 1 of previous calendar year
        fiscal_year_start = datetime(year - 1, 10, 1)
        # Calculate week start date (add weeks * 7 days)
        week_start = fiscal_year_start + timedelta(weeks=week - 1)
        return week_start
    except Exception as e:
        print(f"Error parsing fiscal week {fiscal_week}: {e}")
        return pd.NaT

# Clean metric columns to remove commas and convert to numeric
metric_columns = ['Sessions', 'PDP Add to Cart Units', 'Units Sold']
for col in metric_columns:
    if df[col].dtype == 'object':  # Check if column is string type
        df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')

# Add new date columns
df['Quarter_Start_Date'] = df['FISCAL_QTR_YEAR_NAME'].apply(parse_fiscal_quarter)
df['Week_Start_Date'] = df['FISCAL_WEEK_YEAR_NAME'].apply(parse_fiscal_week)

# Display last few rows of the DataFrame
print("\nLast few rows of the DataFrame:")
print(df.tail())

# Display basic info about the DataFrame
print("\nDataFrame Info:")
print(df.info())

# Export the processed DataFrame to a CSV file
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(script_dir, 'processed_data.csv')
df.to_csv(csv_path, index=False)
print("\nDataFrame exported to 'processed_data.csv'")