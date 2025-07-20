import pandas as pd
import requests
from io import BytesIO
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
from statsmodels.tsa.arima.model import ARIMA
import warnings

# Suppress ARIMA warnings
warnings.filterwarnings("ignore")

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

# Print column names for debugging
print("\nColumns in the DataFrame:")
print(df.columns.tolist())

# Check if required columns exist
required_columns = ['Product Code', 'Product', 'FISCAL_QTR_YEAR_NAME', 'FISCAL_WEEK_YEAR_NAME', 'Sessions', 'PDP Add to Cart Units', 'Units Sold']
missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    print(f"Error: The following required columns are missing: {missing_columns}")
    print("Please verify the column names in the Excel file.")
    exit(1)

# Function to parse fiscal quarter and year
def parse_fiscal_quarter(fiscal_qtr):
    try:
        year = int(fiscal_qtr[2:4]) + 2000
        quarter = int(fiscal_qtr[-1])
        month = {1: 10, 2: 1, 3: 4, 4: 7}[quarter]
        if quarter == 1:
            year -= 1
        return datetime(year, month, 1)
    except Exception as e:
        print(f"Error parsing fiscal quarter {fiscal_qtr}: {e}")
        return pd.NaT

# Function to parse fiscal week and year
def parse_fiscal_week(fiscal_week):
    try:
        year = int(fiscal_week[2:4]) + 2000
        week = int(fiscal_week[5:])
        fiscal_year_start = datetime(year - 1, 10, 1)
        week_start = fiscal_year_start + timedelta(weeks=week - 1)
        return week_start
    except Exception as e:
        print(f"Error parsing fiscal week {fiscal_week}: {e}")
        return pd.NaT

# Clean metric columns to remove commas and convert to numeric
metric_columns = ['Sessions', 'PDP Add to Cart Units', 'Units Sold']
for col in metric_columns:
    if col in df.columns and df[col].dtype == 'object':
        df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
    else:
        print(f"Warning: Column '{col}' not found or not of type object")

# Add new date columns
df['Quarter_Start_Date'] = df['FISCAL_QTR_YEAR_NAME'].apply(parse_fiscal_quarter)
df['Week_Start_Date'] = df['FISCAL_WEEK_YEAR_NAME'].apply(parse_fiscal_week)

# Filter for rows where both Product Code and Product are populated
df = df.dropna(subset=['Product Code', 'Product'])

# Ensure Week_Start_Date is sorted
df = df.sort_values('Week_Start_Date')

# Function to fit ARIMA model and forecast
def forecast_metric(data, metric, product_code, product, steps=10, order=(1,1,1)):
    try:
        # Filter data for specific product_code and product
        ts_data = data[(data['Product Code'] == product_code) & (data['Product'] == product)]
        ts = ts_data.set_index('Week_Start_Date')[metric].dropna()
        if len(ts) < 2:  # Ensure enough data points
            print(f"Not enough data for {metric} with Product Code {product_code} and Product {product}")
            return pd.Series([None] * steps)
        # Fit ARIMA model
        model = ARIMA(ts, order=order)
        model_fit = model.fit()
        # Forecast
        forecast = model_fit.forecast(steps=steps)
        return forecast
    except Exception as e:
        print(f"Error forecasting {metric} for Product Code {product_code}, Product {product}: {e}")
        return pd.Series([None] * steps)

# Forecast 10 weeks ahead
forecast_steps = 10
last_date = df['Week_Start_Date'].max()
forecast_dates = [last_date + timedelta(weeks=i+1) for i in range(forecast_steps)]

# Get unique product_code and product combinations
product_combinations = df[['Product Code', 'Product']].drop_duplicates()

# Create forecast DataFrame
forecast_rows = []
for _, row in product_combinations.iterrows():
    product_code = row['Product Code']
    product = row['Product']
    last_fiscal_week = df[df['Product Code'] == product_code]['FISCAL_WEEK_YEAR_NAME'].iloc[-1]
    last_week_num = int(last_fiscal_week[5:]) if last_fiscal_week else 1
    last_year = int(last_fiscal_week[2:4]) + 2000 if last_fiscal_week else last_date.year % 100
    last_quarter = df[df['Product Code'] == product_code]['FISCAL_QTR_YEAR_NAME'].iloc[-1]
    
    for i in range(forecast_steps):
        week_num = last_week_num + i + 1
        year_adjust = 0
        if week_num > 52:  # Handle fiscal year rollover
            week_num = week_num % 52
            year_adjust = 1
        fiscal_week = f"FY{(last_year % 100) + year_adjust:02d}W{week_num:02d}"
        
        forecast_row = {
            'Product Code': product_code,
            'Product': product,
            'FISCAL_WEEK_YEAR_NAME': fiscal_week,
            'FISCAL_QTR_YEAR_NAME': last_quarter,  # Assume same quarter for simplicity
            'Week_Start_Date': forecast_dates[i],
            'Quarter_Start_Date': parse_fiscal_quarter(last_quarter)
        }
        
        # Forecast each metric
        for metric in metric_columns:
            forecast_values = forecast_metric(df, metric, product_code, product, steps=forecast_steps)
            forecast_row[metric] = forecast_values.values[i]
        
        forecast_rows.append(forecast_row)

# Create forecast DataFrame
forecast_df = pd.DataFrame(forecast_rows)

# Append forecasts to original DataFrame
df = pd.concat([df, forecast_df], ignore_index=True)

# Export the processed DataFrame with forecasts to CSV
df.to_csv('processed_data_with_forecasts.csv', index=False)
print("\nDataFrame with forecasts exported to 'processed_data_with_forecasts.csv'")

# Display last few rows including forecasts
print("\nLast few rows of the DataFrame with forecasts:")
print(df.tail(forecast_steps + 2))