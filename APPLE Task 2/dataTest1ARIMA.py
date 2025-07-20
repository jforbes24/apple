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
url = "https://raw.githubusercontent.com/jforbes24/apple/main/D%26T%20Data%20Test%20No%201.xlsx"
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
    if df[col].dtype == 'object':
        df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')

# Add new date columns
df['Quarter_Start_Date'] = df['FISCAL_QTR_YEAR_NAME'].apply(parse_fiscal_quarter)
df['Week_Start_Date'] = df['FISCAL_WEEK_YEAR_NAME'].apply(parse_fiscal_week)

# Ensure Week_Start_Date is sorted
df = df.sort_values('Week_Start_Date')

# Function to fit ARIMA model and forecast
def forecast_metric(data, metric, steps=4, order=(1,1,1)):
    try:
        # Prepare time series data
        ts = data.set_index('Week_Start_Date')[metric].dropna()
        # Fit ARIMA model
        model = ARIMA(ts, order=order)
        model_fit = model.fit()
        # Forecast
        forecast = model_fit.forecast(steps=steps)
        return forecast
    except Exception as e:
        print(f"Error forecasting {metric}: {e}")
        return pd.Series([None] * steps)

# Forecast 4 weeks ahead
forecast_steps = 4
last_date = df['Week_Start_Date'].max()
forecast_dates = [last_date + timedelta(weeks=i+1) for i in range(forecast_steps)]

# Create forecast DataFrame
forecast_data = {
    'Week_Start_Date': forecast_dates,
    'FISCAL_WEEK_YEAR_NAME': [f"FY{(last_date.year % 100) + (1 if last_date.month >= 10 else 0)}W{i+1}" for i in range(forecast_steps)],
    'FISCAL_QTR_YEAR_NAME': [df['FISCAL_QTR_YEAR_NAME'].iloc[-1]] * forecast_steps  # Assume same quarter
}

# Forecast each metric
for metric in metric_columns:
    forecast_values = forecast_metric(df, metric, steps=forecast_steps)
    forecast_data[metric] = forecast_values.values

# Create forecast DataFrame
forecast_df = pd.DataFrame(forecast_data)

# Append forecasts to original DataFrame
df = pd.concat([df, forecast_df], ignore_index=True)

# Export the processed DataFrame with forecasts to CSV
df.to_csv('processed_data_with_forecasts.csv', index=False)
print("\nDataFrame with forecasts exported to 'processed_data_with_forecasts.csv'")

# Display last few rows including forecasts
print("\nLast few rows of the DataFrame with forecasts:")
print(df.tail(forecast_steps + 2))