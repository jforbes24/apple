import pandas as pd
import requests
from io import BytesIO
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import pmdarima as pm
import warnings
import numpy as np

# Suppress warnings
warnings.filterwarnings("ignore")

# Set pandas options
pd.set_option('display.max_columns', None)

# Load environment variables
load_dotenv()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    print("Error: GITHUB_TOKEN not found in .env file")
    exit(1)

# Download the Excel file
url = "https://raw.githubusercontent.com/jforbes24/apple/main/APPLE%20Task%201/D%26T%20Data%20Test%20No%201.xlsx"
headers = {"Authorization": f"token {GITHUB_TOKEN}"}
try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    df = pd.read_excel(BytesIO(response.content), engine='openpyxl')
    print("DataFrame loaded successfully!")
    print("Raw DataFrame columns:", df.columns.tolist())
except Exception as e:
    print(f"Error loading the Excel file: {e}")
    exit(1)

# Check required columns
required_columns = ['Product Code', 'Product', 'FISCAL_QTR_YEAR_NAME', 'FISCAL_WEEK_YEAR_NAME', 'Sessions', 'PDP Add to Cart Units', 'Units Sold']
missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    print(f"Error: The following required columns are missing: {missing_columns}")
    exit(1)

# Add Data_Type column
df['Data_Type'] = 'Actual'

# Clean metric columns
metric_columns = ['Sessions', 'PDP Add to Cart Units', 'Units Sold']
for col in metric_columns:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)

# Parse fiscal quarter and week
def parse_fiscal_quarter(fiscal_qtr):
    try:
        if not isinstance(fiscal_qtr, str) or not fiscal_qtr.startswith('FY') or len(fiscal_qtr) < 5:
            print(f"Invalid fiscal quarter format: {fiscal_qtr}")
            raise ValueError(f"Invalid fiscal quarter format: {fiscal_qtr}")
        year = int(fiscal_qtr[2:4]) + 2000
        quarter = int(fiscal_qtr[-1])
        month = {1: 10, 2: 1, 3: 4, 4: 7}[quarter]
        if quarter == 1:
            year -= 1
        return datetime(year, month, 1)
    except Exception as e:
        print(f"Error parsing fiscal quarter '{fiscal_qtr}': {e}")
        return pd.NaT

def parse_fiscal_week(fiscal_week):
    try:
        if not isinstance(fiscal_week, str) or not fiscal_week.startswith('FY') or 'W' not in fiscal_week or len(fiscal_week) < 6:
            print(f"Invalid fiscal week format: {fiscal_week}")
            raise ValueError(f"Invalid fiscal week format: {fiscal_week}")
        year_str = fiscal_week[2:4]
        week_str = fiscal_week.split('W')[-1]
        year = int(year_str) + 2000
        week = int(week_str)
        fiscal_year_start = datetime(year - 1, 10, 1)
        week_start = fiscal_year_start + timedelta(weeks=week - 1)
        return week_start
    except Exception as e:
        print(f"Error parsing fiscal week '{fiscal_week}': {e}")
        return pd.NaT

# Generate fiscal periods for future dates
def generate_fiscal_period(date, last_fiscal_week, last_fiscal_quarter):
    try:
        last_year = int(last_fiscal_week[2:4]) + 2000
        last_week_num = int(last_fiscal_week[5:])
        last_date = parse_fiscal_week(last_fiscal_week)
        if pd.isna(last_date):
            raise ValueError(f"Cannot generate fiscal period; invalid last_fiscal_week: {last_fiscal_week}")
        weeks_ahead = ((date - last_date).days // 7) + 1
        week_num = last_week_num + weeks_ahead
        year_adjust = 0
        if week_num > 52:
            week_num = week_num % 52 or 52
            year_adjust = (last_week_num + weeks_ahead - 1) // 52
        fiscal_week = f"FY{(last_year % 100) + year_adjust:02d}W{week_num:02d}"
        last_quarter = int(last_fiscal_quarter[-1])
        last_quarter_year = int(last_fiscal_quarter[2:4]) + 2000
        quarter_start = parse_fiscal_quarter(last_fiscal_quarter)
        quarters_ahead = (date.year - quarter_start.year) * 4 + (date.month - quarter_start.month) // 3
        new_quarter = (last_quarter + quarters_ahead - 1) % 4 + 1
        quarter_year = last_quarter_year + (quarters_ahead // 4)
        fiscal_quarter = f"FY{quarter_year % 100:02d}Q{new_quarter}"
        return fiscal_week, fiscal_quarter, parse_fiscal_quarter(fiscal_quarter)
    except Exception as e:
        print(f"Error generating fiscal period: {e}")
        return None, None, pd.NaT

# Add date columns
df['Quarter_Start_Date'] = df['FISCAL_QTR_YEAR_NAME'].apply(parse_fiscal_quarter)
df['Week_Start_Date'] = df['FISCAL_WEEK_YEAR_NAME'].apply(parse_fiscal_week)
print("Columns after adding Week_Start_Date:", df.columns.tolist())
print("Sample Week_Start_Date values:", df['Week_Start_Date'].head().tolist())
print("Rows with NaT in Week_Start_Date:", df['Week_Start_Date'].isna().sum())

# Debug invalid Week_Start_Date values
invalid_weeks = df[df['Week_Start_Date'].isna()]['FISCAL_WEEK_YEAR_NAME'].dropna().unique()
if len(invalid_weeks) > 0:
    print(f"Warning: {len(invalid_weeks)} unique invalid FISCAL_WEEK_YEAR_NAME values found: {invalid_weeks[:10]}")
    print(f"Total rows with invalid Week_Start_Date: {df['Week_Start_Date'].isna().sum()}")
    print(f"Sample of invalid rows:\n{df[df['Week_Start_Date'].isna()][['Product', 'FISCAL_WEEK_YEAR_NAME']].head()}")

# Filter for rows with valid Product and Week_Start_Date
original_len = len(df)
df = df.dropna(subset=['Product', 'Week_Start_Date'])
if df.empty:
    print(f"Error: No valid data after filtering for Product and Week_Start_Date. Dropped {original_len} rows.")
    print("Check FISCAL_WEEK_YEAR_NAME formats in the Excel file.")
    exit(1)
print(f"Filtered {original_len - len(df)} rows with invalid Product or Week_Start_Date. Remaining rows: {len(df)}")
print("Columns after filtering:", df.columns.tolist())

# Aggregate duplicates by Product and Week_Start_Date
agg_dict = {metric: 'sum' for metric in metric_columns}
agg_dict.update({
    'FISCAL_QTR_YEAR_NAME': 'first',
    'FISCAL_WEEK_YEAR_NAME': 'first',
    'Quarter_Start_Date': 'first',
    'Data_Type': 'first',
    'Product Code': 'first'
})
df = df.groupby(['Product', 'Week_Start_Date']).agg(agg_dict).reset_index()
print("Columns after aggregation:", df.columns.tolist())

# Ensure continuous time series
products = df['Product'].unique()
all_weeks = pd.date_range(start=df['Week_Start_Date'].min(), end=df['Week_Start_Date'].max(), freq='W-MON')
complete_data = []
for product in products:
    product_data = df[df['Product'] == product].set_index('Week_Start_Date')
    product_data = product_data.reindex(all_weeks, fill_value=0).reset_index(names='Week_Start_Date')
    product_data['Product'] = product
    product_data['FISCAL_QTR_YEAR_NAME'] = product_data['FISCAL_QTR_YEAR_NAME'].ffill()
    product_data['FISCAL_WEEK_YEAR_NAME'] = product_data['FISCAL_WEEK_YEAR_NAME'].ffill()
    product_data['Quarter_Start_Date'] = product_data['Quarter_Start_Date'].ffill()
    product_data['Data_Type'] = product_data['Data_Type'].fillna('Actual')
    product_data['Product Code'] = product_data['Product Code'].fillna('N/A')
    complete_data.append(product_data)
df = pd.concat(complete_data, ignore_index=True)
print("Columns before forecasting:", df.columns.tolist())

# Forecast function using auto_arima
def forecast_metric(data, metric, product, steps=10):
    try:
        ts_data = data[data['Product'] == product].groupby('Week_Start_Date')[metric].sum()
        ts = ts_data.reindex(all_weeks, fill_value=0)
        if ts.sum() == 0 or len(ts) < 10:
            print(f"Skipping forecast for {metric} with Product {product}: insufficient data")
            return pd.Series([0] * steps)
        model = pm.auto_arima(ts, seasonal=True, m=52, suppress_warnings=True, error_action='ignore')
        forecast = model.predict(n_periods=steps)
        return forecast.clip(min=0)
    except Exception as e:
        print(f"Error forecasting {metric} for Product {product}: {e}")
        return pd.Series([0] * steps)

# Forecast 10 weeks
forecast_steps = 10
last_date = df['Week_Start_Date'].max()
forecast_dates = [last_date + timedelta(weeks=i+1) for i in range(forecast_steps)]

# Create forecast DataFrame
forecast_rows = []
for product in products:
    product_data = df[df['Product'] == product]
    last_fiscal_week = product_data['FISCAL_WEEK_YEAR_NAME'].iloc[-1]
    last_fiscal_quarter = product_data['FISCAL_QTR_YEAR_NAME'].iloc[-1]
    for i, forecast_date in enumerate(forecast_dates):
        fiscal_week, fiscal_quarter, quarter_start_date = generate_fiscal_period(
            forecast_date, last_fiscal_week, last_fiscal_quarter
        )
        forecast_row = {
            'Product': product,
            'FISCAL_WEEK_YEAR_NAME': fiscal_week,
            'FISCAL_QTR_YEAR_NAME': fiscal_quarter,
            'Week_Start_Date': forecast_date,
            'Quarter_Start_Date': quarter_start_date,
            'Data_Type': 'Forecast',
            'Product Code': 'N/A'
        }
        for metric in metric_columns:
            forecast_values = forecast_metric(df, metric, product, steps=forecast_steps)
            forecast_row[metric] = forecast_values.iloc[i]
        forecast_rows.append(forecast_row)

forecast_df = pd.DataFrame(forecast_rows)
df = pd.concat([df, forecast_df], ignore_index=True)

# Ensure consistent date format for Looker Studio
df['Week_Start_Date'] = pd.to_datetime(df['Week_Start_Date']).dt.strftime('%Y-%m-%d')
df['Quarter_Start_Date'] = pd.to_datetime(df['Quarter_Start_Date']).dt.strftime('%Y-%m-%d')

# Export to CSV
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(script_dir, 'processed_data_with_forecasts.csv')
df.to_csv(csv_path, index=False)
print(f"\nDataFrame with forecasts exported to '{csv_path}'")

# Display sample
print("\nSample of DataFrame with forecasts:")
print(df.tail(forecast_steps + 2))