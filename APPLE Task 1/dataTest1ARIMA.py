import pandas as pd
import requests
from io import BytesIO
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import pmdarima as pm
import warnings
import numpy as np
from joblib import Parallel, delayed

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

# Filter out summary rows and handle missing data
df = df[df['Product Code'] != 'NOT GIVEN'].dropna(subset=['Product'])

# Add Data_Type column
df['Data_Type'] = 'Actual'

# Clean metric columns
metric_columns = ['Sessions', 'PDP Add to Cart Units', 'Units Sold']
for col in metric_columns:
    df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)

# Validate fiscal formats
invalid_quarters = df['FISCAL_QTR_YEAR_NAME'].dropna().loc[~df['FISCAL_QTR_YEAR_NAME'].str.match(r'^FY\d{2}Q[1-4]$')]
invalid_weeks = df['FISCAL_WEEK_YEAR_NAME'].dropna().loc[~df['FISCAL_WEEK_YEAR_NAME'].str.match(r'^FY\d{2}W\d{2}$')]
if not invalid_quarters.empty:
    print(f"Warning: Found {len(invalid_quarters)} invalid fiscal quarter formats. Examples: {invalid_quarters.unique()[:5]}")
if not invalid_weeks.empty:
    print(f"Warning: Found {len(invalid_weeks)} invalid fiscal week formats. Examples: {invalid_weeks.unique()[:5]}")

# Parse fiscal quarter and week
def parse_fiscal_quarter(fiscal_qtr):
    try:
        if not isinstance(fiscal_qtr, str) or not fiscal_qtr.startswith('FY'):
            return pd.to_datetime('2000-01-01')
        year = int(fiscal_qtr[2:4]) + 2000
        quarter = int(fiscal_qtr[-1])
        month = {1: 10, 2: 1, 3: 4, 4: 7}[quarter]
        if quarter == 1:
            year -= 1
        return datetime(year, month, 1)
    except Exception as e:
        print(f"Error parsing fiscal quarter '{fiscal_qtr}': {e}")
        return pd.to_datetime('2000-01-01')

def parse_fiscal_week(fiscal_week):
    try:
        if not isinstance(fiscal_week, str) or not fiscal_week.startswith('FY'):
            return pd.to_datetime('2000-01-01')
        year_str = fiscal_week[2:4]
        week_str = fiscal_week.split('W')[-1]
        year = int(year_str) + 2000
        week = int(week_str)
        fiscal_year_start = datetime(year - 1, 10, 1)
        week_start = fiscal_year_start + timedelta(weeks=week - 1)
        return week_start
    except Exception as e:
        print(f"Error parsing fiscal week '{fiscal_week}': {e}")
        return pd.to_datetime('2000-01-01')

# Generate fiscal periods for future dates
def generate_fiscal_period(date, last_fiscal_week, last_fiscal_quarter):
    try:
        if not isinstance(last_fiscal_week, str) or not last_fiscal_week.startswith('FY') or 'W' not in last_fiscal_week:
            last_fiscal_week = 'FY23W53'
            print(f"Invalid last_fiscal_week format, using fallback: {last_fiscal_week}")
        if not isinstance(last_fiscal_quarter, str) or not last_fiscal_quarter.startswith('FY'):
            last_fiscal_quarter = 'FY23Q4'
            print(f"Invalid last_fiscal_quarter format, using fallback: {last_fiscal_quarter}")

        last_year = int(last_fiscal_week[2:4]) + 2000
        last_week_num = int(last_fiscal_week.split('W')[-1])
        last_date = parse_fiscal_week(last_fiscal_week)
        if last_date == pd.to_datetime('2000-01-01'):
            last_date = datetime(2023, 9, 11)  # Approximate FY23W53
            print(f"Cannot parse last_fiscal_week: {last_fiscal_week}, using fallback date")

        weeks_ahead = max(1, (date - last_date).days // 7 + 1)
        week_num = (last_week_num + weeks_ahead - 1) % 52 + 1
        year_adjust = (last_week_num + weeks_ahead - 1) // 52
        fiscal_week = f"FY{(last_year + year_adjust) % 100:02d}W{week_num:02d}"

        last_quarter = int(last_fiscal_quarter[-1])
        last_quarter_year = int(last_fiscal_quarter[2:4]) + 2000
        quarter_start = parse_fiscal_quarter(last_fiscal_quarter)
        if quarter_start == pd.to_datetime('2000-01-01'):
            quarter_start = datetime(2023, 7, 1)  # FY23Q4 start
            print(f"Cannot parse last_fiscal_quarter: {last_fiscal_quarter}, using fallback")

        quarters_ahead = (date.year - quarter_start.year) * 4 + (date.month - quarter_start.month) // 3
        new_quarter = (last_quarter + quarters_ahead - 1) % 4 + 1
        quarter_year = last_quarter_year + (quarters_ahead // 4)
        fiscal_quarter = f"FY{quarter_year % 100:02d}Q{new_quarter}"

        return fiscal_week, fiscal_quarter, parse_fiscal_quarter(fiscal_quarter)
    except Exception as e:
        print(f"Error generating fiscal period for date {date}: {e}, using fallback")
        return f"FY{(date.year % 100):02d}W01", f"FY{(date.year % 100):02d}Q1", pd.to_datetime(f"{date.year}-01-01")

# Add date columns
df['Quarter_Start_Date'] = df['FISCAL_QTR_YEAR_NAME'].apply(parse_fiscal_quarter)
df['Week_Start_Date'] = df['FISCAL_WEEK_YEAR_NAME'].apply(parse_fiscal_week)

# Filter out rows with invalid Week_Start_Date, but preserve more data
original_len = len(df)
df = df[df['Week_Start_Date'] != pd.to_datetime('2000-01-01')]
print(f"Filtered {original_len - len(df)} rows with invalid Week_Start_Date. Remaining rows: {len(df)}")

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

# Ensure continuous time series
products = df['Product'].unique()
all_weeks = pd.date_range(start=df['Week_Start_Date'].min(), end=df['Week_Start_Date'].max(), freq='W-MON')
complete_data = []
for product in products:
    product_data = df[df['Product'] == product].set_index('Week_Start_Date')
    product_data = product_data.reindex(all_weeks, fill_value=0).reset_index(names='Week_Start_Date')
    product_data['Product'] = product
    product_data['FISCAL_QTR_YEAR_NAME'] = product_data['FISCAL_QTR_YEAR_NAME'].ffill().bfill()
    product_data['FISCAL_WEEK_YEAR_NAME'] = product_data['FISCAL_WEEK_YEAR_NAME'].ffill().bfill()
    product_data['Quarter_Start_Date'] = product_data['Quarter_Start_Date'].ffill().bfill()
    product_data['Data_Type'] = product_data['Data_Type'].fillna('Actual')
    product_data['Product Code'] = product_data['Product Code'].fillna('N/A')
    complete_data.append(product_data)
df = pd.concat(complete_data, ignore_index=True)

# Forecast function using auto_arima
def forecast_metric(data, metric, product, steps=10, min_data_points=10):
    try:
        ts_data = data[data['Product'] == product].groupby('Week_Start_Date')[metric].sum()
        ts = ts_data.reindex(all_weeks, fill_value=0)
        non_zero_count = (ts != 0).sum()
        if non_zero_count < min_data_points or len(ts) < min_data_points:
            print(f"Skipping forecast for {metric} with Product {product}: only {non_zero_count} non-zero data points")
            return pd.Series([0] * steps, index=forecast_dates[:steps])
        model = pm.auto_arima(ts, seasonal=True, m=52, suppress_warnings=True, error_action='ignore',
                              max_order=None, stepwise=True)
        forecast = model.predict(n_periods=steps)
        return pd.Series(forecast.clip(min=0), index=forecast_dates[:steps])
    except Exception as e:
        print(f"Error forecasting {metric} for Product {product}: {e}")
        return pd.Series([0] * steps, index=forecast_dates[:steps])

# Forecast 10 weeks
forecast_steps = 10
last_date = df['Week_Start_Date'].max()
forecast_dates = [last_date + timedelta(weeks=i+1) for i in range(forecast_steps)]

# Parallel forecasting
def forecast_product(product, df, metric_columns, forecast_dates):
    product_data = df[df['Product'] == product]
    last_fiscal_week = product_data['FISCAL_WEEK_YEAR_NAME'].iloc[-1] if not product_data['FISCAL_WEEK_YEAR_NAME'].empty else 'FY23W53'
    last_fiscal_quarter = product_data['FISCAL_QTR_YEAR_NAME'].iloc[-1] if not product_data['FISCAL_QTR_YEAR_NAME'].empty else 'FY23Q4'
    forecast_rows = []
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
            'Product Code': product_data['Product Code'].iloc[-1] if not product_data['Product Code'].isna().all() else 'N/A'
        }
        for metric in metric_columns:
            forecast_values = forecast_metric(df, metric, product, steps=forecast_steps)
            forecast_row[metric] = forecast_values.iloc[i]
        forecast_rows.append(forecast_row)
    return forecast_rows

forecast_rows = Parallel(n_jobs=-1)(
    delayed(forecast_product)(product, df, metric_columns, forecast_dates) for product in products
)
forecast_rows = [row for sublist in forecast_rows for row in sublist]
forecast_df = pd.DataFrame(forecast_rows)
df = pd.concat([df, forecast_df], ignore_index=True)

# Ensure consistent date format
df['Week_Start_Date'] = pd.to_datetime(df['Week_Start_Date']).dt.strftime('%Y-%m-%d')
df['Quarter_Start_Date'] = pd.to_datetime(df['Quarter_Start_Date']).dt.strftime('%Y-%m-%d')

# Export to CSV
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(script_dir, 'processed_data_with_forecasts.csv')
df.to_csv(csv_path, index=False)
print(f"\nDataFrame with forecasts exported to '{csv_path}'")

# Export diagnostics
skipped_forecasts = []
for product in products:
    for metric in metric_columns:
        try:
            forecast = forecast_metric(df, metric, product, steps=1)
            if forecast.sum() == 0:
                skipped_forecasts.append(f"{metric} for {product}")
        except Exception as e:
            skipped_forecasts.append(f"{metric} for {product} (error: {str(e)})")
            print(f"Error in diagnostics for {metric} with Product {product}: {e}")

diagnostics = {
    'Invalid_Fiscal_Weeks': invalid_weeks.tolist(),
    'Invalid_Fiscal_Quarters': invalid_quarters.tolist(),
    'Skipped_Forecasts': skipped_forecasts,
    'Row_Counts': {
        'Original': original_len,
        'After_Filtering': len(df) - len(forecast_df),
        'Forecasted': len(forecast_df)
    }
}
diagnostic_path = os.path.join(script_dir, 'diagnostics.json')
pd.Series(diagnostics).to_json(diagnostic_path, indent=2)
print(f"Diagnostics exported to '{diagnostic_path}'")

# Display sample
print("\nSample of DataFrame with forecasts:")
print(df.tail(forecast_steps + 2))