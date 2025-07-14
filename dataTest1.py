import pandas as pd
import requests
from io import BytesIO
from dotenv import load_dotenv
import os

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
    print("\nFirst few rows of the DataFrame:")
    print(df.head())
except Exception as e:
    print(f"Error loading the Excel file: {e}")
    exit(1)

# Display basic info about the DataFrame
print("\nDataFrame Info:")
print(df.info())