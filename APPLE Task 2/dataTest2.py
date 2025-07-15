import pandas as pd
import requests
import os
from io import BytesIO
import zipfile
import xml.etree.ElementTree as ET
import shutil

# URL of the Numbers file
url = "https://github.com/jforbes24/apple/raw/main/D%26T%20Test%20No%202.numbers"

# Download the Numbers file with headers to mimic a browser request
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
try:
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to download the file. HTTP Status Code: {response.status_code}")
        print(f"Response Text: {response.text[:500]}")  # Print first 500 chars of response for debugging
        raise Exception("Failed to download the file")
except requests.RequestException as e:
    print(f"Error during request: {e}")
    raise

# Numbers files are zip archives; extract the contents
numbers_file = BytesIO(response.content)

# Create a temporary directory to extract the file
temp_dir = "temp_numbers"
os.makedirs(temp_dir, exist_ok=True)
try:
    with zipfile.ZipFile(numbers_file, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)
except zipfile.BadZipFile:
    print("Error: The downloaded file is not a valid zip file. It may not be a Numbers file.")
    shutil.rmtree(temp_dir)
    raise

# Find the main data file (index.xml) within the Numbers file
index_file_path = os.path.join(temp_dir, "index.xml")
if not os.path.exists(index_file_path):
    print(f"Error: index.xml not found in {temp_dir}")
    shutil.rmtree(temp_dir)
    raise FileNotFoundError("index.xml not found in the Numbers file")

# Parse the XML to extract table data
try:
    tree = ET.parse(index_file_path)
    root = tree.getroot()
except ET.ParseError as e:
    print(f"Error parsing XML: {e}")
    shutil.rmtree(temp_dir)
    raise

# Namespace handling for Numbers XML
ns = {'iwork': 'http://developer.apple.com/namespaces/iwork'}

# Find the table data
table = root.find(".//iwork:table", ns)
if table is None:
    print("Error: No table found in the Numbers file XML")
    shutil.rmtree(temp_dir)
    raise ValueError("No table found in the Numbers file")

rows = []

# Extract cell data
for row in table.findall(".//iwork:grid/iwork:row", ns):
    row_data = []
    for cell in row.findall(".//iwork:cell", ns):
        cell_text = cell.find(".//iwork:text", ns)
        row_data.append(cell_text.text if cell_text is not None else "")
    rows.append(row_data)

# Convert to DataFrame
if not rows:
    print("Error: No rows extracted from the table")
    shutil.rmtree(temp_dir)
    raise ValueError("No data extracted from the table")

df = pd.DataFrame(rows)

# Remove the first and third rows (0-based index: 0 and 2)
if len(df) >= 3:
    df = df.drop([0, 2]).reset_index(drop=True)
else:
    print(f"Warning: DataFrame has only {len(df)} rows, cannot remove first and third rows")

# Set the second row (now index 0 after dropping the first row) as the header
if not df.empty:
    df.columns = df.iloc[0]
    df = df.drop(0).reset_index(drop=True)
else:
    print("Error: DataFrame is empty after processing")
    shutil.rmtree(temp_dir)
    raise ValueError("DataFrame is empty")

# Convert the first column to string
if df.shape[1] > 0:
    df.iloc[:, 0] = df.iloc[:, 0].astype(str)
else:
    print("Warning: No columns available to convert to string")

# Delete the fifth column (0-based index: 4)
if df.shape[1] >= 5:
    df = df.drop(df.columns[4], axis=1)
else:
    print(f"Warning: DataFrame has only {df.shape[1]} columns, cannot delete fifth column")

# Save the DataFrame to a CSV file
output_file = "processed_output.csv"
df.to_csv(output_file, index=False)

# Clean up temporary files
shutil.rmtree(temp_dir)

print(f"CSV file saved as {output_file}")