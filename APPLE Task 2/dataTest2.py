import pandas as pd
import os
from io import BytesIO
import zipfile
import xml.etree.ElementTree as ET
import shutil

# Path to the local Numbers file
local_file_path = "/Users/jforbes84/Documents/GitHub/apple/APPLE Task 2/D&T Test No 2.numbers"

# Read the local Numbers file
try:
    with open(local_file_path, "rb") as f:
        numbers_file = BytesIO(f.read())
except FileNotFoundError:
    print(f"Error: File not found at {local_file_path}")
    raise

# Create a temporary directory to extract the file
temp_dir = "temp_numbers"
os.makedirs(temp_dir, exist_ok=True)
try:
    with zipfile.ZipFile(numbers_file, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)
except zipfile.BadZipFile:
    print("Error: The file is not a valid zip file. It may not be a Numbers file.")
    shutil.rmtree(temp_dir)
    raise

# Debug: List all files in the extracted directory
print("Extracted files in temp_numbers:")
for root, dirs, files in os.walk(temp_dir):
    for file in files:
        print(os.path.join(root, file))

# Find the main data file (index.xml or alternative)
index_file_path = None
possible_index_files = ["index.xml", "Data"]
for possible_file in possible_index_files:
    candidate_path = os.path.join(temp_dir, possible_file)
    if os.path.exists(candidate_path):
        index_file_path = candidate_path
        print(f"Found data file: {index_file_path}")
        break
    # Check for nested directories (e.g., Contents/index.xml)
    for root, _, files in os.walk(temp_dir):
        if possible_file in files:
            index_file_path = os.path.join(root, possible_file)
            print(f"Found data file: {index_file_path}")
            break
    if index_file_path:
        break

if not index_file_path:
    print(f"Error: No data file (index.xml or Data) found in {temp_dir}")
    shutil.rmtree(temp_dir)
    raise FileNotFoundError("No data file found in the Numbers file")

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