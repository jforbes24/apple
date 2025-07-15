import pandas as pd
import requests
import os
from io import BytesIO
import zipfile
import xml.etree.ElementTree as ET

# URL of the Numbers file
url = "https://github.com/jforbes24/apple/raw/main/D%26T%20Test%20No%202.numbers"

# Download the Numbers file
response = requests.get(url)
if response.status_code != 200:
    raise Exception("Failed to download the file")

# Numbers files are zip archives; extract the contents
numbers_file = BytesIO(response.content)

# Create a temporary directory to extract the file
with zipfile.ZipFile(numbers_file, 'r') as zip_ref:
    zip_ref.extractall("temp_numbers")

# Find the main data file (index.xml) within the Numbers file
index_file_path = os.path.join("temp_numbers", "index.xml")

# Parse the XML to extract table data
tree = ET.parse(index_file_path)
root = tree.getroot()

# Namespace handling for Numbers XML
ns = {'iwork': 'http://developer.apple.com/namespaces/iwork'}

# Find the table data
table = root.find(".//iwork:table", ns)
rows = []

# Extract cell data
for row in table.findall(".//iwork:grid/iwork:row", ns):
    row_data = []
    for cell in row.findall(".//iwork:cell", ns):
        cell_text = cell.find(".//iwork:text", ns)
        row_data.append(cell_text.text if cell_text is not None else "")
    rows.append(row_data)

# Convert to DataFrame
df = pd.DataFrame(rows)

# Remove the first and third rows (0-based index: 0 and 2)
df = df.drop([0, 2]).reset_index(drop=True)

# Set the second row (now index 0 after dropping the first row) as the header
df.columns = df.iloc[0]
df = df.drop(0).reset_index(drop=True)

# Convert the first column to string
df.iloc[:, 0] = df.iloc[:, 0].astype(str)

# Delete the fifth column (0-based index: 4)
if df.shape[1] >= 5:
    df = df.drop(df.columns[4], axis=1)

# Save the DataFrame to a CSV file
output_file = "processed_output.csv"
df.to_csv(output_file, index=False)

# Clean up temporary files
import shutil
shutil.rmtree("temp_numbers")

print(f"CSV file saved as {output_file}")