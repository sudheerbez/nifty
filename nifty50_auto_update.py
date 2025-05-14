import yfinance as yf
import pandas as pd
from datetime import datetime
import os
import kagglehub

# Step 1: Download Kaggle Data (1990-2024)
path = kagglehub.dataset_download("adarshde/nifty-50-data-1990-2024")
print("Path to dataset files:", path)

# Load Kaggle Data
kaggle_file = path + "/NIFTY 50_Historical_PR_01011990to11102024.csv"
kaggle_data = pd.read_csv(kaggle_file)

# Filter data for dates before 2007-09-17
kaggle_data['Date'] = pd.to_datetime(kaggle_data['Date'])
# Ensure Kaggle Data Date is timezone-naive
kaggle_data['Date'] = kaggle_data['Date'].dt.tz_localize(None)
kaggle_data = kaggle_data[kaggle_data['Date'] < "2007-09-17"]

# Check if 'Volume' column exists
expected_columns = ['Date', 'Open', 'High', 'Low', 'Close']
if 'Volume' in kaggle_data.columns:
    expected_columns.append('Volume')

kaggle_data = kaggle_data[expected_columns].round(2)

# Step 2: Fetch New Data from Yahoo Finance (2007-09-17 to Today)
nifty50 = yf.Ticker("^NSEI")
new_data = nifty50.history(start="2007-09-17", end=datetime.today().strftime("%Y-%m-%d"))

# Check if data is empty
if new_data.empty:
    print("No data found. Please check the ticker symbol or try another data source.")
else:
    # Rounding all numeric columns to two decimals
    new_data = new_data[['Open', 'High', 'Low', 'Close', 'Volume']].round(2)
    new_data.reset_index(inplace=True)
    new_data.rename(columns={'Date': 'Date'}, inplace=True)
    # Ensure Yahoo Finance Data Date is timezone-naive
    new_data['Date'] = pd.to_datetime(new_data['Date']).dt.tz_localize(None)

# Step 3: Combine Kaggle Data with New Data
combined_data = pd.concat([kaggle_data, new_data], ignore_index=True)
combined_data.drop_duplicates(subset='Date', keep='first', inplace=True)
combined_data.sort_values('Date', inplace=True)

# Convert numeric columns to float and handle non-numeric values
numeric_columns = ['Open', 'High', 'Low', 'Close']
for column in numeric_columns:
    combined_data[column] = pd.to_numeric(combined_data[column], errors='coerce')

# Drop any rows with missing values in these columns
combined_data.dropna(subset=numeric_columns, inplace=True)

# Calculate daily point change and percentage change
combined_data['Point_Change'] = (combined_data['Close'] - combined_data['Open']).round(2)
combined_data['Percentage_Change'] = ((combined_data['Point_Change'] / combined_data['Open']) * 100).round(2)

# Step 4: Save the Combined Data
home_dir = os.path.expanduser("~")
csv_filename = os.path.join(home_dir, "Desktop", "NIFTY50_Historical_Data_From_1995_Clean.csv")
combined_data.to_csv(csv_filename, index=False)

print(f"NIFTY 50 Historical Data saved and updated as {csv_filename}")

# Displaying the latest update date and last few rows for verification
print("\nNIFTY 50 Data Updated Successfully:")
print(combined_data.tail())

import subprocess

# Step 5: Push the CSV file to GitHub
github_repo_path = os.path.join(home_dir, "Desktop", "nifty50_data")  # Ensure this path is your cloned GitHub repo
csv_filename_in_repo = os.path.join(github_repo_path, "NIFTY50_Historical_Data_From_1995_Clean.csv")

# Copy the CSV file to the GitHub repository folder
os.system(f"cp {csv_filename} {csv_filename_in_repo}")

# Git commands to add, commit, and push
try:
    subprocess.run(["git", "-C", github_repo_path, "add", "NIFTY50_Historical_Data_From_1995_Clean.csv"], check=True)
    subprocess.run(["git", "-C", github_repo_path, "commit", "-m", "Updated NIFTY 50 historical data"], check=True)
    subprocess.run(["git", "-C", github_repo_path, "push"], check=True)
    print("CSV file successfully pushed to GitHub.")
except subprocess.CalledProcessError as e:
    print(f"Error during GitHub push: {e}")