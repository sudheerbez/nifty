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

# Fetch India VIX Data without API Key (Advanced Web Scraping from Screener.in)
from bs4 import BeautifulSoup
import requests

def fetch_india_vix():
    try:
        print("Attempting to fetch India VIX data directly from Screener.in...")
        screener_url = "https://www.screener.in/company/INDIAVIX/consolidated/"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        }

        session = requests.Session()
        session.headers.update(headers)
        response = session.get(screener_url)
        soup = BeautifulSoup(response.text, "html.parser")

        # Debugging: Display the first 500 characters of the page
        print("\n--- HTML Content Preview ---")
        print(response.text[:500])
        print("\n--- End of Preview ---\n")

        # Attempt to extract India VIX value using multiple methods
        vix_value = None
        # Method 1: Using span with ₹ symbol
        for span in soup.find_all("span"):
            if "₹" in span.text:
                try:
                    vix_value = float(span.text.replace("₹", "").replace(",", "").strip())
                    print(f"VIX value extracted using Method 1: {vix_value}")
                    break
                except:
                    continue

        # Method 2: Using alternate method if first fails
        if not vix_value:
            vix_section = soup.find("div", class_="company-ratios")
            if vix_section:
                vix_text = vix_section.get_text()
                for line in vix_text.splitlines():
                    if "₹" in line:
                        try:
                            vix_value = float(line.replace("₹", "").replace(",", "").strip())
                            print(f"VIX value extracted using Method 2: {vix_value}")
                            break
                        except:
                            continue

        if vix_value:
            print(f"India VIX data fetched successfully from Screener.in: {vix_value}")
            return pd.DataFrame([{
                'Date': datetime.today().strftime("%Y-%m-%d"),
                'India_VIX': vix_value
            }])
        else:
            raise ValueError("India VIX data could not be extracted from Screener.in.")

    except Exception as e:
        print(f"Error fetching India VIX data from Screener.in: {e}")
        print("Using local India VIX data as fallback...")
        try:
            local_vix_file = "/Users/sudheer/Documents/GitHub/nifty/India_VIX_Historical_Data.csv"
            vix_data = pd.read_csv(local_vix_file)
            vix_data['Date'] = pd.to_datetime(vix_data['Date'])
            print("India VIX data loaded from local file.")
            return vix_data
        except FileNotFoundError:
            print("Local VIX file not found. Creating empty VIX data with NaN values.")
            return pd.DataFrame(columns=['Date', 'India_VIX'])

# Use the fetch_india_vix function to get India VIX data
vix_data = fetch_india_vix()

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

# Ensure Date columns in both dataframes are in consistent datetime format
combined_data['Date'] = pd.to_datetime(combined_data['Date'])
vix_data['Date'] = pd.to_datetime(vix_data['Date'])

# Merge VIX Data with Combined NIFTY Data if VIX Data is available
if vix_data is not None:
    combined_data = pd.merge(combined_data, vix_data[['Date', 'India_VIX']], on='Date', how='left')
    print("India VIX data merged with NIFTY data.")
else:
    combined_data['India_VIX'] = None
    print("India VIX data not available. Column added with NaN values.")

# Step 4: Save the Combined Data Locally with India VIX
local_csv_filename = os.path.join("/Users/sudheer/Documents/GitHub/nifty", "NIFTY50_Historical_Data_From_1995_Clean.csv")
combined_data.to_csv(local_csv_filename, index=False)

print(f"NIFTY 50 Historical Data saved and updated locally as {local_csv_filename}")

# Displaying the latest update date and last few rows for verification
print("\nNIFTY 50 Data Updated Successfully:")
print(combined_data.tail())

# Run Machine Learning Model (Separate Script)
import nifty50_yearlydata