import pandas as pd
import os

# Load the combined NIFTY 50 data
local_csv_filename = os.path.join("/Users/sudheer/Documents/GitHub/nifty", "NIFTY50_Historical_Data_From_1995_Clean.csv")
combined_data = pd.read_csv(local_csv_filename)

# Ensure Date column is in datetime format
combined_data['Date'] = pd.to_datetime(combined_data['Date'])

# Extract Yearly Data

combined_data['Year'] = combined_data['Date'].dt.year

# Ensure data is sorted by Year and Date for correct first/last calculations
combined_data = combined_data.sort_values(['Year', 'Date'])

# Calculate Starting Price, End of Year Close, Year High, and Year Low
yearly_data = combined_data.groupby('Year').agg(
    start_close=('Close', 'first'),
    end_close=('Close', 'last'),
    high=('High', 'max'),
    low=('Low', 'min')
).reset_index()

# Calculate Yearly Returns (Percentage)
yearly_returns = combined_data.groupby('Year').agg(
    return_pct=('Close', lambda x: ((x.iloc[-1] - x.iloc[0]) / x.iloc[0]) * 100)
).reset_index()

# Calculate Yearly Points Change (Absolute)
yearly_points_change = combined_data.groupby('Year').agg(
    points_change=('Close', lambda x: (x.iloc[-1] - x.iloc[0]))
).reset_index()

# Calculate Yearly High to Low Percentage Change
yearly_high_low = combined_data.groupby('Year').agg(
    high_low_pct=('High', lambda x: ((x.max() - combined_data.loc[x.index, 'Low'].min()) / combined_data.loc[x.index, 'Low'].min()) * 100)
).reset_index()

# Calculate Points Change from Start of the Year High and Low
yearly_high_low_points = combined_data.groupby('Year').agg(
    high_low_points=('High', lambda x: (x.max() - combined_data.loc[x.index, 'Low'].min()))
).reset_index()

# Calculate Yearly Starting to Low Points Change (Negative)
yearly_start_low_points = combined_data.groupby('Year').agg(
    start_low_points=('Low', lambda x: (x.min() - combined_data.loc[x.index[0], 'Close']))
).reset_index()

# Calculate Yearly Starting to Low Percentage Change (Negative)
yearly_start_low_percentage = combined_data.groupby('Year').agg(
    start_low_pct=('Low', lambda x: ((x.min() - combined_data.loc[x.index[0], 'Close']) / combined_data.loc[x.index[0], 'Close']) * 100)
).reset_index()

# Calculate Yearly Starting to High Points Change
yearly_start_high_points = combined_data.groupby('Year').agg(
    start_high_points=('High', lambda x: (x.max() - combined_data.loc[x.index[0], 'Close']))
).reset_index()

# Calculate Yearly Starting to High Percentage Change
yearly_start_high_percentage = combined_data.groupby('Year').agg(
    start_high_pct=('High', lambda x: ((x.max() - combined_data.loc[x.index[0], 'Close']) / combined_data.loc[x.index[0], 'Close']) * 100)
).reset_index()

# Calculate Year Start to Year End Close Price Percentage Change
yearly_start_to_end_percentage = combined_data.groupby('Year').agg(
    start_end_pct=('Close', lambda x: ((x.iloc[-1] - x.iloc[0]) / x.iloc[0]) * 100)
).reset_index()

# Calculate Year Start to Mid-Year Close Price Percentage Change
# Find the mid-date for each year and calculate the percentage change from start to mid-year close
mid_pct_list = []
for year, group in combined_data.groupby('Year'):
    group = group.sort_values('Date')
    n = len(group)
    if n == 0:
        continue
    mid_idx = n // 2
    start_close = group.iloc[0]['Close']
    mid_close = group.iloc[mid_idx]['Close']
    pct_change = ((mid_close - start_close) / start_close) * 100
    mid_pct_list.append({'Year': year, 'start_mid_pct': pct_change})
yearly_start_to_mid_percentage = pd.DataFrame(mid_pct_list)

# Merge the dataframes
yearly_data = pd.merge(yearly_data, yearly_returns, on='Year')
yearly_data = pd.merge(yearly_data, yearly_points_change, on='Year')
yearly_data = pd.merge(yearly_data, yearly_high_low, on='Year')
yearly_data = pd.merge(yearly_data, yearly_high_low_points, on='Year')
yearly_data = pd.merge(yearly_data, yearly_start_low_points, on='Year')
yearly_data = pd.merge(yearly_data, yearly_start_low_percentage, on='Year')
yearly_data = pd.merge(yearly_data, yearly_start_high_points, on='Year')
yearly_data = pd.merge(yearly_data, yearly_start_high_percentage, on='Year')
yearly_data = pd.merge(yearly_data, yearly_start_to_end_percentage, on='Year')
yearly_data = pd.merge(yearly_data, yearly_start_to_mid_percentage, on='Year')

# Display the yearly data
print("\nYearly NIFTY 50 Returns, Points Change, and High-to-Low Change:")
print(yearly_data)

# Save the yearly data to a CSV file
yearly_csv_filename = os.path.join("/Users/sudheer/Documents/GitHub/nifty", "NIFTY50_Yearly_Data.csv")
yearly_data.to_csv(yearly_csv_filename, index=False)
print(f"\nYearly NIFTY 50 data saved locally as {yearly_csv_filename}")