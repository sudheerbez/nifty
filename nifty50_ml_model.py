import pandas as pd
import os

# Load the combined NIFTY 50 data
local_csv_filename = os.path.join("/Users/sudheer/Documents/GitHub/nifty", "NIFTY50_Historical_Data_From_1995_Clean.csv")
combined_data = pd.read_csv(local_csv_filename)

# Ensure Date column is in datetime format
combined_data['Date'] = pd.to_datetime(combined_data['Date'])

# Extract Yearly Data

combined_data['Year'] = combined_data['Date'].dt.year

# Calculate Starting Price, End of Year Close, Year High, and Year Low
yearly_data = combined_data.groupby('Year').agg(
    Yearly_Starting_Price=('Close', 'first'),
    Yearly_End_Price=('Close', 'last'),
    Yearly_High=('High', 'max'),
    Yearly_Low=('Low', 'min')
).reset_index()

# Calculate Yearly Returns (Percentage)
yearly_returns = combined_data.groupby('Year').apply(
    lambda x: ((x.iloc[-1]['Close'] - x.iloc[0]['Close']) / x.iloc[0]['Close']) * 100
).reset_index(name='Yearly_Return_Percentage')

# Calculate Yearly Points Change (Absolute)
yearly_points_change = combined_data.groupby('Year').apply(
    lambda x: (x.iloc[-1]['Close'] - x.iloc[0]['Close'])
).reset_index(name='Yearly_Points_Change')

# Calculate Yearly High to Low Percentage Change
yearly_high_low = combined_data.groupby('Year').apply(
    lambda x: ((x['High'].max() - x['Low'].min()) / x['Low'].min()) * 100
).reset_index(name='Yearly_High_to_Low_Change_Percentage')

# Calculate Points Change from Start of the Year High and Low
yearly_high_low_points = combined_data.groupby('Year').apply(
    lambda x: (x['High'].max() - x['Low'].min())
).reset_index(name='Yearly_High_to_Low_Points_Change')

# Calculate Yearly Starting to Low Points Change (Negative)
yearly_start_low_points = combined_data.groupby('Year').apply(
    lambda x: (x['Low'].min() - x.iloc[0]['Close'])
).reset_index(name='Yearly_Starting_to_Low_Points_Change')

# Calculate Yearly Starting to Low Percentage Change (Negative)
yearly_start_low_percentage = combined_data.groupby('Year').apply(
    lambda x: ((x['Low'].min() - x.iloc[0]['Close']) / x.iloc[0]['Close']) * 100
).reset_index(name='Yearly_Starting_to_Low_Percentage_Change')

# Calculate Yearly Starting to High Points Change
yearly_start_high_points = combined_data.groupby('Year').apply(
    lambda x: (x['High'].max() - x.iloc[0]['Close'])
).reset_index(name='Yearly_Starting_to_High_Points_Change')

# Calculate Yearly Starting to High Percentage Change
yearly_start_high_percentage = combined_data.groupby('Year').apply(
    lambda x: ((x['High'].max() - x.iloc[0]['Close']) / x.iloc[0]['Close']) * 100
).reset_index(name='Yearly_Starting_to_High_Percentage_Change')

# Merge the dataframes
yearly_data = pd.merge(yearly_data, yearly_returns, on='Year')
yearly_data = pd.merge(yearly_data, yearly_points_change, on='Year')
yearly_data = pd.merge(yearly_data, yearly_high_low, on='Year')
yearly_data = pd.merge(yearly_data, yearly_high_low_points, on='Year')
yearly_data = pd.merge(yearly_data, yearly_start_low_points, on='Year')
yearly_data = pd.merge(yearly_data, yearly_start_low_percentage, on='Year')
yearly_data = pd.merge(yearly_data, yearly_start_high_points, on='Year')
yearly_data = pd.merge(yearly_data, yearly_start_high_percentage, on='Year')

# Display the yearly data
print("\nYearly NIFTY 50 Returns, Points Change, and High-to-Low Change:")
print(yearly_data)

# Save the yearly data to a CSV file
yearly_csv_filename = os.path.join("/Users/sudheer/Documents/GitHub/nifty", "NIFTY50_Yearly_Data.csv")
yearly_data.to_csv(yearly_csv_filename, index=False)
print(f"\nYearly NIFTY 50 data saved locally as {yearly_csv_filename}")