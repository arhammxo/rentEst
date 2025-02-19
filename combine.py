import pandas as pd

# Load both datasets
for_sale_df = pd.read_csv('for_sale_20250220_0113.csv')
zillow_df = pd.read_csv('cleaned_zillow_data.csv')

# Rename RegionName to zip_code for clarity in Zillow data
zillow_df = zillow_df.rename(columns={'RegionName': 'zip_code'})

# Create a dictionary mapping zip codes to PTR values from Zillow data
zip_ptr_mapping = zillow_df.set_index('zip_code')['PTR'].to_dict()  # Note uppercase PTR

# Map the PTR values to the for_sale listings based on zip code
for_sale_df['PTR'] = for_sale_df['zip_code'].map(zip_ptr_mapping)

# Save the updated CSV
for_sale_df.to_csv('updated_for_sale_with_ptr.csv', index=False)