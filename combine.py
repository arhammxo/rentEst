import pandas as pd

# Load both datasets
for_sale_df = pd.read_csv('for_sale_20250224_2014.csv')
zillow_df = pd.read_csv('cleaned_zillow_data.csv')

# Rename RegionName to zip_code for clarity in Zillow data
zillow_df = zillow_df.rename(columns={'RegionName': 'zip_code'})

# Create a dictionary mapping zip codes to PTR values from Zillow data
zip_ptr_mapping = zillow_df.set_index('zip_code')['PTR'].to_dict()  # Note uppercase PTR

# Prepare closest zip code matching
zillow_df['zip_int'] = zillow_df['zip_code'].astype(int)
zip_int_to_str = zillow_df.set_index('zip_int')['zip_code'].to_dict()
available_zips = list(zip_int_to_str.keys())

def find_closest_zip(missing_zip):
    try:
        missing_int = int(missing_zip)
        closest_zip_int = min(available_zips, key=lambda x: abs(x - missing_int))
        return zip_int_to_str[closest_zip_int]
    except ValueError:
        return None

# Map the PTR values to the for_sale listings based on zip code
for_sale_df['PTR'] = for_sale_df['zip_code'].map(zip_ptr_mapping)

# Fill missing PTR values with closest available zip code's PTR
missing_mask = for_sale_df['PTR'].isna()
for_sale_df.loc[missing_mask, 'PTR'] = for_sale_df.loc[missing_mask, 'zip_code'].apply(
    lambda z: zip_ptr_mapping.get(find_closest_zip(z), None)
)

# Save the updated CSV
for_sale_df.to_csv('updated_for_sale_with_ptr.csv', index=False)