import csv

# Conversion factor for price-to-rent ratio calculation
CONVERSION_FACTOR = 1 / 1014888

def clean_zillow_data(input_file, output_file):
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        # Read and write headers
        headers = next(reader)
        new_headers = headers[:9] + ['LastValue', 'PTR']
        writer.writerow(new_headers)
        
        # Process each row
        for row in reader:
            # Get static columns
            identifiers = row[:9]
            
            # Find last non-empty value in time series columns
            date_values = [v.strip() for v in row[9:] if v.strip()]
            last_value = date_values[-1] if date_values else ''
            
            # Calculate price-to-rent ratio
            ptr = str(float(last_value) * CONVERSION_FACTOR) if last_value else ''
            
            # Write cleaned row with new ptr field
            writer.writerow(identifiers + [last_value, ptr])

if __name__ == '__main__':
    clean_zillow_data('zillow_rent_data.csv', 'cleaned_zillow_data.csv')