import csv

input_file = 'updated_for_sale_with_ptr.csv'
output_file = 'cap_rate.csv'

with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
     open(output_file, 'w', newline='', encoding='utf-8') as outfile:

    reader = csv.DictReader(infile)
    # Add all new columns to headers
    fieldnames = reader.fieldnames + ['bre', 'af', 'fre_monthly', 'fre', 'tax_used', 'hoa_fee_used', 'noi', 'cap_rate']
    
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    
    for row in reader:
        # Calculate BRE and add to row
        try:
            sqft_str = row.get('sqft', '') or row.get('lot_sqft', '')
            price_per_sqft_str = row.get('price_per_sqft', '')
            
            # Check if we have all required values for calculation
            if not all([sqft_str, price_per_sqft_str]):
                raise ValueError("Missing required fields")
                
            sqft = float(sqft_str)
            price_per_sqft = float(price_per_sqft_str)
            
            # Only use calculation if values are positive
            if sqft > 0 and price_per_sqft > 0:
                row['bre'] = sqft * price_per_sqft
            else:
                raise ValueError("Invalid values")
                
        except (ValueError, KeyError) as e:
            # Fallback to list_price if any calculation component is missing/invalid
            row['bre'] = row.get('list_price', '0') or '0'  # Handle empty list_price
            # Ensure sqft has a default value for AF calculation
            sqft = float(
                (row.get('sqft') or row.get('lot_sqft') or '0')  # Chain fallbacks
            )

        # Add the AF column calculation
        row['af'] = 1 + (
            0.05 + 
            0.02 * float(row.get('beds') or '0') +  # Handle empty beds
            0.01 * float(row.get('full_baths') or '0') +  # Handle empty baths
            0.05 * int(sqft > 2000)  # Changed to native int conversion
        )
        
        # Add FRE monthly calculation with PTR validation
        try:
            ptr_value = float(row['PTR']) if row['PTR'] else 0.0
            bre_value = float(row['bre'])
            af_value = row['af']  # Already calculated as float
            row['fre_monthly'] = ptr_value * bre_value * af_value
        except (ValueError, KeyError):
            row['fre_monthly'] = 0.0
            row['fre'] = 0.0
            continue  # Skip to next row if PTR is invalid

        row['fre'] = row['fre_monthly'] * 12

        # Add NOI calculation
        try:
            fre_value = float(row['fre'])
            tax = float(row.get('tax') or '0')
            
            # New tax calculation when tax is 0
            if tax == 0:
                list_price = float(row.get('list_price', '0') or '0')
                if list_price > 0:
                    tax = 0.01 * list_price
            row['tax_used'] = tax  # Add tax value used
            
            hoa_fee = float(row.get('hoa_fee') or '0')
            
            # Update HOA fee to 2% of list_price if zero
            if hoa_fee == 0:
                list_price = float(row.get('list_price', '0') or '0')
                if list_price > 0:
                    hoa_fee = (0.0015 * list_price)/12
            row['hoa_fee_used'] = hoa_fee  # Add HOA fee value used

            row['noi'] = fre_value - (tax + (hoa_fee * 12))
            
            # Calculate cap rate
            list_price = float(row.get('list_price', '0') or '0')
            if list_price != 0:
                row['cap_rate'] = round((row['noi'] / list_price) * 100, 2)
            else:
                row['cap_rate'] = 0.0
        except (ValueError, KeyError):
            row['noi'] = 0
            row['cap_rate'] = 0.0
        
        writer.writerow(row)