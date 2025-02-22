import csv

input_file = 'cash_yield.csv'
output_file = 'zip_averages.csv'

# Data storage structure: {zip_code: {'sum': x, 'count': y}}
price_data = {}
rent_data = {}

with open(input_file, 'r', encoding='utf-8') as infile:
    reader = csv.DictReader(infile)
    
    for row in reader:
        zip_code = row.get('zip_code', '').strip()
        if not zip_code:
            continue
        
        # Process price per sqft
        try:
            price = float(row['price_per_sqft'])
            if price > 0:
                price_data.setdefault(zip_code, {'sum': 0.0, 'count': 0})
                price_data[zip_code]['sum'] += price
                price_data[zip_code]['count'] += 1
        except (ValueError, KeyError):
            pass
        
        # Process rent per sqft (using FRE which is annual rent)
        try:
            fre = float(row['fre'])
            sqft = float(row.get('sqft', 0) or row.get('lot_sqft', 0))
            
            if fre > 0 and sqft > 0:
                rent_per_sqft = fre / sqft  # Annual rent per sqft
                rent_data.setdefault(zip_code, {'sum': 0.0, 'count': 0})
                rent_data[zip_code]['sum'] += rent_per_sqft
                rent_data[zip_code]['count'] += 1
        except (ValueError, KeyError, ZeroDivisionError):
            pass

# Write results to CSV
with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    writer = csv.writer(outfile)
    writer.writerow(['zip_code', 'avg_price_per_sqft', 'avg_rent_per_sqft'])
    
    # Combine all unique zip codes from both datasets
    all_zips = set(price_data.keys()).union(set(rent_data.keys()))
    
    for zip_code in sorted(all_zips):
        # Calculate price average
        price_avg = price_data.get(zip_code, {'sum': 0, 'count': 0})
        price_value = price_avg['sum'] / price_avg['count'] if price_avg['count'] > 0 else 0
        
        # Calculate rent average
        rent_avg = rent_data.get(zip_code, {'sum': 0, 'count': 0})
        rent_value = rent_avg['sum'] / rent_avg['count'] if rent_avg['count'] > 0 else 0
        
        writer.writerow([
            zip_code,
            round(price_value, 2),
            round(rent_value, 2)
        ]) 