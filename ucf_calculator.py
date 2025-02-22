import csv

input_file = 'cash_yield.csv'
output_file = 'final.csv'

with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
     open(output_file, 'w', newline='', encoding='utf-8') as outfile:

    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames + ['ucf_year1', 'ucf_year2', 'ucf_year3', 'ucf_year4', 'ucf_year5', 
                                      'lmp', 'lcf_year1', 'lcf_year2', 'lcf_year3', 'lcf_year4', 'lcf_year5',
                                      'eb_cash', 'eev', 'exit_value', 'cash_on_cash', 'irr']
    
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    
    for row in reader:
        try:
            tax_used = float(row['tax_used'])
        except (ValueError, KeyError):
            tax_used = 0.0

        # Calculate UCF for each year
        for year in range(1, 6):
            noi_key = f'noi_year{year}'
            ucf_key = f'ucf_year{year}'
            
            try:
                noi_value = float(row.get(noi_key, 0))
                row[ucf_key] = round(noi_value - tax_used, 2)
            except (ValueError, KeyError):
                row[ucf_key] = 0.0

        # Calculate LMP from first year's UCF
        row['lmp'] = round(row['ucf_year1'] / 1.2, 2)

        # Calculate LCF for each year
        for year in range(1, 6):
            lcf_key = f'lcf_year{year}'
            row[lcf_key] = round(row[f'ucf_year{year}'] - row['lmp'], 2)

        # Calculate EB as sum of all LCF values
        lcf_values = [row[f'lcf_year{year}'] for year in range(1, 6)]
        row['eb_cash'] = round(sum(lcf_values), 2)

        # Load mortgage data for same property
        mortgage_balance = 0.0
        with open('mortgage.csv', 'r', encoding='utf-8') as mortgage_file:
            mortgage_reader = csv.DictReader(mortgage_file)
            for mortgage_row in mortgage_reader:
                if mortgage_row['property_id'] == row['property_id']:
                    mortgage_balance = float(mortgage_row.get('mortgage_ending_balance', 0))
                    break

        # Calculate EEV and Exit Value
        try:
            noi_year5 = float(row.get('noi_year5', 0))
            cap_rate = float(row.get('cap_rate', 0))
            row['eev'] = round(noi_year5 / cap_rate, 2) if cap_rate != 0 else 0.0
        except (ValueError, KeyError):
            row['eev'] = 0.0

        try:
            row['exit_value'] = round(row['eev'] - mortgage_balance + row['eb_cash'], 2)
        except KeyError:
            row['exit_value'] = 0.0

        # Calculate Cash-on-Cash and IRR
        try:
            cash_equity = float(row.get('cash_equity', 0))
            row['cash_on_cash'] = round(row['exit_value'] / cash_equity, 2) if cash_equity != 0 else 0.0
        except (ValueError, KeyError):
            row['cash_on_cash'] = 0.0

        try:
            row['irr'] = round(row['cash_on_cash'] ** -0.8, 2) if row['cash_on_cash'] != 0 else 0.0
        except (ValueError, KeyError):
            row['irr'] = 0.0

        writer.writerow(row) 