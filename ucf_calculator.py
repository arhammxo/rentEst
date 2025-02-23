import csv
import numpy as np
import numpy_financial as npf

def cumprinc(rate, nper, pv, start_period, end_period):
    """Calculate cumulative principal paid over a period."""
    monthly_rate = rate / 12
    periods = nper * 12
    schedule = np.zeros(periods)
    payment = npf.pmt(monthly_rate, periods, pv)
    
    for period in range(start_period-1, end_period):
        interest_payment = pv * monthly_rate
        principal_payment = payment - interest_payment
        schedule[period] = principal_payment
        pv -= principal_payment
    
    return np.sum(schedule[start_period-1:end_period])

def calculate_cumprinc_for_period(K40, G18, G19, K30):
    start_period = (K40 - 1) * 12 + 1
    end_period = K40 * 12
    return -cumprinc(G18, G19, K30, start_period, end_period)

# Mortgage calculation constants
G18 = 7.500 / 100  # 7.5% interest rate
G19 = 15           # 15 year loan term
K40_values = [1, 2, 3, 4, 5]

input_file = 'cash_yield.csv'
output_file = 'final.csv'

with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
     open(output_file, 'w', newline='', encoding='utf-8') as outfile:

    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames + ['ucf_year1', 'ucf_year2', 'ucf_year3', 'ucf_year4', 'ucf_year5', 
                                      'lmp', 'lcf_year1', 'lcf_year2', 'lcf_year3', 'lcf_year4', 'lcf_year5', 
                                      'mpp', 'mortgage_ending_balance',
                                      'eb_cash', 'eev', 'exit_value', 'cash_on_cash', 'irr'
                                      ]
    
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

        # Calculate mortgage balance directly
        try:
            cash_equity = float(row.get('cash_equity', 0))
            total = sum(
                calculate_cumprinc_for_period(K40, G18, G19, cash_equity)
                for K40 in K40_values
            )
            ending_balance = cash_equity - total
            row['mpp'] = round(total, 2)
            row['mortgage_ending_balance'] = round(ending_balance, 2)
        except (ValueError, TypeError):
            row['mpp'] = 0.0
            row['mortgage_ending_balance'] = 0.0

        # Calculate EEV and Exit Value
        try:
            noi_year5 = float(row.get('noi_year5', 0))
            cap_rate = float(row.get('cap_rate', 0)) / 100  # Convert percentage to decimal
            row['eev'] = round(noi_year5 / cap_rate, 4) if cap_rate != 0 else 0.0
        except (ValueError, KeyError):
            row['eev'] = 0.0

        try:
            row['exit_value'] = round(row['eev'] - row['mortgage_ending_balance'] + row['eb_cash'], 4)
        except KeyError:
            row['exit_value'] = 0.0

        # Calculate Cash-on-Cash and IRR
        try:
            exit_value = float(row.get('exit_value', 0))
            cash_equity = float(row.get('cash_equity', 0))
            row['cash_on_cash'] = round(exit_value / cash_equity, 4) if cash_equity != 0 else 0.0
        except (ValueError, KeyError, TypeError):
            row['cash_on_cash'] = 0.0

        try:
            coc = float(row['cash_on_cash'])
            row['irr'] = round((coc ** (1/5)) - 1, 4) if coc != 0 else 0.0
        except (ValueError, KeyError, TypeError):
            row['irr'] = 0.0

        writer.writerow(row) 