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

# Constants from the problem statement
G18 = 7.500 / 100  # 7.5% interest rate
G19 = 15           # 15 year loan term
K40_values = [1, 2, 3, 4, 5]

input_file = 'cash_yield.csv'
output_file = 'mortgage.csv'

with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
     open(output_file, 'w', newline='', encoding='utf-8') as outfile:

    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames + ['mpp'] + ['mortgage_ending_balance']
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    
    for row in reader:
        try:
            # Get cash_equity value, default to 0 if missing/invalid
            cash_equity = float(row.get('cash_equity', 0) or 0)
            
            # Calculate cumulative principal sum
            total = sum(
                calculate_cumprinc_for_period(K40, G18, G19, cash_equity)
                for K40 in K40_values
            )

            # Calculate ending balance
            ending_balance = cash_equity - total
            
            row['mpp'] = round(total, 2)
            row['mortgage_ending_balance'] = round(ending_balance, 2)
        except (ValueError, TypeError):
            row['mpp'] = 0.0
            row['mortgage_ending_balance'] = 0.0
        
        writer.writerow(row)

print("Processing complete. Output saved to", output_file) 