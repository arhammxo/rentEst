import csv
import math
import numpy as np
import numpy_financial as npf
from datetime import datetime
import os.path
import sys

# =====================================================================
# GLOBAL CONSTANTS AND CONFIGURATION
# =====================================================================

# Input and output file paths
PROPERTY_DATA_FILE = 'for_sale_20250220_0113.csv'
ZILLOW_RENT_DATA_FILE = 'zillow_rent_data.csv'
OUTPUT_FINAL_FILE = 'investment_analysis_results.csv'

# Temporary files for intermediate steps
TEMP_ZORI_ESTIMATES = 'temp_zori_estimates.csv'
TEMP_CASH_FLOW = 'temp_cash_flow.csv'

# Neighborhood quality factors based on ZIP codes
# Higher scores = better neighborhoods = higher growth potential, lower risk
NEIGHBORHOOD_QUALITY = {
    # NYC premium neighborhoods
    '10001': 0.9, '10011': 0.95, '10014': 0.95, '10016': 0.9, '10017': 0.9, '10018': 0.9, '10019': 0.92,
    '10021': 0.95, '10022': 0.95, '10023': 0.95, '10024': 0.95, '10025': 0.9, '10028': 0.95, '10075': 0.95,
    # Brooklyn premium neighborhoods
    '11201': 0.9, '11215': 0.88, '11217': 0.88, '11231': 0.85, '11238': 0.85,
    # Default for others
    'default': 0.75
}

# Property type characteristic modifiers
PROPERTY_TYPE_MODIFIERS = {
    # For rental adjustments
    'rent': {
        'Condo': 1.15,      # Premium for amenities and newer condition
        'Co-op': 0.95,      # Discount for restrictions and often older buildings
        'Single Family': 1.05,
        'Multi Family': 0.90, # Per unit discount for multi-family
        'Townhouse': 1.10,   # Premium for privacy and space
        'Luxury': 1.25,      # Premium segment
        'default': 1.0
    },
    # For growth rate adjustments
    'growth': {
        'Condo': 1.05,
        'Co-op': 0.95,
        'Single Family': 1.02,
        'Multi Family': 1.08,
        'Townhouse': 1.04,
        'default': 1.0
    }
}

# Base mortgage rates by price ranges
BASE_RATES = {
    'under_250k': 8.000,  # Higher rates for lower-priced properties (potentially higher risk)
    '250k_500k': 7.750,
    '500k_750k': 7.500,
    '750k_1m': 7.250,
    'over_1m': 7.000,   # Better rates for luxury properties
}

# Loan term options by price and property type
LOAN_TERMS = {
    'under_500k': 15,     # 15-year terms for lower values
    '500k_750k': 20,      # 20-year terms for mid-range
    'over_750k': 25,      # 25-year terms for higher values
}

# =====================================================================
# ZORI DATA PROCESSING FUNCTIONS
# =====================================================================

def load_zori_data():
    """
    Load and process Zillow Observed Rent Index (ZORI) data.
    Returns dictionaries with rent values, growth rates, and seasonality data by zip code.
    """
    print(f"Loading ZORI data from {ZILLOW_RENT_DATA_FILE}...")
    zori_by_zip = {}
    growth_rates_by_zip = {}
    seasonality_patterns = {}
    state_avg_rents = {}
    
    with open(ZILLOW_RENT_DATA_FILE, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        date_columns = [col for col in reader.fieldnames if col.startswith('20')]
        sorted_date_columns = sorted(date_columns)
        
        # Get latest date and historical comparison dates
        latest_date = sorted_date_columns[-1]
        one_year_ago_date = sorted_date_columns[-13]  # 12 months back
        five_years_ago_date = sorted_date_columns[-61]  # 5 years back
        
        # Process each zip code row
        for row in reader:
            try:
                zip_code = str(int(float(row['RegionName'])))
                state = row['State']
                
                # Get latest rent value
                latest_rent = float(row[latest_date]) if row[latest_date] else None
                
                # Calculate 1-year growth rate
                one_year_ago_rent = float(row[one_year_ago_date]) if row[one_year_ago_date] else None
                one_year_growth = ((latest_rent / one_year_ago_rent) - 1) * 100 if latest_rent and one_year_ago_rent else None
                
                # Calculate 5-year CAGR
                five_years_ago_rent = float(row[five_years_ago_date]) if row[five_years_ago_date] else None
                five_year_cagr = (math.pow(latest_rent / five_years_ago_rent, 1/5) - 1) * 100 if latest_rent and five_years_ago_rent else None
                
                # Calculate seasonal patterns (month-over-month changes)
                monthly_patterns = {}
                last_24_months = sorted_date_columns[-24:]
                for i in range(1, len(last_24_months)):
                    current_month = last_24_months[i]
                    prev_month = last_24_months[i-1]
                    month_num = int(current_month.split('-')[1])
                    
                    current_rent = float(row[current_month]) if row[current_month] else None
                    prev_rent = float(row[prev_month]) if row[prev_month] else None
                    
                    if current_rent and prev_rent:
                        change_pct = ((current_rent / prev_rent) - 1) * 100
                        if month_num not in monthly_patterns:
                            monthly_patterns[month_num] = []
                        monthly_patterns[month_num].append(change_pct)
                
                # Store the data
                if latest_rent:
                    zori_by_zip[zip_code] = latest_rent
                    
                    growth_rates_by_zip[zip_code] = {
                        'one_year': one_year_growth if one_year_growth else 0,
                        'five_year_cagr': five_year_cagr if five_year_cagr else 0
                    }
                    
                    seasonality_patterns[zip_code] = monthly_patterns
                    
                    # Track state averages
                    if state not in state_avg_rents:
                        state_avg_rents[state] = {'sum': 0, 'count': 0}
                    state_avg_rents[state]['sum'] += latest_rent
                    state_avg_rents[state]['count'] += 1
                    
            except (ValueError, TypeError, ZeroDivisionError):
                continue
    
    # Calculate average monthly seasonality across all zip codes
    avg_seasonality = {}
    for month in range(1, 13):
        all_changes = []
        for zip_code, patterns in seasonality_patterns.items():
            if month in patterns:
                all_changes.extend(patterns[month])
        
        if all_changes:
            avg_seasonality[month] = sum(all_changes) / len(all_changes)
        else:
            avg_seasonality[month] = 0
            
    # Calculate state average rents
    state_averages = {}
    for state, data in state_avg_rents.items():
        if data['count'] > 0:
            state_averages[state] = data['sum'] / data['count']
    
    print(f"Loaded ZORI data for {len(zori_by_zip)} zip codes across {len(state_averages)} states")
    return zori_by_zip, growth_rates_by_zip, avg_seasonality, state_averages

def find_closest_zip_with_data(target_zip, zori_by_zip):
    """
    Find the closest zip code that has ZORI data.
    Returns the original zip if it exists in the data, otherwise finds closest available zip.
    """
    if target_zip in zori_by_zip:
        return target_zip
        
    # If the zip code isn't found, find the closest one
    try:
        target_zip_int = int(target_zip)
        closest_zip = None
        min_distance = float('inf')
        
        for zip_code in zori_by_zip.keys():
            try:
                zip_int = int(zip_code)
                distance = abs(zip_int - target_zip_int)
                
                if distance < min_distance:
                    min_distance = distance
                    closest_zip = zip_code
            except ValueError:
                continue
                
        return closest_zip
    except ValueError:
        # If we can't convert to int, return None
        return None

# =====================================================================
# PROPERTY CHARACTERISTIC ADJUSTMENT FUNCTIONS
# =====================================================================

def calculate_bed_bath_factor(beds, baths):
    """
    Calculate an adjustment factor based on bedroom and bathroom count.
    Returns a multiplier reflecting the rental premium/discount for the configuration.
    """
    bed_value = 0
    bath_value = 0
    
    # Non-linear bedroom value
    if beds is not None:
        if beds == 0:  # Studio
            bed_value = 0.85
        elif beds == 1:
            bed_value = 1.0  # Base case
        elif beds == 2:
            bed_value = 1.2
        elif beds == 3:
            bed_value = 1.35
        elif beds >= 4:
            bed_value = 1.45  # Diminishing returns after 4 bedrooms
        else:
            bed_value = 1.0  # Default
    else:
        bed_value = 1.0
        
    # Bathroom value
    if baths is not None:
        if baths < 1:
            bath_value = 0.9
        elif baths == 1:
            bath_value = 1.0  # Base case
        elif baths == 1.5:
            bath_value = 1.05
        elif baths == 2:
            bath_value = 1.1
        elif baths <= 3:
            bath_value = 1.2
        else:
            bath_value = 1.25
    else:
        bath_value = 1.0
    
    # Combine with premium for bed/bath ratio
    if beds and baths and beds > 0:
        ratio = baths / beds
        if ratio >= 1.5:  # Premium for high bath-to-bed ratio
            return (bed_value + bath_value) / 2 * 1.05
    
    return (bed_value + bath_value) / 2

def calculate_size_factor(sqft):
    """
    Calculate an adjustment factor based on property size.
    Returns a multiplier reflecting the rental premium/discount for the size.
    """
    if not sqft or sqft <= 0:
        return 1.0
        
    # Diminishing returns for larger units
    if sqft < 500:
        return 0.85
    elif sqft < 750:
        return 0.95
    elif sqft < 1000:
        return 1.0  # Base case
    elif sqft < 1500:
        return 1.1
    elif sqft < 2000:
        return 1.2
    elif sqft < 3000:
        return 1.3
    else:
        return 1.4

def calculate_condition_factor(year_built):
    """
    Calculate an adjustment factor based on property age and condition.
    Returns a multiplier reflecting the rental premium/discount for the age.
    """
    if not year_built or year_built <= 0:
        return 1.0
        
    current_year = datetime.now().year
    age = current_year - year_built
    
    if age < 3:
        return 1.15  # New construction premium
    elif age < 10:
        return 1.1
    elif age < 20:
        return 1.05
    elif age < 40:
        return 1.0  # Base case
    elif age < 75:
        return 0.95
    else:
        return 0.9

def calculate_amenity_score(row):
    """
    Calculate an amenity score based on property description and features.
    Returns a multiplier reflecting the rental premium for amenities.
    """
    score = 1.0
    
    # Check for doorman/luxury building indicators in text description
    description = str(row.get('text', '')).lower()
    luxury_keywords = ['doorman', 'concierge', 'luxury', 'high-end', 'renovated', 
                      'marble', 'stainless', 'premium', 'upscale', 'views', 'pool',
                      'gym', 'fitness', 'modern', 'updated', 'granite', 'new appliances']
    
    # Count luxury keywords in description
    keyword_count = sum(1 for keyword in luxury_keywords if keyword in description)
    score += min(0.2, 0.01 * keyword_count)  # Cap at 20% boost
    
    # Check for specific amenities
    if row.get('parking_garage') and float(row.get('parking_garage', 0) or 0) > 0:
        score += 0.05
        
    # Check HOA fee as proxy for amenities
    try:
        hoa_fee = float(row.get('hoa_fee', 0) or 0)
        if hoa_fee > 500:
            score += 0.05
        elif hoa_fee > 1000:
            score += 0.1
    except (ValueError, TypeError):
        pass
        
    return score

def get_neighborhood_factor(zip_code):
    """
    Get the neighborhood quality factor for a given zip code.
    Returns a factor between 0.75 and 0.95 representing neighborhood quality.
    """
    return NEIGHBORHOOD_QUALITY.get(zip_code, NEIGHBORHOOD_QUALITY['default'])

def calculate_down_payment_pct(list_price, neighborhood_factor):
    """
    Calculate the appropriate down payment percentage based on property price and neighborhood.
    Returns a percentage between 30% and 60%.
    """
    # Base down payment percentage by price range
    if list_price < 200000:
        down_payment_pct = 0.35  # 35% down for cheaper properties
    elif list_price < 500000:
        down_payment_pct = 0.40  # 40% down for lower mid-range
    elif list_price < 750000:
        down_payment_pct = 0.45  # 45% down for upper mid-range
    elif list_price < 1000000:
        down_payment_pct = 0.50  # 50% down for higher end
    else:
        down_payment_pct = 0.55  # 55% down for luxury properties
    
    # Additional adjustment based on neighborhood quality
    # Better neighborhoods can get more favorable financing
    down_payment_pct -= (neighborhood_factor - 0.75) * 0.05
    
    # Ensure down payment is between 30% and 60%
    return min(0.60, max(0.30, down_payment_pct))

def determine_mortgage_terms(list_price, neighborhood_factor):
    """
    Determine appropriate mortgage interest rate and term based on property characteristics.
    Returns interest rate and loan term.
    """
    # Base rate by price range
    if list_price < 250000:
        interest_rate = BASE_RATES['under_250k']
    elif list_price < 500000:
        interest_rate = BASE_RATES['250k_500k']
    elif list_price < 750000:
        interest_rate = BASE_RATES['500k_750k']
    elif list_price < 1000000:
        interest_rate = BASE_RATES['750k_1m']
    else:
        interest_rate = BASE_RATES['over_1m']
    
    # Adjustment based on neighborhood quality (up to 0.5% reduction for prime areas)
    neighborhood_adjustment = (neighborhood_factor - 0.75) * 1.0
    interest_rate -= neighborhood_adjustment
    
    # Cap the range of possible interest rates
    interest_rate = min(9.0, max(6.0, interest_rate))
    
    # Determine loan term based on property price
    if list_price < 500000:
        loan_term = LOAN_TERMS['under_500k']
    elif list_price < 750000:
        loan_term = LOAN_TERMS['500k_750k']
    else:
        loan_term = LOAN_TERMS['over_750k']
    
    return interest_rate, loan_term

def calculate_growth_rate(zip_code, neighborhood_factor, property_style, growth_rates_by_zip):
    """
    Calculate customized growth rate based on location, property type, and historical data.
    Returns annual growth rate percentage.
    """
    # Get growth data for this zip code or use default
    growth_data = growth_rates_by_zip.get(zip_code, {'one_year': 3.0, 'five_year_cagr': 3.0})
    five_year_cagr = growth_data['five_year_cagr']
    
    # Get property type modifier
    property_type_modifier = PROPERTY_TYPE_MODIFIERS['growth'].get(property_style, PROPERTY_TYPE_MODIFIERS['growth']['default'])
    
    # Base growth rate (between 2-6%)
    base_growth_rate = min(6.0, max(2.0, five_year_cagr))
    
    # Apply neighborhood and property type adjustments
    neighborhood_adjustment = neighborhood_factor * 0.02  # Up to 2% additional based on neighborhood
    property_adjustment = (property_type_modifier - 1.0) * 0.01  # Up to 1% additional based on property type
    
    # Final growth rate (capped between 2% and 10%)
    growth_rate = min(10.0, max(2.0, base_growth_rate + neighborhood_adjustment * 100 + property_adjustment * 100))
    
    return growth_rate / 100  # Return as decimal

def calculate_exit_cap_rate(entry_cap_rate, growth_rate, neighborhood_factor):
    """
    Calculate the exit cap rate, typically higher than entry cap rate to reflect future risk.
    Returns exit cap rate as a decimal.
    """
    # Convert entry cap rate to decimal if it's in percentage form
    if entry_cap_rate > 1:
        entry_cap_rate = entry_cap_rate / 100
    
    # Base cap rate expansion
    base_expansion = 0.01  # 1% base expansion
    
    # Reduce expansion for better neighborhoods and higher growth properties
    neighborhood_adjustment = (neighborhood_factor - 0.75) * 0.015
    growth_adjustment = (growth_rate - 0.03) * 0.5
    
    cap_rate_expansion = max(0, base_expansion - neighborhood_adjustment - growth_adjustment)
    exit_cap_rate = entry_cap_rate + cap_rate_expansion
    
    # Ensure exit cap rate is reasonable (min 4%, max 10%)
    return min(0.10, max(0.04, exit_cap_rate))

# =====================================================================
# RENTAL INCOME ESTIMATION FUNCTIONS
# =====================================================================

def estimate_rental_income(row, zori_by_zip, growth_rates_by_zip, avg_seasonality, state_averages):
    """
    Estimate rental income using ZORI data and property characteristics.
    Returns monthly rent, annual rent, growth rate, 5-year projections, and gross rent multiplier.
    """
    try:
        # Get the property zip code
        zip_code = str(int(float(row.get('zip_code', 0) or 0)))
        state = row.get('state')
        
        # Find ZORI data for this zip code
        if zip_code not in zori_by_zip:
            zip_code = find_closest_zip_with_data(zip_code, zori_by_zip)
            
        if not zip_code:
            # Fall back to state average if available
            if state in state_averages:
                base_zori_rent = state_averages[state]
            else:
                return None, None, None, None, None
        else:
            # Get base ZORI rent for this zip
            base_zori_rent = zori_by_zip[zip_code]
        
        # Get property characteristics
        beds = float(row.get('beds', 0) or 0)
        full_baths = float(row.get('full_baths', 0) or 0)
        half_baths = float(row.get('half_baths', 0) or 0)
        baths = full_baths + (0.5 * half_baths)
        sqft = float(row.get('sqft', 0) or 0)
        year_built = float(row.get('year_built', 0) or 0)
        property_style = str(row.get('style', '')).strip() or 'default'
        
        # Calculate adjustment factors
        bed_bath_factor = calculate_bed_bath_factor(beds, baths)
        size_factor = calculate_size_factor(sqft)
        condition_factor = calculate_condition_factor(year_built)
        amenity_factor = calculate_amenity_score(row)
        property_type_factor = PROPERTY_TYPE_MODIFIERS['rent'].get(property_style, PROPERTY_TYPE_MODIFIERS['rent']['default'])
        
        # Current month for seasonality
        current_month = datetime.now().month
        seasonality_factor = 1 + (avg_seasonality.get(current_month, 0) / 100)
        
        # Special case for multi-family properties
        if property_style == 'Multi Family' and beds >= 4:
            # Assume it's a multi-unit building with separate rentable units
            units = max(2, beds // 2)  # Estimate number of units
            adjusted_rent = base_zori_rent * units * 0.85  # 15% discount for multi-unit
        else:
            # Weighted adjustment calculation
            adjustment_factor = (
                bed_bath_factor * 0.35 +  # 35% weight to beds/baths
                size_factor * 0.25 +      # 25% weight to size
                condition_factor * 0.15 +  # 15% weight to condition/age
                amenity_factor * 0.15 +   # 15% weight to amenities
                property_type_factor * 0.10  # 10% weight to property type
            )
            
            # Apply seasonality
            adjustment_factor *= seasonality_factor
            
            # Calculate adjusted rent
            adjusted_rent = base_zori_rent * adjustment_factor
        
        # Get neighborhood factor for growth rate calculation
        neighborhood_factor = get_neighborhood_factor(zip_code)
        
        # Calculate property-specific growth rate
        growth_rate = calculate_growth_rate(zip_code, neighborhood_factor, property_style, growth_rates_by_zip)
        
        # Calculate 5-year rent projections
        rent_projections = [adjusted_rent]
        for year in range(1, 5):
            projected_rent = rent_projections[-1] * (1 + growth_rate)
            rent_projections.append(projected_rent)
        
        # Calculate annual rent
        annual_rent = adjusted_rent * 12
        
        # Calculate gross rent multiplier (price to annual rent)
        try:
            list_price = float(row.get('list_price', 0) or 0)
            grm = list_price / annual_rent if annual_rent > 0 else 0
        except (ValueError, ZeroDivisionError):
            grm = 0
            
        return adjusted_rent, annual_rent, growth_rate * 100, rent_projections, grm
        
    except Exception as e:
        print(f"Error estimating rental income: {e}")
        return None, None, None, None, None

# =====================================================================
# INVESTMENT METRICS CALCULATION FUNCTIONS
# =====================================================================

def calculate_cash_flow_metrics(row, is_zori_based=True):
    """
    Calculate cash flow metrics based on rental income and property characteristics.
    Returns a dictionary with all calculated metrics.
    """
    metrics = {}
    
    try:
        # Get property values
        list_price = float(row.get('list_price', 0) or 0)
        if list_price <= 0:
            return metrics
            
        # Determine which rental income to use
        if is_zori_based and row.get('zori_monthly_rent'):
            monthly_rent = float(row.get('zori_monthly_rent', 0))
            annual_rent = float(row.get('zori_annual_rent', 0))
            growth_rate = float(row.get('zori_growth_rate', 3.0)) / 100  # Convert to decimal
        else:
            # Fallback to original PTR calculation if ZORI data isn't available
            ptr_value = float(row.get('PTR', 0) or 0)
            if ptr_value <= 0:
                return metrics
                
            sqft = float(row.get('sqft', 0) or 0)
            price_per_sqft = float(row.get('price_per_sqft', 0) or 0)
            
            # Calculate BRE
            if sqft > 0 and price_per_sqft > 0:
                bre = sqft * price_per_sqft
            else:
                bre = list_price
                
            # Calculate adjustment factor
            beds = float(row.get('beds', 0) or 0)
            full_baths = float(row.get('full_baths', 0) or 0)
            af = 1 + (0.05 + 0.02 * beds + 0.01 * full_baths + 0.05 * int(sqft > 2000))
            
            # Calculate FRE
            monthly_rent = ptr_value * bre * af
            annual_rent = monthly_rent * 12
            growth_rate = 0.03  # Default 3% growth rate
        
        metrics['monthly_rent'] = monthly_rent
        metrics['annual_rent'] = annual_rent
        
        # Get property characteristics for calculations
        zip_code = str(int(float(row.get('zip_code', 0) or 0)))
        neighborhood_factor = get_neighborhood_factor(zip_code)
        property_style = str(row.get('style', '')).strip() or 'default'
        
        # Calculate expenses
        tax = float(row.get('tax', 0) or 0)
        if tax == 0:
            tax = 0.01 * list_price  # Estimate tax at 1% of list price
        metrics['tax_used'] = tax
        
        hoa_fee = float(row.get('hoa_fee', 0) or 0)
        if hoa_fee == 0:
            hoa_fee = (0.0015 * list_price) / 12  # Estimate HOA at 0.15% of list price annually
        metrics['hoa_fee_used'] = hoa_fee
        
        # Calculate variable down payment percentage
        down_payment_pct = calculate_down_payment_pct(list_price, neighborhood_factor)
        metrics['down_payment_pct'] = down_payment_pct
        
        # Calculate mortgage terms
        interest_rate, loan_term = determine_mortgage_terms(list_price, neighborhood_factor)
        metrics['interest_rate'] = interest_rate
        metrics['loan_term'] = loan_term
        
        # Calculate transaction costs and cash equity
        transaction_cost = 0.01 * list_price
        cash_equity = down_payment_pct * (list_price + transaction_cost)
        metrics['transaction_cost'] = transaction_cost
        metrics['cash_equity'] = cash_equity
        
        # Calculate NOI for 5 years
        current_rent = annual_rent
        for year in range(1, 6):
            noi = current_rent - (hoa_fee * 12)
            metrics[f'noi_year{year}'] = noi
            current_rent *= (1 + growth_rate)  # Apply growth rate
        
        # Calculate cap rate
        metrics['cap_rate'] = (metrics['noi_year1'] / list_price) * 100 if list_price > 0 else 0
        
        # Calculate unlevered cash flow (UCF)
        for year in range(1, 6):
            metrics[f'ucf_year{year}'] = metrics[f'noi_year{year}'] - tax
        
        metrics['ucf'] = metrics['ucf_year1']  # First year UCF
        
        # Calculate cash yield
        metrics['cash_yield'] = (metrics['ucf'] / cash_equity) * 100 if cash_equity > 0 else 0
        
        return metrics
        
    except Exception as e:
        print(f"Error calculating cash flow metrics: {e}")
        return metrics

def calculate_mortgage_metrics(row, metrics):
    """
    Calculate mortgage-related metrics including principal payments, loan balance, and debt service.
    Updates the metrics dictionary with these values.
    """
    try:
        cash_equity = metrics.get('cash_equity', 0)
        list_price = float(row.get('list_price', 0) or 0)
        transaction_cost = metrics.get('transaction_cost', 0)
        down_payment_pct = metrics.get('down_payment_pct', 0.5)
        
        # Calculate loan amount
        total_cost = list_price + transaction_cost
        loan_amount = total_cost * (1 - down_payment_pct)
        metrics['loan_amount'] = loan_amount
        
        # Get interest rate and term
        interest_rate = metrics.get('interest_rate', 7.5) / 100  # Convert to decimal
        loan_term = metrics.get('loan_term', 15)
        
        # Calculate monthly payment
        monthly_rate = interest_rate / 12
        total_periods = loan_term * 12
        if monthly_rate > 0 and total_periods > 0:
            monthly_payment = npf.pmt(monthly_rate, total_periods, -loan_amount)
            metrics['monthly_payment'] = monthly_payment
            metrics['annual_debt_service'] = monthly_payment * 12
        else:
            metrics['monthly_payment'] = 0
            metrics['annual_debt_service'] = 0
            
        # Calculate principal payments for years 1-5
        total_principal = 0
        remaining_balance = loan_amount
        
        for year in range(1, 6):
            year_start_period = (year - 1) * 12 + 1
            year_end_period = year * 12
            
            # Calculate principal paid this year
            principal_paid = 0
            for period in range(year_start_period, year_end_period + 1):
                interest_payment = remaining_balance * monthly_rate
                principal_payment = monthly_payment - interest_payment
                principal_paid += principal_payment
                remaining_balance -= principal_payment
            
            metrics[f'principal_paid_year{year}'] = principal_paid
            total_principal += principal_paid
            
            # Store ending balance at each year
            metrics[f'loan_balance_year{year}'] = remaining_balance
            
            # Ensure ucf_year{year} exists before calculating lcf
            ucf_key = f'ucf_year{year}'
            if ucf_key in metrics:
                # Calculate levered cash flow (LCF)
                metrics[f'lcf_year{year}'] = metrics[ucf_key] - metrics['annual_debt_service']
            else:
                metrics[f'lcf_year{year}'] = 0  # Default if UCF is missing
        
        # Store total principal payments and final mortgage balance
        metrics['total_principal_paid'] = total_principal
        metrics['final_loan_balance'] = remaining_balance
        
        # Calculate accumulated cash flow (ensure lcf values exist)
        lcf_sum = 0
        for year in range(1, 6):
            lcf_key = f'lcf_year{year}'
            if lcf_key in metrics:
                lcf_sum += metrics[lcf_key]
        
        metrics['accumulated_cash_flow'] = lcf_sum
        
        return metrics
    
    except Exception as e:
        print(f"Error calculating mortgage metrics: {e}")
        return metrics

def calculate_investment_returns(row, metrics):
    """
    Calculate investment return metrics including exit value, cash-on-cash return, and IRR.
    Updates the metrics dictionary with these values.
    """
    try:
        # Calculate exit cap rate
        cap_rate = metrics.get('cap_rate', 0) / 100  # Convert to decimal
        growth_rate = float(row.get('zori_growth_rate', 3.0)) / 100
        
        # Get neighborhood factor
        zip_code = str(int(float(row.get('zip_code', 0) or 0)))
        neighborhood_factor = get_neighborhood_factor(zip_code)
        
        # Calculate exit cap rate
        exit_cap_rate = calculate_exit_cap_rate(cap_rate, growth_rate, neighborhood_factor)
        metrics['exit_cap_rate'] = exit_cap_rate * 100  # Store as percentage
        
        # Calculate exit value using year 5 NOI and exit cap rate
        noi_year5 = metrics.get('noi_year5', 0)
        if exit_cap_rate > 0:
            exit_value = noi_year5 / exit_cap_rate
            metrics['exit_value'] = exit_value
        else:
            metrics['exit_value'] = 0
        
        # Calculate equity at exit
        final_loan_balance = metrics.get('final_loan_balance', 0)
        accumulated_cash_flow = metrics.get('accumulated_cash_flow', 0)
        equity_at_exit = metrics['exit_value'] - final_loan_balance + accumulated_cash_flow
        metrics['equity_at_exit'] = equity_at_exit
        
        # Calculate cash-on-cash return
        cash_equity = metrics.get('cash_equity', 0)
        if cash_equity > 0:
            cash_on_cash = equity_at_exit / cash_equity
            metrics['cash_on_cash'] = cash_on_cash
            
            # Calculate IRR
            if cash_on_cash > 0:
                irr = (cash_on_cash ** (1/5)) - 1  # 5-year holding period
                metrics['irr'] = irr * 100  # Store as percentage
            else:
                metrics['irr'] = 0
        else:
            metrics['cash_on_cash'] = 0
            metrics['irr'] = 0
        
        return metrics
    
    except Exception as e:
        print(f"Error calculating investment returns: {e}")
        return metrics

# =====================================================================
# MAIN PROCESSING FUNCTIONS
# =====================================================================

def process_rental_estimates():
    """
    Process properties and calculate ZORI-based rental estimates.
    Saves results to a temporary file.
    """
    print("Starting rental income estimation...")
    
    # Load ZORI data
    zori_by_zip, growth_rates_by_zip, avg_seasonality, state_averages = load_zori_data()
    
    # Process property data
    with open(PROPERTY_DATA_FILE, 'r', newline='', encoding='utf-8') as infile, \
         open(TEMP_ZORI_ESTIMATES, 'w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        
        # Add new fields for ZORI estimates
        fieldnames = reader.fieldnames + [
            'zori_monthly_rent',
            'zori_annual_rent',
            'zori_growth_rate',
            'zori_rent_year1',
            'zori_rent_year2',
            'zori_rent_year3',
            'zori_rent_year4',
            'zori_rent_year5',
            'gross_rent_multiplier'
        ]
        
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        count = 0
        for row in reader:
            # Calculate ZORI-based rental estimate
            monthly_rent, annual_rent, growth_rate, projections, grm = estimate_rental_income(
                row, zori_by_zip, growth_rates_by_zip, avg_seasonality, state_averages)
            
            # Add the values to the row
            if monthly_rent:
                row['zori_monthly_rent'] = round(monthly_rent, 2)
                row['zori_annual_rent'] = round(annual_rent, 2)
                row['zori_growth_rate'] = round(growth_rate, 2)
                row['gross_rent_multiplier'] = round(grm, 2) if grm else None
                
                # Add 5-year projections
                for i, proj in enumerate(projections):
                    row[f'zori_rent_year{i+1}'] = round(proj, 2)
            else:
                row['zori_monthly_rent'] = None
                row['zori_annual_rent'] = None
                row['zori_growth_rate'] = None
                row['zori_rent_year1'] = None
                row['zori_rent_year2'] = None
                row['zori_rent_year3'] = None
                row['zori_rent_year4'] = None
                row['zori_rent_year5'] = None
                row['gross_rent_multiplier'] = None
                
            writer.writerow(row)
            count += 1
            if count % 1000 == 0:
                print(f"Processed {count} properties...")
    
    print(f"Completed rental estimation for {count} properties")
    return count

def process_investment_metrics():
    """
    Process properties with rental estimates and calculate investment metrics.
    Saves results to a temporary file.
    """
    print("Starting investment metrics calculation...")
    
    # Process property data with rental estimates
    with open(TEMP_ZORI_ESTIMATES, 'r', newline='', encoding='utf-8') as infile, \
         open(TEMP_CASH_FLOW, 'w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        
        # Add new fields for investment metrics
        additional_fields = [
            'monthly_rent', 'annual_rent', 'tax_used', 'hoa_fee_used',
            'down_payment_pct', 'interest_rate', 'loan_term',
            'transaction_cost', 'cash_equity', 
            'noi_year1', 'noi_year2', 'noi_year3', 'noi_year4', 'noi_year5',
            'cap_rate', 'ucf', 'cash_yield',
            'ucf_year1', 'ucf_year2', 'ucf_year3', 'ucf_year4', 'ucf_year5'
        ]
        
        fieldnames = reader.fieldnames + additional_fields
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        count = 0
        for row in reader:
            # Calculate cash flow metrics
            metrics = calculate_cash_flow_metrics(row, is_zori_based=True)
            
            # Add metrics to the row
            for key, value in metrics.items():
                row[key] = round(value, 2) if isinstance(value, (int, float)) else value
                
            writer.writerow(row)
            count += 1
            if count % 1000 == 0:
                print(f"Processed {count} properties...")
    
    print(f"Completed cash flow metrics for {count} properties")
    return count

def process_final_metrics():
    """
    Process properties with cash flow metrics and calculate final investment returns.
    Saves results to the final output file.
    """
    print("Starting final investment returns calculation...")
    
    # Process property data with cash flow metrics
    with open(TEMP_CASH_FLOW, 'r', newline='', encoding='utf-8') as infile, \
         open(OUTPUT_FINAL_FILE, 'w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        
        # Add new fields for final metrics
        additional_fields = [
            'loan_amount', 'monthly_payment', 'annual_debt_service',
            'principal_paid_year1', 'principal_paid_year2', 'principal_paid_year3', 
            'principal_paid_year4', 'principal_paid_year5',
            'loan_balance_year1', 'loan_balance_year2', 'loan_balance_year3', 
            'loan_balance_year4', 'loan_balance_year5',
            'lcf_year1', 'lcf_year2', 'lcf_year3', 'lcf_year4', 'lcf_year5',
            'total_principal_paid', 'final_loan_balance', 'accumulated_cash_flow',
            'exit_cap_rate', 'exit_value', 'equity_at_exit', 'cash_on_cash', 'irr'
        ]
        
        fieldnames = reader.fieldnames + additional_fields
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        count = 0
        for row in reader:
            # Extract metrics to build the metrics dictionary
            metrics = {}
            # First, get all the UCF values
            for i in range(1, 6):
                key = f'ucf_year{i}'
                if key in row and row[key]:
                    try:
                        metrics[key] = float(row[key])
                    except (ValueError, TypeError):
                        metrics[key] = 0
                else:
                    metrics[key] = 0
                    
            # Now get all other metrics
            for field in reader.fieldnames:
                if field in ['monthly_rent', 'annual_rent', 'tax_used', 'hoa_fee_used', 
                           'down_payment_pct', 'interest_rate', 'loan_term', 
                           'transaction_cost', 'cash_equity', 'cap_rate', 'ucf', 'cash_yield',
                           'noi_year1', 'noi_year2', 'noi_year3', 'noi_year4', 'noi_year5']:
                    if field in row and row[field]:
                        try:
                            metrics[field] = float(row[field])
                        except (ValueError, TypeError):
                            metrics[field] = 0
                    else:
                        metrics[field] = 0
            
            # Calculate mortgage metrics
            metrics = calculate_mortgage_metrics(row, metrics)
            
            # Calculate investment returns
            metrics = calculate_investment_returns(row, metrics)
            
            # Add metrics to the row
            for key, value in metrics.items():
                row[key] = round(value, 2) if isinstance(value, (int, float)) else value
                
            writer.writerow(row)
            count += 1
            if count % 1000 == 0:
                print(f"Processed {count} properties...")
    
    print(f"Completed final investment metrics for {count} properties")
    return count

def clean_up_temp_files():
    """Remove temporary files."""
    try:
        if os.path.exists(TEMP_ZORI_ESTIMATES):
            os.remove(TEMP_ZORI_ESTIMATES)
        if os.path.exists(TEMP_CASH_FLOW):
            os.remove(TEMP_CASH_FLOW)
    except Exception as e:
        print(f"Error cleaning up temporary files: {e}")

def main():
    """Main function to run the complete investment analysis workflow."""
    print("\n======== REAL ESTATE INVESTMENT ANALYSIS WORKFLOW ========\n")
    print(f"Input property data: {PROPERTY_DATA_FILE}")
    print(f"Input ZORI data: {ZILLOW_RENT_DATA_FILE}")
    print(f"Output file: {OUTPUT_FINAL_FILE}\n")
    
    try:
        # Step 1: Calculate rental income estimates
        process_rental_estimates()
        
        # Step 2: Calculate cash flow metrics
        process_investment_metrics()
        
        # Step 3: Calculate final investment returns
        process_final_metrics()
        
        # Clean up
        clean_up_temp_files()
        
        print("\n========= ANALYSIS COMPLETE =========")
        print(f"Results saved to {OUTPUT_FINAL_FILE}")
        
    except Exception as e:
        print(f"Error in main workflow: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()