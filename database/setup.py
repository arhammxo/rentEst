import sqlite3
import pandas as pd
import os
import sys
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_setup.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Database configuration
DB_FILE = 'investment_properties.db'
CSV_FILE = 'investment_analysis_results.csv'

def setup_database():
    """
    Set up the SQLite database with investment property data from CSV.
    Creates tables, imports data, and adds necessary indices.
    """
    start_time = datetime.now()
    logger.info(f"Starting database setup at {start_time}")
    
    # Check if CSV file exists
    if not os.path.exists(CSV_FILE):
        logger.error(f"CSV file not found: {CSV_FILE}")
        sys.exit(1)
    
    # Connect to SQLite database (creates if it doesn't exist)
    try:
        conn = sqlite3.connect(DB_FILE)
        logger.info(f"Connected to database: {DB_FILE}")
    except sqlite3.Error as e:
        logger.error(f"SQLite connection error: {e}")
        sys.exit(1)
    
    try:
        # Read CSV file
        logger.info(f"Reading CSV file: {CSV_FILE}")
        df = pd.read_csv(CSV_FILE)
        
        # Log basic data stats
        logger.info(f"CSV contains {len(df)} rows and {len(df.columns)} columns")
        
        # Basic data cleaning
        logger.info("Cleaning data...")
        
        # Replace NaN with appropriate values based on column type
        numeric_cols = df.select_dtypes(include=['number']).columns
        string_cols = df.select_dtypes(include=['object']).columns
        
        # Fill numeric columns with 0
        df[numeric_cols] = df[numeric_cols].fillna(0)
        
        # Fill string columns with empty string
        df[string_cols] = df[string_cols].fillna('')
        
        # Convert specific columns to appropriate types
        if 'zip_code' in df.columns:
            df['zip_code'] = df['zip_code'].astype(int)
        
        # Remove any duplicate properties based on property_id
        if 'property_id' in df.columns:
            original_count = len(df)
            df = df.drop_duplicates(subset=['property_id'])
            dupes_removed = original_count - len(df)
            if dupes_removed > 0:
                logger.info(f"Removed {dupes_removed} duplicate properties")
        
        # Create main properties table
        logger.info("Creating properties table...")
        df.to_sql('properties', conn, if_exists='replace', index=False)
        
        # Create indices for faster querying
        logger.info("Creating database indices...")
        cursor = conn.cursor()
        
        # Create index on property_id for lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_property_id ON properties(property_id)')
        
        # Create indices for common search fields
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_city ON properties(city)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_zip_code ON properties(zip_code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_state ON properties(state)')
        
        # Create indices for sorting by investment metrics
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cap_rate ON properties(cap_rate)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cash_yield ON properties(cash_yield)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_irr ON properties(irr)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cash_on_cash ON properties(cash_on_cash)')
        
        # Create spatial index for location-based queries
        if 'latitude' in df.columns and 'longitude' in df.columns:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_location ON properties(latitude, longitude)')
        
        # Create index for price range filtering
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_list_price ON properties(list_price)')
        
        # Create indexes for property characteristics
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_beds ON properties(beds)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_baths ON properties(full_baths, half_baths)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sqft ON properties(sqft)')
        
        # Commit changes
        conn.commit()
        
        # Create materialized views for common queries
        logger.info("Creating materialized views...")
        
        # View for top investment properties by cap rate
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS view_top_cap_rate AS
        SELECT 
            property_id, full_street_line, city, state, zip_code,
            beds, full_baths, half_baths, sqft, list_price, 
            zori_monthly_rent, cap_rate, cash_yield, irr, cash_on_cash
        FROM properties
        WHERE cap_rate > 0
        ORDER BY cap_rate DESC
        LIMIT 1000
        ''')
        
        # View for top cash flow properties
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS view_top_cash_flow AS
        SELECT 
            property_id, full_street_line, city, state, zip_code,
            beds, full_baths, half_baths, sqft, list_price,
            zori_monthly_rent, cap_rate, cash_yield, irr, cash_on_cash,
            lcf_year1
        FROM properties
        WHERE lcf_year1 > 0
        ORDER BY lcf_year1 DESC
        LIMIT 1000
        ''')
        
        # View for top IRR properties
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS view_top_irr AS
        SELECT 
            property_id, full_street_line, city, state, zip_code,
            beds, full_baths, half_baths, sqft, list_price,
            zori_monthly_rent, cap_rate, cash_yield, irr, cash_on_cash
        FROM properties
        WHERE irr > 0
        ORDER BY irr DESC
        LIMIT 1000
        ''')
        
        # Create a lookup table for cities
        cursor.execute('''
        CREATE TABLE city_lookup AS
        SELECT DISTINCT city, state, COUNT(*) as property_count
        FROM properties
        GROUP BY city, state
        ORDER BY state, city
        ''')
        
        # Create a lookup table for zip codes
        cursor.execute('''
        CREATE TABLE zipcode_lookup AS
        SELECT DISTINCT zip_code, city, state, COUNT(*) as property_count
        FROM properties
        GROUP BY zip_code, city, state
        ORDER BY state, city, zip_code
        ''')
        
        # Commit all changes
        conn.commit()
        
        # Verify data was inserted properly
        cursor.execute("SELECT COUNT(*) FROM properties")
        count = cursor.fetchone()[0]
        logger.info(f"Successfully imported {count} properties into the database")
        
        # Verify spatial data
        if 'latitude' in df.columns and 'longitude' in df.columns:
            cursor.execute("SELECT COUNT(*) FROM properties WHERE latitude != 0 AND longitude != 0")
            spatial_count = cursor.fetchone()[0]
            logger.info(f"{spatial_count} properties have valid geospatial coordinates")
        
        # Close connection
        conn.close()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Database setup completed successfully in {duration:.2f} seconds")
        
        # Return summary info
        return {
            "total_properties": count,
            "database_file": DB_FILE,
            "setup_duration_seconds": duration
        }
        
    except Exception as e:
        logger.error(f"Error during database setup: {str(e)}")
        conn.close()
        raise

def create_derived_tables():
    """
    Create additional tables with summarized data for analytics.
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        logger.info("Creating derived tables for analytics...")
        
        # Create table with market statistics by city
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_stats_by_city AS
        SELECT 
            city, 
            state,
            COUNT(*) as property_count,
            AVG(list_price) as avg_price,
            MIN(list_price) as min_price,
            MAX(list_price) as max_price,
            AVG(zori_monthly_rent) as avg_rent,
            AVG(cap_rate) as avg_cap_rate,
            AVG(cash_yield) as avg_cash_yield,
            AVG(irr) as avg_irr,
            AVG(price_per_sqft) as avg_price_per_sqft
        FROM properties
        GROUP BY city, state
        HAVING COUNT(*) >= 5
        ORDER BY state, city
        ''')
        
        # Create table with market statistics by zip code
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_stats_by_zipcode AS
        SELECT 
            zip_code,
            city, 
            state,
            COUNT(*) as property_count,
            AVG(list_price) as avg_price,
            MIN(list_price) as min_price,
            MAX(list_price) as max_price,
            AVG(zori_monthly_rent) as avg_rent,
            AVG(cap_rate) as avg_cap_rate,
            AVG(cash_yield) as avg_cash_yield,
            AVG(irr) as avg_irr,
            AVG(price_per_sqft) as avg_price_per_sqft
        FROM properties
        GROUP BY zip_code, city, state
        HAVING COUNT(*) >= 3
        ORDER BY state, city, zip_code
        ''')
        
        # Create table with property type statistics
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stats_by_property_type AS
        SELECT 
            style as property_type,
            COUNT(*) as property_count,
            AVG(list_price) as avg_price,
            AVG(zori_monthly_rent) as avg_rent,
            AVG(cap_rate) as avg_cap_rate,
            AVG(cash_yield) as avg_cash_yield,
            AVG(irr) as avg_irr
        FROM properties
        WHERE style IS NOT NULL AND style != ''
        GROUP BY style
        HAVING COUNT(*) >= 5
        ORDER BY property_count DESC
        ''')
        
        # Commit changes
        conn.commit()
        
        # Log results
        cursor.execute("SELECT COUNT(*) FROM market_stats_by_city")
        city_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM market_stats_by_zipcode")
        zip_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM stats_by_property_type")
        type_count = cursor.fetchone()[0]
        
        logger.info(f"Created analytics tables: {city_count} cities, {zip_count} zip codes, {type_count} property types")
        
        conn.close()
        return {
            "cities_analyzed": city_count,
            "zipcodes_analyzed": zip_count,
            "property_types_analyzed": type_count
        }
        
    except Exception as e:
        logger.error(f"Error creating derived tables: {str(e)}")
        raise

def validate_database():
    """
    Run validation checks on the database to ensure data integrity.
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Check for null property IDs
        cursor.execute("SELECT COUNT(*) FROM properties WHERE property_id IS NULL OR property_id = 0")
        null_ids = cursor.fetchone()[0]
        
        # Check for negative metrics
        cursor.execute("SELECT COUNT(*) FROM properties WHERE cap_rate < 0 OR cash_yield < 0")
        negative_metrics = cursor.fetchone()[0]
        
        # Check for unrealistic values
        cursor.execute("SELECT COUNT(*) FROM properties WHERE cap_rate > 30") # Cap rates above 30% are suspicious
        high_cap_rates = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM properties WHERE irr > 50") # IRRs above 50% are suspicious
        high_irrs = cursor.fetchone()[0]
        
        # Log validation results
        logger.info("=== Database Validation Results ===")
        logger.info(f"Properties with null IDs: {null_ids}")
        logger.info(f"Properties with negative metrics: {negative_metrics}")
        logger.info(f"Properties with suspiciously high cap rates (>30%): {high_cap_rates}")
        logger.info(f"Properties with suspiciously high IRRs (>50%): {high_irrs}")
        
        # Check for data integrity
        if null_ids > 0 or negative_metrics > 0:
            logger.warning("Database contains potentially problematic data - review logs")
        else:
            logger.info("Database passed basic validation checks")
        
        conn.close()
        
        return {
            "null_ids": null_ids,
            "negative_metrics": negative_metrics,
            "high_cap_rates": high_cap_rates,
            "high_irrs": high_irrs,
            "validation_passed": (null_ids == 0 and negative_metrics == 0)
        }
        
    except Exception as e:
        logger.error(f"Error during database validation: {str(e)}")
        raise

if __name__ == "__main__":
    # Check if database already exists
    db_exists = os.path.exists(DB_FILE)
    if db_exists:
        logger.warning(f"Database {DB_FILE} already exists. It will be overwritten.")
        confirm = input("Continue? (y/n): ")
        if confirm.lower() != 'y':
            logger.info("Database setup cancelled by user")
            sys.exit(0)
    
    try:
        # Step 1: Create and populate main database
        setup_results = setup_database()
        
        # Step 2: Create derived analytical tables
        derived_results = create_derived_tables()
        
        # Step 3: Validate database
        validation_results = validate_database()
        
        # Summarize results
        logger.info("=== Database Setup Summary ===")
        logger.info(f"Total properties imported: {setup_results['total_properties']}")
        logger.info(f"Setup completed in {setup_results['setup_duration_seconds']:.2f} seconds")
        logger.info(f"Cities analyzed: {derived_results['cities_analyzed']}")
        logger.info(f"Zip codes analyzed: {derived_results['zipcodes_analyzed']}")
        logger.info(f"Database validation: {'PASSED' if validation_results['validation_passed'] else 'FAILED'}")
        
        print("\nDatabase setup complete!")
        print(f"- {setup_results['total_properties']} properties imported")
        print(f"- {derived_results['cities_analyzed']} cities with detailed market stats")
        print(f"- {derived_results['zipcodes_analyzed']} zip codes with detailed market stats")
        print(f"\nDatabase file: {DB_FILE}")
        
    except Exception as e:
        logger.error(f"Database setup failed: {str(e)}")
        print(f"\nError: Database setup failed - {str(e)}")
        sys.exit(1)