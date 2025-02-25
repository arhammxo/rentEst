from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional
import sqlite3
from pydantic import BaseModel
import json

app = FastAPI(title="Real Estate Investment API")

# Database connection helper
def get_db_connection():
    conn = sqlite3.connect('investment_properties.db')
    conn.row_factory = sqlite3.Row  # This enables column access by name
    return conn

# Property model
class Property(BaseModel):
    property_id: int
    full_street_line: str
    city: str
    state: str
    zip_code: int
    beds: float
    full_baths: float
    sqft: float
    list_price: float
    zori_monthly_rent: float
    cap_rate: float
    cash_yield: float
    irr: float
    cash_on_cash: float

# City lookup model
class City(BaseModel):
    city: str
    state: str
    property_count: int

@app.get("/cities/", response_model=List[City])
async def get_cities(state: Optional[str] = None):
    """Get list of cities with available properties"""
    conn = get_db_connection()
    try:
        if state:
            cursor = conn.execute(
                "SELECT city, state, property_count FROM city_lookup WHERE state = ? ORDER BY city",
                (state,)
            )
        else:
            cursor = conn.execute(
                "SELECT city, state, property_count FROM city_lookup ORDER BY state, city"
            )
        
        cities = [dict(row) for row in cursor.fetchall()]
        return cities
    finally:
        conn.close()

@app.get("/properties/city/{city}", response_model=List[Property])
async def get_properties_by_city(
    city: str,
    state: Optional[str] = None,
    min_cap_rate: float = 0,
    max_price: Optional[int] = None,
    sort_by: str = "cap_rate",
    limit: int = 50
):
    """Get properties by city name"""
    conn = get_db_connection()
    try:
        query = """
        SELECT 
            property_id, full_street_line, city, state, zip_code,
            beds, full_baths, sqft, list_price, zori_monthly_rent,
            cap_rate, cash_yield, irr, cash_on_cash
        FROM properties 
        WHERE city = ?
        """
        params = [city]
        
        if state:
            query += " AND state = ?"
            params.append(state)
        
        if min_cap_rate > 0:
            query += " AND cap_rate >= ?"
            params.append(min_cap_rate)
            
        if max_price:
            query += " AND list_price <= ?"
            params.append(max_price)
        
        # Validate sort field
        valid_sort_fields = ["cap_rate", "cash_yield", "irr", "list_price"]
        if sort_by not in valid_sort_fields:
            sort_by = "cap_rate"
        
        query += f" ORDER BY {sort_by} DESC LIMIT ?"
        params.append(limit)
        
        cursor = conn.execute(query, params)
        properties = [dict(row) for row in cursor.fetchall()]
        
        if not properties:
            if state:
                raise HTTPException(status_code=404, detail=f"No properties found in {city}, {state}")
            else:
                raise HTTPException(status_code=404, detail=f"No properties found in {city}")
        
        return properties
    finally:
        conn.close()

@app.get("/properties/zipcode/{zipcode}", response_model=List[Property])
async def get_properties_by_zipcode(
    zipcode: int,
    min_cap_rate: float = 0,
    max_price: Optional[int] = None,
    sort_by: str = "cap_rate",
    limit: int = 50
):
    """Get properties by ZIP code"""
    conn = get_db_connection()
    try:
        query = """
        SELECT 
            property_id, full_street_line, city, state, zip_code,
            beds, full_baths, sqft, list_price, zori_monthly_rent,
            cap_rate, cash_yield, irr, cash_on_cash
        FROM properties 
        WHERE zip_code = ?
        """
        params = [zipcode]
        
        if min_cap_rate > 0:
            query += " AND cap_rate >= ?"
            params.append(min_cap_rate)
            
        if max_price:
            query += " AND list_price <= ?"
            params.append(max_price)
        
        # Validate sort field
        valid_sort_fields = ["cap_rate", "cash_yield", "irr", "list_price"]
        if sort_by not in valid_sort_fields:
            sort_by = "cap_rate"
        
        query += f" ORDER BY {sort_by} DESC LIMIT ?"
        params.append(limit)
        
        cursor = conn.execute(query, params)
        properties = [dict(row) for row in cursor.fetchall()]
        
        if not properties:
            raise HTTPException(status_code=404, detail=f"No properties found with ZIP code {zipcode}")
        
        return properties
    finally:
        conn.close()

@app.get("/market-stats/city/{city}")
async def get_city_stats(city: str, state: Optional[str] = None):
    """Get market statistics for a specific city"""
    conn = get_db_connection()
    try:
        if state:
            cursor = conn.execute(
                "SELECT * FROM market_stats_by_city WHERE city = ? AND state = ?",
                (city, state)
            )
        else:
            cursor = conn.execute(
                "SELECT * FROM market_stats_by_city WHERE city = ?",
                (city,)
            )
        
        stats = cursor.fetchone()
        if not stats:
            if state:
                raise HTTPException(status_code=404, detail=f"No statistics found for {city}, {state}")
            else:
                raise HTTPException(status_code=404, detail=f"No statistics found for {city}")
        
        return dict(stats)
    finally:
        conn.close()

@app.get("/market-stats/zipcode/{zipcode}")
async def get_zipcode_stats(zipcode: int):
    """Get market statistics for a specific ZIP code"""
    conn = get_db_connection()
    try:
        cursor = conn.execute(
            "SELECT * FROM market_stats_by_zipcode WHERE zip_code = ?",
            (zipcode,)
        )
        
        stats = cursor.fetchone()
        if not stats:
            raise HTTPException(status_code=404, detail=f"No statistics found for ZIP code {zipcode}")
        
        return dict(stats)
    finally:
        conn.close()