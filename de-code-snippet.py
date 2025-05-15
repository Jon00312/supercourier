# SuperCourier - Mini ETL Pipeline
# Starter code for the Data Engineering mini-challenge

import sqlite3
import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
import random
import os

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('supercourier_mini_etl')

# Constants
DB_PATH = 'supercourier_mini.db'
WEATHER_PATH = 'weather_data.json'
OUTPUT_PATH = 'deliveries.csv'

# 1. FUNCTION TO GENERATE SQLITE DATABASE (you can modify as needed)
def create_sqlite_database():
    """
    Creates a simple SQLite database with a deliveries table
    """
    logger.info("Creating SQLite database...")
    
    # Remove database if it already exists
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create deliveries table
    cursor.execute('''
    CREATE TABLE deliveries (
        delivery_id INTEGER PRIMARY KEY,
        pickup_datetime TEXT,
        package_type TEXT,
        delivery_zone TEXT,
        recipient_id INTEGER
    )
    ''')
    
    # Available package types and delivery zones
    package_types = ['Small', 'Medium', 'Large', 'X-Large', 'Special']
    delivery_zones = ['Urban', 'Suburban', 'Rural', 'Industrial', 'Shopping Center']
    
    # Generate 1000 random deliveries
    deliveries = []
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)  # 3 months
    
    for i in range(1, 1001):
        # Random date within last 3 months
        timestamp = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        
        # Random selection of package type and zone
        package_type = random.choices(
            package_types, 
            weights=[25, 30, 20, 15, 10]  # Relative probabilities
        )[0]
        
        delivery_zone = random.choice(delivery_zones)
        
        # Add to list
        deliveries.append((
            i,  # delivery_id
            timestamp.strftime('%Y-%m-%d %H:%M:%S'),  # pickup_datetime
            package_type,
            delivery_zone,
            random.randint(1, 100)  # fictional recipient_id
        ))
    
    # Insert data
    cursor.executemany(
        'INSERT INTO deliveries VALUES (?, ?, ?, ?, ?)',
        deliveries
    )
    
    # Commit and close
    conn.commit()
    conn.close()
    
    logger.info(f"Database created with {len(deliveries)} deliveries")
    return True

# 2. FUNCTION TO GENERATE WEATHER DATA
def generate_weather_data():
    """
    Generates fictional weather data for the last 3 months
    """
    logger.info("Generating weather data...")
    
    conditions = ['Sunny', 'Cloudy', 'Rainy', 'Windy', 'Snowy', 'Foggy']
    weights = [30, 25, 20, 15, 5, 5]  # Relative probabilities
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    
    weather_data = {}
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        weather_data[date_str] = {}
        
        # For each day, generate weather for each hour
        for hour in range(24):
            # More continuity in conditions
            if hour > 0 and random.random() < 0.7:
                # 70% chance of keeping same condition as previous hour
                condition = weather_data[date_str].get(str(hour-1), 
                                                      random.choices(conditions, weights=weights)[0])
            else:
                condition = random.choices(conditions, weights=weights)[0]
            
            weather_data[date_str][str(hour)] = condition
        
        current_date += timedelta(days=1)
    
    # Save as JSON
    with open(WEATHER_PATH, 'w', encoding='utf-8') as f:
        json.dump(weather_data, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Weather data generated for period {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
    return weather_data

# 3. EXTRACTION FUNCTIONS
def extract_sqlite_data():
    """
    Extracts delivery data from SQLite database
    """
    logger.info("Extracting data from SQLite database...")
    
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM deliveries"
    df = pd.read_sql(query, conn)
    conn.close()
    
    logger.info(f"Extraction complete: {len(df)} records")
    return df

def load_weather_data():
    """
    Loads weather data from JSON file
    """
    logger.info("Loading weather data...")
    
    with open(WEATHER_PATH, 'r', encoding='utf-8') as f:
        weather_data = json.load(f)
    
    logger.info(f"Weather data loaded for {len(weather_data)} days")
    return weather_data

# 4. TRANSFORMATION FUNCTIONS

def enrich_with_weekday_and_hour(df):
    logger.info("Enriching with weekday and hour of the day...")

    # Convert date column to datetime
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])

    df['weekday'] = df['pickup_datetime'].dt.day_name()
    df['hour'] = df['pickup_datetime'].dt.hour
    return df

def enrich_with_random_distance_and_delivery_time(df):
    logger.info("Adding random distance and delivery time...")
    try:
        # Distance has to be simulated, so it will be between 1 and 100 km
        df['distance'] = [random.randint(1, 100) for _ in range(len(df))]
        # Base time is given
        base_time = 30 + df['distance'] * 0.8
        # Actual Delivery time has to be simulated, so it will be between -10% and +50% of the base time
        df['actual_delivery_time'] = round(base_time * np.random.uniform(0.9, 1.5, len(df))).astype(int)
        return df
    except Exception as e:
        logger.error(f"Error in enrich_with_random_distance_and_delivery_time: {e}")
        raise

def enrich_with_weather(df, weather_data):
    """
    Enriches the DataFrame with weather conditions
    """
    logger.info("Enriching with weather data...")
    
    # Convert date column to datetime
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    
    # Function to get weather for a given timestamp
    def get_weather(timestamp):
        date_str = timestamp.strftime('%Y-%m-%d')
        hour_str = str(timestamp.hour)
        
        try:
            return weather_data[date_str][hour_str]
        except KeyError:
            logger.warning(f"Missing weather for {date_str} {hour_str}")
            return None
    
    # Apply function to each row
    df['WeatherCondition'] = df['pickup_datetime'].apply(get_weather)
    return df

def enrich_with_status(df):
    # we use axis = 1 to apply the fonction on every line, instead of axis 0 which applies on every column
    df['status'] = df.apply(calculate_delivery_time, axis=1)
    return df

def calculate_delivery_time(row):

    # 1. Base theoretical time
    base_time = 30 + row['distance'] * 0.8  # en minutes

    # 2. Adjustment factors

    # Package type
    package_factors = {
        'Small': 1.0,
        'Medium': 1.2,
        'Large': 1.5,
        'X-Large': 2.0,
        'Special': 2.5
    }
    package_multiplier = package_factors.get(row['package_type'], 1.0)

    # Delivery zone
    zone_factors = {
        'Urban': 1.2,
        'Suburban': 1.0,
        'Rural': 1.3,
        'Industrial': 0.9,
        'Shopping Center': 1.4
    }
    zone_multiplier = zone_factors.get(row['delivery_zone'], 1.0)

    # Weather condition
    weather_factors = {
        'Sunny': 1.0,
        'Cloudy': 1.05,
        'Rainy': 1.2,
        'Snowy': 1.8,
        'Windy': 1.1,
        'Foggy': 1.3
    }
    weather_multiplier = weather_factors.get(row['WeatherCondition'], 1.0)

    # Peak hours
    if 7 <= row['hour'] <= 9:
        peak_multiplier = 1.3  # Morning
    elif 17 <= row['hour'] <= 19:
        peak_multiplier = 1.4  # Evening
    else:
        peak_multiplier = 1.0

    # Day of the week
    if row['weekday'] in ['Monday', 'Friday']:
        day_multiplier = 1.2
    elif row['weekday'] in ['Saturday', 'Sunday']:
        day_multiplier = 0.9
    else:
        day_multiplier = 1.0

    # Total adjusted theoretical time
    adjusted_time = base_time * package_multiplier * zone_multiplier * weather_multiplier * peak_multiplier * day_multiplier

    # Final delay threshold
    delay_threshold = adjusted_time * 1.2  # 20% margin
    return 'On-time' if delay_threshold > row['actual_delivery_time'] else 'Delayed'


def transform_data(df_deliveries, weather_data):
    """
    Main data transformation function
    To be completed by participants
    """
    logger.info("Transforming data...")
    # 1. Enrich with weekday and hour from dataframe timestamp
    df_deliveries = enrich_with_weekday_and_hour(df_deliveries)
    # 2. Enrich with weather data from another source
    df_deliveries = enrich_with_weather(df_deliveries, weather_data)
    # 3. Simulate distance and delivery time for this exercise
    df_deliveries = enrich_with_random_distance_and_delivery_time(df_deliveries)
    # 4. Determine status (on time/delayed) with a calculated threshold
    df_deliveries = enrich_with_status(df_deliveries)
    
    return df_deliveries  # Return transformed DataFrame

# 5. LOADING FUNCTION
def save_results(df):
    """
    Saves the final DataFrame to CSV
    """
    logger.info("Saving results...")
    try :
        # We rename and then sort and select the columns we need in specific order
        df = df.rename(columns={
        'delivery_id': 'Delivery_ID',
        'pickup_datetime': 'Pickup_DateTime',
        'weekday': 'Weekday',
        'hour': 'Hour',
        'package_type': 'Package_Type',
        'distance': 'Distance',
        'delivery_zone': 'Delivery_Zone',
        'WeatherCondition': 'Weather_Condition',
        'actual_delivery_time': 'Actual_Delivery_Time',
        'status': 'Status'
        })
        desired_order = [
        "Delivery_ID",
        "Pickup_DateTime",
        "Weekday",
        "Hour",
        "Package_Type",
        "Distance",
        "Delivery_Zone",
        "Weather_Condition",
        "Actual_Delivery_Time",
        "Status"
        ]

        df = df[desired_order]

        # Save to CSV
        df.to_csv(OUTPUT_PATH, index=False)
    
        logger.info(f"Results saved to {OUTPUT_PATH}")
        return True
    except Exception as e:
        logger.error(f"Error saving results: {e}")
        return False

# MAIN FUNCTION
def run_pipeline():
    """
    Runs the ETL pipeline end-to-end
    """
    try:
        logger.info("Starting SuperCourier ETL pipeline")
        
        # Step 1: Generate data sources
        create_sqlite_database()
        weather_data = generate_weather_data()
        
        # Step 2: Extraction
        df_deliveries = extract_sqlite_data()
        
        # Step 3: Transformation
        df_transformed = transform_data(df_deliveries, weather_data)
        
        # Step 4: Loading
        save_results(df_transformed)

        # Step 5: Stats
        on_time = df_transformed[df_transformed['status'] == 'On-time'].shape[0]
        delayed = df_transformed[df_transformed['status'] == 'Delayed'].shape[0]
        total = len(df_transformed)

        if total > 0:
            delay_ratio = delayed / total * 100
            ontime_ratio = on_time / total * 100
            logger.info(f"Delivery Status Summary: {on_time} On-time ({ontime_ratio:.1f}%) | {delayed} Delayed ({delay_ratio:.1f}%)")
        else:
            logger.warning("No data available to compute statistics.")
        

        logger.info("ETL pipeline completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error during pipeline execution: {str(e)}")
        return False

# Main entry point
if __name__ == "__main__":
    run_pipeline()
