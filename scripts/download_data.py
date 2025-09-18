#!/usr/bin/env python3
"""
IoT Temperature Data Download Script
Downloads sample IoT temperature data for the pipeline
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import argparse

def generate_sample_iot_data(num_records=10000, output_file="iot_temperature_data.csv"):
    """
    Generate realistic IoT temperature sensor data
    """
    print(f"Generating {num_records} IoT temperature records...")
    
    # Device IDs (simulating different IoT devices)
    device_ids = [f"device_{i:03d}" for i in range(1, 21)]  # 20 devices
    
    # Generate timestamps (last 30 days)
    end_time = datetime.now()
    start_time = end_time - timedelta(days=30)
    
    # Create base temperature data
    data = []
    
    for device_id in device_ids:
        # Each device has different characteristics
        base_temp = random.uniform(15, 25)  # Base temperature for this device
        temp_variance = random.uniform(2, 8)  # Temperature variance
        
        # Generate records for this device
        device_records = []
        current_time = start_time
        
        while current_time <= end_time:
            # Simulate temperature readings every 5-15 minutes
            interval = random.randint(5, 15)
            current_time += timedelta(minutes=interval)
            
            if current_time > end_time:
                break
                
            # Generate temperature with realistic patterns
            hour = current_time.hour
            
            # Day/night temperature variation
            if 6 <= hour <= 18:  # Daytime
                temp_multiplier = 1.0 + (hour - 12) * 0.05  # Warmer during day
            else:  # Nighttime
                temp_multiplier = 0.8 + (hour - 6) * 0.02  # Cooler at night
            
            # Add some randomness and anomalies
            if random.random() < 0.05:  # 5% chance of anomaly
                temperature = base_temp + random.uniform(-20, 20)  # Anomaly
            else:
                temperature = base_temp + (random.random() - 0.5) * temp_variance * temp_multiplier
            
            # Add some missing values (1% chance)
            if random.random() < 0.01:
                temperature = None
            
            device_records.append({
                'device_id': device_id,
                'timestamp': current_time.strftime('%Y-%m-%d %H:%M:%S'),
                'temperature': temperature,
                'humidity': random.uniform(30, 80) if temperature is not None else None,
                'location': f"zone_{random.randint(1, 5)}",
                'battery_level': random.uniform(20, 100)
            })
        
        data.extend(device_records)
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Shuffle the data
    df = df.sample(frac=1).reset_index(drop=True)
    
    # Take only the requested number of records
    df = df.head(num_records)
    
    # Save to CSV in current directory
    output_path = output_file
    df.to_csv(output_path, index=False)
    print(f"✅ Generated {len(df)} records and saved to {output_path}")
    
    # Print summary statistics
    print("\n📊 Data Summary:")
    print(f"   Total records: {len(df)}")
    print(f"   Unique devices: {df['device_id'].nunique()}")
    print(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"   Temperature range: {df['temperature'].min():.2f}°C to {df['temperature'].max():.2f}°C")
    print(f"   Missing values: {df['temperature'].isnull().sum()}")
    
    return output_path

def download_kaggle_data():
    """
    Download data from Kaggle using curl command
    """
    import subprocess
    import os
    
    print("Downloading data from Kaggle...")
    
    # Create landing zone directory if it doesn't exist
    landing_zone = "data/landing_zone"
    os.makedirs(landing_zone, exist_ok=True)
    
    try:
        # Use curl to download the dataset
        cmd = [
            "curl", "-L", "-o", f"{landing_zone}/temperature-readings-iot-devices.zip",
            "https://www.kaggle.com/api/v1/datasets/download/atulanandjha/temperature-readings-iot-devices"
        ]
        
        print("Executing download command...")
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # Extract the zip file
        import zipfile
        with zipfile.ZipFile(f"{landing_zone}/temperature-readings-iot-devices.zip", 'r') as zip_ref:
            zip_ref.extractall(landing_zone)
        
        # Remove the zip file
        os.remove(f"{landing_zone}/temperature-readings-iot-devices.zip")
        
        print(f"✅ Data downloaded and extracted to {landing_zone}/")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Download failed: {e}")
        print("Falling back to generated data...")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Falling back to generated data...")
        return False

def main():
    parser = argparse.ArgumentParser(description='Download IoT temperature data')
    parser.add_argument('--records', type=int, default=10000, help='Number of records to generate')
    parser.add_argument('--output', default='iot_temperature_data.csv', help='Output filename')
    parser.add_argument('--source', choices=['generate', 'kaggle'], default='kaggle', help='Data source')
    
    args = parser.parse_args()
    
    print("🌡️  IoT Temperature Data Downloader")
    print("=" * 50)
    
    if args.source == 'kaggle':
        success = download_kaggle_data()
        if not success:
            print("Falling back to generated data...")
            args.source = 'generate'
    
    if args.source == 'generate':
        output_path = generate_sample_iot_data(args.records, args.output)
        print(f"\n✅ Data ready at: {output_path}")
        print(f"\n📁 File created: {args.output}")
        
        print("\n🚀 Next steps:")
        print("   1. Check the data: ls data/landing_zone/")
        print("   2. Run the pipeline: docker compose up -d")
        print("   3. Check Airflow UI: http://localhost:8080")
        print("   4. Check Spark UI: http://localhost:8081")

if __name__ == "__main__":
    main()
