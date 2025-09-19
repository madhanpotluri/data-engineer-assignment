#!/usr/bin/env python3
"""
IoT Data Simulator
Generates realistic IoT temperature data every 5 seconds
"""

import pandas as pd
import time
import random
import os
from datetime import datetime, timezone, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IoTSimulator:
    def __init__(self, output_dir="data/landing_zone", interval_seconds=5):
        self.output_dir = output_dir
        self.interval_seconds = interval_seconds
        self.utc_plus_4 = timezone(timedelta(hours=4))
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Device configurations
        self.devices = [
            {"id": "device_001", "location": "indoor", "room": "living_room"},
            {"id": "device_002", "location": "indoor", "room": "bedroom"},
            {"id": "device_003", "location": "indoor", "room": "kitchen"},
            {"id": "device_004", "location": "outdoor", "room": "garden"},
            {"id": "device_005", "location": "outdoor", "room": "patio"},
            {"id": "device_006", "location": "indoor", "room": "office"},
            {"id": "device_007", "location": "outdoor", "room": "garage"},
            {"id": "device_008", "location": "indoor", "room": "basement"}
        ]
        
        # Temperature ranges by location
        self.temp_ranges = {
            "indoor": {"min": 18, "max": 28, "base": 22},
            "outdoor": {"min": 5, "max": 35, "base": 20}
        }
        
        logger.info(f"🚀 IoT Simulator initialized")
        logger.info(f"📁 Output directory: {output_dir}")
        logger.info(f"⏱️  Interval: {interval_seconds} seconds")
        logger.info(f"🌡️  Devices: {len(self.devices)}")

    def generate_temperature_reading(self, device):
        """Generate a realistic temperature reading for a device"""
        location = device["location"]
        temp_range = self.temp_ranges[location]
        
        # Base temperature with some variation
        base_temp = temp_range["base"]
        variation = random.uniform(-3, 3)
        
        # Time-based variation (colder at night, warmer during day)
        current_hour = datetime.now(self.utc_plus_4).hour
        if 6 <= current_hour <= 18:  # Daytime
            time_variation = random.uniform(0, 2)
        else:  # Nighttime
            time_variation = random.uniform(-2, 0)
        
        # Random noise
        noise = random.uniform(-1, 1)
        
        # Calculate final temperature
        temperature = base_temp + variation + time_variation + noise
        
        # Ensure temperature is within realistic bounds
        temperature = max(temp_range["min"], min(temp_range["max"], temperature))
        
        return round(temperature, 2)

    def generate_single_record(self):
        """Generate a single IoT temperature record"""
        # Select random device
        device = random.choice(self.devices)
        
        # Generate timestamp (UTC+4)
        timestamp = datetime.now(self.utc_plus_4)
        
        # Generate temperature reading
        temperature = self.generate_temperature_reading(device)
        
        # Generate unique ID in the format of the original data
        record_id = f"__export__.temp_log_{random.randint(100000, 999999)}_{random.randint(10000000, 99999999):x}"
        
        # Convert location to match original format
        location_mapping = {
            "indoor": "In",
            "outdoor": "Out"
        }
        
        # Create record matching original format
        record = {
            "id": record_id,
            "room_id/id": "Room Admin",
            "noted_date": timestamp.strftime("%d-%m-%Y %H:%M"),
            "temp": int(temperature),  # Original data uses integer temperatures
            "out/in": location_mapping[device["location"]],
            "room": device["room"]  # Add room column for pipeline compatibility
        }
        
        return record

    def save_record(self, record):
        """Save a single record to CSV file"""
        # Create filename with timestamp
        timestamp = datetime.now(self.utc_plus_4)
        filename = f"iot_data_{timestamp.strftime('%Y%m%d_%H%M%S')}.csv"
        filepath = os.path.join(self.output_dir, filename)
        
        # Create DataFrame and save
        df = pd.DataFrame([record])
        df.to_csv(filepath, index=False)
        
        logger.info(f"💾 Saved record: {filename} - ID: {record['id']}, Temp: {record['temp']}°C, Location: {record['out/in']}")
        return filepath

    def run_simulation(self, duration_minutes=None):
        """Run the IoT simulation"""
        logger.info("🎯 Starting IoT data simulation...")
        logger.info("Press Ctrl+C to stop")
        
        start_time = time.time()
        record_count = 0
        
        try:
            while True:
                # Generate and save record
                record = self.generate_single_record()
                filepath = self.save_record(record)
                record_count += 1
                
                # Check if duration limit reached
                if duration_minutes:
                    elapsed_minutes = (time.time() - start_time) / 60
                    if elapsed_minutes >= duration_minutes:
                        logger.info(f"⏰ Simulation completed after {duration_minutes} minutes")
                        break
                
                # Wait for next interval
                time.sleep(self.interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("🛑 Simulation stopped by user")
        
        elapsed_time = time.time() - start_time
        logger.info(f"📊 Simulation Summary:")
        logger.info(f"   Records generated: {record_count}")
        logger.info(f"   Duration: {elapsed_time:.1f} seconds")
        logger.info(f"   Average interval: {elapsed_time/record_count:.1f} seconds per record")

def main():
    """Main function to run the simulator"""
    import argparse
    
    parser = argparse.ArgumentParser(description="IoT Data Simulator")
    parser.add_argument("--output-dir", default="data/landing_zone", help="Output directory for CSV files")
    parser.add_argument("--interval", type=int, default=5, help="Interval between records in seconds")
    parser.add_argument("--duration", type=int, help="Simulation duration in minutes (optional)")
    
    args = parser.parse_args()
    
    # Create and run simulator
    simulator = IoTSimulator(
        output_dir=args.output_dir,
        interval_seconds=args.interval
    )
    
    simulator.run_simulation(duration_minutes=args.duration)

if __name__ == "__main__":
    main()

