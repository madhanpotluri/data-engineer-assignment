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
    def __init__(self, output_dir="data/landing_zone", interval_seconds=5, duplicate_probability=0.3, anomaly_probability=0.15, missing_probability=0.1):
        self.output_dir = output_dir
        self.interval_seconds = interval_seconds
        self.duplicate_probability = duplicate_probability  # 30% chance of creating duplicates
        self.anomaly_probability = anomaly_probability  # 15% chance of creating anomalies
        self.missing_probability = missing_probability  # 10% chance of creating missing values
        self.utc_plus_4 = timezone(timedelta(hours=4))
        self.record_counter = 0
        self.duplicate_scenarios = []
        self.anomaly_scenarios = []
        self.missing_value_scenarios = []
        
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
        
        # Duplicate scenarios for testing
        self.duplicate_scenarios = [
            {"type": "retry", "description": "Network retry - same data, different timestamp"},
            {"type": "correction", "description": "Data correction - updated temperature"},
            {"type": "reprocessing", "description": "Reprocessing - same data, new batch"},
            {"type": "sensor_fault", "description": "Sensor fault - corrected reading"}
        ]
        
        # Anomaly scenarios for testing
        self.anomaly_scenarios = [
            {"type": "extreme_temp", "description": "Extreme temperature reading (sensor malfunction)"},
            {"type": "sudden_spike", "description": "Sudden temperature spike (equipment failure)"},
            {"type": "negative_temp", "description": "Negative temperature (sensor calibration issue)"},
            {"type": "impossible_value", "description": "Impossible temperature value (sensor error)"},
            {"type": "outlier_pattern", "description": "Statistical outlier (environmental anomaly)"}
        ]
        
        # Missing value scenarios for testing
        self.missing_value_scenarios = [
            {"type": "null_temp", "description": "Missing temperature value (sensor offline)"},
            {"type": "null_location", "description": "Missing location data (GPS failure)"},
            {"type": "null_timestamp", "description": "Missing timestamp (clock sync issue)"},
            {"type": "partial_data", "description": "Partial data loss (network interruption)"},
            {"type": "empty_record", "description": "Empty record (data corruption)"}
        ]
        
        logger.info(f"🚀 Enhanced IoT Simulator initialized")
        logger.info(f"📁 Output directory: {output_dir}")
        logger.info(f"⏱️  Interval: {interval_seconds} seconds")
        logger.info(f"🌡️  Devices: {len(self.devices)}")
        logger.info(f"🔄 Duplicate probability: {duplicate_probability*100}%")
        logger.info(f"🧪 Testing scenarios: {len(self.duplicate_scenarios)}")

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
        """Generate a single IoT temperature record with potential duplicates"""
        self.record_counter += 1
        
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

    def generate_duplicate_record(self, base_record, scenario_type):
        """Generate a duplicate record based on a scenario"""
        duplicate = base_record.copy()
        
        if scenario_type == "retry":
            # Network retry - same data, slightly different timestamp
            original_time = datetime.strptime(base_record["noted_date"], "%d-%m-%Y %H:%M")
            new_time = original_time + timedelta(seconds=random.randint(1, 30))
            duplicate["noted_date"] = new_time.strftime("%d-%m-%Y %H:%M")
            duplicate["id"] = f"__export__.temp_log_{random.randint(100000, 999999)}_{random.randint(10000000, 99999999):x}"
            logger.info(f"🔄 Generated retry duplicate for {base_record['id']}")
            
        elif scenario_type == "correction":
            # Data correction - updated temperature
            duplicate["temp"] = base_record["temp"] + random.randint(-2, 2)
            duplicate["id"] = f"__export__.temp_log_{random.randint(100000, 999999)}_{random.randint(10000000, 99999999):x}"
            logger.info(f"🔧 Generated correction duplicate for {base_record['id']} (temp: {base_record['temp']} → {duplicate['temp']})")
            
        elif scenario_type == "reprocessing":
            # Reprocessing - same data, new batch
            duplicate["id"] = f"__export__.temp_log_{random.randint(100000, 999999)}_{random.randint(10000000, 99999999):x}"
            logger.info(f"🔄 Generated reprocessing duplicate for {base_record['id']}")
            
        elif scenario_type == "sensor_fault":
            # Sensor fault - corrected reading
            duplicate["temp"] = base_record["temp"] + random.randint(-5, 5)
            duplicate["id"] = f"__export__.temp_log_{random.randint(100000, 999999)}_{random.randint(10000000, 99999999):x}"
            logger.info(f"⚠️ Generated sensor fault correction for {base_record['id']} (temp: {base_record['temp']} → {duplicate['temp']})")
        
        return duplicate

    def generate_anomaly_record(self, base_record, scenario_type):
        """Generate an anomalous record based on a scenario"""
        anomaly = base_record.copy()
        
        if scenario_type == "extreme_temp":
            # Extreme temperature (sensor malfunction)
            anomaly["temp"] = random.choice([-50, 150, 200])  # Impossible temperatures
            logger.info(f"🔥 Generated extreme temperature anomaly: {base_record['temp']}°C → {anomaly['temp']}°C")
            
        elif scenario_type == "sudden_spike":
            # Sudden temperature spike
            base_temp = base_record["temp"]
            spike = random.randint(20, 50)  # 20-50 degree spike
            anomaly["temp"] = base_temp + spike
            logger.info(f"📈 Generated sudden spike anomaly: {base_record['temp']}°C → {anomaly['temp']}°C")
            
        elif scenario_type == "negative_temp":
            # Negative temperature (indoor should not be negative)
            if base_record["out/in"] == "In":
                anomaly["temp"] = random.randint(-20, -1)
                logger.info(f"❄️ Generated negative temperature anomaly: {base_record['temp']}°C → {anomaly['temp']}°C")
            
        elif scenario_type == "impossible_value":
            # Impossible temperature value
            anomaly["temp"] = random.choice([999, -999, 1000])
            logger.info(f"🚫 Generated impossible value anomaly: {base_record['temp']}°C → {anomaly['temp']}°C")
            
        elif scenario_type == "outlier_pattern":
            # Statistical outlier (very different from normal range)
            if base_record["out/in"] == "In":
                anomaly["temp"] = random.randint(40, 60)  # Way too hot for indoor
            else:
                anomaly["temp"] = random.randint(-30, -10)  # Way too cold for outdoor
            logger.info(f"📊 Generated outlier anomaly: {base_record['temp']}°C → {anomaly['temp']}°C")
        
        return anomaly

    def generate_missing_value_record(self, base_record, scenario_type):
        """Generate a record with missing values based on a scenario"""
        missing = base_record.copy()
        
        if scenario_type == "null_temp":
            # Missing temperature value
            missing["temp"] = None
            logger.info(f"🌡️ Generated missing temperature: {base_record['temp']}°C → NULL")
            
        elif scenario_type == "null_location":
            # Missing location data
            missing["out/in"] = None
            missing["room"] = None
            logger.info(f"📍 Generated missing location data")
            
        elif scenario_type == "null_timestamp":
            # Missing timestamp
            missing["noted_date"] = None
            logger.info(f"⏰ Generated missing timestamp")
            
        elif scenario_type == "partial_data":
            # Partial data loss
            missing["temp"] = None
            missing["room"] = None
            logger.info(f"📉 Generated partial data loss")
            
        elif scenario_type == "empty_record":
            # Empty record (data corruption)
            missing["id"] = ""
            missing["temp"] = None
            missing["noted_date"] = None
            missing["out/in"] = None
            missing["room"] = None
            logger.info(f"💥 Generated empty record (data corruption)")
        
        return missing

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
        """Run the IoT simulation with duplicate generation"""
        logger.info("🎯 Starting Enhanced IoT data simulation with duplicates...")
        logger.info("Press Ctrl+C to stop")
        
        start_time = time.time()
        record_count = 0
        duplicate_count = 0
        anomaly_count = 0
        missing_count = 0
        recent_records = []  # Store recent records for duplicate generation
        
        try:
            while True:
                # Generate and save record
                record = self.generate_single_record()
                filepath = self.save_record(record)
                record_count += 1
                
                # Store recent record for potential duplicates
                recent_records.append(record)
                if len(recent_records) > 10:  # Keep only last 10 records
                    recent_records.pop(0)
                
                # Check if we should generate a duplicate
                if random.random() < self.duplicate_probability and recent_records:
                    # Select a random recent record to duplicate
                    base_record = random.choice(recent_records)
                    scenario = random.choice(self.duplicate_scenarios)
                    
                    # Generate duplicate
                    duplicate_record = self.generate_duplicate_record(base_record, scenario["type"])
                    duplicate_filepath = self.save_record(duplicate_record)
                    duplicate_count += 1
                    
                    logger.info(f"🧪 Duplicate scenario: {scenario['description']}")
                
                # Check if we should generate an anomaly
                if random.random() < self.anomaly_probability:
                    scenario = random.choice(self.anomaly_scenarios)
                    anomaly_record = self.generate_anomaly_record(record, scenario["type"])
                    anomaly_filepath = self.save_record(anomaly_record)
                    anomaly_count += 1
                    
                    logger.info(f"🚨 Anomaly scenario: {scenario['description']}")
                
                # Check if we should generate missing values
                if random.random() < self.missing_probability:
                    scenario = random.choice(self.missing_value_scenarios)
                    missing_record = self.generate_missing_value_record(record, scenario["type"])
                    missing_filepath = self.save_record(missing_record)
                    missing_count += 1
                    
                    logger.info(f"❓ Missing value scenario: {scenario['description']}")
                
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
        logger.info(f"📊 Enhanced Simulation Summary:")
        logger.info(f"   Total records generated: {record_count}")
        logger.info(f"   Duplicate records: {duplicate_count}")
        logger.info(f"   Anomaly records: {anomaly_count}")
        logger.info(f"   Missing value records: {missing_count}")
        logger.info(f"   Clean records: {record_count - duplicate_count - anomaly_count - missing_count}")
        logger.info(f"   Duplicate rate: {(duplicate_count/record_count)*100:.1f}%")
        logger.info(f"   Anomaly rate: {(anomaly_count/record_count)*100:.1f}%")
        logger.info(f"   Missing value rate: {(missing_count/record_count)*100:.1f}%")
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

