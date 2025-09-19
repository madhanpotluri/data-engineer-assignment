#!/usr/bin/env python3
"""
Test script for IoT Simulator
"""

import os
import sys
import time
from iot_simulator import IoTSimulator

def test_iot_simulator():
    """Test the IoT simulator with a short duration"""
    print("🧪 Testing IoT Simulator...")
    
    # Create test directory
    test_dir = "data/test_landing_zone"
    os.makedirs(test_dir, exist_ok=True)
    
    # Create simulator
    simulator = IoTSimulator(
        output_dir=test_dir,
        interval_seconds=2  # Faster for testing
    )
    
    # Run for 30 seconds (15 records)
    print("⏱️  Running simulator for 30 seconds...")
    simulator.run_simulation(duration_minutes=0.5)  # 0.5 minutes = 30 seconds
    
    # Check generated files
    files = os.listdir(test_dir)
    csv_files = [f for f in files if f.endswith('.csv')]
    
    print(f"📊 Test Results:")
    print(f"   Files generated: {len(csv_files)}")
    print(f"   Expected: ~15 files")
    
    if len(csv_files) > 0:
        print(f"   Sample files: {csv_files[:3]}")
        
        # Check content of first file
        first_file = os.path.join(test_dir, csv_files[0])
        with open(first_file, 'r') as f:
            content = f.read()
            print(f"   Sample content: {content.strip()}")
    
    print("✅ IoT Simulator test completed!")
    return len(csv_files)

if __name__ == "__main__":
    test_iot_simulator()

