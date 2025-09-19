#!/usr/bin/env python3
"""
Test script for the ELT job
"""

import sys
import os

# Add the scripts directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from elt_job import run_elt_job

def main():
    print("🧪 Testing ELT Job")
    print("=" * 50)
    
    # Test parameters
    input_path = "data/landing_zone/IOT-temp.csv"
    output_db = "postgres"
    
    print(f"Input path: {input_path}")
    print(f"Output database: {output_db}")
    print()
    
    # Check if input file exists
    if not os.path.exists(input_path):
        print(f"❌ Input file not found: {input_path}")
        print("Please run the download script first:")
        print("python3 scripts/download_data.py")
        return
    
    try:
        # Run the ELT job
        run_elt_job(input_path, output_db)
        print("\n✅ ELT job test completed successfully!")
        
    except Exception as e:
        print(f"\n❌ ELT job test failed: {e}")
        return

if __name__ == "__main__":
    main()

