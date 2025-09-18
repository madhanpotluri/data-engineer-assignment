#!/usr/bin/env python3
"""
Docker-based test script for the ELT job
This script runs the ELT job inside the Spark container
"""

import subprocess
import os
import sys

def run_elt_in_docker():
    """
    Run the ELT job inside the Spark Docker container
    """
    print("🐳 Testing ELT Job in Docker")
    print("=" * 50)
    
    # Check if Docker is running
    try:
        subprocess.run(["docker", "ps"], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        print("❌ Docker is not running. Please start Docker first.")
        return False
    
    # Check if containers are running
    try:
        result = subprocess.run(["docker", "compose", "ps"], check=True, capture_output=True, text=True)
        if "spark-master" not in result.stdout:
            print("❌ Spark containers are not running. Please start them first:")
            print("docker compose up -d")
            return False
    except subprocess.CalledProcessError:
        print("❌ Docker Compose not available. Please start the services manually.")
        return False
    
    print("✅ Docker and containers are running")
    
    # Run the ELT job in the Spark container
    print("\n🚀 Running ELT job in Spark container...")
    
    try:
        # Execute the ELT job in the Spark container
        cmd = [
            "docker", "exec", "-it", "spark-master",
            "python3", "/scripts/elt_job.py",
            "--input_path", "/data/landing_zone/IOT-temp.csv",
            "--output_db", "postgres"
        ]
        
        print(f"Executing: {' '.join(cmd)}")
        result = subprocess.run(cmd, check=True, text=True)
        
        print("\n✅ ELT job completed successfully!")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ ELT job failed: {e}")
        return False

def check_data_availability():
    """
    Check if the input data is available in the container
    """
    print("📁 Checking data availability...")
    
    try:
        # Check if data file exists in container
        cmd = ["docker", "exec", "spark-master", "ls", "-la", "/data/landing_zone/"]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        
        print("Files in landing zone:")
        print(result.stdout)
        
        if "IOT-temp.csv" in result.stdout:
            print("✅ Input data file found")
            return True
        else:
            print("❌ Input data file not found")
            return False
            
    except subprocess.CalledProcessError as e:
        print(f"❌ Error checking data: {e}")
        return False

def main():
    print("🧪 Docker-based ELT Job Test")
    print("=" * 50)
    
    # Check if data is available
    if not check_data_availability():
        print("\n📥 Please download data first:")
        print("python3 scripts/download_data.py")
        return
    
    # Run the ELT job
    if run_elt_in_docker():
        print("\n🎉 Test completed successfully!")
        print("\n📊 Next steps:")
        print("1. Check the database: docker exec -it postgres psql -U postgres -d iot_data -c 'SELECT COUNT(*) FROM iot_temperature_readings;'")
        print("2. Check rejected records: docker exec -it postgres psql -U postgres -d iot_data -c 'SELECT COUNT(*) FROM rejected_temperature_readings;'")
        print("3. View data: docker exec -it postgres psql -U postgres -d iot_data -c 'SELECT * FROM iot_temperature_readings LIMIT 5;'")
    else:
        print("\n❌ Test failed. Check the logs above for details.")

if __name__ == "__main__":
    main()
