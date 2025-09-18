#!/bin/bash

# Docker-based ELT Job Test Script

echo "🐳 Testing ELT Job in Docker"
echo "=============================="

# Check if Docker is running
if ! docker ps > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if containers are running
if ! docker compose ps | grep -q "spark-master"; then
    echo "❌ Spark containers are not running. Starting them..."
    docker compose up -d
    echo "⏳ Waiting for containers to be ready..."
    sleep 10
fi

echo "✅ Docker and containers are running"

# Check if data file exists
echo "📁 Checking data availability..."
if docker exec spark-master ls /data/landing_zone/IOT-temp.csv > /dev/null 2>&1; then
    echo "✅ Input data file found"
else
    echo "❌ Input data file not found. Please download data first:"
    echo "python3 scripts/download_data.py"
    exit 1
fi

# Run the ELT job
echo ""
echo "🚀 Running ELT job in Spark container..."
echo "Command: docker exec -it spark-master python3 /scripts/elt_job.py --input_path /data/landing_zone/IOT-temp.csv --output_db postgres"
echo ""

docker exec -it spark-master python3 /scripts/elt_job.py \
    --input_path /data/landing_zone/IOT-temp.csv \
    --output_db postgres

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ ELT job completed successfully!"
    echo ""
    echo "📊 Checking results..."
    echo ""
    echo "Clean records count:"
    docker exec postgres psql -U postgres -d iot_data -c "SELECT COUNT(*) as clean_records FROM iot_temperature_readings;"
    echo ""
    echo "Rejected records count:"
    docker exec postgres psql -U postgres -d iot_data -c "SELECT COUNT(*) as rejected_records FROM rejected_temperature_readings;"
    echo ""
    echo "Sample clean data:"
    docker exec postgres psql -U postgres -d iot_data -c "SELECT * FROM iot_temperature_readings LIMIT 5;"
else
    echo ""
    echo "❌ ELT job failed. Check the logs above for details."
    exit 1
fi
