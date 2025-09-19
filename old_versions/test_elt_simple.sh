#!/bin/bash

# Simple ELT Job Test Script
echo "🧪 Simple ELT Job Test"
echo "======================"

# Check if containers are running
if ! docker compose ps | grep -q "spark-master"; then
    echo "❌ Spark containers are not running. Please start them first:"
    echo "docker compose up -d"
    exit 1
fi

echo "✅ Containers are running"

# Install missing dependencies in the running container
echo "📦 Installing missing dependencies..."
docker exec spark-master pip3 install py4j numpy

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
echo "🚀 Running ELT job..."
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
    docker exec postgres psql -U postgres -d iot_data -c "SELECT COUNT(*) as clean_records FROM iot_temperature_readings;" 2>/dev/null || echo "Table not found - will be created on first run"
    echo ""
    echo "Rejected records count:"
    docker exec postgres psql -U postgres -d iot_data -c "SELECT COUNT(*) as rejected_records FROM rejected_temperature_readings;" 2>/dev/null || echo "Table not found - will be created on first run"
else
    echo ""
    echo "❌ ELT job failed. Check the logs above for details."
    exit 1
fi

