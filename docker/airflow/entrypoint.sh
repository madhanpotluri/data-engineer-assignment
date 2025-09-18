#!/usr/bin/env bash

# This is a basic entrypoint script for Apache Airflow.
# It will wait for the PostgreSQL database to be ready before initializing the database and starting the webserver/scheduler.

# Exit immediately if a command exits with a non-zero status.
set -e

# Check if the database is ready
echo "Waiting for PostgreSQL database to be ready..."
while ! pg_isready -h postgres -p 5432; do
  echo "PostgreSQL not ready, waiting..."
  sleep 2
done
echo "PostgreSQL database is ready!"

# Initialize the Airflow database
echo "Initializing Airflow database..."
airflow db migrate

# Create the default Airflow user (only if it doesn't exist)
echo "Creating Airflow user..."
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin || echo "User already exists or creation failed"

# Start the specified service (webserver or scheduler)
echo "Starting Airflow service: $1"
exec airflow "$@"
