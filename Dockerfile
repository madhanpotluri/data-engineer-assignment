# Use the official Apache Airflow image as the base
FROM apache/airflow:2.8.1-python3.11

# Switch to the root user to install system dependencies
USER root

# Install any system packages needed (e.g., for database clients)
# RUN apt-get update && apt-get install -y --no-install-recommends \
#      libpq-dev \
#      && apt-get autoremove -yqq --purge \
#      && apt-get clean \
#      && rm -rf /var/lib/apt/lists/*

# Switch back to the 'airflow' user
USER airflow

# Copy the requirements file into the container
COPY requirements.txt .

# Install Python packages from the requirements file
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAGs and other project files into the image
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/

# Set the home directory
WORKDIR /opt/airflow