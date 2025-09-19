# Use a trusted Spark image from Bitnami as the base
# This image already includes Java, Scala, and Spark
FROM bitnami/spark:3.5.1

# Switch to root user to install Python packages
USER root

# Install system dependencies and Python packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    python3-dev \
    build-essential \
    wget \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create jars directory and download PostgreSQL JDBC driver
RUN mkdir -p /opt/spark/jars && \
    wget -O /opt/spark/jars/postgresql-42.5.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.5.0.jar

# Copy the requirements file for the PySpark jobs
#COPY docker/spark/requirements-pyspark.txt /requirements.txt
COPY docker/spark-worker/requirements-pyspark.txt /requirements.txt

# Install Python packages using pip3
RUN pip3 install --no-cache-dir -r /requirements.txt

# Create directories and copy scripts
RUN mkdir -p /scripts /data
COPY scripts/ /scripts/
COPY data/ /data/

# Set proper permissions
RUN chmod -R 755 /scripts /data

# Switch back to the default user
USER 1001

# Command to start the Spark worker
CMD ["/opt/bitnami/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077"]