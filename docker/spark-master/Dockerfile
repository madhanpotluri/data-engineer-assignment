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
COPY docker/spark-master/requirements-pyspark.txt /requirements.txt

# Install Python packages using pip3
RUN pip3 install --no-cache-dir -r /requirements.txt

# Create directories and copy scripts
RUN mkdir -p /scripts /data
COPY scripts/ /scripts/
COPY data/ /data/

# Set proper permissions
RUN chmod -R 755 /scripts /data

# Create necessary directories and set ownership
RUN mkdir -p /home/1001/.ivy2/local /tmp/spark-local /tmp/spark-warehouse && \
    chown -R 1001:1001 /home/1001 /tmp/spark-local /tmp/spark-warehouse

# Switch back to the default user
USER 1001

# Set environment variables for proper Spark operation
ENV HOME=/home/1001
ENV IVY_LOCAL_REPO=/home/1001/.ivy2/local
ENV SPARK_LOCAL_DIR=/tmp/spark-local
ENV SPARK_WAREHOUSE_DIR=/tmp/spark-warehouse

# Command to start the Spark master
CMD ["/opt/bitnami/spark/bin/spark-class", "org.apache.spark.deploy.master.Master", "--host", "0.0.0.0", "--port", "7077", "--webui-port", "8080"]