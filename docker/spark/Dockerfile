# Use a trusted Spark image from Bitnami as the base
# This image already includes Java, Scala, and Spark
FROM bitnami/spark:3.5.1

# Switch to root user to install Python packages
USER root

# Install Python's package manager
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file for the PySpark jobs
COPY ./docker/spark/requirements-pyspark.txt /requirements.txt

# Install Python packages
RUN pip install --no-cache-dir -r /requirements.txt

# Create a shared directory for your Spark jobs
RUN mkdir /app
COPY scripts/ /app/scripts/
COPY dags/ /app/dags/
WORKDIR /app

# Switch back to the default user
USER 1001

# Command to start the Spark worker
CMD ["/opt/bitnami/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077"]