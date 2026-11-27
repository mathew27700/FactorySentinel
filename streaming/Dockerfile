# Image for Spark Structured Streaming jobs. Keep the tag in sync with
# docker-compose.yml and the pyspark version in requirements-dev.txt.
FROM apache/spark:4.2.0-scala2.13-java17-python3-ubuntu

# The job writes to Postgres with psycopg2 (plain Python, no JDBC jar needed).
USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.13

WORKDIR /opt/factorysentinel
COPY jobs/ ./jobs/
# Lets the Python workers import validate.py (used inside a UDF).
ENV PYTHONPATH=/opt/factorysentinel/jobs
