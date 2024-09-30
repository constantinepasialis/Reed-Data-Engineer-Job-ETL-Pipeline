# Use the official Airflow image
FROM apache/airflow:2.10.2-python3.9

# Install any additional Python packages if needed
RUN pip install --no-cache-dir <apache-airflow>

