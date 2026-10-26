FROM apache/airflow:2.10.5-python3.11
RUN pip install --no-cache-dir --constraint \
    https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt \
    "apache-airflow==2.10.5" \
    docker==7.1.0 kafka-python-ng==2.2.3 minio==7.2.15
