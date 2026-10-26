FROM apache/spark:3.5.3-python3
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
# Match Spark 3.5 / Scala 2.12. Iceberg S3FileIO avoids Hadoop/S3A dependency conflicts.
RUN curl -fSL --retry 3 -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar \
      https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar \
    && curl -fSL --retry 3 -o /opt/spark/jars/iceberg-aws-bundle-1.7.1.jar \
      https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar \
    && curl -fSL --retry 3 -o /opt/spark/jars/postgresql-42.7.5.jar \
      https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar
# This Spark image ships Python 3.8; MinIO 7.2.11+ requires Python >=3.9.
RUN python3 -m pip install --no-cache-dir minio==7.2.10
COPY jobs /opt/spark/work-dir/jobs
USER 185
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "1g"]
CMD ["/opt/spark/work-dir/jobs/handoff_smoke.py", "--help"]
