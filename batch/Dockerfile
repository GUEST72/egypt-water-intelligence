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
# Bounded Kafka reads of the Flink output topics. Versions from the spark-sql-kafka 3.5.3 POM.
RUN M=https://repo.maven.apache.org/maven2 \
    && curl -fSL --retry 3 -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar \
      $M/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
    && curl -fSL --retry 3 -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
      $M/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
    && curl -fSL --retry 3 -o /opt/spark/jars/kafka-clients-3.4.1.jar \
      $M/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    && curl -fSL --retry 3 -o /opt/spark/jars/commons-pool2-2.11.1.jar \
      $M/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
# This Spark image ships Python 3.8; MinIO 7.2.11+ requires Python >=3.9.
RUN python3 -m pip install --no-cache-dir minio==7.2.10
# Event logs feed the Spark History Server (docker compose service spark-history).
RUN mkdir -p /opt/spark/spark-events /opt/spark/conf && chown 185 /opt/spark/spark-events \
    && printf '%s\n' \
      'spark.eventLog.enabled true' \
      'spark.eventLog.dir file:///opt/spark/spark-events' \
      'spark.history.fs.logDirectory file:///opt/spark/spark-events' \
      > /opt/spark/conf/spark-defaults.conf
COPY jobs /opt/spark/work-dir/jobs
USER 185
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "1g"]
CMD ["/opt/spark/work-dir/jobs/handoff_smoke.py", "--help"]
