FROM flink:1.20-java17

# Python + PyFlink
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev curl && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir apache-flink==1.20.1

# Kafka connector (SQL fat jar,   Table API  DataStream)
RUN curl -fSL -o /opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar

RUN mkdir -p /opt/flink/state/checkpoints /opt/flink/state/savepoints && chown -R flink:flink /opt/flink/state    
