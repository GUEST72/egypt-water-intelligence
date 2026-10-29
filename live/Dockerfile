FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
RUN python -m pip install "kafka-python-ng==2.2.3" "psycopg[binary]>=3.2,<3.3"
WORKDIR /app
COPY bridge.py .
USER nobody
ENTRYPOINT ["python", "bridge.py"]
