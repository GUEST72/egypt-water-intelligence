FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY pyproject.toml requirements.txt README.md ./
COPY src ./src
COPY schemas ./schemas
COPY configs ./configs
COPY scripts ./scripts
RUN python -m pip install --upgrade pip && python -m pip install -r requirements.txt && python -m pip install .

COPY tests ./tests

ENTRYPOINT ["water-generator"]
