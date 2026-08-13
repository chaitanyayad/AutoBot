# One image, two roles. The API and the workers run the same code — they differ
# only in the command compose gives them — so a worker can never drift out of
# step with the scheduler it shares a state machine with.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Dependencies first: this layer is cached until requirements.txt changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY examples ./examples
COPY scripts ./scripts
COPY schema.sql ./schema.sql

# Nothing here needs root, and a worker executes task handlers.
RUN useradd --create-home --uid 1000 orchestrator && chown -R orchestrator /app
USER orchestrator

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
