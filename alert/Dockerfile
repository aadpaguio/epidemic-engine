FROM python:3.11-slim

WORKDIR /app

COPY alert/requirements.txt /app/alert/requirements.txt
RUN pip install --no-cache-dir -r /app/alert/requirements.txt

COPY alert /app/alert

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

CMD ["python", "-m", "alert.alert_service"]
