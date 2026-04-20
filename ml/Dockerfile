FROM python:3.11-slim

WORKDIR /app

COPY ml /app/ml

RUN pip install --no-cache-dir \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    scikit-learn \
    joblib

CMD ["python", "ml/predict.py"]