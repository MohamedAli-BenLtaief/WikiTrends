FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir requests sseclient-py kafka-python ujson urllib3
COPY ingestor.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "ingestor.py"]