FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python requests ujson
COPY writer.py .
CMD ["python", "writer.py"]