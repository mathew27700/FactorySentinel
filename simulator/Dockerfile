FROM python:3.11.17-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

# Keep these versions the same as requirements-dev.txt.
RUN pip install --no-cache-dir kafka-python==3.0.11 pandas==3.1.1

COPY simulator.py .

CMD ["python", "simulator.py"]
