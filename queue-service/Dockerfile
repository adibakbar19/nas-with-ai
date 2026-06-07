FROM python:3.12-slim

WORKDIR /app
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8005
CMD ["python", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8005", "--log-level", "info"]
