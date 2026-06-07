FROM python:3.12-slim

WORKDIR /app
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8004
CMD ["python", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8004", "--log-level", "info"]
