FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements-api.txt ./
RUN pip install --no-cache-dir -r requirements-api.txt

COPY . .
RUN pip install --no-cache-dir -e .

CMD ["uvicorn", "nas_processor.src.api.app:app", "--host", "0.0.0.0", \
     "--port", "8001"]
