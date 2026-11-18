FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONWARNINGS=ignore
CMD ["bash", "-c", "python -m pytest -q && python -m src.erasure.pipeline && python scripts/benchmark.py"]
