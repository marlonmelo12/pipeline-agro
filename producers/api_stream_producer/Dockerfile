FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# O comando usa a porta 5001 como no seu código original
CMD ["python", "producer.py"]