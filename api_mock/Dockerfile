# Usa uma imagem base leve do Python
FROM python:3.9-slim

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia o arquivo de dependências primeiro (isso otimiza o cache do Docker)
COPY requirements.txt .

# Instala as dependências listadas no requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copia o resto dos arquivos do módulo (app.py, populate_db.py, etc.) para o contêiner
COPY . .

# O comando que será executado quando o contêiner iniciar
CMD ["python", "app.py"]