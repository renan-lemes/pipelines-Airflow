FROM apache/airflow:2.9.0

# Copia o arquivo requirements.txt para dentro da imagem
COPY requirements.txt .

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt
