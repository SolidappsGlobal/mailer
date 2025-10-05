# Arquivo principal para Cloud Run
FROM python:3.11-slim

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Definir diretório de trabalho
WORKDIR /app

# Criar usuário não-root para evitar warnings
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Copiar requirements primeiro (para cache de dependências)
COPY requirements.txt .

# Atualizar pip e instalar dependências Python
RUN pip install --no-cache-dir --upgrade pip --root-user-action=ignore && \
    pip install --no-cache-dir -r requirements.txt --root-user-action=ignore

# Copiar código da aplicação
COPY main.py .

# Mudar ownership para o usuário não-root
RUN chown -R appuser:appuser /app

# Mudar para usuário não-root
USER appuser

# Definir porta
ENV PORT=8080

# Expor porta
EXPOSE 8080

# Comando para executar
CMD ["python", "main.py"]
