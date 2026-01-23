FROM python:3.11-slim

WORKDIR /app

# Instala dependências do sistema necessárias (opcional, adicione se precisar de algo específico)
# RUN apt-get update && apt-get install -y gcc

# Copia e instala as dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# O código fonte será montado via volume no docker-compose para facilitar o desenvolvimento
# Mas podemos definir o comando padrão para rodar a indexação se desejado, 
# ou deixar o container rodando (tail -f) para execução manual.
CMD ["tail", "-f", "/dev/null"]
