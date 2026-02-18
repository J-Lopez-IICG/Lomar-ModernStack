# 1. Imagen base más ligera (Python oficial)
FROM python:3.12-slim-bookworm

# 2. Instalamos Java (Spark lo necesita) y herramientas básicas
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 3. Variables de entorno
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=python3
WORKDIR /app

# 4. Instalamos dependencias (Primero solo el requirements para aprovechar el caché)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copiamos los archivos críticos del proyecto
# No copiamos "todo" a ciegas para evitar meter basura
COPY src/ /app/src/
COPY conf/ /app/conf/
COPY pyproject.toml /app/
COPY *.jar /app/
COPY *.json /app/ 

# 7. Ejecución
# Usamos "bash -c" para cargar los secretos antes de correr kedro
CMD ["/bin/bash", "-c", "source .secrets.sh && kedro run"]