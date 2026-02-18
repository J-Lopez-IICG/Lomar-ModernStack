# 1. Imagen base
FROM python:3.12-slim-bookworm

# 2. Instalamos Java y herramientas
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    iproute2 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 3. Variables de entorno generales (NO CLAVES)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH="${PYTHONPATH}:/app/src"
WORKDIR /app

# 4. Dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copiamos el proyecto
# OJO: NO copiamos .secrets.sh aquí. Eso es ilegal en seguridad.
COPY src/ /app/src/
COPY conf/ /app/conf/
COPY pyproject.toml /app/
# Copiamos el JSON de GCP porque es un archivo físico (esto es aceptable en repos privados)
# Pero las contraseñas de texto NO.
COPY *.jar /app/
COPY *.json /app/

# 6. Registro del proyecto
RUN pip install --no-cache-dir .

# 7. Ejecución Limpia
# Ya no hacemos source. Esperamos que las variables vengan "del cielo" (Docker Compose)
CMD ["kedro", "run"]