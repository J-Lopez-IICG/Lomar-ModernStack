# Usa la imagen de Spark que ya tienes en Docker Desktop
ARG BASE_IMAGE=jupyter/pyspark-notebook:latest
FROM $BASE_IMAGE

USER root

# Actualizamos pip e instalamos tus librerías
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Seteamos donde vivirá el proyecto dentro de Docker
WORKDIR /home/jovyan/work

# Copiamos tu proyecto (el punto significa 'todo lo de esta carpeta')
COPY --chown=jovyan:users . .

# Puertos para Jupyter, Spark y Kedro-Viz
EXPOSE 8888 4040 4141

CMD ["kedro", "run"]