# Utiliza una imagen base de Python
FROM python:3.14.7

# Establece el directorio de trabajo en /app
WORKDIR /app

# Copia el archivo requirements.txt e instala las dependencias
COPY /app/requirements.txt .

RUN  apt-get update
RUN  apt install -y xmlsec1 libxmlsec1 libxmlsec1-dev gcc

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Expone el puerto en el que se ejecutará la aplicación
EXPOSE 8001

