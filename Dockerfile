# Usar una imagen base de Python
FROM python:3.9-slim

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Copiar los archivos de tu proyecto al contenedor
COPY . .

# Instalar las dependencias del proyecto
RUN pip install --no-cache-dir -r requirements.txt

# Exponer el puerto que usará tu aplicación (si es necesario)
EXPOSE 8000

# Comando para ejecutar la aplicación
#CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]  # Ajusta "api.py" al nombre de tu archivo principal
CMD uvicorn api:app --host 0.0.0.0 --port 8000

