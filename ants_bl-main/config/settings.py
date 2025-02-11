# config/settings.py
import os
from dotenv import load_dotenv
load_dotenv()

# Configuración de la base de datos
DATABASE_HOST = os.getenv("DATABASE_HOST")
DATABASE_PORT = os.getenv("DATABASE_PORT")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASS = os.getenv("DATABASE_PASS")
DATABASE_NAME = os.getenv("DATABASE_NAME")

# Construir la cadena de conexión de la base de datos
DATABASE_URL = f"postgresql://{DATABASE_USER}:{DATABASE_PASS}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"
DATABASE_URL_TEST = 'postgresql://sempat_user:sempat_pass@localhost/test_ants'
