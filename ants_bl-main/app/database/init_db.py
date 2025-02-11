# init_db.py
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from models import Base  # Ajusta la importación según la estructura de tu proyecto
from config.settings import DATABASE_URL_TEST  # Asegúrate de que esta es la cadena de conexión correcta

engine = create_engine(DATABASE_URL_TEST)
#Base.metadata.create_all(engine)

#print("Las tablas han sido creadas.")
