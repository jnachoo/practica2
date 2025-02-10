import os
from databases import Database
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session


# Obtén la URL de la base de datos desde las variables de entorno
DATABASE_URL = os.getenv("DATABASE_URL")

# Conecta la base de datos
database = Database(DATABASE_URL)

# Motor SQLAlchemy (para los modelos)
engine = create_engine(DATABASE_URL)

# Sesión SQLAlchemy (para transacciones síncronas)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base para modelos SQLAlchemy
Base = declarative_base()

# Función para obtener la sesión
def get_bd():
    db = SessionLocal()
    try:
        yield db  # Devuelve la sesión de la base de datos
    finally:
        db.close()