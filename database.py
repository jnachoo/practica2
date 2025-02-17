import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Asegúrate de que DATABASE_URL tenga el prefijo asíncrono, por ejemplo:
# "postgresql+asyncpg://user:password@host:port/dbname"
DATABASE_URL = "postgresql+asyncpg://fgonzalez:practica.01%23@192.168.74.110:5432/ants_api"



# Crear un motor asíncrono
engine = create_async_engine(DATABASE_URL, echo=True)

# Crear una fábrica de sesiones asíncronas
SessionLocal = async_sessionmaker(bind=engine, expire_on_commit=False)

# Base para los modelos
Base = declarative_base()

# Dependencia para obtener la sesión asíncrona
async def get_db():
    async with SessionLocal() as session:
        yield session
