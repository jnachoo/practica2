import os
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# Construct DATABASE_URL from environment variables
DATABASE_URL = f"postgresql+asyncpg://{os.getenv('DATABASE_USER')}:{os.getenv('DATABASE_PASS')}@{os.getenv('DATABASE_HOST')}:{os.getenv('DATABASE_PORT')}/{os.getenv('DATABASE_NAME')}"

logger.info(f"Intentando conectar a la base de datos en: {os.getenv('DATABASE_HOST')}:{os.getenv('DATABASE_PORT')}")

# Crear motor asincrono
try:
    engine = create_async_engine(DATABASE_URL, echo=True)
    logger.info("Conexión a la base de datos establecida correctamente")
except Exception as e:
    logger.error(f"Error al conectar a la base de datos: {str(e)}")

# Crear una fábrica de sesiones asíncronas
SessionLocal = async_sessionmaker(bind=engine, expire_on_commit=False)

# Base para los modelos
Base = declarative_base()

# Dependencia para obtener la sesión asíncrona
async def get_db():
    async with SessionLocal() as session:
        yield session
