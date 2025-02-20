from fastapi import APIRouter, HTTPException, Query, Depends
from database import get_db
from datetime import datetime
from typing import List, Annotated, Optional
from rutas.autenticacion import check_rol, get_current_user
from models import User
import aiohttp
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

router = APIRouter()

# Configuración de ZenRows
ZENROWS_API_KEY = os.getenv('ZEN_ROWS_API')
if not ZENROWS_API_KEY:
    raise ValueError("ZEN_ROWS_API not found in .env file")

ZENROWS_PROXY = f"http://{ZENROWS_API_KEY}:@api.zenrows.com:8001"

@router.get("/prueba/")
async def prueba():
    """
    Endpoint de prueba para verificar la conexión con ZenRows
    """
    url = "https://httpbin.io/anything"
    
    async with aiohttp.ClientSession() as session:
        try:
            # Encabezados específicos de ZenRows
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            
            # Parámetros de ZenRows
            params = {
                "apikey": ZENROWS_API_KEY,
                "js_render": "true",
                "antibot": "true"
            }
            
            async with session.get(
                url,
                proxy=ZENROWS_PROXY,
                headers=headers,
                params=params,
                ssl=False
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    raise HTTPException(
                        status_code=response.status,
                        detail=f"Error en la petición a ZenRows: {response.status}"
                    )
                    
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error al hacer la petición: {str(e)}"
            )
