from fastapi import APIRouter, HTTPException,Query, Depends
from database import get_db
from datetime import datetime
from typing import List, Annotated, Optional
from rutas.autenticacion import check_rol,get_current_user
from models import User
import requests

router = APIRouter()

@router.get("/prueba/")
async def prueba():
    
    url = "https://httpbin.io/anything"
    proxy = "http://fe625ce84f8d2e2d04a8ec5177710814141fdac8:@api.zenrows.com:8001"
    proxies = {"http": proxy, "https": proxy}
    response = requests.get(url, proxies=proxies, verify=False)
    print(response.text)

    return response.text
