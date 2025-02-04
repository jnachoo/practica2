from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from database import database
#from rutas.bls_endpoints import cargar_navieras 
from rutas import bls_endpoints, containers_endpoints, requests_endpoints, paradas_endpoints,validaciones_endpoints,autenticacion

app = FastAPI()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
#origins = [
#    "http://localhost:3000",  #  URL del front-end
#    "http://192.168.x.x:3000",  # IP de la maquina del front
#]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Conectar la base de datos
@app.on_event("startup")
async def startup():
    await database.connect()

# Desconectar la base de datos
@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


# Registrar rutas
app.include_router(bls_endpoints.router)
app.include_router(containers_endpoints.router)
app.include_router(requests_endpoints.router)
app.include_router(paradas_endpoints.router)
app.include_router(validaciones_endpoints.router)
app.include_router(autenticacion.router)

@app.get("/")
def leer_raiz():
    
    return {
        "mensaje":"Bienvenido a Brains",
        "info":ver_info()
    }

#-------------------------------
#----------INFORMACIÓN----------
#-------------------------------
@app.get("/info")
def ver_info():
    mensaje = {
        "GET": "CONSULTAS API",
        "1.": "Estas son las rutas relacionadas a bls",
        "1.0": "/bls",
        "1.1": "/bls/fecha/escribir_fecha",
        "1.2": "/bls/id/escribir_id",
        "1.3": "/bls/code/escribir_code",
        "1.4": "/bls/etapa/escribir_etapa",
        "1.5": "/bls/naviera/escribir_naviera",
        "2.": "Estas son las rutas relacionadas a requests",
        "2.0": "/requests  Super filtro",
        "2.1": "/requests/id_bl/escribir_id_bl",
        "2.2": "/requests/bl_code/escribir_bl_code",
        "3.": "Estas son las rutas relacionadas a paradas",
        "3.0": "/paradas  Super filtro",
        "3.1": "/paradas/bl_code/escribir_bl_code",
        "3.2": "/paradas/locode/escribir_locode",
        "3.3": "/paradas/pais/escribir_pais",
        "4.": "Estas son las rutas relacionadas a container",
        "4.0": "/containers",
        "4.1": "/containers/code/escribir_code_container",
        "4.2": "/containers/bl_code/escribir_code_container",
        "": "",
        "GET VALIDACIONES": "VALIDACIONES API",
        "5.": "Estas son las rutas relacionadas a validación en línea",
        "5.1": "/validacion_locode_nulo",
        "5.2": "/validacion_cruce_contenedores",
        "5.3": "/validacion_containers_repetidos",
        "5.4": "/validacion_paradas_pol_pod",
        "5.5": "/validacion_orden_repetida",
        "5.6": "/validacion_impo_distinta_CL",
        "5.7": "/validacion_bls_impo",
        "5.8": "/validacion_expo_distinta_CL",
        "5.9": "/validacion_bls_expo",
        "5.10": "/validacion_paradas_expo",
        "5.11": "/validacion_dias_impo",
        "5.12": "/validacion_requests_expo",
        "6.": "Estas son las validaciones de tendencia",
        "6.1": "/tendencia_navieras/escribir_nombre_naviera",
        "6.2": "/tendencia_etapa/escribir_numero_etapa",
        "6.3": "/tendencia_contenedor_dryreefer/escribir_nombre_contenedor",
        "6.4": "/tendencia_por_origen/escribir_locode",
        "6.5": "/tendencia_por_destino/escribir_locode",
    }
    return mensaje