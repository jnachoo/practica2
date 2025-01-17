from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
from databases import Database
from datetime import datetime
import asyncio 


# Obtén la URL de la base de datos desde las variables de entorno
DATABASE_URL = os.getenv("DATABASE_URL")

# Conecta la base de datos
database = Database(DATABASE_URL)

app = FastAPI()

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

@app.get("/")
def leer_raiz():
    
    return {
        "mensaje":"Bienvenido a Brains",
        "info":ver_info()
    }

# Modelo para el CRUD 
class Item(BaseModel):
    id: int #solo numeros
    texto: str #cualquier cadena de texto

# Conectar la base de datos
@app.on_event("startup")
async def startup():
    await database.connect()

# Desconectar la base de datos
@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

# Endpoints API 

#------------------------------------------
# -----------------BLS---------------------
#------------------------------------------

@app.get("/bls")
async def ver_bls():
    query = """
                select b.id,b.code as bl_code, e.nombre  as etapa, n.nombre  as naviera ,sb.descripcion_status as status, 
                TO_CHAR(b.fecha, 'YYYY-MM-DD') as fecha ,TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') as fecha_proxima_revision   
                from bls b --875.294
                join etapa e on e.id =b.id_etapa
                join navieras n on n.id =b.id_naviera
                join status_bl sb on b.id_status = sb.id
                where b.id <2000;
                """
    try:
        result = await database.fetch_all(query=query)
        return result
    except Exception as e:
        return {"error": f"Error ejecutando la consulta: {str(e)}"}
#def get_bls():
#    return g.ver_bls()

@app.get("/bls/fecha/{fecha}")
async def bls_fecha(fecha: str):

    query = """
                select distinct b.id,b.code ,e.nombre as Etapa,n.nombre as Naviera,c.size,c.type,c.contenido,t.orden,t.terminal,t.status 
                ,p.locode,p.lugar,TO_CHAR(t.fecha, 'YYYY-MM-DD') as fecha
                from bls b 
                join etapa e on e.id =b.id_etapa
                join tracking t on t.id_bl = b.id 
                join paradas p on p.id = t.id_parada 
                join container_viaje cv on cv.id_bl = b.id 
                join containers c on c.id =cv.id_container 
                join navieras n on n.id =b.id_naviera 
                where 1=1
            """
    values = {}
    
    if fecha and len(fecha)==4:
        fecha = int(fecha)
        query += " AND EXTRACT(YEAR FROM b.fecha) = :fecha"
        values["fecha"] = fecha
        mensaje = f"Los bls encontrados en el año {fecha} son:"
    elif fecha and len(fecha)==10:
        fecha = datetime.strptime(fecha, "%Y-%m-%d").date()
        query += " AND b.fecha >= :fecha"
        values["fecha"] = fecha
        mensaje = f"Los bls encontrados desde {fecha} hasta el día de hoy son:"
    elif fecha and len(fecha)==21:
        nueva_fecha = fecha.split('+')
        fecha_i = datetime.strptime(nueva_fecha[0],"%Y-%m-%d").date()
        fecha_f = datetime.strptime(nueva_fecha[1],"%Y-%m-%d").date()
        query += " AND b.fecha >= :fecha_i AND b.fecha <= :fecha_f"
        values["fecha_i"] = fecha_i
        values["fecha_f"] = fecha_f
        mensaje = f"Los bls encontrados desde {fecha_i} hasta {fecha_f} son:"
    else:
        return {"mensaje":"debes usar el formato: para año AAAA, desde: AAAA-MM-DD, desde-hasta: AAAA-MM-DD+AAAA-MM-DD"}
        
    query += " order by b.code ,t.orden ;"
    results = await database.fetch_all(query=query, values=values)
    if not results:
        ver_info()
        raise HTTPException(status_code=404, detail="bls no encontrados")
    return {
        "mensaje":mensaje,
        "results":results
        }

@app.get("/bls/id/{id}")
async def ver_bls_id(id:int):
    query = """   
                select distinct b.id,b.code ,e.nombre as Etapa,n.nombre as Naviera,c.size,c.type,c.contenido,t.orden,t.terminal,t.status 
                ,p.locode,p.lugar,TO_CHAR(t.fecha, 'YYYY-MM-DD') as fecha
                from bls b 
                join etapa e on e.id =b.id_etapa
                join tracking t on t.id_bl = b.id 
                join paradas p on p.id = t.id_parada 
                join container_viaje cv on cv.id_bl = b.id 
                join containers c on c.id =cv.id_container 
                join navieras n on n.id =b.id_naviera 
                where b.id = :id
                order by t.orden;
            """
    result = await database.fetch_all(query=query, values={"id": id})
    
    # Si no se encuentra la naviera, devolver el listado completo con un mensaje
    if not result:
        ver_info()
        raise HTTPException(status_code=404, detail="ID de bl no encontrado")
    return result

@app.get("/bls/code/{code}")
async def ver_bls_id(code:str):
    query = """                
                select b.id,b.code as bl_code, e.nombre  as etapa, n.nombre  as naviera ,sb.descripcion_status as status, 
                TO_CHAR(b.fecha, 'YYYY-MM-DD') as fecha ,TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') as fecha_proxima_revision   
                from bls b --875.294
                join etapa e on e.id =b.id_etapa
                join navieras n on n.id =b.id_naviera
                join status_bl sb on b.id_status = sb.id
                where b.code like :code;
            """
    code = f"{code}%"
    result = await database.fetch_all(query=query, values={"code": code})
    
    # Si no se encuentra la naviera, devolver el listado completo con un mensaje
    if not result:
        ver_info()
        raise HTTPException(status_code=404, detail="Code de bl no encontrado")
    return result

@app.get("/bls/naviera/{nombre}")
async def ver_bls_id(nombre:str):
    query = """                
                select b.id,b.code as bl_code, e.nombre  as etapa, n.nombre  as naviera ,sb.descripcion_status as status, 
                TO_CHAR(b.fecha, 'YYYY-MM-DD') as fecha ,TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') as fecha_proxima_revision   
                from bls b --875.294
                join etapa e on e.id =b.id_etapa
                join navieras n on n.id =b.id_naviera
                join status_bl sb on b.id_status = sb.id
                where n.nombre like :nombre;
            """
    nombre = nombre.upper()
    nombre = f"{nombre}%"
    result = await database.fetch_all(query=query, values={"nombre": nombre})
    
    # Si no se encuentra la naviera, devolver el listado completo con un mensaje
    if not result:
        ver_info()
        raise HTTPException(status_code=404, detail="Naviera de bl no encontrado")
    return result

@app.get("/bls/etapa/{etapa}")
async def ver_bls_id(etapa:str):
    query = """                
                select b.id,b.code as bl_code, e.nombre  as etapa, n.nombre  as naviera ,sb.descripcion_status as status, 
                TO_CHAR(b.fecha, 'YYYY-MM-DD') as fecha ,TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') as fecha_proxima_revision   
                from bls b --875.294
                join etapa e on e.id =b.id_etapa
                join navieras n on n.id =b.id_naviera
                join status_bl sb on b.id_status = sb.id
                where e.nombre like :etapa;
            """
    etapa = etapa.upper()
    etapa = f"{etapa}%"
    result = await database.fetch_all(query=query, values={"etapa": etapa})
    
    # Si no se encuentra la naviera, devolver el listado completo con un mensaje
    if not result:
        ver_info()
        raise HTTPException(status_code=404, detail="Etapa de bl no encontrado")
    return result



#------------------------------------------
# ---------------REQUESTS------------------
#------------------------------------------

@app.get("/requests")
async def requests():
    query = """
            select r.id as id_request,h.id as id_html, b.code as bl_code,s.descripcion_status , r.mensaje,rr.descripcion as respuesta_request,
            b.fecha as fecha_bl, r.fecha as fecha_request   
            from requests r
            join html_descargados_temp h on r.id_html = h.id
            join respuesta_requests rr on rr.id = r.id_respuesta 
            join bls b on b.id = r.id_bl
            join status_bl s on s.id = b.id_status 
            where r.id <20
            """
    result = await database.fetch_all(query=query)
    if not result:
        ver_info()
        raise HTTPException(status_code=404, detail="La ruta no existe")
    return result

@app.get("/requests/{id_bl}")
async def requests_id_bl(id_bl: int):
    query = """
            select r.id as id_request,h.id as id_html, b.code as bl_code,s.descripcion_status , r.mensaje,rr.descripcion as respuesta_request,
            b.fecha as fecha_bl, r.fecha as fecha_request   
            from requests r
            join html_descargados_temp h on r.id_html = h.id
            join respuesta_requests rr on rr.id = r.id_respuesta 
            join bls b on b.id = r.id_bl
            join status_bl s on s.id = b.id_status 
            where b.id = :id_bl;
            """
    result = await database.fetch_all(query=query, values={"id_bl": id_bl})
    if not result:
        ver_info()
        raise HTTPException(status_code=404, detail="ID de bl no encontrado")
    return result

@app.get("/requests/{code}")
async def requests_code(code: str):
    query = """
            select r.id as id_request,h.id as id_html, b.code as bl_code,s.descripcion_status , r.mensaje,rr.descripcion as respuesta_request,
            b.fecha as fecha_bl, r.fecha as fecha_request   
            from requests r
            join html_descargados_temp h on r.id_html = h.id
            join respuesta_requests rr on rr.id = r.id_respuesta 
            join bls b on b.id = r.id_bl
            join status_bl s on s.id = b.id_status 
            where b.code = :code;
            """
    code = f"{code}%"
    result = await database.fetch_all(query=query, values={"code": code})
    if not result:
        ver_info()
        raise HTTPException(status_code=404, detail="Códdigo de bl no encontrado")
    return result


# 4. Ver informacion
@app.get("/info")
def ver_info():
    mensaje = {
        "1.":"Estas son las rutas relacionadas a bls",
        "1.0":"/bls",
        "1.1":"/bls/fecha/escribir_fecha",
        "1.2":"/bls/id/escribir_id",
        "1.3":"/bls/code/escribir_code",
        "2.":"Estas son las rutas relacionadas a requests",
        "2.0":"/requests",
        "2.1":"/request",
    }
    return mensaje

