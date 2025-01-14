from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from databases import Database
from datetime import datetime
import asyncio 

import os
from databases import Database

# Obtén la URL de la base de datos desde las variables de entorno
DATABASE_URL = os.getenv("DATABASE_URL")

# Conecta la base de datos
database = Database(DATABASE_URL)

app = FastAPI()

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

# 1. Leer todos los registros / FUNCIONA
@app.get("/bls")
async def ver_bls():
    query = """
                select coalesce(b.bl_code, 'no se encuentra') as Codigo_bl,
                coalesce (n.nombre, 'no se encuentra') as Naviera , coalesce (c.code, 'no se encuetra') as codigo_container,
                coalesce (dc.nombre_type ||' '|| dc.dryreef, 'no se encuentra') as tipo,
                coalesce (dc.nombre_size, 'no se encuentra') as size,
                case
                    when b.etapa = 1 then 'exportacion'
                    when b.etapa = 2 then 'importacion'
                    else 'otro'
                end as etapa,
                coalesce(c.pol, 'no se encuentra') as pol, coalesce(c.pod, 'no se encuentra') as pod,
                coalesce(b.mercado, 'no se encuentra') as mercado,
                coalesce(b.fecha_bl::text , 'no se encuentra') as Fecha_bl
                from bls b 
                join navieras n 
                on n.id = b.naviera_id 
                join containers c 
                on c.bl_id = b.id 
                join dict_containers dc 
                on dc.size = c.size and dc.type = c.type 
                where b.id <29235;
            """
    result = await database.fetch_all(query=query)
    
    # Si no se encuentra la naviera, devolver el listado completo con un mensaje
    if not result:
        ver_info()
        raise HTTPException(status_code=404, detail="En estos momentos no se puede realizar esta operación")
    return result

@app.get("/bls/fecha/{fecha}")
async def bls_fecha(fecha: str):

    query = """
                select b.id,b.code,e.nombre,n.nombre,c.size,c.type,c.contenido,t.orden,t.terminal ,t.status ,p.locode,p.lugar ,t.fecha from bls b 
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
            select coalesce(b.bl_code, 'no se encuentra') as Codigo_bl,
                coalesce (n.nombre, 'no se encuentra') as Naviera , coalesce (c.code, 'no se encuetra') as codigo_container,
                coalesce (dc.nombre_type ||' '|| dc.dryreef, 'no se encuentra') as tipo,
                coalesce (dc.nombre_size, 'no se encuentra') as size,
                case
                    when b.etapa = 1 then 'exportacion'
                    when b.etapa = 2 then 'importacion'
                    else 'otro'
                end as etapa,
                coalesce(c.pol, 'no se encuentra') as pol, coalesce(c.pod, 'no se encuentra') as pod,
                coalesce(b.mercado, 'no se encuentra') as mercado,
                coalesce(b.fecha_bl::text , 'no se encuentra') as Fecha_bl
                from bls b 
                join navieras n 
                on n.id = b.naviera_id 
                join containers c 
                on c.bl_id = b.id 
                join dict_containers dc 
                on dc.size = c.size and dc.type = c.type
                where b.id = :id;
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
                select coalesce(b.bl_code, 'no se encuentra') as Codigo_bl,
                coalesce (n.nombre, 'no se encuentra') as Naviera , coalesce (c.code, 'no se encuetra') as codigo_container,
                coalesce (dc.nombre_type ||' '|| dc.dryreef, 'no se encuentra') as tipo,
                coalesce (dc.nombre_size, 'no se encuentra') as size,
                case
                    when b.etapa = 1 then 'exportacion'
                    when b.etapa = 2 then 'importacion'
                    else 'otro'
                end as etapa,
                coalesce(c.pol, 'no se encuentra') as pol, coalesce(c.pod, 'no se encuentra') as pod,
                coalesce(b.mercado, 'no se encuentra') as mercado,
                coalesce(b.fecha_bl::text , 'no se encuentra') as Fecha_bl
                from bls b 
                join navieras n 
                on n.id = b.naviera_id 
                join containers c 
                on c.bl_id = b.id 
                join dict_containers dc 
                on dc.size = c.size and dc.type = c.type
                where b.bl_code like :code;
            """
    code = f"{code}%"
    result = await database.fetch_all(query=query, values={"code": code})
    
    # Si no se encuentra la naviera, devolver el listado completo con un mensaje
    if not result:
        ver_info()
        raise HTTPException(status_code=404, detail="Code de bl no encontrado")
    return result

@app.get("/requests")
async def requests():
    query = """
            select b.id, b.bl_code ,bsd.state_description as caso_bl,b.fecha_bl, 
            rcd.case_description as caso_request , date(r.timestamp) as fecha_request
            from bls b
            join bl_state_dictionary bsd 
            on bsd.state_code = b.state_code
            join requests r 
            on r.bl_id = b.id 
            join request_case_dictionary rcd 
            on rcd.case_code = r.response_code
            order by b.fecha_bl desc;
            """
    result = await database.fetch_all(query=query)
    if not result:
        ver_info()
        raise HTTPException(status_code=404, detail="La ruta no existe")
    return result

# 2. Ver requests
@app.get("/requests/{bl_id}")
async def requests_bl_id(bl_id: int):
    query = """
            select b.id, b.bl_code ,bsd.state_description as caso_bl,b.fecha_bl, 
            rcd.case_description as caso_request , date(r.timestamp) as fecha_request
            from bls b
            join bl_state_dictionary bsd 
            on bsd.state_code = b.state_code
            join requests r 
            on r.bl_id = b.id 
            join request_case_dictionary rcd 
            on rcd.case_code = r.response_code
            where b.id = :id;
            """
    result = await database.fetch_all(query=query, values={"id": bl_id})
    if not result:
        ver_info()
        raise HTTPException(status_code=404, detail="ID de bl no encontrado")
    return result

# 3. Ver container por code
@app.get("/container/code/{container_codigo}")
async def container_code(container_codigo: str):
    query = "SELECT * FROM containers WHERE code = :texto;"
    result = await database.fetch_one(query=query, values={"texto": container_codigo})
    if not result:
        raise HTTPException(status_code=404, detail="Codigo de container no encontrado")
    return result

# 4. Ver container por bl_id
@app.get("/container/bl_id/{bl_id}")
async def container_bl_id(bl_id: int):
    query = "SELECT * FROM containers WHERE bl_id = :id;"
    result = await database.fetch_one(query=query, values={"id": bl_id})
    if not result:
        raise HTTPException(status_code=404, detail="ID de bl no encontrado")
    return result

# 5. Ver navieras
@app.get("/navieras/{naviera_id}")
async def navieras_todas(naviera_id: int):
    # Consulta para buscar la naviera por ID
    query = "SELECT id, nombre FROM navieras WHERE id = :naviera_id;"
    result = await database.fetch_all(query=query, values={"naviera_id": naviera_id})
    
    # Si no se encuentra la naviera, devolver el listado completo con un mensaje
    if not result:
        query = "SELECT id, nombre FROM navieras order by id;"
        result = await database.fetch_all(query=query, values={})
        return {
            "mensaje": "La naviera con el ID proporcionado no fue encontrada. El listado completo de navieras es:",
            "navieras": result
        }
    
    # Si se encuentra la naviera, devolver el resultado
    return result

# 6. Ver expo / impo
@app.get("/navieras/{naviera_id}/{impo_expo}")
async def navieras_impo_expo(impo_expo: str, naviera_id: int):
    # Validación del id
    if naviera_id>13 and naviera_id<0:
        raise HTTPException(status_code=400, detail="ID de naviera no válido")

    # Determinar etapa (1 para 'expo', 2 para 'impo')
    etapa = 1 if impo_expo.lower() == "expo" else 2 if impo_expo.lower() == "impo" else None
    if etapa is None:
        raise HTTPException(status_code=400, detail="Código debe ser 'expo' para exportación o 'impo' para importación")

    # Obtener el nombre de la naviera correspondiente al ID
    #naviera_nombre = navieras[id]

    # Consulta SQL
    query = """ SELECT 'La naviera ' || n.nombre || ' tiene ' || COUNT(b.id_etapa) || ' exportaciones' AS mensaje
        FROM bls b join navieras n ON b.id_naviera = n.id WHERE b.id_etapa = :etapa and b.id_naviera = :naviera_id GROUP BY n.nombre;""" if impo_expo.lower() =="expo" else """
        SELECT 'La naviera ' || n.nombre || ' tiene ' || COUNT(b.id_etapa) || ' importaciones' AS mensaje
        FROM bls b join navieras n ON b.naviera_id = n.id WHERE b.etapa = :etapa and b.naviera_id = :naviera_id GROUP BY n.nombre""" 

    # Ejecución de la consulta
    result = await database.fetch_one(query=query, values={ "etapa": etapa, "naviera_id": naviera_id})
    if not result:
        raise HTTPException(status_code=404, detail="Item no encontrado")

    return result


@app.get("/naviera/{naviera_id}")
async def naviera(naviera_id: int):
    # Validación del ID de naviera
    if naviera_id > 13 or naviera_id < 1:
        raise HTTPException(status_code=400, detail="ID de naviera no válido, ingresar entre 1 y 13")

    # Consulta SQL para contar exportaciones e importaciones
    query = """
        SELECT 'La naviera ' || n.nombre || ' tiene ' || 
               SUM(CASE WHEN b.id_etapa = 1 THEN 1 ELSE 0 END) || ' exportaciones y ' || 
               SUM(CASE WHEN b.id_etapa = 2 THEN 1 ELSE 0 END) || ' importaciones' AS mensaje
        FROM bls b
        JOIN navieras n ON b.id_naviera = n.id
        WHERE b.id_naviera = :naviera_id
        GROUP BY n.nombre;
    """

    # Ejecución de la consulta
    result = await database.fetch_one(query=query, values={"naviera_id": naviera_id})
    if not result:
        ver_info()
        raise HTTPException(status_code=404, detail="Naviera no encontrada")

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
        "3.":"/naviera/escrbir_id_naviera"
    }
    return mensaje

