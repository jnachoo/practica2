from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from databases import Database
from datetime import datetime
import asyncio 

# Configuración de la base de datos
DATABASE_URL = "postgresql://jpailamilla:practica.01%23@192.168.74.99:5432/ants_bl"
database = Database(DATABASE_URL)

app = FastAPI()

@app.get("/")
def leer_raiz():
    return ("BRAINS")

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
@app.get("/bls/{fecha}")
async def bls_fecha(fecha: str):

    query = """
                select b.bl_code as Codigo_bl, n.nombre as Naviera , b.fecha_bl as Fecha_bl,
                case
                    when b.etapa = 1 then 'exportacion'
                    when b.etapa = 2 then 'importacion'
                    else 'otro'
                end as etapa
                from bls b 
                join navieras n 
                on n.id = b.naviera_id 
                where 1=1
            """
    values = {}
    
    if fecha and len(fecha)==4:
        fecha = int(fecha)
        query += " AND EXTRACT(YEAR FROM b.fecha_bl) = :fecha"
        values["fecha"] = fecha
        mensaje = f"Los bls encontrados en el año {fecha} son:"
    elif fecha and len(fecha)==10:
        fecha = datetime.strptime(fecha, "%Y-%m-%d").date()
        query += " AND b.fecha_bl >= :fecha"
        values["fecha"] = fecha
        mensaje = f"Los bls encontrados desde {fecha} hasta el día de hoy son:"
    elif fecha and len(fecha)==21:
        nueva_fecha = fecha.split('+')
        fecha_i = datetime.strptime(nueva_fecha[0],"%Y-%m-%d").date()
        fecha_f = datetime.strptime(nueva_fecha[1],"%Y-%m-%d").date()
        query += " AND b.fecha_bl >= :fecha_i AND b.fecha_bl <= :fecha_f"
        values["fecha_i"] = fecha_i
        values["fecha_f"] = fecha_f
        mensaje = f"Los bls encontrados desde {fecha_i} hasta {fecha_f} son:"
    else:
        return {"mensaje":"debes usar el formato: para año AAAA, desde: AAAA-MM-DD, desde-hasta: AAAA-MM-DD+AAAA-MM-DD"}
        
    query += " ORDER BY b.fecha_bl;"
    results = await database.fetch_all(query=query, values=values)
    return {
        "mensaje":mensaje,
        "results":results
        }

# 2. Ver container por ID
@app.get("/container/id/{container_id}")
async def container_id(container_id: int):
    query = "SELECT * FROM containers WHERE id_container = :container_id;"
    result = await database.fetch_all(query=query, values={"container_id": container_id})
    if not result:
        raise HTTPException(status_code=404, detail="ID de container no encontrado")
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
    query = """ SELECT 'La naviera ' || n.nombre || ' tiene ' || COUNT(b.etapa) || ' exportaciones' AS mensaje
        FROM bls b join navieras n ON b.naviera_id = n.id WHERE b.etapa = :etapa and b.naviera_id = :naviera_id GROUP BY n.nombre;""" if impo_expo.lower() =="expo" else """
        SELECT 'La naviera ' || n.nombre || ' tiene ' || COUNT(b.etapa) || ' importaciones' AS mensaje
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
               SUM(CASE WHEN b.etapa = 1 THEN 1 ELSE 0 END) || ' exportaciones y ' || 
               SUM(CASE WHEN b.etapa = 2 THEN 1 ELSE 0 END) || ' importaciones' AS mensaje
        FROM bls b
        JOIN navieras n ON b.naviera_id = n.id
        WHERE b.naviera_id = :naviera_id
        GROUP BY n.nombre;
    """

    # Ejecución de la consulta
    result = await database.fetch_one(query=query, values={"naviera_id": naviera_id})
    if not result:
        raise HTTPException(status_code=404, detail="Item no encontrado")

    return result

