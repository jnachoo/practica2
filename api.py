from fastapi import FastAPI, HTTPException, Query
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
async def ver_bls(
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
):
    query = """
                select b.id,b.code as bl_code, e.nombre  as etapa,b.pol,b.pod, n.nombre  as naviera ,sb.descripcion_status as status, 
                TO_CHAR(b.fecha, 'YYYY-MM-DD') as fecha ,TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') as fecha_proxima_revision   
                from bls b --875.294
                join etapa e on e.id =b.id_etapa
                join navieras n on n.id =b.id_naviera
                join status_bl sb on b.id_status = sb.id
                LIMIT :limit OFFSET :offset;
                """
    try:
        result = await database.fetch_all(query=query, values={"limit": limit, "offset": offset})
        if not result:
            ver_info()
            raise HTTPException(status_code=404, detail="Bl no encontrado")
        return result
    except Exception as e:
        return {"error": f"Error ejecutando la consulta bls: {str(e)}"}
#def get_bls():
#    return g.ver_bls()

@app.get("/bls/fecha/{fecha}")
async def bls_fecha(
    fecha: str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):

    query = """
                select b.id,b.code as bl_code, e.nombre  as etapa, b.pol,b.pod, n.nombre  as naviera ,sb.descripcion_status as status, 
                TO_CHAR(b.fecha, 'YYYY-MM-DD') as fecha ,TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') as fecha_proxima_revision   
                from bls b --875.294
                join etapa e on e.id =b.id_etapa
                join navieras n on n.id =b.id_naviera
                join status_bl sb on b.id_status = sb.id
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
        
        # Agregar orden, límite y desplazamiento
    query += " ORDER BY b.code LIMIT :limit OFFSET :offset"
    values["limit"] = limit
    values["offset"] = offset

    try:
        results = await database.fetch_all(query=query, values=values)
        if not results:
            ver_info()  # Función adicional que mencionaste
            raise HTTPException(status_code=404, detail="BLs no encontrados")
        return {
            "mensaje": mensaje,
            "results": results
        }
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta BLs: {str(e)}"}

@app.get("/bls/id/{id}")
async def ver_bls_id(
    id:int,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """   
                select b.id,b.code as bl_code, e.nombre  as etapa, b.pol,b.pod, n.nombre  as naviera ,sb.descripcion_status as status, 
                TO_CHAR(b.fecha, 'YYYY-MM-DD') as fecha ,TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') as fecha_proxima_revision   
                from bls b --875.294
                join etapa e on e.id =b.id_etapa
                join navieras n on n.id =b.id_naviera
                join status_bl sb on b.id_status = sb.id
                where b.id = :id
                LIMIT :limit OFFSET :offset;
            """
    
    # Si no se encuentra la naviera, devolver el listado completo con un mensaje
    try:
        result = await database.fetch_all(query=query, values={"id": id,"limit": limit, "offset": offset})
        if not result:
            ver_info()
            raise HTTPException(status_code=404, detail="ID de bl no encontrado")
        return result
    except Exception as e: return {"error": f"error al ejecutar la consulta filtro_id_bls{str(e)} "}

@app.get("/bls/code/{code}")
async def ver_bls_id(
    code:str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """                
                select b.id,b.code as bl_code, e.nombre  as etapa, b.pol,b.pod, n.nombre  as naviera ,sb.descripcion_status as status, 
                TO_CHAR(b.fecha, 'YYYY-MM-DD') as fecha ,TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') as fecha_proxima_revision   
                from bls b --875.294
                join etapa e on e.id =b.id_etapa
                join navieras n on n.id =b.id_naviera
                join status_bl sb on b.id_status = sb.id
                where b.code like :code
                LIMIT :limit OFFSET :offset;
            """
    code = f"{code}%"
    # Si no se encuentra la naviera, devolver el listado completo con un mensaje
    try:
        result = await database.fetch_all(query=query, values={"code": code,"limit": limit, "offset": offset})
        if not result:
            ver_info()
            raise HTTPException(status_code=404, detail="Code de bl no encontrado")
        return result
    except Exception as e: return {"error": f"error al ejecutar la consulta filtro_code_bls{str(e)} "}


@app.get("/bls/naviera/{nombre}")
async def ver_bls_id(
    nombre:str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """                
                select b.id,b.code as bl_code, e.nombre  as etapa, b.pol,b.pod, n.nombre  as naviera ,sb.descripcion_status as status, 
                TO_CHAR(b.fecha, 'YYYY-MM-DD') as fecha ,TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') as fecha_proxima_revision   
                from bls b --875.294
                join etapa e on e.id =b.id_etapa
                join navieras n on n.id =b.id_naviera
                join status_bl sb on b.id_status = sb.id
                where n.nombre like :nombre
                LIMIT :limit OFFSET :offset;
            """
    nombre = nombre.upper()
    nombre = f"{nombre}%"
    
    # Si no se encuentra la naviera, devolver el listado completo con un mensaje
    try:
        result = await database.fetch_all(query=query, values={"nombre": nombre,"limit": limit, "offset": offset})
        if not result:
            ver_info()
            raise HTTPException(status_code=404, detail="Naviera de bl no encontrado")
        return result
    except Exception as e: return {"error": f"error al ejecutar la consulta filtro_navieras_bls{str(e)} "}

@app.get("/bls/etapa/{etapa}")
async def ver_bls_id(
    etapa:str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """                
                select b.id,b.code as bl_code, e.nombre  as etapa,b.pol,b.pod, n.nombre  as naviera ,sb.descripcion_status as status, 
                TO_CHAR(b.fecha, 'YYYY-MM-DD') as fecha ,TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') as fecha_proxima_revision   
                from bls b --875.294
                join etapa e on e.id =b.id_etapa
                join navieras n on n.id =b.id_naviera
                join status_bl sb on b.id_status = sb.id
                where e.nombre like :etapa
                LIMIT :limit OFFSET :offset;
            """
    etapa = etapa.upper()
    etapa = f"{etapa}%"
    # Si no se encuentra la naviera, devolver el listado completo con un mensaje
    try:
        result = await database.fetch_all(query=query, values={"etapa": etapa,"limit": limit, "offset": offset})
        if not result:
            ver_info()
            raise HTTPException(status_code=404, detail="Etapa de bl no encontrado")
        return result
    except Exception as e: return {"error": f"error al ejecutar la consulta filtro_etapa_bls{str(e)} "}



#------------------------------------------
# ---------------REQUESTS------------------
#------------------------------------------

@app.get("/requests/")
async def requests(
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0),  # Índice de inicio, por defecto 0
    id_request: int = Query(None),
    id_html: int = Query(None),
    bl_code: str = Query(None),
    bl_status: str = Query(None),
    mensaje: str = Query(None),
    respuesta_request: str = Query(None),
    order_by: str = Query(None, regex="^(r\\.id|h\\.id|b\\.code|s\\.descripcion_status|r\\.mensaje|rr\\.descripcion|b\\.fecha)$"),  # Campos válidos para ordenación
    order: str = Query("ASC", regex="^(ASC|DESC|asc|desc)$")    
):

    query = """
            select r.id as id_request,h.id as id_html, b.code as bl_code,
            s.descripcion_status , r.mensaje,rr.descripcion as respuesta_request,
            b.fecha as fecha_bl, r.fecha as fecha_request   
            from requests r
            join html_descargados_temp h on r.id_html = h.id
            join respuesta_requests rr on rr.id = r.id_respuesta 
            join bls b on b.id = r.id_bl
            join status_bl s on s.id = b.id_status 
            where 1=1
            """
    # Agregar filtros dinámicos
    values = {}

    if id_request:
        query += " AND r.id = :id_request"
        values["id_request"] = id_request
    if id_html:
        query += " AND h.id = :id_html"
        values["id_html"] = id_html
    if bl_code:
        query += " AND b.code ILIKE :bl_code"
        values["bl_code"] = f"{bl_code}%"
    if bl_status:
        query += " AND s.descripcion_status ILIKE :bl_status"
        values["bl_status"] = f"{bl_status}%"
    if mensaje:
        query += " AND r.mensaje ILIKE :mensaje"
        values["mensaje"] = f"{mensaje}%"
    if respuesta_request:
        query += " AND rr.descripcion ILIKE :respuesta_request"
        values["respuesta_request"] = f"{respuesta_request}%"

    # Ordenación dinámica
    if order_by:
        query += f" ORDER BY {order_by} {order}"

    # Agregar límites y desplazamiento
    query += " LIMIT :limit OFFSET :offset"
    values["limit"] = limit
    values["offset"] = offset

    try:
        result = await database.fetch_all(query=query, values=values)
        if not result:
            ver_info()
            raise HTTPException(status_code=404, detail="Datos no encontrados")
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta request: {str(e)}"}
    

@app.get("/requests/id_bl/{id_bl}")
async def requests_id_bl(
    id_bl: int,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """
            select r.id as id_request,h.id as id_html, b.code as bl_code,s.descripcion_status , r.mensaje,rr.descripcion as respuesta_request,
            b.fecha as fecha_bl, r.fecha as fecha_request   
            from requests r
            join html_descargados_temp h on r.id_html = h.id
            join respuesta_requests rr on rr.id = r.id_respuesta 
            join bls b on b.id = r.id_bl
            join status_bl s on s.id = b.id_status 
            where b.id = :id_bl
            LIMIT :limit OFFSET :offset;
            """
    try:
        result = await database.fetch_all(query=query, values={"id_bl": id_bl, "limit": limit, "offset": offset})
        if not result:
            ver_info()
            raise HTTPException(status_code=404, detail="ID de bl no encontrado")
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta filtro_request_id_bl: {str(e)}"}
    

@app.get("/requests/bl_code/{code}")
async def requests_code(
    code: str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """
            select r.id as id_request,h.id as id_html, b.code as bl_code,s.descripcion_status , r.mensaje,rr.descripcion as respuesta_request,
            b.fecha as fecha_bl, r.fecha as fecha_request   
            from requests r
            join html_descargados_temp h on r.id_html = h.id
            join respuesta_requests rr on rr.id = r.id_respuesta 
            join bls b on b.id = r.id_bl
            join status_bl s on s.id = b.id_status 
            where b.code like :code
            LIMIT :limit OFFSET :offset;
            """
    code = code.upper()
    code = f"{code}%"
    try:
        result = await database.fetch_all(query=query, values={"code": code, "limit": limit, "offset": offset})
        if not result:
            ver_info()
            raise HTTPException(status_code=404, detail="Código de bl no encontrado")
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta filtro_request_code_bl: {str(e)}"}


#-------------------------------
#------------PARADAS------------
#-------------------------------

# http://localhost:8000/paradas/?locode=C&pais=chil&order_by=t.orden&bl_code=SCL
@app.get("/paradas/")
async def super_filtro_paradas(
    bl_code: str = Query(None),
    locode: str = Query(None),
    pais: str = Query(None),
    lugar: str = Query(None),
    is_pol: bool = Query(None),
    is_pod: bool = Query(None),
    orden: int = Query(None),
    status: str = Query(None),
    order_by: str = Query(None, regex="^(b\\.code|t\\.orden|t\\.status|p\\.locode|p\\.pais)$"),  # Campos válidos para ordenación
    order: str = Query("ASC", regex="^(ASC|DESC)$"), 
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
):

    # Consulta a la base de datos ants_api
    query = """
        SELECT b.code AS bl_code, t.orden, t.status, p.locode, p.pais, p.lugar,
               t.is_pol, t.is_pod
        FROM tracking t
        JOIN paradas p ON p.id = t.id_parada
        JOIN bls b ON b.id = t.id_bl
        WHERE 1=1
    """
    values = {}

    # Agregar filtros dinámicos
    if bl_code:
        query += " AND b.code ILIKE :bl_code"
        values["bl_code"] = f"{bl_code}%"
    if locode:
        query += " AND p.locode ILIKE :locode"
        values["locode"] = f"{locode}%"
    if pais:
        query += " AND p.pais ILIKE :pais"
        values["pais"] = f"{pais}%"
    if lugar:
        query += " AND p.lugar ILIKE :lugar"
        values["lugar"] = f"{lugar}%"
    if is_pol is not None:
        query += " AND t.is_pol = :is_pol"
        values["is_pol"] = bool(is_pol)
    if is_pod is not None:
        query += " AND t.is_pod = :is_pod"
        values["is_pod"] = bool(is_pod)
    if orden:
        query += " AND t.orden = :orden"
        values["orden"] = orden
    if status:
        query += " AND t.status ILIKE :status"
        values["status"] = f"{status}%"

    # Ordenación dinámica
    if order_by:
        query += f" ORDER BY {order_by} {order}"

    # Agregar límites y desplazamiento
    query += " LIMIT :limit OFFSET :offset"
    values["limit"] = limit
    values["offset"] = offset

    try:
        # Ejecutar la consulta
        result = await database.fetch_all(query=query, values=values)
        if not result:
            ver_info()  # Función que muestra todas las rutas posibles
            raise HTTPException(status_code=404, detail="Datos no encontrados")
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta: {str(e)}"}

    
@app.get("/paradas/bl_code/{bl_code}")
async def ver_paradas(
    bl_code: str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """
                select b.code as bl_code,t.orden,t.status,p.locode,p.pais,p.lugar,
                t.is_pol ,t.is_pod 
                from tracking t
                join paradas p on p.id = t.id_parada
                join bls b on b.id = t.id_bl
                where b.code like :bl_code 
                order by t.orden
                LIMIT :limit OFFSET :offset;
            """
    bl_code = bl_code.upper()
    bl_code = f"{bl_code}%"
    try:
        result = await database.fetch_all(query=query, values={"bl_code": bl_code, "limit": limit, "offset": offset})
        if not result:
            ver_info()
            raise HTTPException(status_code=404, detail="Paradas no encontradas")
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta paradas_filtro_bl_code: {str(e)}"}
    

@app.get("/paradas/locode/{locode}")
async def ver_paradas_locode(
    locode: str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """
                select b.code as bl_code,t.orden,t.status,p.locode,p.pais,p.lugar,
                t.is_pol ,t.is_pod 
                from tracking t
                join paradas p on p.id = t.id_parada
                join bls b on b.id = t.id_bl
                where p.locode like :locode 
                order by t.orden
                LIMIT :limit OFFSET :offset;
            """
    print(f"Valor locode enviado a la consulta: {locode}")
    locode = locode.upper()
    locode = f"{locode}%"
    try:
        print(f"Valor locode enviado a la consulta: {locode}")

        result = await database.fetch_all(query=query, values={"locode": locode, "limit": limit, "offset": offset})
        if not result:
            ver_info()
            raise HTTPException(status_code=404, detail="Paradas no encontradas")
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta paradas_filtro_locode: {str(e)}"}
    

@app.get("/paradas/pais/{pais}")
async def ver_paradas_pais(    
    pais: str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
):
    query = """
                select b.code as bl_code,t.orden,t.status,p.locode,p.pais,p.lugar,
                t.is_pol ,t.is_pod 
                from tracking t
                join paradas p on p.id = t.id_parada
                join bls b on b.id = t.id_bl
                where p.pais like :pais 
                order by t.orden
                LIMIT :limit OFFSET :offset;
            """
    pais = pais.upper()
    pais = f"{pais}%"
    try:
        result = await database.fetch_all(query=query, values={"pais": pais, "limit": limit, "offset": offset})
        if not result:
            ver_info()
            raise HTTPException(status_code=404, detail="Paradas no encontradas")
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta paradas_filtro_pais: {str(e)}"}
    
#-------------------------------------------
# ---------------CONTAINERS-----------------
#-------------------------------------------

@app.get("/containers")
async def ver_container(
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
):
    query = """
                select c.code as container_code , b.code as bl_code ,c.size,c.type,c.contenido
                from containers c 
                join container_viaje cv on cv.id_container = c.id 
                join bls b on b.id = cv.id_bl
                LIMIT :limit OFFSET :offset;
            """
    try:
        result = await database.fetch_all(query=query, values={"limit": limit, "offset": offset})
        if not result:
            ver_info()
            raise HTTPException(status_code=404, detail="Containers no encontrados")
        return result
    except Exception as e: return {"error": f"Error al ejecutar la consulta containers:{str(e)}"}

@app.get("/containers/code/{code}")
async def ver_container(
    code : str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """
                select c.code as container_code , b.code as bl_code ,c.size,c.type,c.contenido
                from containers c 
                join container_viaje cv on cv.id_container = c.id 
                join bls b on b.id = cv.id_bl
                where c.code like :code
                LIMIT :limit OFFSET :offset;
            """
    code = f"{code}%"
    try:
        result = await database.fetch_all(query=query, values={"code":code, "limit": limit, "offset": offset})
        if not result:
            ver_info()
            raise HTTPException(status_code=404, detail="Containers no encontrados")
        return result
    except Exception as e: return {"error": f"Error al ejecutar la consulta containers:{str(e)}"}

@app.get("/containers/bl_code/{code}")
async def ver_container(
    code : str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """
                select c.code as container_code , b.code as bl_code ,c.size,c.type,c.contenido
                from containers c 
                join container_viaje cv on cv.id_container = c.id 
                join bls b on b.id = cv.id_bl
                where b.code like :code
                LIMIT :limit OFFSET :offset;
            """
    code = f"{code}%"
    try:
        result = await database.fetch_all(query=query, values={"code":code, "limit": limit, "offset": offset})
        if not result:
            ver_info()
            raise HTTPException(status_code=404, detail="Containers no encontrados")
        return result
    except Exception as e: return {"error": f"Error al ejecutar la consulta containers:{str(e)}"}

# Multi-filtro futuro
@app.get("/containers/{code}")
async def ver_container(
    code : str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """
                select c.code as container_code , b.code as bl_code ,c.size,c.type,c.contenido
                from containers c 
                join container_viaje cv on cv.id_container = c.id 
                join bls b on b.id = cv.id_bl 
                where c.code like :code
                LIMIT :limit OFFSET :offset;
            """
    code = f"{code}%"
    try:
        result = await database.fetch_all(query=query, values={"code":code, "limit": limit, "offset": offset})
        if not result:
            ver_info()
            raise HTTPException(status_code=404, detail="Containers no encontrados")
        return result
    except Exception as e: return {"error": f"Error al ejecutar la consulta containers:{str(e)}"}
    
#-------------------------------
#----------INFORMACIÓN----------
#-------------------------------
@app.get("/info")
def ver_info():
    mensaje = {
        "1.":"Estas son las rutas relacionadas a bls",
        "1.0":"/bls",
        "1.1":"/bls/fecha/escribir_fecha",
        "1.2":"/bls/id/escribir_id",
        "1.3":"/bls/code/escribir_code",
        "1.4":"/bls/etapa/escribir_etapa",
        "1.5":"/bls/naviera/escribir_naviera",
        "2.":"Estas son las rutas relacionadas a requests",
        "2.0":"/requests",
        "2.1":"/requests/id_bl/escribir_id_bl",
        "2.2":"/requests/bl_code/escribir_bl_code",
        "3.":"Estas son las rutas relacionadas a paradas",
        "3.0":"/paradas",
        "3.1":"/paradas/bl_code/escribir_bl_code",
        "3.2":"/paradas/locode/escribir_locode",
        "3.3":"/paradas/pais/escribir_pais",
        "4.":"Estas son las rutas relacionadas a validación en línea",
        "4.1":"/validacion_locode_nulo",
        "4.2":"/validacion_cruce_contenedores",
        "4.3":"/validacion_bls_repetidos",
        "4.4":"/validacion_paradas_pol_pod",
        "4.5":"/validacion_orden_repetida",
        "4.6":"/validacion_impo_distinta_CL",
        "4.7":"/validacion_bls_impo",
        "4.8":"/validacion_expo_distinta_CL",
        "4.9":"/validacion_bls_expo",
        "4.10":"/validacion_paradas_expo",
        "4.11":"/validacion_dias_impo",
        "4.12":"/validacion_requests_expo",
        "5.":"Estas son las validaciones de tendencia",
        "5.1":"/tendencia_navieras/escribir_nombre_naviera",
        "5.2":"/tendencia_etapa/escribir_numero_etapa",
        "5.3":"/tendencia_contenedor_dryreefer/escribir_nombre_contenedor",
        "5.4":"/tendencia_por_origen/escribir_locode",
        "5.5":"/tendencia_por_destino/escribir_locode", 
    }
    return mensaje

#---------------------------------
#-------VALIDACION EN LINEA-------
#---------------------------------

@app.get("/validacion_locode_nulo")
async def val():
    query = """
                SELECT * FROM verificar_locode_nulo();
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de locode nulo."}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de locode nulo: {str(e)}"}


@app.get("/validacion_cruce_contenedores")
async def val():
    query = """
                SELECT * FROM consultar_cruce_contenedores('2022-01-06', '2025-01-06');
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de cruce con diccionario de contenedores."}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación del cruce con diccionario de contenedores: {str(e)}"}
    
@app.get("/validacion_bls_repetidos")
async def val():
    query = """
                 SELECT * FROM consultar_bls_repetidos();
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de bls repetidos."}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de bls repetidos: {str(e)}"}
    
@app.get("/validacion_paradas_pol_pod")
async def val():
    query = """
                SELECT * FROM obtener_paradas_pol_pod();
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de paradas que sean pol y pod."}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta: {str(e)}"}
    
@app.get("/validacion_orden_repetida")
async def val():
    query = """
                 SELECT * FROM obtener_paradas_con_orden_repetida();
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de orden repetida en la tabla de paradas."}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de orden repetida en tabla de paradas: {str(e)}"}
   
@app.get("/validacion_impo_distinta_CL")
async def val():
    query = """
                 SELECT * FROM verificar_registros_etapa2_pais_distinto_cl();
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de parada POD distinta a Chile en importación."}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de parada POD distinta a Chile en importación: {str(e)}"}
    
@app.get("/validacion_bls_impo")
async def val():
    query = """
                 SELECT * FROM validar_bls_impo(2);
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de país de destino dentro de ('CL', 'AR', 'BO', 'PY', 'UY')"}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de país de destino dentro de ('CL', 'AR', 'BO', 'PY', 'UY'): {str(e)}"}

@app.get("/validacion_expo_distinta_CL")
async def val():
    query = """
                 SELECT * FROM verificar_registros_etapa1_pais_distinto_cl();
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de parada POL distinta a Chile en exportación"}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de parada POL distinta a Chile en exportación: {str(e)}"} 

@app.get("/validacion_bls_expo")
async def val():
    query = """
                 SELECT * FROM validar_bls_expo(1);
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de país de origen dentro de ('CL', 'AR', 'BO')"}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de país de origen dentro de ('CL', 'AR', 'BO'): {str(e)}"}
    
@app.get("/validacion_paradas_expo")
async def val():
    query = """
                SELECT * FROM obtener_paradas_con_validacion();
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de país de destino y POD distinto de Chile"}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de país de destino y POD distinto de Chile: {str(e)}"}


@app.get("/validacion_dias_impo")
async def val():
    query = """
                SELECT * FROM obtener_diferencia_requests_importacion();
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de request en importaciones"}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de request en importaciones: {str(e)}"}
    
@app.get("/validacion_requests_expo")
async def val():
    query = """
                SELECT * FROM obtener_requests_expo();
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de request en exportaciones"}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de request en exportaciones: {str(e)}"}
    
    #locode = f"'{locode}'"

#-------------------------------------------
#----------VALIDACIONES TENDENCIA-----------
#-------------------------------------------

@app.get("/tendencia_navieras/{nombre}")
async def tendencia_navieras(nombre: str):
    query = """
                SELECT 
                    n.nombre,
                    SUM(oc.c20 + oc.c40 * 2) AS teus
                FROM output_containers oc
                LEFT JOIN bls b ON b.code = oc.codigo 
                LEFT JOIN navieras n ON n.id = b.id_naviera 
                WHERE n.nombre ILIKE :nombre
                GROUP BY
                    n.nombre
                HAVING SUM(oc.c20 + oc.c40 * 2) > 0;
                """
    nombre = f"{nombre}%"
    try:
        # Ejecutamos la consulta pasando el parámetro 'nombre'
        result = await database.fetch_all(query=query, values={"nombre": nombre})
        
        # Verificamos si no hay resultados
        if not result:
            return {"message": "No existen datos que cumplan con la naviera seleccionada"}
        
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}

@app.get("/tendencia_etapa/{etapa}")
async def tendencia_etapa(etapa: str):
    query = """
            SELECT 
                n.nombre,
                b.id_etapa,
                SUM(oc.c20 + oc.c40 * 2) AS teus
            FROM output_containers oc
            LEFT JOIN bls b ON b.code = oc.codigo
            LEFT JOIN navieras n ON n.id = b.id_naviera
            WHERE b.id_etapa = :etapa
            GROUP BY
                n.nombre,
                b.id_etapa
            HAVING SUM(oc.c20 + oc.c40 * 2) > 0;
                """
    etapa = f"{etapa}%"
    try:
        # Ejecutamos la consulta pasando el parámetro 'nombre'
        result = await database.fetch_all(query=query, values={"etapa": etapa})
        
        # Verificamos si no hay resultados
        if not result:
            return {"message": "No existen datos que cumplan con la etapa seleccionada"}
        
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}

@app.get("/tendencia_contenedor_dryreefer/{contenido}")
async def tendencia_contenedor_dryreefer(contenido: str):
    query = """
                SELECT 
                n.nombre,
                oc."dry/reefer",
                SUM(oc.c20 + oc.c40 * 2) AS teus
            FROM output_containers oc
            JOIN bls b on b.code = oc.codigo
            LEFT JOIN navieras n ON n.id = b.id_naviera
            WHERE oc."dry/reefer" ILIKE :contenido
            GROUP BY
                n.nombre,
                oc."dry/reefer"
            HAVING SUM(oc.c20 + oc.c40 * 2) > 0;;
                """
    contenido = f"{contenido}%"
    try:
        # Ejecutamos la consulta pasando el parámetro 'nombre'
        result = await database.fetch_all(query=query, values={"contenido": contenido})
        
        # Verificamos si no hay resultados
        if not result:
            return {"message": "No existen datos que cumplan con el tipo de contenido seleccionado"}
        
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}
    
@app.get("/tendencia_por_origen/{origen_locode}")
async def tendencia_por_origen(origen_locode: str):
    query = """
            SELECT DISTINCT
                n.nombre,
                SUM(oc.c20 + oc.c40 * 2) AS teus,
                p.locode AS o
            FROM output_containers oc
            LEFT JOIN bls b ON b.code = oc.codigo
            LEFT JOIN navieras n ON n.id = b.id_naviera 
            LEFT JOIN tracking t ON t.id = oc.id_origen 
            LEFT JOIN paradas p  ON p.id = t.id_parada 
            WHERE p.locode ILIKE :origen_locode
            GROUP BY
                n.nombre,
                p.locode
            HAVING SUM(oc.c20 + oc.c40 * 2) > 0;
            """
    origen_locode = f"{origen_locode}%"
    try:
        # Ejecutamos la consulta pasando el parámetro 'nombre'
        result = await database.fetch_all(query=query, values={"origen_locode": origen_locode})
        
        # Verificamos si no hay resultados
        if not result:
            return {"message": "No existen datos que cumplan con el origen seleccionado"}
        
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}
    
        
@app.get("/tendencia_por_destino/{destino_locode}")
async def tendencia_por_destino(destino_locode: str):
    query = """
            SELECT DISTINCT
                n.nombre,
                SUM(oc.c20 + oc.c40 * 2) AS teus,
                p.locode AS o
            FROM output_containers oc
            LEFT JOIN bls b ON b.code = oc.codigo
            LEFT JOIN navieras n ON n.id = b.id_naviera 
            LEFT JOIN tracking t ON t.id = oc.id_destino 
            LEFT JOIN paradas p  ON p.id = t.id_parada 
            WHERE p.locode ILIKE :destino_locode
            GROUP BY
                n.nombre,
                p.locode
            HAVING SUM(oc.c20 + oc.c40 * 2) > 0;
            """
    destino_locode = f"{destino_locode}%"
    try:
        # Ejecutamos la consulta pasando el parámetro 'nombre'
        result = await database.fetch_all(query=query, values={"destino_locode": destino_locode})
        
        # Verificamos si no hay resultados
        if not result:
            return {"message": "No existen datos que cumplan con el destino seleccionado"}
        
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}