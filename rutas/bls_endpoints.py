from fastapi import APIRouter, HTTPException,Query
from database import database
from datetime import datetime

router = APIRouter()

#-----------GET----------
#-----------GET----------
#-----------GET----------

# Ejemplo de url
# http://localhost:8000/bls/?pol=CL&order_by=b.fecha&order=desc&fecha=2024-07-20&etapa=expo
@router.get("/bls/")
async def super_filtro_bls(
    bl_code: str = Query(None),  # Código del BL
    etapa: str = Query(None),  # Nombre de la etapa
    pol: str = Query(None),  # Puerto de origen
    pod: str = Query(None),  # Puerto de destino
    naviera: str = Query(None),  # Nombre de la naviera
    status: str = Query(None),  # Descripción del estado
    fecha: str = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$"),  # Fecha en formato YYYY-MM-DD
    fecha_proxima_revision: str = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$"),  # Próxima revisión
    order_by: str = Query(None,
        regex=r"^(b\.code|e\.nombre|b\.pol|b\.pod|n\.nombre|sb\.descripcion_status|b\.fecha|b\.proxima_revision)$"), # Campos válidos para ordenación
    order: str = Query("ASC", regex="^(ASC|DESC|asc|desc)$"),  # Dirección de ordenación
    limit: int = Query(500, ge=1),  # Número de resultados por página
    offset: int = Query(0, ge=0),  # Índice de inicio
):
    # Consulta base
    query = """
        SELECT b.id, b.code AS bl_code, e.nombre AS etapa, b.pol, b.pod, n.nombre AS naviera, 
               sb.descripcion_status AS status, 
               TO_CHAR(b.fecha, 'YYYY-MM-DD') AS fecha, 
               TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') AS fecha_proxima_revision
        FROM bls b
        JOIN etapa e ON e.id = b.id_etapa
        JOIN navieras n ON n.id = b.id_naviera
        JOIN status_bl sb ON b.id_status = sb.id
        WHERE 1=1
    """
    values = {}

    # Agregar filtros dinámicos
    if bl_code:
        query += " AND b.code ILIKE :bl_code"
        values["bl_code"] = f"{bl_code}%"
    if etapa:
        query += " AND e.nombre ILIKE :etapa"
        values["etapa"] = f"{etapa}%"
    if pol:
        query += " AND b.pol ILIKE :pol"
        values["pol"] = f"{pol}%"
    if pod:
        query += " AND b.pod ILIKE :pod"
        values["pod"] = f"{pod}%"
    if naviera:
        query += " AND n.nombre ILIKE :naviera"
        values["naviera"] = f"{naviera}%"
    if status:
        query += " AND sb.descripcion_status ILIKE :status"
        values["status"] = f"{status}%"
    if fecha:
        query += " AND TO_CHAR(b.fecha, 'YYYY-MM-DD') = :fecha"
        values["fecha"] = fecha
    if fecha_proxima_revision:
        query += " AND TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') = :fecha_proxima_revision"
        values["fecha_proxima_revision"] = fecha_proxima_revision

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
            raise HTTPException(status_code=404, detail="BL no encontrado")
        return result
    except Exception as e:
        return {"error": f"Error ejecutando la consulta bls: {str(e)}"}


@router.get("/bls/fecha/{fecha}")
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
              # Función adicional que mencionaste
            raise HTTPException(status_code=404, detail="BLs no encontrados")
        return {
            "mensaje": mensaje,
            "results": results
        }
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta BLs: {str(e)}"}

@router.get("/bls/id/{id}")
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
            
            raise HTTPException(status_code=404, detail="ID de bl no encontrado")
        return result
    except Exception as e: return {"error": f"error al ejecutar la consulta filtro_id_bls{str(e)} "}

@router.get("/bls/code/{code}")
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
            
            raise HTTPException(status_code=404, detail="Code de bl no encontrado")
        return result
    except Exception as e: return {"error": f"error al ejecutar la consulta filtro_code_bls{str(e)} "}


@router.get("/bls/naviera/{nombre}")
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
            
            raise HTTPException(status_code=404, detail="Naviera de bl no encontrado")
        return result
    except Exception as e: return {"error": f"error al ejecutar la consulta filtro_navieras_bls{str(e)} "}

@router.get("/bls/etapa/{etapa}")
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
            raise HTTPException(status_code=404, detail="Etapa de bl no encontrado")
        return result
    except Exception as e: return {"error": f"error al ejecutar la consulta filtro_etapa_bls{str(e)} "}

#-----------POST----------
#-----------POST----------
#-----------POST----------

