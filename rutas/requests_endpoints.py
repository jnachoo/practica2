from fastapi import APIRouter, HTTPException,Query
from database import database
from datetime import datetime

router = APIRouter()

# Ejemplo de url 
# http://localhost:8000/requests/?mensaje=exito&bl_status=Toda&bl_code=hlcuri&order_by=h.id&order=asc
@router.get("/requests/")
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
            raise HTTPException(status_code=404, detail="Datos no encontrados")
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta request: {str(e)}"}
    
@router.get("/requests/id_bl/{id_bl}")
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
            raise HTTPException(status_code=404, detail="ID de bl no encontrado")
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta filtro_request_id_bl: {str(e)}"}
    

@router.get("/requests/bl_code/{code}")
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
            raise HTTPException(status_code=404, detail="Código de bl no encontrado")
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta filtro_request_code_bl: {str(e)}"}
