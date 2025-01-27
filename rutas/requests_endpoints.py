from fastapi import APIRouter, HTTPException,Query
from database import database
from datetime import datetime
from urllib.parse import unquote

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
    order_by: str = Query(None, regex="^(r\\.id|h\\.id|b\\.code|s\\.descripcion_status|r\\.mensaje|rr\\.descripcion|b\\.fecha|r\\.fecha)$"),  # Campos válidos para ordenación
    order: str = Query("ASC", regex="^(ASC|DESC|asc|desc)$")    
):

    query = """
            select r.id as id_request,h.id as id_html, b.code as bl_code,
            s.descripcion_status , r.mensaje,rr.descripcion as respuesta_request,
            b.fecha as fecha_bl, r.fecha as fecha_request   
            from requests r
            join html_descargados h on r.id_html = h.id
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
            join html_descargados h on r.id_html = h.id
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
            join html_descargados h on r.id_html = h.id
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

@router.patch("/requests/{id_request}")
async def actualizar_parcial_request(
    id_request: int,
    #id_html: int = Query(None),
    bl_code: str = Query(None),
    descripcion_status: str = Query(None),
    mensaje: str = Query(None),
    descripcion_respuesta: str = Query(None),
    fecha_bl: str = Query(None),
    fecha_request: str = Query(None)
):
    # Construir las consultas dinámicamente
    fields_requests = []
    fields_html = []
    fields_bls = []
    fields_status = []
    fields_respuesta_requests = []

    values_requests = {"id_request": id_request}
    values_respuesta_requests = {}
    values_status = {}

    # Campos para la tabla `requests`
    if mensaje is not None:
        fields_requests.append("mensaje = :mensaje")
        values_requests["mensaje"] = mensaje
    if fecha_request is not None:
        fields_requests.append("fecha = :fecha_request")
        values_requests["fecha_request"] = fecha_request 

    # Campos para la tabla `html_descargados`
    #if id_html is not None:
    #    fields_html.append("id = :id_html")
    #    values_requests["id_html"] = id_html

    # Campos para la tabla `bls`
    if bl_code is not None:
        fields_bls.append("code = :bl_code")
        values_requests["bl_code"] = bl_code
    if fecha_bl is not None:
        fecha_bl = datetime.strptime(fecha_bl, "%Y-%m-%d").date()
        fields_bls.append("fecha = :fecha_bl")
        values_requests["fecha_bl"] = fecha_bl

    # Campos para la tabla `respuesta_requests`
    if descripcion_respuesta is not None:
        fields_respuesta_requests.append("descripcion = :descripcion_respuesta")
        values_requests["descripcion_respuesta"] = descripcion_respuesta
    
    # Campos para la tabla status_bl
    if descripcion_status is not None:
        descripcion_status = unquote(descripcion_status)
        fields_status.append("descripcion_status =:descripcion_status")
        values_requests["descripcion_status"] = descripcion_status

    # Validar que al menos un campo se proporcionó
    if not (fields_requests or fields_html or fields_bls or fields_respuesta_requests or fields_status):
        raise HTTPException(status_code=400, detail="No se proporcionaron campos para actualizar")

    # Bandera para saber si algo fue actualizado
    filas_actualizadas = 0

    # Ejecutar la consulta para la tabla `requests` si hay campos
    if fields_requests:
        query_requests = f"""
            UPDATE requests
            SET {', '.join(fields_requests)}
            WHERE id = :id_request
            RETURNING id;
        """
        resultado_requests = await database.execute(query=query_requests, values=values_requests)
        if resultado_requests:
            filas_actualizadas += 1
        print(f"Resultado del update requests: {resultado_requests}")


    # Ejecutar la consulta para la tabla `bls` si hay campos
    if fields_bls:
        # Asegúrate de que todas las claves necesarias están presentes
        query_bls = f"""
            UPDATE bls
            SET {', '.join(fields_bls)}
            WHERE id = (SELECT id_bl FROM requests WHERE id = :id_request)
            RETURNING id;
        """
        resultado_bls = await database.execute(query=query_bls, values=values_requests)
        if resultado_bls:
            filas_actualizadas += 1
        print(f"Resultado del update bls: {resultado_bls}")


    # Ejecutar la consulta para la tabla `respuesta_requests` si hay campos
    if fields_respuesta_requests:
        query_respuesta_requests = f"""
            UPDATE respuesta_requests
            SET {', '.join(fields_respuesta_requests)}
            WHERE id = (SELECT id_respuesta FROM requests WHERE id = :id_request)
            RETURNING id;
        """
        resultado_respuesta = await database.execute(query=query_respuesta_requests, values=values_requests)
        if resultado_respuesta:
            filas_actualizadas += 1
        print(f"Resultado del update respuesta_requests: {resultado_respuesta}")

    # Ejecutar la consulta para la tabla `html_descargados` si hay campos
    if fields_status:
        query_status = f"""
            UPDATE status_bl
            SET {', '.join(fields_status)}
            WHERE id = (SELECT id_status FROM bls WHERE id = (SELECT id_bl FROM requests WHERE id = :id_request))
            RETURNING id;
        """
        resultado_status = await database.execute(query=query_status, values=values_requests)
        if resultado_status:
            filas_actualizadas += 1
        print(f"Resultado del update status: {resultado_status}")

    # Validar si algo fue actualizado
    if filas_actualizadas == 0:
        raise HTTPException(status_code=404, detail="No se encontró el registro para actualizar")

    return {"mensaje": f"Se actualizaron {filas_actualizadas} tablas con éxito"}