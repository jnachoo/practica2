from fastapi import APIRouter, HTTPException,Query,Body
from database import database
from datetime import datetime
from pydantic import BaseModel

router = APIRouter()

class Item(BaseModel):
    numero: int #solo numeros
    texto: str #cualquier cadena de texto
    booleano: bool #cualquier bool

# Ejemplo de url 
# http://localhost:8000/containers/?size=40&type=High&bl_code=238&order_by=c.code
@router.get("/containers/")
async def super_filtro_containers(
    codigo_container: str = Query(None),
    bl_code: str = Query(None),
    size: int = Query(None), 
    type: str = Query(None),  
    contenido: str = Query(None), 
    order_by: str = Query(None, regex="^(c\\.code|b\\.code|c\\.size|c\\.type|c\\.contenido)$"), 
    order: str = Query("ASC", regex="^(ASC|DESC|asc|desc)$"),  
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0),  # Índice de inicio, por defecto 0
):
    # Consulta base
    query = """
        SELECT c.code AS container_code, b.code AS bl_code, c.size, c.type, c.contenido
        FROM containers c
        JOIN container_viaje cv ON cv.id_container = c.id
        JOIN bls b ON b.id = cv.id_bl
        WHERE 1=1
    """
    values = {}

    # Agregar filtros dinámicos
    if codigo_container:
        query += " AND c.code ILIKE :codigo_container"
        values["codigo_container"] = f"{codigo_container}%"
    if bl_code:
        query += " AND b.code ILIKE :bl_code"
        values["bl_code"] = f"{bl_code}%"
    if size is not None:
        query += " AND c.size ILIKE :size"
        values["size"] = f"{size}%"
    if type:
        query += " AND c.type ILIKE :type"
        values["type"] = f"{type}%"
    if contenido:
        query += " AND c.contenido ILIKE :contenido"
        values["contenido"] = f"{contenido}%"

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
            raise HTTPException(status_code=404, detail="Containers no encontrados")
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta containers: {str(e)}"}

@router.get("/containers/code/{code}")
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
            raise HTTPException(status_code=404, detail="Containers no encontrados")
        return result
    except Exception as e: return {"error": f"Error al ejecutar la consulta containers:{str(e)}"}

@router.get("/containers/bl_code/{code}")
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
            raise HTTPException(status_code=404, detail="Containers no encontrados")
        return result
    except Exception as e: return {"error": f"Error al ejecutar la consulta containers:{str(e)}"}


@router.get("/containers/{code}")
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
            raise HTTPException(status_code=404, detail="Containers no encontrados")
        return result
    except Exception as e: return {"error": f"Error al ejecutar la consulta containers:{str(e)}"}


@router.patch("/containers/{container_id}")
async def actualizar_parcial_container(
    container_id: int,
    container_code: str = Body(None),
    size: str = Body(None),
    type: str = Body(None),
    contenido: str = Body(None),
):
    # Construir la consulta dinámicamente
    fields = []
    values = {"container_id": container_id}
    if container_code is not None:
        fields.append("code = :container_code")
        values["container_code"] = container_code
    if size is not None:
        fields.append("size = :size")
        values["size"] = size
    if type is not None:
        fields.append("type = :type")
        values["type"] = type
    if contenido is not None:
        fields.append("contenido = :contenido")
        values["contenido"] = contenido

    if not fields:
        raise HTTPException(status_code=400, detail="No se proporcionaron campos para actualizar")

    query = f"UPDATE containers SET {', '.join(fields)} WHERE id = :container_id"

    try:
        await database.execute(query=query, values=values)
        return {"message": "Container actualizado parcialmente"}
    except Exception as e:
        return {"error": f"Error al actualizar parcialmente el container: {str(e)}"}
