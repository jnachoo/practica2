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
        SELECT cv.id as id_container_viaje,c.code AS container_code, b.code AS bl_code, c.size, c.type, c.contenido
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


@router.patch("/containers/{id_container_viaje}")
async def actualizar_parcial_container(
    id_container_viaje: int,
    container_code: str = Query(None),
    bl_code: str = Query(None),
    size: str = Query(None),
    type: str = Query(None),
    contenido: str = Query(None)
):
    # Construir la consulta dinámicamente
    fields_container = []
    values_container = {"id_container_viaje": id_container_viaje}
    fields_bls = []
    values_bls = {"id_container_viaje": id_container_viaje}

    if bl_code is not None:
        fields_bls.append("code = :bl_code")
        values_bls["bl_code"] = bl_code


    if container_code is not None:
        fields_container.append("code = :container_code")
        values_container["container_code"] = container_code
    if size is not None:
        fields_container.append("size = :size")
        values_container["size"] = size
    if type is not None:
        fields_container.append("type = :type")
        values_container["type"] = type
    if contenido is not None:
        fields_container.append("contenido = :contenido")
        values_container["contenido"] = contenido

    if not fields_container and not fields_bls:
        raise HTTPException(status_code=400, detail="No se proporcionaron campos para actualizar")

    # Bandera para saber si algo fue actualizado
    filas_actualizadas = 0

    # Ejecutar la consulta para la tabla tracking si hay campos
    if fields_container:
        query_container = f"""
            UPDATE containers
            SET {', '.join(fields_container)}
            WHERE id = (
                SELECT id_container
                FROM container_viaje
                WHERE id =:id_container_viaje
            )
            RETURNING id;
        """
        # Ejecutar la consulta
        resultado_c = await database.execute(query=query_container, values=values_container)
        if resultado_c : filas_actualizadas += 1
        print(f"Resultado del update container: {resultado_c}")

    # Ejecutar la consulta para la tabla tracking si hay campos
    if fields_bls:
        query_bls = f"""
            UPDATE bls
            SET {', '.join(fields_bls)}
            WHERE id = (
                SELECT id_bl
                FROM container_viaje
                WHERE id =:id_container_viaje
            )
            RETURNING id;
        """
        # Ejecutar la consulta
        resultado_b = await database.execute(query=query_bls, values=values_bls)
        if resultado_b : filas_actualizadas += 1
        print(f"Resultado del update container: {resultado_b}")

    # Validar si se actualizaron filas
    if filas_actualizadas == 0:
        raise HTTPException(status_code=400, detail="No se pudo actualizar nada")

    return {"message": "Actualización realizada con éxito"}
