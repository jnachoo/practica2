from db.database import database
from typing import Dict, Any, Optional

async def fetch_containers(filters: Dict[str, Any], order_by: Optional[str] = None, 
                         order: str = "ASC", limit: int = 500, offset: int = 0):
    query = """
        SELECT cv.id as id_container_viaje,c.code AS container_code, 
               b.code AS bl_code, c.size, c.type, c.contenido
        FROM containers c
        JOIN container_viaje cv ON cv.id_container = c.id
        JOIN bls b ON b.id = cv.id_bl
        WHERE 1=1
    """
    values = {}

    if filters.get("codigo_container"):
        query += " AND c.code ILIKE :codigo_container"
        values["codigo_container"] = f"{filters['codigo_container']}%"
    if filters.get("bl_code"):
        query += " AND b.code ILIKE :bl_code"
        values["bl_code"] = f"{filters['bl_code']}%"
    if filters.get("size"):
        query += " AND c.size ILIKE :size"
        values["size"] = f"{filters['size']}%"
    if filters.get("type"):
        query += " AND c.type ILIKE :type"
        values["type"] = f"{filters['type']}%"
    if filters.get("contenido"):
        query += " AND c.contenido ILIKE :contenido"
        values["contenido"] = f"{filters['contenido']}%"

    if order_by:
        query += f" ORDER BY {order_by} {order}"

    query += " LIMIT :limit OFFSET :offset"
    values.update({"limit": limit, "offset": offset})

    return await database.fetch_all(query=query, values=values)

async def get_container_id(code: str) -> Optional[int]:
    query = "SELECT id FROM containers WHERE code = :code"
    return await database.fetch_val(query=query, values={"code": code})

async def get_container_size(size: str) -> Optional[str]:
    query = "SELECT nombre_size FROM dict_containers WHERE size = :size LIMIT 1"
    return await database.fetch_val(query=query, values={"size": size})

async def get_container_type(type: str) -> Optional[str]:
    query = "SELECT nombre_type FROM dict_containers WHERE type = :type LIMIT 1"
    return await database.fetch_val(query=query, values={"type": type})

async def update_container(id: int, fields: Dict[str, Any]) -> Optional[int]:
    set_clause = ", ".join(f"{k} = :{k}" for k in fields.keys())
    query = f"""
        UPDATE containers
        SET {set_clause}
        WHERE id = :id
        RETURNING id;
    """
    values = {**fields, "id": id}
    return await database.execute(query=query, values=values)

async def create_container(values: Dict[str, Any]) -> int:
    query = """
        INSERT INTO containers (code, size, type, contenido)
        VALUES (:code, :size, :type, :contenido)
        RETURNING id;
    """
    return await database.execute(query=query, values=values)

async def fetch_container_by_code(code: str, limit: int, offset: int):
    query = """
        SELECT cv.id as id_container_viaje, c.code AS container_code, 
               b.code AS bl_code, c.size, c.type, c.contenido
        FROM containers c
        JOIN container_viaje cv ON cv.id_container = c.id
        JOIN bls b ON b.id = cv.id_bl
        WHERE c.code LIKE :code
        LIMIT :limit OFFSET :offset;
    """
    return await database.fetch_all(
        query=query, 
        values={"code": f"{code}%", "limit": limit, "offset": offset}
    )

async def fetch_container_by_bl_code(code: str, limit: int, offset: int):
    query = """
        SELECT cv.id as id_container_viaje, c.code AS container_code, 
               b.code AS bl_code, c.size, c.type, c.contenido
        FROM containers c
        JOIN container_viaje cv ON cv.id_container = c.id
        JOIN bls b ON b.id = cv.id_bl
        WHERE b.code LIKE :code
        LIMIT :limit OFFSET :offset;
    """
    return await database.fetch_all(
        query=query, 
        values={"code": f"{code}%", "limit": limit, "offset": offset}
    )

async def create_container_viaje(id_container: int, id_bl: int) -> int:
    query = """
        INSERT INTO container_viaje(id_container, id_bl)
        VALUES (:id_container, :id_bl)
        RETURNING id;
    """
    return await database.execute(
        query=query, 
        values={"id_container": id_container, "id_bl": id_bl}
    )

async def update_container_viaje(
    id_container_viaje: int,
    fields: Dict[str, Any]
) -> Optional[int]:
    set_clause = ", ".join(f"{k} = :{k}" for k in fields.keys())
    query = f"""
        UPDATE container_viaje
        SET {set_clause}
        WHERE id = :id_container_viaje
        RETURNING id;
    """
    values = {**fields, "id_container_viaje": id_container_viaje}
    return await database.execute(query=query, values=values)

async def get_bl_id(bl_code: str) -> Optional[int]:
    query = "SELECT id FROM bls WHERE code = :bl_code"
    return await database.fetch_val(query=query, values={"bl_code": bl_code.upper()})