from db.database import database
from datetime import datetime

async def fetch_bls(filters: dict, order_by: str = None, order: str = "ASC", limit: int = 500, offset: int = 0):
    query = """
        SELECT b.id, b.code AS bl_code, e.nombre AS etapa, n.nombre AS naviera, 
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
    
    if filters.get("bl_code"):
        query += " AND b.code ILIKE :bl_code"
        values["bl_code"] = f"{filters['bl_code']}%"
    if filters.get("etapa"):
        query += " AND e.nombre ILIKE :etapa"
        values["etapa"] = f"{filters['etapa']}%"
    if filters.get("naviera"):
        query += " AND n.nombre ILIKE :naviera"
        values["naviera"] = f"{filters['naviera']}%"
    if filters.get("status"):
        query += " AND sb.descripcion_status ILIKE :status"
        values["status"] = f"{filters['status']}%"
    if filters.get("fecha"):
        query += " AND TO_CHAR(b.fecha, 'YYYY-MM-DD') = :fecha"
        values["fecha"] = filters['fecha']
    if filters.get("fecha_proxima_revision"):
        query += " AND TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') = :fecha_proxima_revision"
        values["fecha_proxima_revision"] = filters['fecha_proxima_revision']
        
    if order_by:
        query += f" ORDER BY {order_by} {order}"
    
    query += " LIMIT :limit OFFSET :offset"
    values["limit"] = limit
    values["offset"] = offset

    return await database.fetch_all(query=query, values=values)

async def fetch_dropdown(query: str):
    result = await database.fetch_all(query)
    if not result:
        return None
    return result

async def fetch_bl_by_id(id: int, limit: int, offset: int):
    query = """   
                SELECT b.id, b.code AS bl_code, e.nombre AS etapa, n.nombre AS naviera,
                sb.descripcion_status AS status, 
                TO_CHAR(b.fecha, 'YYYY-MM-DD') AS fecha,
                TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') AS fecha_proxima_revision   
                FROM bls b
                JOIN etapa e ON e.id = b.id_etapa
                JOIN navieras n ON n.id = b.id_naviera
                JOIN status_bl sb ON b.id_status = sb.id
                WHERE b.id = :id
                LIMIT :limit OFFSET :offset;
            """
    return await database.fetch_all(query=query, values={"id": id, "limit": limit, "offset": offset})

async def insert_bl(values_bls: dict):
    query_bls = """
            INSERT INTO bls (
                code, id_naviera, id_etapa, fecha, proxima_revision,
                nave, id_status, id_carga, mercado, revisado_con_exito, manual_pendiente,
                no_revisar, revisado_hoy, html_descargado
            ) VALUES (
                :code, :id_naviera, :id_etapa, :fecha, :proxima_revision,
                :nave, 18, 211, :mercado, :revisado_con_exito, :manual_pendiente,
                :no_revisar, :revisado_hoy, :html_descargado
            )
            RETURNING id;
    """
    return await database.execute(query=query_bls, values=values_bls)

async def fetch_bls_by_date(fecha: str, limit: int, offset: int):
    query = """
        SELECT b.id, b.code AS bl_code, e.nombre AS etapa, n.nombre AS naviera, 
            sb.descripcion_status AS status, 
            TO_CHAR(b.fecha, 'YYYY-MM-DD') AS fecha, 
            TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') AS fecha_proxima_revision
        FROM bls b
        JOIN etapa e ON e.id = b.id_etapa
        JOIN navieras n ON n.id = b.id_naviera
        JOIN status_bl sb ON b.id_status = sb.id
        WHERE 1=1
    """
    values = {"limit": limit, "offset": offset}
    
    if len(fecha) == 4:
        query += " AND EXTRACT(YEAR FROM b.fecha) = :fecha"
        values["fecha"] = int(fecha)
    elif len(fecha) == 10:
        query += " AND b.fecha >= :fecha"
        values["fecha"] = datetime.strptime(fecha, "%Y-%m-%d").date()
    elif len(fecha) == 21:
        fecha_i, fecha_f = fecha.split('+')
        query += " AND b.fecha >= :fecha_i AND b.fecha <= :fecha_f"
        values["fecha_i"] = datetime.strptime(fecha_i, "%Y-%m-%d").date()
        values["fecha_f"] = datetime.strptime(fecha_f, "%Y-%m-%d").date()

    query += " ORDER BY b.code LIMIT :limit OFFSET :offset"
    return await database.fetch_all(query=query, values=values)

async def fetch_bl_by_code(code: str, limit: int, offset: int):
    query = """
        SELECT b.id, b.code AS bl_code, e.nombre AS etapa, n.nombre AS naviera,
        sb.descripcion_status AS status, 
        TO_CHAR(b.fecha, 'YYYY-MM-DD') AS fecha,
        TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') AS fecha_proxima_revision   
        FROM bls b
        JOIN etapa e ON e.id = b.id_etapa
        JOIN navieras n ON n.id = b.id_naviera
        JOIN status_bl sb ON b.id_status = sb.id
        WHERE b.code LIKE :code
        LIMIT :limit OFFSET :offset;
    """
    return await database.fetch_all(
        query=query, 
        values={"code": f"{code}%", "limit": limit, "offset": offset}
    )

async def fetch_bl_by_naviera(nombre: str, limit: int, offset: int):
    query = """
        SELECT b.id, b.code AS bl_code, e.nombre AS etapa, n.nombre AS naviera,
        sb.descripcion_status AS status, 
        TO_CHAR(b.fecha, 'YYYY-MM-DD') AS fecha,
        TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') AS fecha_proxima_revision   
        FROM bls b
        JOIN etapa e ON e.id = b.id_etapa
        JOIN navieras n ON n.id = b.id_naviera
        JOIN status_bl sb ON b.id_status = sb.id
        WHERE n.nombre LIKE :nombre
        LIMIT :limit OFFSET :offset;
    """
    return await database.fetch_all(
        query=query, 
        values={"nombre": f"{nombre.upper()}%", "limit": limit, "offset": offset}
    )

async def update_bl(id_bl: int, fields: dict):
    """Updates a BL record with the given fields"""
    if not fields:
        return None
    
    set_clauses = []
    values = {"id_bl": id_bl}
    
    for key, value in fields.items():
        set_clauses.append(f"{key} = :{key}")
        values[key] = value
    
    query = f"""
        UPDATE bls
        SET {', '.join(set_clauses)}
        WHERE id = :id_bl
        RETURNING id;
    """
    return await database.execute(query=query, values=values)

async def get_naviera_id(naviera: str):
    query = "SELECT id FROM navieras WHERE nombre ILIKE :naviera"
    return await database.fetch_val(query, {"naviera": f"{naviera.upper()}%"})

async def get_etapa_id(etapa: str):
    query = "SELECT id FROM etapa WHERE nombre ILIKE :etapa"
    return await database.fetch_val(query, {"etapa": f"{etapa.upper()}%"})

async def get_status_id(status: str):
    query = "SELECT id FROM status_bl WHERE descripcion_status = :status"
    return await database.fetch_val(query, {"status": status})