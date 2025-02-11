from datetime import datetime
from fastapi import HTTPException
from repositories.bls_repository import (
    fetch_bls,
    fetch_dropdown,
    fetch_bl_by_id,
    fetch_bl_by_code,
    fetch_bl_by_naviera,
    fetch_bls_by_date,
    insert_bl,
    update_bl,
    get_naviera_id,
    get_etapa_id,
    get_status_id
)

async def get_filtered_bls(
    bl_code: str = None, etapa: str = None, naviera: str = None, status: str = None,
    fecha: str = None, fecha_proxima_revision: str = None,
    order_by: str = None, order: str = "ASC", limit: int = 500, offset: int = 0
):
    filters = {
        "bl_code": bl_code,
        "etapa": etapa,
        "naviera": naviera,
        "status": status,
        "fecha": fecha,
        "fecha_proxima_revision": fecha_proxima_revision
    }
    return await fetch_bls(filters, order_by, order, limit, offset)

async def get_dropdown_names(name_type: str):
    if name_type == "navieras":
        query = "SELECT nombre FROM navieras ORDER BY id"
    elif name_type == "etapa":
        query = "SELECT nombre FROM etapa ORDER BY id"
    elif name_type == "status":
        query = "SELECT descripcion_status FROM status_bl ORDER BY id"
    else:
        return None
    return await fetch_dropdown(query)

async def get_bl_by_id(id: int, limit: int, offset: int):
    return await fetch_bl_by_id(id, limit, offset)

async def get_bl_by_code(code: str, limit: int, offset: int):
    return await fetch_bl_by_code(code, limit, offset)

async def get_bl_by_naviera(nombre: str, limit: int, offset: int):
    return await fetch_bl_by_naviera(nombre, limit, offset)

async def get_bls_by_date(fecha: str, limit: int, offset: int):
    return await fetch_bls_by_date(fecha, limit, offset)

async def create_bl_record(
    code: str, naviera: str, etapa: str, fecha: str, proxima_revision: str,
    nave: str = None, mercado: str = None, revisado_con_exito: bool = None,
    manual_pendiente: bool = None, no_revisar: bool = None,
    revisado_hoy: bool = None, html_descargado: bool = None
):
    # Validar y obtener llaves foraneas
    id_naviera = await get_naviera_id(naviera)
    if not id_naviera:
        raise HTTPException(status_code=404, detail="Naviera not found")
    
    id_etapa = await get_etapa_id(etapa)
    if not id_etapa:
        raise HTTPException(status_code=404, detail="Etapa not found")

    try:
        fecha_dt = datetime.strptime(fecha, "%Y-%m-%d").date()
        proxima_revision_dt = datetime.strptime(proxima_revision, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format")

    values_bls = {
        "code": code,
        "id_naviera": id_naviera,
        "id_etapa": id_etapa,
        "fecha": fecha_dt,
        "proxima_revision": proxima_revision_dt,
        "nave": nave,
        "mercado": mercado,
        "revisado_con_exito": revisado_con_exito,
        "manual_pendiente": manual_pendiente,
        "no_revisar": no_revisar,
        "revisado_hoy": revisado_hoy,
        "html_descargado": html_descargado,
    }
    
    return await insert_bl(values_bls)

async def update_bl_record(
    id_bl: int, bl_code: str = None, etapa: str = None,
    naviera: str = None, status: str = None,
    fecha: str = None, fecha_proxima_revision: str = None
):
    update_fields = {}
    
    if bl_code:
        update_fields["code"] = bl_code
    
    if fecha:
        try:
            update_fields["fecha"] = datetime.strptime(fecha, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format")
            
    if fecha_proxima_revision:
        try:
            update_fields["proxima_revision"] = datetime.strptime(fecha_proxima_revision, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format")
    
    if etapa:
        etapa_id = await get_etapa_id(etapa)
        if not etapa_id:
            raise HTTPException(status_code=404, detail="Etapa not found")
        update_fields["id_etapa"] = etapa_id
    
    if naviera:
        naviera_id = await get_naviera_id(naviera)
        if not naviera_id:
            raise HTTPException(status_code=404, detail="Naviera not found")
        update_fields["id_naviera"] = naviera_id
    
    if status:
        status_id = await get_status_id(status)
        if not status_id:
            raise HTTPException(status_code=404, detail="Status not found")
        update_fields["id_status"] = status_id
    
    if not update_fields:
        raise HTTPException(status_code=400, detail="No fields to update")
    
    return await update_bl(id_bl, update_fields)