from fastapi import APIRouter, HTTPException, Query, Depends
from datetime import datetime
from typing import Optional
from routes.autenticacion import check_rol, get_current_user
from models.models import User
from services.bls_service import (
    get_filtered_bls,
    get_dropdown_names,
    get_bl_by_id,
    get_bl_by_code,
    get_bl_by_naviera,
    get_bls_by_date,
    create_bl_record,
    update_bl_record
)

router = APIRouter()

@router.get("/bls/")
async def super_filtro_bls(
    bl_code: str = Query(None),
    etapa: str = Query(None),
    naviera: str = Query(None),
    status: str = Query(None),
    fecha: str = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$"),
    fecha_proxima_revision: str = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$"),
    order_by: str = Query(None,
        regex="^(b\\.code|e\\.nombre|n\\.nombre|sb\\.descripcion_status|b\\.fecha|b\\.proxima_revision|b\\.id)$"),
    order: str = Query("ASC", regex="^(ASC|DESC|asc|desc)$"),
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
):
    try:
        result = await get_filtered_bls(
            bl_code, etapa, naviera, status, fecha, 
            fecha_proxima_revision, order_by, order, limit, offset
        )
        if not result:
            raise HTTPException(status_code=404, detail="BL no encontrado")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error ejecutando la consulta: {str(e)}")

@router.get("/bls/nombre_navieras")
async def bls_nombre_navieras():
    result = await get_dropdown_names("navieras")
    if not result:
        raise HTTPException(status_code=404, detail="Navieras no retornadas")
    return result

@router.get("/bls/nombre_etapa")
async def bls_nombre_etapa():
    result = await get_dropdown_names("etapa")
    if not result:
        raise HTTPException(status_code=404, detail="Etapas no retornadas")
    return result

@router.get("/bls/descripcion_status_bl")
async def bls_nombre_status_bl():
    result = await get_dropdown_names("status")
    if not result:
        raise HTTPException(status_code=404, detail="Status no retornados")
    return result

@router.get("/bls/fecha/{fecha}")
async def bls_fecha(
    fecha: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0)
):
    if len(fecha) not in [4, 10, 21]:
        return {
            "mensaje": "Formato requerido: AAAA, AAAA-MM-DD, o AAAA-MM-DD+AAAA-MM-DD"
        }
    
    try:
        result = await get_bls_by_date(fecha, limit, offset)
        if not result:
            raise HTTPException(status_code=404, detail="BLs no encontrados")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/bls/id/{id}")
async def ver_bls_id(
    id: int,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0)
):
    try:
        result = await get_bl_by_id(id, limit, offset)
        if not result:
            raise HTTPException(status_code=404, detail="BL no encontrado")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/bls/code/{code}")
async def ver_bls_code(
    code: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0)
):
    try:
        result = await get_bl_by_code(code, limit, offset)
        if not result:
            raise HTTPException(status_code=404, detail="BL no encontrado")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/bls/naviera/{nombre}")
async def ver_bls_naviera(
    nombre: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0)
):
    try:
        result = await get_bl_by_naviera(nombre, limit, offset)
        if not result:
            raise HTTPException(status_code=404, detail="BL no encontrado")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.patch("/bls/{id_bls}")
async def actualizar_parcial_bls(
    id_bl: int,
    bl_code: str = Query(None),
    etapa: str = Query(None),
    naviera: str = Query(None),
    status: str = Query(None),
    fecha: str = Query(None),
    fecha_proxima_revision: str = Query(None),
    current_user: User = Depends(get_current_user)
):
    check_rol(current_user, [1, 2])
    
    try:
        result = await update_bl_record(
            id_bl, bl_code, etapa, naviera, status, 
            fecha, fecha_proxima_revision
        )
        return {"mensaje": "BL actualizado exitosamente", "result": result}
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.post("/bls/")
async def insertar_bls(
    code: str,
    naviera: str,
    etapa: str,
    fecha: str,
    proxima_revision: str,
    nave: str = Query(None),
    mercado: str = Query(None),
    revisado_con_exito: bool = Query(None),
    manual_pendiente: bool = Query(None),
    no_revisar: bool = Query(None),
    revisado_hoy: bool = Query(None),
    html_descargado: bool = Query(None),
    current_user: User = Depends(get_current_user)
):
    check_rol(current_user, [1])
    
    try:
        result = await create_bl_record(
            code, naviera, etapa, fecha, proxima_revision,
            nave, mercado, revisado_con_exito, manual_pendiente,
            no_revisar, revisado_hoy, html_descargado
        )
        return {"message": "BL creado exitosamente", "id_bls": result}
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    




