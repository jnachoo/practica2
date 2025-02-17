from fastapi import APIRouter, HTTPException, Query, Depends
from datetime import datetime
from typing import Optional, List
from sqlalchemy import func, desc, asc, extract
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from pydantic import BaseModel

from rutas.autenticacion import check_rol, get_current_user
from models import User, BL, Etapa, Naviera, StatusBL
from database import get_db  # get_db debe retornar un AsyncSession

router = APIRouter()

# ------------------------------
# Funciones auxiliares para conversión segura
# ------------------------------
def safe_strftime(date_obj):
    try:
        return date_obj.strftime("%Y-%m-%d") if date_obj else None
    except Exception:
        return None

def bl_to_dict(bl: BL) -> dict:
    return {
        "id": bl.id,
        "bl_code": bl.code,  # Usamos bl_code para coincidir con el front
        "etapa": bl.etapa.nombre if bl.etapa else None,
        "naviera": bl.naviera.nombre if bl.naviera else None,
        "status": bl.status.descripcion_status if bl.status else None,
        "fecha": safe_strftime(bl.fecha),
        "fecha_proxima_revision": safe_strftime(bl.proxima_revision)
    }

# ------------------------------
# Esquema Pydantic para la salida de BLs
# ------------------------------
class BLRead(BaseModel):
    id: int
    bl_code: str
    etapa: Optional[str]
    naviera: Optional[str]
    status: Optional[str]
    fecha: Optional[str]
    fecha_proxima_revision: Optional[str]

    class Config:
        orm_mode = True

# ------------------------------
# GET: Super filtro de BLs (con conversión explícita de fechas a cadena)
# ------------------------------
@router.get("/bls/", response_model=List[BLRead])
async def super_filtro_bls(
    bl_code: Optional[str] = Query(None),
    etapa: Optional[str] = Query(None),
    naviera: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    fecha: Optional[str] = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$"),
    fecha_proxima_revision: Optional[str] = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$"),
    order_by: Optional[str] = Query(
        None,
        regex="^(b\\.code|e\\.nombre|n\\.nombre|sb\\.descripcion_status|b\\.fecha|b\\.proxima_revision|b\\.id)$"
    ),
    order: str = Query("ASC", regex="^(ASC|DESC|asc|desc)$"),
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    stmt = select(
        BL.id,
        BL.code.label("bl_code"),  # Alias para que se retorne con el nombre "bl_code"
        func.to_char(BL.fecha, 'YYYY-MM-DD').label("fecha"),
        func.to_char(BL.proxima_revision, 'YYYY-MM-DD').label("fecha_proxima_revision"),
        Etapa.nombre.label("etapa"),
        Naviera.nombre.label("naviera"),
        StatusBL.descripcion_status.label("status")
    ).join(BL.etapa).join(BL.naviera).join(BL.status)
    
    if bl_code:
        stmt = stmt.where(BL.code.ilike(f"{bl_code}%"))
    if etapa:
        stmt = stmt.where(Etapa.nombre.ilike(f"{etapa}%"))
    if naviera:
        stmt = stmt.where(Naviera.nombre.ilike(f"{naviera}%"))
    if status:
        stmt = stmt.where(StatusBL.descripcion_status.ilike(f"{status}%"))
    if fecha:
        stmt = stmt.where(func.to_char(BL.fecha, 'YYYY-MM-DD') == fecha)
    if fecha_proxima_revision:
        stmt = stmt.where(func.to_char(BL.proxima_revision, 'YYYY-MM-DD') == fecha_proxima_revision)
    
    # Mapeo de order_by a columnas (actualizamos para el alias de fecha)
    order_by_mapping = {
        "b.code": BL.code,
        "e.nombre": Etapa.nombre,
        "n.nombre": Naviera.nombre,
        "sb.descripcion_status": StatusBL.descripcion_status,
        "b.fecha": func.to_char(BL.fecha, 'YYYY-MM-DD'),
        "b.proxima_revision": func.to_char(BL.proxima_revision, 'YYYY-MM-DD'),
        "b.id": BL.id,
    }
    if order_by and order_by in order_by_mapping:
        col = order_by_mapping[order_by]
        stmt = stmt.order_by(desc(col)) if order.lower() == "desc" else stmt.order_by(asc(col))
    stmt = stmt.limit(limit).offset(offset)
    
    result = await db.execute(stmt)
    rows = result.all()
    if not rows:
        raise HTTPException(status_code=404, detail="BL no encontrado")
    # Usamos row._mapping para convertir cada fila a dict
    return [dict(row._mapping) for row in rows]

# ------------------------------
# ENDPOINTS DROPDOWN
# ------------------------------
@router.get("/bls/nombre_navieras")
async def bls_nombre_navieras(db: AsyncSession = Depends(get_db)):
    stmt = select(Naviera.nombre).order_by(Naviera.id)
    result = await db.execute(stmt)
    navieras = result.scalars().all()
    if not navieras:
        raise HTTPException(status_code=404, detail="Navieras no retornadas")
    return [{"nombre": n} for n in navieras]

@router.get("/bls/nombre_etapa")
async def bls_nombre_etapa(db: AsyncSession = Depends(get_db)):
    stmt = select(Etapa.nombre).order_by(Etapa.id)
    result = await db.execute(stmt)
    etapas = result.scalars().all()
    if not etapas:
        raise HTTPException(status_code=404, detail="Etapas no retornadas")
    return [{"nombre": e} for e in etapas]

@router.get("/bls/descripcion_status_bl")
async def bls_nombre_status_bl(db: AsyncSession = Depends(get_db)):
    stmt = select(StatusBL.descripcion_status).order_by(StatusBL.id)
    result = await db.execute(stmt)
    statuses = result.scalars().all()
    if not statuses:
        raise HTTPException(status_code=404, detail="Status no retornadas")
    return [{"descripcion_status": s} for s in statuses]

# ------------------------------
# GET: BLs por Fecha (con conversión explícita)
# ------------------------------
@router.get("/bls/fecha/{fecha}")
async def bls_fecha(
    fecha: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    stmt = select(
        BL.id,
        BL.code.label("bl_code"),
        func.to_char(BL.fecha, 'YYYY-MM-DD').label("fecha"),
        func.to_char(BL.proxima_revision, 'YYYY-MM-DD').label("fecha_proxima_revision"),
        Etapa.nombre.label("etapa"),
        Naviera.nombre.label("naviera"),
        StatusBL.descripcion_status.label("status")
    ).join(BL.etapa).join(BL.naviera).join(BL.status)
    
    mensaje = ""
    if fecha and len(fecha) == 4:
        try:
            anio = int(fecha)
        except ValueError:
            raise HTTPException(status_code=400, detail="Año inválido")
        stmt = stmt.where(extract("year", BL.fecha) == anio)
        mensaje = f"Los BLs encontrados en el año {anio} son:"
    elif fecha and len(fecha) == 10:
        try:
            fecha_dt = datetime.strptime(fecha, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Formato de fecha inválido, use YYYY-MM-DD")
        stmt = stmt.where(BL.fecha >= fecha_dt)
        mensaje = f"Los BLs encontrados desde {fecha_dt} hasta hoy son:"
    elif fecha and len(fecha) == 21:
        try:
            fecha_i_str, fecha_f_str = fecha.split('+')
            fecha_i = datetime.strptime(fecha_i_str, "%Y-%m-%d").date()
            fecha_f = datetime.strptime(fecha_f_str, "%Y-%m-%d").date()
        except Exception:
            raise HTTPException(status_code=400, detail="Formato de rango de fechas inválido")
        stmt = stmt.where(BL.fecha.between(fecha_i, fecha_f))
        mensaje = f"Los BLs encontrados desde {fecha_i} hasta {fecha_f} son:"
    else:
        return {"mensaje": "Debes usar el formato: para año AAAA, o desde: AAAA-MM-DD, o rango: AAAA-MM-DD+AAAA-MM-DD"}
    
    stmt = stmt.order_by(BL.code).limit(limit).offset(offset)
    result = await db.execute(stmt)
    rows = result.all()
    if not rows:
        raise HTTPException(status_code=404, detail="BLs no encontrados")
    return {"mensaje": mensaje, "results": [dict(row._mapping) for row in rows]}

# ------------------------------
# GET: BLs por ID
# ------------------------------
@router.get("/bls/id/{id}", response_model=List[BLRead])
async def ver_bls_id(
    id: int,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    stmt = select(BL).join(BL.etapa).join(BL.naviera).join(BL.status)\
        .where(BL.id == id).limit(limit).offset(offset)
    result = await db.execute(stmt)
    bls = result.scalars().all()
    if not bls:
        raise HTTPException(status_code=404, detail="ID de BL no encontrado")
    return [bl_to_dict(bl) for bl in bls]

# ------------------------------
# GET: BLs por Código
# ------------------------------
@router.get("/bls/code/{code}", response_model=List[BLRead])
async def ver_bls_code(
    code: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    stmt = select(BL).join(BL.etapa).join(BL.naviera).join(BL.status)\
        .where(BL.code.ilike(f"{code}%")).limit(limit).offset(offset)
    result = await db.execute(stmt)
    bls = result.scalars().all()
    if not bls:
        raise HTTPException(status_code=404, detail="Código de BL no encontrado")
    return [bl_to_dict(bl) for bl in bls]

# ------------------------------
# GET: BLs por Naviera
# ------------------------------
@router.get("/bls/naviera/{nombre}", response_model=List[BLRead])
async def ver_bls_naviera(
    nombre: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    stmt = select(BL).join(BL.etapa).join(BL.naviera).join(BL.status)\
        .where(Naviera.nombre.ilike(f"{nombre.upper()}%")).limit(limit).offset(offset)
    result = await db.execute(stmt)
    bls = result.scalars().all()
    if not bls:
        raise HTTPException(status_code=404, detail="Naviera de BL no encontrada")
    return [bl_to_dict(bl) for bl in bls]

# ------------------------------
# PATCH: Actualización parcial de BLs
# ------------------------------
@router.patch("/bls/{id_bls}")
async def actualizar_parcial_bls(
    id_bls: int,
    bl_code: Optional[str] = Query(None),
    etapa: Optional[str] = Query(None),
    naviera: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    fecha: Optional[str] = Query(None),
    fecha_proxima_revision: Optional[str] = Query(None),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    check_rol(current_user, [1, 2])
    stmt = select(BL).where(BL.id == id_bls)
    result = await db.execute(stmt)
    bl_instance = result.scalars().first()
    if not bl_instance:
        raise HTTPException(status_code=404, detail="BL no encontrado para actualizar")
    if bl_code is not None:
        bl_instance.code = bl_code
    if fecha is not None:
        try:
            bl_instance.fecha = datetime.strptime(fecha, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Formato de 'fecha' inválido, use YYYY-MM-DD")
    if fecha_proxima_revision is not None:
        try:
            bl_instance.proxima_revision = datetime.strptime(fecha_proxima_revision, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Formato de 'proxima_revision' inválido, use YYYY-MM-DD")
    if etapa is not None:
        stmt_etapa = select(Etapa).where(Etapa.nombre.ilike(etapa.upper()))
        res_etapa = await db.execute(stmt_etapa)
        etapa_instance = res_etapa.scalars().first()
        if not etapa_instance:
            raise HTTPException(status_code=400, detail="La etapa no existe")
        bl_instance.id_etapa = etapa_instance.id
    if naviera is not None:
        stmt_naviera = select(Naviera).where(Naviera.nombre.ilike(naviera.upper()))
        res_naviera = await db.execute(stmt_naviera)
        naviera_instance = res_naviera.scalars().first()
        if not naviera_instance:
            raise HTTPException(status_code=400, detail="La naviera no existe")
        bl_instance.id_naviera = naviera_instance.id
    if status is not None:
        stmt_status = select(StatusBL).where(StatusBL.descripcion_status.ilike(status))
        res_status = await db.execute(stmt_status)
        status_instance = res_status.scalars().first()
        if not status_instance:
            raise HTTPException(status_code=400, detail="El status no existe")
        bl_instance.id_status = status_instance.id
    try:
        await db.commit()
        await db.refresh(bl_instance)
        return {"mensaje": "BL actualizado exitosamente", "bl": bl_to_dict(bl_instance)}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al actualizar BL: {str(e)}")

# ------------------------------
# POST: Insertar un nuevo BL
# ------------------------------
@router.post("/bls/", response_model=BLRead)
async def insertar_bls(
    code: str,
    naviera: str,
    etapa: str,
    fecha: str,
    proxima_revision: str,
    nave: Optional[str] = Query(None),
    mercado: Optional[str] = Query(None),
    revisado_con_exito: Optional[bool] = Query(None),
    manual_pendiente: Optional[bool] = Query(None),
    no_revisar: Optional[bool] = Query(None),
    revisado_hoy: Optional[bool] = Query(None),
    html_descargado: Optional[bool] = Query(None),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    check_rol(current_user, [1])
    stmt_naviera = select(Naviera).where(Naviera.nombre.ilike(f"{naviera.upper()}%"))
    res_naviera = await db.execute(stmt_naviera)
    naviera_instance = res_naviera.scalars().first()
    if not naviera_instance:
        raise HTTPException(status_code=404, detail="Naviera no encontrada")
    stmt_etapa = select(Etapa).where(Etapa.nombre.ilike(f"{etapa.upper()}%"))
    res_etapa = await db.execute(stmt_etapa)
    etapa_instance = res_etapa.scalars().first()
    if not etapa_instance:
        raise HTTPException(status_code=404, detail="Etapa no encontrada")
    try:
        fecha_dt = datetime.strptime(fecha, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Formato de 'fecha' inválido. Use YYYY-MM-DD.")
    try:
        proxima_revision_dt = datetime.strptime(proxima_revision, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Formato de 'proxima_revision' inválido. Use YYYY-MM-DD.")
    new_bl = BL(
        code=code,
        id_naviera=naviera_instance.id,
        id_etapa=etapa_instance.id,
        fecha=fecha_dt,
        proxima_revision=proxima_revision_dt,
        nave=nave,
        mercado=mercado,
        revisado_con_exito=revisado_con_exito,
        manual_pendiente=manual_pendiente,
        no_revisar=no_revisar,
        revisado_hoy=revisado_hoy,
        html_descargado=html_descargado,
        id_status=18,  # Valor por defecto (ajusta según corresponda)
        id_carga=211   # Valor por defecto (ajusta según corresponda)
    )
    db.add(new_bl)
    try:
        await db.commit()
        await db.refresh(new_bl)
        return bl_to_dict(new_bl)
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al insertar el BL: {str(e)}")

# ------------------------------
# GET: Total de BLs
# ------------------------------
@router.get("/bls/total")
async def total_bls(db: AsyncSession = Depends(get_db)):
    stmt = select(func.count()).select_from(BL)
    result = await db.execute(stmt)
    total = result.scalar()
    return {"total_bls": total}

# ------------------------------
# GET: BLs pendientes de revisión
# ------------------------------
@router.get("/bls/pendientes")
async def pendientes_bls(db: AsyncSession = Depends(get_db)):
    stmt = select(func.count()).select_from(BL)\
        .join(StatusBL)\
        .where(StatusBL.descripcion_status.ilike('Not Found pendiente de revisión%'))
    result = await db.execute(stmt)
    total_pendientes = result.scalar()
    return {"total_bls_pendientes": total_pendientes}
