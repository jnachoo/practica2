from fastapi import APIRouter, HTTPException, Query, Depends
from datetime import datetime
from typing import Optional, List
from sqlalchemy import select, func, desc, asc, text
from sqlalchemy.ext.asyncio import AsyncSession
from urllib.parse import unquote
from pydantic import BaseModel

from database import get_db  # get_db retorna un AsyncSession
from models import Paradas, Tracking, BL, StatusBL, HTMLDescargado, RespuestaRequest

router = APIRouter()

# ------------------------------
# GET: Super filtro de paradas (usando ORM) - Versión sin campos de fecha
# ------------------------------
@router.get("/paradas/")
async def super_filtro_paradas(
    bl_code: Optional[str] = Query(None),
    locode: Optional[str] = Query(None),
    pais: Optional[str] = Query(None),
    lugar: Optional[str] = Query(None),
    is_pol: Optional[bool] = Query(None),
    is_pod: Optional[bool] = Query(None),
    orden: Optional[int] = Query(None),
    status: Optional[str] = Query(None),
    order_by: Optional[str] = Query(None, regex="^(b\\.code|t\\.orden|t\\.status|p\\.locode|p\\.pais)$"),
    order: str = Query("ASC", regex="^(ASC|DESC|asc|desc)$"),
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    # Seleccionar únicamente las columnas necesarias (sin campos de fecha)
    stmt = (
        select(
            Tracking.id.label("id_tracking"),
            BL.code.label("bl_code"),
            Tracking.orden,
            Tracking.status,
            Paradas.locode,
            Paradas.pais,
            Paradas.lugar,
            Tracking.is_pol,
            Tracking.is_pod
        )
        .join(Paradas, Tracking.id_parada == Paradas.id)
        .join(BL, Tracking.id_bl == BL.id)
    )
    if bl_code:
        stmt = stmt.where(BL.code.ilike(f"{bl_code}%"))
    if locode:
        stmt = stmt.where(Paradas.locode.ilike(f"{locode}%"))
    if pais:
        stmt = stmt.where(Paradas.pais.ilike(f"{pais}%"))
    if lugar:
        stmt = stmt.where(Paradas.lugar.ilike(f"{lugar}%"))
    if is_pol is not None:
        stmt = stmt.where(Tracking.is_pol == is_pol)
    if is_pod is not None:
        stmt = stmt.where(Tracking.is_pod == is_pod)
    if orden is not None:
        stmt = stmt.where(Tracking.orden == orden)
    if status:
        stmt = stmt.where(Tracking.status.ilike(f"{status}%"))
    
    # Mapeo para ordenación
    order_map = {
        "b.code": BL.code,
        "t.orden": Tracking.orden,
        "t.status": Tracking.status,
        "p.locode": Paradas.locode,
        "p.pais": Paradas.pais
    }
    if order_by and order_by in order_map:
        col = order_map[order_by]
        stmt = stmt.order_by(desc(col)) if order.lower() == "desc" else stmt.order_by(asc(col))
    stmt = stmt.limit(limit).offset(offset)
    
    try:
        result = await db.execute(stmt)
        rows = result.all()
        if not rows:
            raise HTTPException(status_code=404, detail="Datos no encontrados")
        # Convertir cada fila usando row._mapping para obtener un dict
        response = [dict(row._mapping) for row in rows]
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al ejecutar la consulta: {str(e)}")

# ------------------------------
# GET: Dropdown endpoints (locode, pais, lugar, terminal)
# ------------------------------
@router.get("/paradas/locode")
async def paradas_locode(db: AsyncSession = Depends(get_db)):
    stmt = select(Paradas.locode).order_by(Paradas.locode)
    result = await db.execute(stmt)
    locodes = result.scalars().all()
    if not locodes:
        raise HTTPException(status_code=404, detail="Locode no retornados")
    return [{"locode": l} for l in locodes]

@router.get("/paradas/pais")
async def paradas_pais(db: AsyncSession = Depends(get_db)):
    stmt = select(Paradas.pais).distinct().order_by(Paradas.pais)
    result = await db.execute(stmt)
    paises = result.scalars().all()
    if not paises:
        raise HTTPException(status_code=404, detail="Paises no retornados")
    return [{"pais": p} for p in paises]

@router.get("/paradas/lugar")
async def paradas_lugar(db: AsyncSession = Depends(get_db)):
    stmt = select(Paradas.lugar).distinct().order_by(Paradas.lugar)
    result = await db.execute(stmt)
    lugares = result.scalars().all()
    if not lugares:
        raise HTTPException(status_code=404, detail="Lugares no retornados")
    return [{"lugar": l} for l in lugares]

@router.get("/paradas/terminal")
async def paradas_terminal(db: AsyncSession = Depends(get_db)):
    stmt = select(Tracking.terminal).distinct().order_by(Tracking.terminal)
    result = await db.execute(stmt)
    terminals = result.scalars().all()
    if not terminals:
        raise HTTPException(status_code=404, detail="Terminales no retornados")
    return [{"terminal": t} for t in terminals]

# ------------------------------
# GET: Filtrar paradas por BL code
# ------------------------------
@router.get("/paradas/bl_code/{bl_code}")
async def ver_paradas_by_bl_code(
    bl_code: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    stmt = (
        select(
            Tracking.id.label("id_tracking"),
            BL.code.label("bl_code"),
            Tracking.orden,
            Tracking.status,
            Paradas.locode,
            Paradas.pais,
            Paradas.lugar,
            Tracking.is_pol,
            Tracking.is_pod
        )
        .join(BL, Tracking.id_bl == BL.id)
        .join(Paradas, Tracking.id_parada == Paradas.id)
        .where(BL.code.ilike(f"{bl_code.upper()}%"))
        .order_by(Tracking.orden)
        .limit(limit)
        .offset(offset)
    )
    try:
        result = await db.execute(stmt)
        rows = result.all()
        if not rows:
            raise HTTPException(status_code=404, detail="Paradas no encontradas")
        response = [dict(row._mapping) for row in rows]
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al ejecutar la consulta paradas_filtro_bl_code: {str(e)}")

# ------------------------------
# GET: Filtrar paradas por locode
# ------------------------------
@router.get("/paradas/locode/{locode}")
async def ver_paradas_by_locode(
    locode: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    stmt = (
        select(
            Tracking.id.label("id_tracking"),
            BL.code.label("bl_code"),
            Tracking.orden,
            Tracking.status,
            Paradas.locode,
            Paradas.pais,
            Paradas.lugar,
            Tracking.is_pol,
            Tracking.is_pod
        )
        .join(Paradas, Tracking.id_parada == Paradas.id)
        .join(BL, Tracking.id_bl == BL.id)
        .where(Paradas.locode.ilike(f"{locode.upper()}%"))
        .order_by(Tracking.orden)
        .limit(limit)
        .offset(offset)
    )
    try:
        result = await db.execute(stmt)
        rows = result.all()
        if not rows:
            raise HTTPException(status_code=404, detail="Paradas no encontradas")
        response = [dict(row._mapping) for row in rows]
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al ejecutar la consulta paradas_filtro_locode: {str(e)}")

# ------------------------------
# GET: Filtrar paradas por pais
# ------------------------------
@router.get("/paradas/pais/{pais}")
async def ver_paradas_by_pais(
    pais: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    stmt = (
        select(
            Tracking.id.label("id_tracking"),
            BL.code.label("bl_code"),
            Tracking.orden,
            Tracking.status,
            Paradas.locode,
            Paradas.pais,
            Paradas.lugar,
            Tracking.is_pol,
            Tracking.is_pod
        )
        .join(Paradas, Tracking.id_parada == Paradas.id)
        .join(BL, Tracking.id_bl == BL.id)
        .where(Paradas.pais.ilike(f"{pais.upper()}%"))
        .order_by(Tracking.orden)
        .limit(limit)
        .offset(offset)
    )
    try:
        result = await db.execute(stmt)
        rows = result.all()
        if not rows:
            raise HTTPException(status_code=404, detail="Paradas no encontradas")
        response = [dict(row._mapping) for row in rows]
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al ejecutar la consulta paradas_filtro_pais: {str(e)}")

# ------------------------------
# PATCH: Actualización parcial de paradas (usando ORM)
# ------------------------------
@router.patch("/paradas/{locode}")
async def actualizar_parcial_parada(
    locode: str,
    pais: Optional[str] = Query(None),
    lugar: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    if not locode:
        raise HTTPException(status_code=400, detail="Debe escribir un locode")
    stmt = select(Paradas).where(Paradas.locode == locode.upper())
    result = await db.execute(stmt)
    parada_instance = result.scalars().first()
    if not parada_instance:
        raise HTTPException(status_code=404, detail="Parada no encontrada")
    if pais is not None:
        parada_instance.pais = pais.upper()
    if lugar is not None:
        parada_instance.lugar = lugar.upper()
    try:
        await db.commit()
        await db.refresh(parada_instance)
        return {"message": "Actualización realizada con éxito"}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al actualizar paradas: {str(e)}")

# ------------------------------
# PATCH: Actualización parcial de tracking (usando ORM)
# ------------------------------
@router.patch("/tracking/{id_tracking}")
async def actualizar_parcial_tracking(
    id_tracking: int,
    orden: Optional[int] = Query(None),
    status: Optional[str] = Query(None),
    locode: Optional[str] = Query(None),
    is_pol: Optional[bool] = Query(None),
    is_pod: Optional[bool] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    stmt = select(Tracking).where(Tracking.id == id_tracking)
    result = await db.execute(stmt)
    tracking_instance = result.scalars().first()
    if not tracking_instance:
        raise HTTPException(status_code=404, detail="Tracking no encontrado")
    if orden is not None:
        if not isinstance(orden, int) or orden < 0:
            raise HTTPException(status_code=400, detail="La orden debe ser un número positivo.")
        tracking_instance.orden = orden
    if status is not None:
        tracking_instance.status = status.upper()
    if is_pol is not None:
        tracking_instance.is_pol = is_pol
    if is_pod is not None:
        tracking_instance.is_pod = is_pod
    if locode is not None:
        stmt_locode = select(Paradas).where(Paradas.locode == locode.upper())
        result_locode = await db.execute(stmt_locode)
        parada_instance = result_locode.scalars().first()
        if not parada_instance:
            raise HTTPException(status_code=400, detail="El locode no existe en la tabla 'paradas'.")
        tracking_instance.id_parada = parada_instance.id
    try:
        await db.commit()
        await db.refresh(tracking_instance)
        return {"message": "Actualización realizada con éxito"}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al actualizar tracking: {str(e)}")

# ------------------------------
# POST: Insertar una nueva parada (usando ORM)
# ------------------------------
@router.post("/paradas/")
async def insertar_parada(
    locode: str,
    pais: Optional[str] = Query(None),
    lugar: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    if not locode:
        raise HTTPException(status_code=400, detail="El campo 'locode' es obligatorio.")
    locode = locode.upper()
    stmt_check = select(Paradas).where(Paradas.locode == locode)
    result_check = await db.execute(stmt_check)
    existing = result_check.scalars().first()
    if existing:
        raise HTTPException(status_code=400, detail=f"El locode ya existe, id: {existing.id}")
    pais = pais.upper() if pais else "DESCONOCIDO"
    lugar = lugar.upper() if lugar else "DESCONOCIDO"
    new_parada = Paradas(locode=locode, pais=pais, lugar=lugar)
    db.add(new_parada)
    try:
        await db.commit()
        await db.refresh(new_parada)
        return {"message": "Registro creado exitosamente en paradas", "id_parada": new_parada.id}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al insertar parada: {str(e)}")

# ------------------------------
# POST: Insertar un nuevo tracking (usando ORM)
# ------------------------------
@router.post("/tracking/")
async def insertar_tracking(
    bl_code: str,
    locode: str,
    fecha: str,
    orden: Optional[int] = Query(None),
    terminal: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    is_pol: Optional[bool] = Query(None),
    is_pod: Optional[bool] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    if not locode or not bl_code or not fecha:
        raise HTTPException(status_code=400, detail="Los campos 'locode', 'bl_code' y 'fecha' son obligatorios.")
    locode = locode.upper()
    stmt_locode = select(Paradas).where(Paradas.locode == locode)
    result_locode = await db.execute(stmt_locode)
    parada_instance = result_locode.scalars().first()
    if not parada_instance:
        raise HTTPException(status_code=400, detail="El locode no existe")
    bl_code = bl_code.upper()
    stmt_bl = select(BL).where(BL.code == bl_code)
    result_bl = await db.execute(stmt_bl)
    bl_instance = result_bl.scalars().first()
    if not bl_instance:
        raise HTTPException(status_code=400, detail="El bl_code no existe")
    orden = orden if orden is not None else 0
    is_pod = is_pod if is_pod is not None else False
    is_pol = is_pol if is_pol is not None else False
    status = status if status is not None else "DESCONOCIDO"
    terminal = terminal if terminal is not None else "DESCONOCIDO"
    try:
        fecha_dt = datetime.strptime(fecha, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Formato de 'fecha' inválido. Use YYYY-MM-DD.")
    new_tracking = Tracking(
        id_bl=bl_instance.id,
        fecha=fecha_dt,
        status=status,
        orden=orden,
        id_parada=parada_instance.id,
        terminal=terminal,
        is_pol=is_pol,
        is_pod=is_pod
    )
    db.add(new_tracking)
    try:
        await db.commit()
        await db.refresh(new_tracking)
        return {"message": "Registro creado exitosamente en tracking", "id_tracking": new_tracking.id}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al insertar tracking: {str(e)}")
