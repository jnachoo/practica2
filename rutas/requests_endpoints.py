from fastapi import APIRouter, HTTPException, Query, Depends
from datetime import datetime
from typing import Optional, List
from sqlalchemy import select, func, desc, asc, text
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel

from database import get_db
from models import Request, BL, HTMLDescargado, RespuestaRequest, StatusBL, Paradas, Tracking

router = APIRouter()

# ------------------------------
# Esquema Pydantic para la respuesta de Request
# ------------------------------
class RequestRead(BaseModel):
    id_request: int
    id_html: int
    bl_code: str
    descripcion_status: str
    mensaje: Optional[str]
    respuesta_request: Optional[str]
    fecha_bl: Optional[str]
    fecha_request: str

    class Config:
        from_attributes = True  # Para Pydantic V2

# ------------------------------
# GET /requests/ - Consulta general de requests con filtros (usando ORM)
# ------------------------------
@router.get("/requests/", response_model=List[RequestRead])
async def get_requests(
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    id_request: Optional[int] = Query(None),
    id_html: Optional[int] = Query(None),
    bl_code: Optional[str] = Query(None),
    bl_status: Optional[str] = Query(None),
    mensaje: Optional[str] = Query(None),
    respuesta_request: Optional[str] = Query(None),
    order_by: Optional[str] = Query(
        None, regex="^(r\\.id|h\\.id|b\\.code|s\\.descripcion_status|r\\.mensaje|rr\\.descripcion|b\\.fecha|r\\.fecha)$"
    ),
    order: str = Query("ASC", regex="^(ASC|DESC|asc|desc)$"),
    db: AsyncSession = Depends(get_db)
):
    # Convertimos las fechas a texto en la consulta para evitar que asyncpg las decodifique
    stmt = (
        select(
            Request.id.label("id_request"),
            HTMLDescargado.id.label("id_html"),
            BL.code.label("bl_code"),
            StatusBL.descripcion_status.label("descripcion_status"),
            Request.mensaje,
            RespuestaRequest.descripcion.label("respuesta_request"),
            func.to_char(BL.fecha, 'YYYY-MM-DD').label("fecha_bl"),
            func.to_char(Request.fecha, 'YYYY-MM-DD HH24:MI:SS').label("fecha_request")
        )
        .join(HTMLDescargado, Request.id_html == HTMLDescargado.id)
        .join(RespuestaRequest, Request.id_respuesta == RespuestaRequest.id)
        .join(BL, Request.id_bl == BL.id)
        .join(StatusBL, BL.id_status == StatusBL.id)
    )
    if id_request is not None:
        stmt = stmt.where(Request.id == id_request)
    if id_html is not None:
        stmt = stmt.where(HTMLDescargado.id == id_html)
    if bl_code:
        stmt = stmt.where(BL.code.ilike(f"{bl_code}%"))
    if bl_status:
        stmt = stmt.where(StatusBL.descripcion_status.ilike(f"{bl_status}%"))
    if mensaje:
        stmt = stmt.where(Request.mensaje.ilike(f"{mensaje}%"))
    if respuesta_request:
        stmt = stmt.where(RespuestaRequest.descripcion.ilike(f"{respuesta_request}%"))
    
    # Mapeo para ordenación
    order_by_mapping = {
        "r.id": Request.id,
        "h.id": HTMLDescargado.id,
        "b.code": BL.code,
        "s.descripcion_status": StatusBL.descripcion_status,
        "r.mensaje": Request.mensaje,
        "rr.descripcion": RespuestaRequest.descripcion,
        # Si se ordena por fechas, se puede ordenar por la fecha original (aunque en la salida se muestra como texto)
        "b.fecha": BL.fecha,
        "r.fecha": Request.fecha,
    }
    if order_by and order_by in order_by_mapping:
        col = order_by_mapping[order_by]
        stmt = stmt.order_by(desc(col)) if order.lower() == "desc" else stmt.order_by(asc(col))
    stmt = stmt.limit(limit).offset(offset)
    
    try:
        result = await db.execute(stmt)
        rows = result.all()
        if not rows:
            raise HTTPException(status_code=404, detail="Datos no encontrados")
        output = [dict(row._mapping) for row in rows]
        return output
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al ejecutar la consulta request: {str(e)}")

# ------------------------------
# GET /requests/id_bl/{id_bl} - Filtrado por id_bl de BL
# ------------------------------
@router.get("/requests/id_bl/{id_bl}", response_model=List[RequestRead])
async def get_requests_by_id_bl(
    id_bl: int,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    stmt = (
        select(
            Request.id.label("id_request"),
            HTMLDescargado.id.label("id_html"),
            BL.code.label("bl_code"),
            StatusBL.descripcion_status.label("descripcion_status"),
            Request.mensaje,
            RespuestaRequest.descripcion.label("respuesta_request"),
            func.to_char(BL.fecha, 'YYYY-MM-DD').label("fecha_bl"),
            func.to_char(Request.fecha, 'YYYY-MM-DD HH24:MI:SS').label("fecha_request")
        )
        .join(HTMLDescargado, Request.id_html == HTMLDescargado.id)
        .join(RespuestaRequest, Request.id_respuesta == RespuestaRequest.id)
        .join(BL, Request.id_bl == BL.id)
        .join(StatusBL, BL.id_status == StatusBL.id)
        .where(BL.id == id_bl)
        .limit(limit)
        .offset(offset)
    )
    try:
        result = await db.execute(stmt)
        rows = result.all()
        if not rows:
            raise HTTPException(status_code=404, detail="ID de BL no encontrado")
        output = [dict(row._mapping) for row in rows]
        return output
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al ejecutar la consulta filtro_request_id_bl: {str(e)}")

# ------------------------------
# GET /requests/bl_code/{code} - Filtrado por BL code
# ------------------------------
@router.get("/requests/bl_code/{code}", response_model=List[RequestRead])
async def get_requests_by_bl_code(
    code: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    stmt = (
        select(
            Request.id.label("id_request"),
            HTMLDescargado.id.label("id_html"),
            BL.code.label("bl_code"),
            StatusBL.descripcion_status.label("descripcion_status"),
            Request.mensaje,
            RespuestaRequest.descripcion.label("respuesta_request"),
            func.to_char(BL.fecha, 'YYYY-MM-DD').label("fecha_bl"),
            func.to_char(Request.fecha, 'YYYY-MM-DD HH24:MI:SS').label("fecha_request")
        )
        .join(HTMLDescargado, Request.id_html == HTMLDescargado.id)
        .join(RespuestaRequest, Request.id_respuesta == RespuestaRequest.id)
        .join(BL, Request.id_bl == BL.id)
        .join(StatusBL, BL.id_status == StatusBL.id)
        .where(BL.code.ilike(f"{code.upper()}%"))
        .limit(limit)
        .offset(offset)
    )
    try:
        result = await db.execute(stmt)
        rows = result.all()
        if not rows:
            raise HTTPException(status_code=404, detail="Código de BL no encontrado")
        output = [dict(row._mapping) for row in rows]
        return output
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al ejecutar la consulta filtro_request_code_bl: {str(e)}")

# ------------------------------
# PATCH /requests/{id_request} - Actualización parcial de un Request
# ------------------------------
@router.patch("/requests/{id_request}")
async def actualizar_parcial_request(
    id_request: int,
    bl_code: Optional[str] = Query(None),
    descripcion_status: Optional[str] = Query(None),
    mensaje: Optional[str] = Query(None),
    descripcion_respuesta: Optional[str] = Query(None),
    fecha_bl: Optional[str] = Query(None),
    fecha_request: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    rows_updated = 0
    try:
        # Actualizar Request
        stmt_req = select(Request).where(Request.id == id_request)
        result_req = await db.execute(stmt_req)
        req_instance = result_req.scalars().first()
        if not req_instance:
            raise HTTPException(status_code=404, detail="Request no encontrado")
        if mensaje is not None:
            req_instance.mensaje = mensaje
            rows_updated += 1
        if fecha_request is not None:
            try:
                req_instance.fecha = datetime.strptime(fecha_request, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de 'fecha_request' inválido. Use YYYY-MM-DD HH:MM:SS.")
            rows_updated += 1

        # Actualizar BL (bl_code, fecha_bl y status)
        if bl_code is not None or fecha_bl is not None or descripcion_status is not None:
            stmt_bl = select(BL).where(BL.id == select(Request.id_bl).where(Request.id == id_request).scalar_subquery())
            result_bl = await db.execute(stmt_bl)
            bl_instance = result_bl.scalars().first()
            if not bl_instance:
                raise HTTPException(status_code=404, detail="BL no encontrado para actualizar")
            if bl_code is not None:
                bl_code = bl_code.upper()
                stmt_check = select(BL).where(BL.code == bl_code)
                result_check = await db.execute(stmt_check)
                if result_check.scalars().first() is None:
                    raise HTTPException(status_code=400, detail="El bl_code no existe en la tabla 'bls'.")
                bl_instance.code = bl_code
                rows_updated += 1
            if fecha_bl is not None:
                try:
                    bl_instance.fecha = datetime.strptime(fecha_bl, "%Y-%m-%d").date()
                except ValueError:
                    raise HTTPException(status_code=400, detail="Formato de 'fecha_bl' inválido. Use YYYY-MM-DD.")
                rows_updated += 1
            if descripcion_status is not None:
                stmt_status = select(StatusBL).where(StatusBL.descripcion_status == descripcion_status)
                result_status = await db.execute(stmt_status)
                status_instance = result_status.scalars().first()
                if not status_instance:
                    raise HTTPException(status_code=400, detail="El status no existe en 'status_bl'.")
                bl_instance.id_status = status_instance.id
                rows_updated += 1

        # Actualizar RespuestaRequest
        if descripcion_respuesta is not None:
            stmt_resp = select(RespuestaRequest).where(RespuestaRequest.descripcion == descripcion_respuesta)
            result_resp = await db.execute(stmt_resp)
            resp_instance = result_resp.scalars().first()
            if not resp_instance:
                raise HTTPException(status_code=400, detail="La respuesta no existe en 'respuesta_requests'.")
            req_instance.id_respuesta = resp_instance.id
            rows_updated += 1

        if rows_updated == 0:
            raise HTTPException(status_code=404, detail="No se encontró el registro para actualizar")
        await db.commit()
        await db.refresh(req_instance)
        return {"mensaje": f"Se actualizaron {rows_updated} conjuntos de datos con éxito"}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al actualizar request: {str(e)}")

# ------------------------------
# POST /requests/ - Insertar un nuevo Request
# ------------------------------
@router.post("/requests/")
async def insertar_request(
    url: str,
    mensaje: str,
    sucess: bool,
    id_bl: Optional[int] = Query(None),
    id_html: Optional[int] = Query(None),
    id_respuesta: Optional[int] = Query(None),
    fecha: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    try:
        if not url or not mensaje or sucess is None:
            raise HTTPException(status_code=400, detail="Los campos 'url', 'mensaje' y 'sucess' son obligatorios.")
        if fecha:
            try:
                fecha_dt = datetime.strptime(fecha, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de 'fecha' inválido. Use YYYY-MM-DD HH:MM:SS.")
        else:
            fecha_dt = datetime.now()
        query_insert = """
            INSERT INTO requests (id_bl, url, fecha, mensaje, sucess, id_html, id_respuesta)
            VALUES (:id_bl, :url, :fecha, :mensaje, :sucess, :id_html, :id_respuesta)
            RETURNING id;
        """
        values_insert = {
            "id_bl": id_bl,
            "url": url,
            "fecha": fecha_dt,
            "mensaje": mensaje,
            "sucess": sucess,
            "id_html": id_html,
            "id_respuesta": id_respuesta,
        }
        result_insert = await db.execute(text(query_insert), values_insert)
        await db.commit()
        id_request = result_insert.scalar()
        if not id_request:
            raise HTTPException(status_code=500, detail="Error al insertar el registro en 'requests'.")
        return {"message": "Request creado exitosamente", "id_request": id_request}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al insertar el request: {str(e)}")
