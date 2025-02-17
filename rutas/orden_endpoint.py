# archivo: routers/orden_routes.py

from fastapi import APIRouter, HTTPException, Query, Depends, BackgroundTasks
from datetime import datetime
from typing import Optional
import json
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db  # Retorna un AsyncSession
from rutas.autenticacion import check_rol, get_current_user
from rutas.scraper_service import run_scraper_order
from models import User  # Otros modelos se importan en models.py

router = APIRouter()

# ===============================
# Endpoints para Orden Descargas
# ===============================

@router.get("/orden_descarga/")
async def superfiltro_orden_descargas(
    id: Optional[int] = Query(None),
    nombre_usuario: Optional[str] = Query(None),
    fecha_creacion: Optional[str] = Query(None),
    fecha_programacion: Optional[str] = Query(None),
    descripcion: Optional[str] = Query(None),
    order_by: Optional[str] = Query(
        None,
        regex="^(od\\.id|u\\.nombre|od\\.fecha_creacion|od\\.fecha_programacion|od\\.descripcion)$"
    ),
    order: str = Query("ASC", regex="^(ASC|DESC|asc|desc)$"),
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    query = """
        SELECT od.id, u.nombre_usuario, od.fecha_creacion, od.fecha_programacion, od.descripcion, od.enviar_correo
        FROM orden_descargas od
        JOIN usuarios u ON u.id = od.id_usuario
        WHERE 1=1
    """
    values = {}
    if id is not None:
        query += " AND od.id = :id"
        values["id"] = id
    if nombre_usuario:
        query += " AND u.nombre_usuario = :nombre_usuario"
        values["nombre_usuario"] = nombre_usuario
    if fecha_creacion:
        try:
            fecha_creacion_dt = datetime.strptime(fecha_creacion, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise HTTPException(status_code=400, detail="Formato inválido para fecha_creacion, use YYYY-MM-DD HH:MM:SS")
        query += " AND od.fecha_creacion = :fecha_creacion"
        values["fecha_creacion"] = fecha_creacion_dt
    if fecha_programacion:
        try:
            fecha_programacion_dt = datetime.strptime(fecha_programacion, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise HTTPException(status_code=400, detail="Formato inválido para fecha_programacion, use YYYY-MM-DD HH:MM:SS")
        query += " AND od.fecha_programacion = :fecha_programacion"
        values["fecha_programacion"] = fecha_programacion_dt
    if descripcion:
        query += " AND od.descripcion = :descripcion"
        values["descripcion"] = descripcion
    if order_by:
        query += f" ORDER BY {order_by} {order}"
    query += " LIMIT :limit OFFSET :offset"
    values["limit"] = limit
    values["offset"] = offset

    try:
        result = await db.execute(text(query), values)
        rows = result.mappings().all()
        if not rows:
            raise HTTPException(status_code=404, detail="Orden de descarga no encontrada")
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error ejecutando la consulta orden descarga: {str(e)}")


@router.patch("/orden_descarga/")
async def orden_descarga_editar(
    id_orden: int,
    nombre_usuario: Optional[str] = Query(None),
    fecha_creacion: Optional[str] = Query(None),
    fecha_programacion: Optional[str] = Query(None),
    descripcion: Optional[str] = Query(None),
    enviar_correo: Optional[bool] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    fields_usuario = []
    fields_orden = []
    values_usuario = {"id_orden": id_orden}
    values_orden = {"id_orden": id_orden}
    filas_actualizadas = 0

    if nombre_usuario is not None:
        query_nombre_usuario = "SELECT id FROM usuarios WHERE nombre_usuario = :nombre_usuario"
        result = await db.execute(text(query_nombre_usuario), {"nombre_usuario": nombre_usuario})
        id_usuario = result.scalar()
        if id_usuario is None:
            raise HTTPException(status_code=400, detail=f"El usuario {nombre_usuario} no existe.")
        fields_usuario.append("id_usuario = :id_usuario")
        values_usuario["id_usuario"] = id_usuario

    if fecha_creacion is not None:
        try:
            fecha_creacion_dt = datetime.strptime(fecha_creacion, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise HTTPException(status_code=400, detail="Formato inválido para fecha_creacion, use YYYY-MM-DD HH:MM:SS")
        fields_orden.append("fecha_creacion = :fecha_creacion")
        values_orden["fecha_creacion"] = fecha_creacion_dt

    if fecha_programacion is not None:
        try:
            fecha_programacion_dt = datetime.strptime(fecha_programacion, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise HTTPException(status_code=400, detail="Formato inválido para fecha_programacion, use YYYY-MM-DD HH:MM:SS")
        fields_orden.append("fecha_programacion = :fecha_programacion")
        values_orden["fecha_programacion"] = fecha_programacion_dt

    if descripcion is not None:
        fields_orden.append("descripcion = :descripcion")
        values_orden["descripcion"] = descripcion

    if enviar_correo is not None:
        fields_orden.append("enviar_correo = :enviar_correo")
        values_orden["enviar_correo"] = enviar_correo

    if not (fields_usuario or fields_orden):
        raise HTTPException(status_code=400, detail="No se proporcionaron campos para actualizar")

    try:
        if fields_usuario:
            query_usuario = f"""
                UPDATE orden_descargas
                SET {', '.join(fields_usuario)}
                WHERE id = :id_orden
                RETURNING id;
            """
            res = await db.execute(text(query_usuario), values_usuario)
            if res.scalar():
                filas_actualizadas += 1

        if fields_orden:
            query_orden = f"""
                UPDATE orden_descargas
                SET {', '.join(fields_orden)}
                WHERE id = :id_orden
                RETURNING id;
            """
            res = await db.execute(text(query_orden), values_orden)
            if res.scalar():
                filas_actualizadas += 1

        if filas_actualizadas == 0:
            raise HTTPException(status_code=404, detail="No se encontró el registro para actualizar")

        await db.commit()
        return {"mensaje": f"Se actualizaron {filas_actualizadas} tablas con éxito"}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al actualizar orden descarga: {str(e)}")


@router.post("/orden_descarga/")
async def orden_descarga_crear(
    nombre_usuario: str,
    fecha_creacion: str,
    fecha_programacion: str,
    descripcion: Optional[str] = None,
    enviar_correo: bool = Query(False, description="Indica si se enviará correo al finalizar la orden"),
    detalles: Optional[str] = Query(
        None,
        description=(
            "JSON string de detalles de la orden. Ejemplo: "
            '[{"id_request": 1, "id_bls": 2}, {"id_request": 3, "id_bls": 4}]'
        )
    ),
    db: AsyncSession = Depends(get_db)
): 
    query_usuario = "SELECT id FROM usuarios WHERE nombre_usuario ILIKE :nombre_usuario"
    result = await db.execute(text(query_usuario), {"nombre_usuario": f"{nombre_usuario}%"})
    id_usuario = result.scalar()
    if id_usuario is None:
        raise HTTPException(status_code=404, detail=f"Usuario {nombre_usuario} no encontrado")

    if not (nombre_usuario and fecha_creacion and fecha_programacion):
        raise HTTPException(
            status_code=400, 
            detail="Los campos nombre_usuario, fecha_creacion y fecha_programacion son obligatorios"
        )
    try:
        fecha_creacion_dt = datetime.strptime(fecha_creacion, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        raise HTTPException(status_code=400, detail="Formato de 'fecha_creacion' inválido. Use YYYY-MM-DD HH:MM:SS")
    try:
        fecha_programacion_dt = datetime.strptime(fecha_programacion, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        raise HTTPException(status_code=400, detail="Formato de 'fecha_programacion' inválido. Use YYYY-MM-DD HH:MM:SS")

    query_orden = """
        INSERT INTO orden_descargas (
            id_usuario, fecha_creacion, fecha_programacion, descripcion, enviar_correo
        ) VALUES (
            :id_usuario, :fecha_creacion, :fecha_programacion, :descripcion, :enviar_correo
        )
        RETURNING id;
    """
    values_orden = {
        "id_usuario": id_usuario,
        "fecha_creacion": fecha_creacion_dt,
        "fecha_programacion": fecha_programacion_dt,
        "descripcion": descripcion,
        "enviar_correo": enviar_correo
    }
    try:
        res = await db.execute(text(query_orden), values_orden)
        id_orden = res.scalar()
        if not id_orden:
            raise HTTPException(status_code=500, detail="ERROR al insertar en orden descargas")
        
        if detalles:
            try:
                detalles_list = json.loads(detalles)
                if not isinstance(detalles_list, list):
                    raise HTTPException(status_code=400, detail="El parámetro 'detalles' debe ser un arreglo")
            except Exception as e:
                raise HTTPException(status_code=400, detail="Error al parsear 'detalles': " + str(e))
            
            for detalle in detalles_list:
                id_request = detalle.get("id_request")
                id_bls = detalle.get("id_bls")
                if id_request is None or id_bls is None:
                    raise HTTPException(status_code=400, detail="Cada detalle debe contener 'id_request' y 'id_bls'")
                query_detalle = """
                    INSERT INTO orden_detalle (id_cabecera, id_request, id_bls)
                    VALUES (:id_cabecera, :id_request, :id_bls)
                """
                values_detalle = {"id_cabecera": id_orden, "id_request": id_request, "id_bls": id_bls}
                await db.execute(text(query_detalle), values_detalle)
        await db.commit()
        return {"mensaje": f"Registro creado con éxito en orden descargas, el id es: {id_orden}", "id": id_orden}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al insertar el registro en orden descargas: {str(e)}")


@router.delete("/orden_descarga/")
async def delete_orden_descarga(
    id: int = Query(..., description="ID de la orden a eliminar"),
    db: AsyncSession = Depends(get_db)
):
    try:
        query_detalles = "DELETE FROM orden_detalle WHERE id_cabecera = :id;"
        await db.execute(text(query_detalles), {"id": id})
        
        query_orden = "DELETE FROM orden_descargas WHERE id = :id RETURNING id;"
        res = await db.execute(text(query_orden), {"id": id})
        deleted_id = res.scalar()
        
        if not deleted_id:
            raise HTTPException(status_code=404, detail="Orden de descarga no encontrada")
        
        await db.commit()
        return {"mensaje": "Orden de descarga y sus detalles eliminados exitosamente", "id": deleted_id}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al eliminar la orden de descarga: {str(e)}")


@router.post("/orden_descarga/execute")
async def execute_order(
    order_id: int = Query(..., description="ID de la orden a ejecutar"),
    navieras: Optional[str] = Query(None, description="Lista de navieras separadas por coma"),
    dia: Optional[int] = Query(None, description="Día del campo fecha_bl"),
    mes: Optional[int] = Query(None, description="Mes del campo fecha_bl"),
    anio: Optional[int] = Query(None, description="Año del campo fecha_bl"),
    bls: Optional[str] = Query(None, description="Lista de BLs (separadas por coma)"),
    diario: bool = Query(False, description="Flag: diario"),
    semanal: bool = Query(False, description="Flag: semanal"),
    mensual: bool = Query(False, description="Flag: mensual"),
    csv: bool = Query(False, description="Flag: csv"),
    background_tasks: BackgroundTasks = None,
    db: AsyncSession = Depends(get_db)
):
    query = "SELECT * FROM orden_descargas WHERE id = :order_id"
    res = await db.execute(text(query), {"order_id": order_id})
    order = res.fetchone()
    if not order:
        raise HTTPException(status_code=404, detail="Orden de descarga no encontrada")
    
    filters = {}
    if navieras:
        filters["navieras"] = [n.strip() for n in navieras.split(",")]
    if dia is not None:
        filters["dia"] = dia
    if mes is not None:
        filters["mes"] = mes
    if anio is not None:
        filters["anio"] = anio
    if bls:
        filters["bls"] = [b.strip() for b in bls.split(",")]
    filters["diario"] = diario or (not (diario or semanal or mensual or csv))
    if semanal:
        filters["semanal"] = semanal
    if mensual:
        filters["mensual"] = mensual
    if csv:
        filters["csv"] = csv

    background_tasks.add_task(run_scraper_order, order_id, filters)
    
    return {"mensaje": f"Ejecutando la orden {order_id} con filtros: {filters}"}


# ==========================================
# Endpoints para CRUD de Orden Detalle
# ==========================================

@router.get("/orden_detalle/")
async def get_orden_detalle(
    id: Optional[int] = Query(None),
    id_cabecera: Optional[int] = Query(None),
    id_request: Optional[int] = Query(None),
    id_bls: Optional[int] = Query(None),
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    query = "SELECT * FROM orden_detalle WHERE 1=1"
    values = {}
    if id is not None:
        query += " AND id = :id"
        values["id"] = id
    if id_cabecera is not None:
        query += " AND id_cabecera = :id_cabecera"
        values["id_cabecera"] = id_cabecera
    if id_request is not None:
        query += " AND id_request = :id_request"
        values["id_request"] = id_request
    if id_bls is not None:
        query += " AND id_bls = :id_bls"
        values["id_bls"] = id_bls

    query += " LIMIT :limit OFFSET :offset"
    values["limit"] = limit
    values["offset"] = offset

    try:
        res = await db.execute(text(query), values)
        rows = res.mappings().all()
        return rows or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener orden_detalle: {str(e)}")


@router.post("/orden_detalle/")
async def create_orden_detalle(
    id_cabecera: int,
    id_request: int,
    id_bls: int,
    db: AsyncSession = Depends(get_db)
):
    query_cabecera = "SELECT id FROM orden_descargas WHERE id = :id_cabecera"
    res = await db.execute(text(query_cabecera), {"id_cabecera": id_cabecera})
    cabecera = res.scalar()
    if not cabecera:
        raise HTTPException(status_code=404, detail="Orden cabecera no encontrada")
    query = """
        INSERT INTO orden_detalle (id_cabecera, id_request, id_bls)
        VALUES (:id_cabecera, :id_request, :id_bls)
        RETURNING id;
    """
    values = {"id_cabecera": id_cabecera, "id_request": id_request, "id_bls": id_bls}
    try:
        res = await db.execute(text(query), values)
        new_record = res.fetchone()
        if not new_record:
            raise HTTPException(status_code=500, detail="Error al crear registro en orden_detalle")
        await db.commit()
        return {"mensaje": "Registro de orden_detalle creado exitosamente", "id": new_record["id"]}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al crear registro en orden_detalle: {str(e)}")


@router.patch("/orden_detalle/")
async def update_orden_detalle(
    id: int,
    id_cabecera: Optional[int] = Query(None),
    id_request: Optional[int] = Query(None),
    id_bls: Optional[int] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    fields = []
    values = {"id": id}
    if id_cabecera is not None:
        query_cabecera = "SELECT id FROM orden_descargas WHERE id = :id_cabecera"
        res = await db.execute(text(query_cabecera), {"id_cabecera": id_cabecera})
        cabecera = res.scalar()
        if not cabecera:
            raise HTTPException(status_code=404, detail="Orden cabecera no encontrada")
        fields.append("id_cabecera = :id_cabecera")
        values["id_cabecera"] = id_cabecera
    if id_request is not None:
        fields.append("id_request = :id_request")
        values["id_request"] = id_request
    if id_bls is not None:
        fields.append("id_bls = :id_bls")
        values["id_bls"] = id_bls
    if not fields:
        raise HTTPException(status_code=400, detail="No se proporcionaron campos para actualizar")
    query = f"UPDATE orden_detalle SET {', '.join(fields)} WHERE id = :id RETURNING id;"
    try:
        res = await db.execute(text(query), values)
        updated_record = res.fetchone()
        if not updated_record:
            raise HTTPException(status_code=404, detail="Registro de orden_detalle no encontrado")
        await db.commit()
        return {"mensaje": "Registro de orden_detalle actualizado exitosamente", "id": updated_record["id"]}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al actualizar registro en orden_detalle: {str(e)}")


@router.delete("/orden_detalle/")
async def delete_orden_detalle(
    id: int = Query(..., description="ID del detalle a eliminar"),
    db: AsyncSession = Depends(get_db)
):
    query = "DELETE FROM orden_detalle WHERE id = :id RETURNING id;"
    try:
        res = await db.execute(text(query), {"id": id})
        deleted_record = res.fetchone()
        if not deleted_record:
            raise HTTPException(status_code=404, detail="Registro de orden_detalle no encontrado")
        await db.commit()
        return {"mensaje": "Registro de orden_detalle eliminado exitosamente", "id": deleted_record["id"]}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al eliminar registro en orden_detalle: {str(e)}")
