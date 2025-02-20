# archivo: routers/orden_routes.py

from fastapi import APIRouter, HTTPException, Query, Depends, BackgroundTasks
from datetime import datetime
from typing import Optional, List, Dict
import json
from sqlalchemy import text, select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession
import math
import logging
from sqlalchemy.dialects.postgresql import insert

from database import get_db  # Retorna un AsyncSession
from rutas.autenticacion import check_rol, get_current_user
from rutas.scraper_service import run_scraper_order
from models import (
    BL,
    OrdenDescarga,
    OrdenDetalle,
    Naviera,
    StatusBL
)

router = APIRouter()

logger = logging.getLogger(__name__)

# Valores de tiempo estándar por naviera (en minutos) para propósitos de prueba
TIEMPO_ESTIMADO_NAVIERA = {
    "MAERSK": 120,      # 120 minutos (2 horas) por cada 1000 BLs
    "MSC": 150,         # 150 minutos (2.5 horas) por cada 1000 BLs
    "COSCO": 140,       # 140 minutos por cada 1000 BLs
    "EVERGREEN": 135,   # 135 minutos por cada 1000 BLs
    "HAPAG-LLOYD": 130, # 130 minutos por cada 1000 BLs
    "ONE": 145,         # 145 minutos por cada 1000 BLs
    "CMA-CGM": 138,     # 138 minutos por cada 1000 BLs
    "DEFAULT": 120      # Por defecto: 120 minutos por cada 1000 BLs
}

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


async def get_bls(
    db: AsyncSession,
    navieras: List[int],
    fecha_inicio: str,
    fecha_termino: str,
    estados: List[int],
    bls: List[str] = None  # Estos son los códigos BL
) -> List[int]:
    try:
        fecha_inicio_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d").date()
        fecha_termino_dt = datetime.strptime(fecha_termino, "%Y-%m-%d").date()
        
        logger.info(f"Buscando BLs con criterios:")
        logger.info(f"Navieras: {navieras}")
        logger.info(f"Estados: {estados}")
        logger.info(f"Fecha inicio: {fecha_inicio_dt}")
        logger.info(f"Fecha término: {fecha_termino_dt}")
        if bls:
            logger.info(f"BLs específicos: {bls}")

        # Todas las condiciones se combinan con AND
        conditions = [
            BL.id_naviera.in_(navieras),  # Cualquiera de estas navieras
            BL.id_status.in_(estados),     # Y cualquiera de estos estados
            BL.fecha.between(fecha_inicio_dt, fecha_termino_dt)  # Y dentro del rango de fechas
        ]
        
        if bls:
            conditions.append(BL.code.in_(bls))  # Cambiado de bl_code a code para coincidir con el modelo

        # Obtener los IDs de los BLs que coinciden
        query = select(BL.id).where(and_(*conditions))

        result = await db.execute(query)
        ids = [row[0] for row in result.fetchall()]
        
        if not ids:
            logger.warning("No se encontraron BLs con los criterios especificados")
            
        return ids

    except ValueError as e:
        raise HTTPException(
            status_code=400, 
            detail=f"Formato de fecha inválido. Use YYYY-MM-DD: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error en get_bls: {str(e)}")
        raise

async def add_orden(
    db: AsyncSession,
    id_orden: int,
    ids_bls: List[int]
) -> None:
    """
    Inserta registros en orden_detalle usando bulk insert
    """
    try:
        values = [
            {"id_cabecera": id_orden, "id_bls": bl_id}
            for bl_id in ids_bls
        ]
        stmt = insert(OrdenDetalle).values(values)
        await db.execute(stmt)
        await db.commit()
    except Exception as e:
        await db.rollback()
        logger.error(f"Error en add_orden: {str(e)}")
        raise

async def summary_order(
    db: AsyncSession,
    id_order: int
) -> List[Dict]:
    """
    Genera resumen de BLs por naviera con tiempos estimados
    Fórmula: (nbls/1000 * estimado_naviera) en minutos
    Ejemplo: 
    - Si hay 2500 BLs y estimado_naviera es 120 minutos
    - (2500/1000 * 120) = 2.5 * 120 = 300 minutos
    """
    try:
        query = select(
            Naviera.nombre,
            func.count(BL.id).label('cantidad')
        ).join(
            OrdenDetalle, OrdenDetalle.id_bls == BL.id
        ).join(
            Naviera, Naviera.id == BL.id_naviera
        ).where(
            OrdenDetalle.id_cabecera == id_order
        ).group_by(
            Naviera.nombre
        )
        
        result = await db.execute(query)
        summary = []
        
        for row in result:
            naviera, cantidad = row
            # Obtener tiempo estándar para la naviera (valor por defecto si no se encuentra)
            tiempo_base = TIEMPO_ESTIMADO_NAVIERA.get(naviera.upper(), TIEMPO_ESTIMADO_NAVIERA["DEFAULT"])
            # Calcular tiempo estimado: (nbls/1000 * estimado_naviera)
            tiempo_estimado = math.ceil((cantidad / 1000) * tiempo_base)
            
            logger.info(f"""
                Cálculo para {naviera}:
                - Cantidad BLs: {cantidad}
                - Factor (cantidad/1000): {cantidad/1000}
                - Tiempo base (minutos): {tiempo_base}
                - Tiempo estimado (minutos): {tiempo_estimado}
            """)
            
            summary.append({
                "naviera": naviera,
                "cantidad_bls": cantidad,
                "tiempo_estimado": tiempo_estimado
            })
        
        await db.commit()  
        return summary
    except Exception as e:
        await db.rollback()
        logger.error(f"Error en summary_order: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al generar resumen: {str(e)}")

@router.post("/orden/crear")
async def crear_orden(orden_data: Dict, db: AsyncSession = Depends(get_db)):
    """
    Crear orden de descarga con los siguientes parámetros:
    - id_orden: ID de la orden
    - navieras: Lista de navieras separadas por coma (ej: "MAERSK, MSC, COSCO")
    - estados: Lista de estados separados por coma
    - fecha_inicio: Fecha inicio en formato YYYY-MM-DD
    - fecha_termino: Fecha término en formato YYYY-MM-DD
    - bls: Lista de códigos BL separados por coma (ej: "HLCUXM12410AVQV2, MSCUAB123456789")
    """
    try:
        # Obtener IDs de navieras desde sus nombres
        navieras_list = [nav.strip().upper() for nav in orden_data["navieras"].split(",")]
        query_navieras = select(Naviera.id, Naviera.nombre).where(Naviera.nombre.in_(navieras_list))
        result = await db.execute(query_navieras)
        navieras_found = result.fetchall()
        
        if not navieras_found:
            raise HTTPException(
                status_code=404,
                detail=f"No se encontraron navieras con los nombres: {navieras_list}"
            )
        
        ids_navieras = [row[0] for row in navieras_found]
        nombres_encontrados = [row[1] for row in navieras_found]
        
        # Registrar navieras no encontradas
        navieras_no_encontradas = set(navieras_list) - set(nombres_encontrados)
        if navieras_no_encontradas:
            logger.warning(f"Navieras no encontradas: {navieras_no_encontradas}")

        # Obtener IDs de estados desde sus descripciones (coincidencia exacta de la base de datos)
        estados_list = [estado.strip() for estado in orden_data["estados"].split(",")]
        query_estados = select(StatusBL.id, StatusBL.descripcion_status).where(
            StatusBL.descripcion_status.in_(estados_list)
        )
        result = await db.execute(query_estados)
        estados_found = result.fetchall()
        
        if not estados_found:
            # Obtener todos los estados disponibles para un mejor mensaje de error
            query_all_estados = select(StatusBL.descripcion_status)
            result = await db.execute(query_all_estados)
            estados_disponibles = [row[0] for row in result.fetchall()]
            
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No se encontraron estados con las descripciones: {estados_list}.\n"
                    f"Estados disponibles:\n" + "\n".join(f"- {estado}" for estado in estados_disponibles)
                )
            )

        ids_estados = [row[0] for row in estados_found]
        estados_encontrados = [row[1] for row in estados_found]
        
        # Registrar estados no encontrados
        estados_no_encontrados = set(estados_list) - set(estados_encontrados)
        if estados_no_encontrados:
            logger.warning(f"Estados no encontrados: {estados_no_encontrados}")

        # Convertir cadena de códigos BL a lista y normalizar
        bls_list = [bl.strip().upper() for bl in orden_data["bls"].split(",") if bl.strip()]
        if bls_list:
            logger.info(f"Lista de códigos BL a buscar: {bls_list}")

        # Obtener BLs que coincidan con los criterios
        ids_bls = await get_bls(
            db=db,
            navieras=ids_navieras,
            fecha_inicio=orden_data["fecha_inicio"],
            fecha_termino=orden_data["fecha_termino"],
            estados=ids_estados,
            bls=bls_list if bls_list else None
        )

        if not ids_bls:
            raise HTTPException(
                status_code=404,
                detail="No se encontraron BLs con los criterios especificados"
            )

        # Agregar a orden_detalle
        await add_orden(
            db=db,
            id_orden=orden_data["id_orden"],
            ids_bls=ids_bls
        )

        return {
            "mensaje": "Orden creada exitosamente",
            "id_orden": orden_data["id_orden"],
            "cantidad_bls": len(ids_bls),
            "detalles": {
                "navieras_encontradas": {
                    "total": len(ids_navieras),
                    "nombres": nombres_encontrados,
                    "no_encontradas": list(navieras_no_encontradas) if navieras_no_encontradas else []
                },
                "estados_encontrados": {
                    "total": len(ids_estados),
                    "descripciones": estados_encontrados,
                    "no_encontrados": list(estados_no_encontrados) if estados_no_encontrados else []
                },
                "bls_procesados": len(ids_bls)
            }
        }

    except Exception as e:
        await db.rollback()
        if isinstance(e, HTTPException):
            raise e
        logger.error(f"Error en crear_orden: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/orden/{id_orden}/resumen")
async def obtener_resumen_orden(
    id_orden: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Obtiene el resumen de una orden con estimaciones de tiempo por naviera
    """
    try:
        # Verificar que la orden existe
        orden = await db.execute(
            select(OrdenDescarga).where(OrdenDescarga.id == id_orden)
        )
        if not orden.scalar():
            raise HTTPException(status_code=404, detail="Orden no encontrada")
            
        # Obtener resumen
        resumen = await summary_order(db, id_orden)
        
        # Calcular tiempo total estimado
        tiempo_total = sum(item["tiempo_estimado"] for item in resumen)
        
        return {
            "id_orden": id_orden,
            "resumen_por_naviera": resumen,
            "tiempo_total_estimado": tiempo_total
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
