from fastapi import APIRouter, HTTPException, Query, Depends, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy.future import select
from sqlalchemy import func, text
import asyncio
import logging
import json

from database import get_db
from models import BL, Naviera, StatusBL, Etapa, HTMLDescargado, RespuestaRequest, Request
from .bls_endpoints import bl_to_dict
from .test_agent import TestAgent  # Importar solo el agente de prueba por ahora

# Comentar los agentes reales por ahora
# from app.agents.maersk_zen import AgenteMaerskZen
# from app.agents.hapag_zen import AgenteHapagZen
# from app.agents.msc_zen import AgenteMSC
# from app.agents.cma_zen import AgenteCMA
# from app.agents.one_zen import AgenteONE
# from app.agents.cosco_zen import AgenteCOSCO

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/rutina",
    tags=["Rutina"],
    responses={404: {"description": "Not found"}},
)

# Modificar el mapeo de agentes para usar solo TestAgent
AGENT_MAPPING = {
    "MAERSK": TestAgent,
    "HAPAG-LLOYD": TestAgent,
    "MSC": TestAgent,
    "CMA": TestAgent,
    "ONE": TestAgent,
    "COSCO": TestAgent,
}

async def get_bls_for_routine(
    db: AsyncSession,
    naviera: str = None,
    estados: List[int] = None,
    limit: int = 10,
    mes: Optional[int] = None,
    anio: Optional[int] = None,
    bl_code: Optional[str] = None
) -> List[dict]:
    """
    Obtiene los BLs para procesar según los filtros especificados
    """
    try:
        # Construir la consulta base
        query = select(BL)
        
        # Aplicar filtros
        if naviera:
            query = query.filter(BL.naviera == naviera)
        if estados:
            query = query.filter(BL.id_status.in_(estados))
        if mes and anio:
            query = query.filter(
                BL.fecha >= datetime(anio, mes, 1),
                BL.fecha < datetime(anio, mes + 1, 1) if mes < 12 else datetime(anio + 1, 1, 1)
            )
        if bl_code:
            query = query.filter(BL.code == bl_code)
        
        # Limitar resultados
        query = query.limit(limit)
        
        # Ejecutar consulta
        result = await db.execute(query)
        bls = result.scalars().all()
        
        # Convertir a diccionarios
        return [{"id": bl.id, "bl_code": bl.code, "naviera": bl.naviera} for bl in bls]
        
    except Exception as e:
        logger.error(f"Error obteniendo BLs: {str(e)}")
        return []

@router.post("/")
async def ejecutar_rutina(
    background_tasks: BackgroundTasks,
    navieras: List[str] = Query(None),
    modo: str = Query("diario", regex="^(diario|semanal|mensual|manual)$"),
    mes: Optional[int] = Query(None, ge=1, le=12),
    anio: Optional[int] = Query(None, ge=2000, le=2100),
    bl_code: Optional[str] = Query(None),
    estado: Optional[int] = Query(None),
    limit: int = Query(10, ge=1, le=50),
    db: AsyncSession = Depends(get_db)
):
    """
    Ejecuta la rutina de scraping para los BLs especificados
    
    Args:
        navieras: Lista de navieras a procesar
        modo: Modo de operación (diario/semanal/mensual/manual)
        mes: Mes para filtrar BLs
        anio: Año para filtrar BLs
        bl_code: Código de BL específico
        estado: Estado específico de BL
        limit: Límite de BLs a procesar
    """
    # Map modes to status IDs (based on rutina.py logic)
    modo_estados = {
        "diario": [1, 17, 3],
        "semanal": [5, 3, 14],
        "mensual": [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 17],
        "manual": [99]
    }

    estados = [estado] if estado else modo_estados.get(modo, [1, 17, 3])
    
    try:
        # Obtener navieras disponibles si no se especifica ninguna
        if not navieras:
            stmt = select(Naviera.nombre)
            result = await db.execute(stmt)
            navieras = [row[0] for row in result.all()]

        # Procesar BLs para cada naviera
        all_bls = []
        for naviera in navieras:
            bls = await get_bls_for_routine(
                db=db,
                naviera=naviera,
                estados=estados,
                limit=limit,
                mes=mes,
                anio=anio,
                bl_code=bl_code
            )
            all_bls.extend(bls)

        # Agregar tarea de fondo para procesar
        background_tasks.add_task(process_bls_async, all_bls, db)

        return {
            "message": "Rutina iniciada",
            "bls_seleccionados": len(all_bls),
            "modo": modo,
            "estados": estados
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error executing routine: {str(e)}"
        )

async def process_bls_async(bls: List[dict], db: AsyncSession):
    try:
        for bl in bls:
            # Obtener instancia de naviera
            stmt = select(Naviera).where(Naviera.nombre == bl['naviera'])
            result = await db.execute(stmt)
            naviera = result.scalars().first()
            
            if not naviera:
                logger.error(f"Naviera not found for BL {bl['bl_code']}")
                continue

            # Obtener clase de agente desde el mapeo
            agent_class = AGENT_MAPPING.get(naviera.nombre)
            if not agent_class:
                logger.error(f"No agente disponible para la naviera {naviera.nombre}")
                continue

            # Inicializar agente
            agent = agent_class(None, db)
            
            try:
                # Obtener instancia de BL
                stmt = select(BL).where(BL.id == bl['id'])
                result = await db.execute(stmt)
                bl_instance = result.scalars().first()

                if not bl_instance:
                    logger.error(f"BL not found: {bl['bl_code']}")
                    continue

                # Intentar extraer datos de BL
                response = await agent.scrape_bl(bl['bl_code'])
                
                # Actualizar BL según el resultado de scraping
                if response and hasattr(response, 'status_code'):
                    if response.status_code == 200:
                        # Actualizar estado de BL y otros campos
                        bl_instance.id_status = response.get('status', 18)
                        bl_instance.revisado_hoy = True
                        bl_instance.proxima_revision = datetime.now() + timedelta(days=1)
                        
                        # Guardar respuesta de solicitud
                        request_response = RespuestaRequest(
                            id_bl=bl_instance.id,
                            url=agent.get_tracking_url(bl['bl_code']),
                            status_code=response.status_code,
                            caso=1,  # caso de éxito
                            mensaje="Success"
                        )
                        db.add(request_response)
                    else:
                        # Manejar respuesta de error
                        bl_instance.id_status = 18  # estado de error
                        request_response = RespuestaRequest(
                            id_bl=bl_instance.id,
                            url=agent.get_tracking_url(bl['bl_code']),
                            status_code=response.status_code,
                            caso=99,  # error case
                            mensaje=f"HTTP Error: {response.status_code}"
                        )
                        db.add(request_response)

                await db.commit()
                logger.info(f"Successfully processed BL {bl['bl_code']}")
                
            except Exception as e:
                logger.error(f"Error processing BL {bl['bl_code']}: {str(e)}")
                await db.rollback()
                
                # Guardar respuesta de error
                try:
                    error_response = RespuestaRequest(
                        id_bl=bl['id'],
                        url=agent.get_tracking_url(bl['bl_code']),
                        status_code=500,
                        caso=99,
                        mensaje=str(e)
                    )
                    db.add(error_response)
                    await db.commit()
                except Exception as e2:
                    logger.error(f"Error saving error response: {str(e2)}")

    except Exception as e:
        logger.error(f"Error in process_bls_async: {str(e)}")

@router.post("/test/")
async def test_rutina(
    bl_code: str = Query(..., description="Código BL para probar"),
    db: AsyncSession = Depends(get_db)
):
    """
    Endpoint de prueba para verificar el procesamiento de un solo BL
    """
    try:
        agent = TestAgent(db)
        
        # Obtener BL
        stmt = select(BL).where(BL.code == bl_code)
        result = await db.execute(stmt)
        bl = result.scalar_one_or_none()
        
        if not bl:
            raise HTTPException(status_code=404, detail="BL no encontrado")
        
        # Obtener respuesta de prueba
        response = await agent.scrape_bl(bl_code)
        
        # Obtener el máximo ID de respuesta_requests y incrementarlo
        stmt = text("SELECT COALESCE(MAX(id), 0) + 1 FROM respuesta_requests")
        result = await db.execute(stmt)
        next_id = result.scalar()

        # Crear RespuestaRequest con ID explícito
        respuesta = RespuestaRequest(
            id=next_id,
            descripcion="Test successful"
        )
        db.add(respuesta)
        await db.flush()

        # Crear HTMLDescargado
        html_descargado = HTMLDescargado(
            ruta_full="test/path",
            nombre=f"test_{bl_code}.html",
            info=1,
            fecha_descarga=datetime.now(),
            ruta_relativa="test",
            tipo_archivo=1,
            en_s3=False,
            en_pabrego=True
        )
        db.add(html_descargado)
        await db.flush()

        # Crear Request con todas las relaciones requeridas
        request = Request(
            id_bl=bl.id,
            url=agent.get_tracking_url(bl_code),
            fecha=datetime.now(),
            mensaje="Test successful",
            sucess=True,
            id_html=html_descargado.id,
            id_respuesta=respuesta.id
        )
        db.add(request)
        
        await db.commit()
        
        return {
            "message": "Test completado exitosamente",
            "bl_code": bl_code,
            "response": response
        }
        
    except Exception as e:
        logger.error(f"Error en test_rutina: {str(e)}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
