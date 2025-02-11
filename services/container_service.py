from fastapi import HTTPException
from typing import Dict, Any, Optional
from repositories.container_repository import (
    fetch_containers,
    get_container_id,
    get_container_size,
    get_container_type,
    fetch_container_by_code,
    fetch_container_by_bl_code,
    create_container_viaje,
    get_bl_id,
    update_container,
    create_container
)

async def get_filtered_containers(
    codigo_container: Optional[str] = None,
    bl_code: Optional[str] = None,
    size: Optional[str] = None,
    type: Optional[str] = None,
    contenido: Optional[str] = None,
    order_by: Optional[str] = None,
    order: str = "ASC",
    limit: int = 500,
    offset: int = 0
):
    filters = {
        "codigo_container": codigo_container,
        "bl_code": bl_code,
        "size": size,
        "type": type,
        "contenido": contenido
    }
    return await fetch_containers(filters, order_by, order, limit, offset)

async def update_container_record(
    container_code: str,
    size: Optional[str] = None,
    type: Optional[str] = None,
    contenido: Optional[str] = None
) -> Dict[str, Any]:
    # Get container ID
    container_id = await get_container_id(container_code.upper())
    if not container_id:
        raise HTTPException(status_code=404, detail="Container no encontrado")

    update_fields = {}

    if size:
        size = size.upper()
        nombre_size = await get_container_size(size)
        if not nombre_size:
            raise HTTPException(status_code=400, detail="Size inválido")
        update_fields["size"] = nombre_size

    if type:
        type = type.upper()
        nombre_type = await get_container_type(type)
        if not nombre_type:
            raise HTTPException(status_code=400, detail="Type inválido")
        update_fields["type"] = nombre_type

    if contenido:
        contenido = contenido.upper()
        if contenido not in ["DRY", "REEFER"]:
            raise HTTPException(status_code=400, detail="Contenido es inválido")
        update_fields["contenido"] = contenido

    if not update_fields:
        raise HTTPException(status_code=400, detail="No hay campos para actualizar")

    result = await update_container(container_id, update_fields)
    if not result:
        raise HTTPException(status_code=500, detail="Actualizar Container falló")

    return {"message": "Container actualizado exitosamente"}

async def get_container_by_code(code: str, limit: int, offset: int):
    result = await fetch_container_by_code(code, limit, offset)
    if not result:
        raise HTTPException(status_code=404, detail="Container no encontrado")
    return result

async def get_container_by_bl_code(code: str, limit: int, offset: int):
    result = await fetch_container_by_bl_code(code, limit, offset)
    if not result:
        raise HTTPException(status_code=404, detail="Container no encontrado")
    return result

async def create_container_record(
    code: str,
    size: Optional[str] = None,
    type: Optional[str] = None,
    contenido: Optional[str] = None,
) -> Dict[str, Any]:
    code = code.upper()
    existing_container = await get_container_id(code)
    if existing_container:
        raise HTTPException(
            status_code=400, 
            detail=f"Container con codigo {code} ya existe"
        )

    values = {
        "code": code,
        "size": size.upper() if size else "DESCONOCIDO",
        "type": type.upper() if type else "DESCONOCIDO",
        "contenido": contenido.upper() if contenido else "DESCONOCIDO"
    }

    container_id = await create_container(values)
    if not container_id:
        raise HTTPException(
            status_code=500, 
            detail="Error al crear Container"
        )
    
    return {
        "message": "Container creado exitosamente",
        "id_container": container_id
    }

async def create_container_viaje_record(
    container_code: str,
    bl_code: str
) -> Dict[str, Any]:
    container_id = await get_container_id(container_code.upper())
    if not container_id:
        raise HTTPException(
            status_code=404, 
            detail="Container no encontrado"
        )

    bl_id = await get_bl_id(bl_code.upper())
    if not bl_id:
        raise HTTPException(
            status_code=404, 
            detail="BL no encontrado"
        )

    container_viaje_id = await create_container_viaje(container_id, bl_id)
    if not container_viaje_id:
        raise HTTPException(
            status_code=500, 
            detail="Error al crear Container_viaje"
        )

    return {
        "message": "Container_viaje Creado exitosamente",
        "id_container_viaje": container_viaje_id
    }