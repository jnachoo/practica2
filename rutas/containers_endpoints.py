from fastapi import APIRouter, HTTPException, Query, Depends, Body
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel
from sqlalchemy import select, func, desc, asc, extract
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from rutas.autenticacion import check_rol, get_current_user
from models import User, Container, ContainerViaje, BL

router = APIRouter()

# --- Esquemas Pydantic para respuesta ---

class ContainerViajeRead(BaseModel):
    id_container_viaje: int
    container_code: str
    bl_code: str
    size: Optional[str]
    type: Optional[str]
    contenido: Optional[str]

    class Config:
        # Para Pydantic V2, se recomienda usar from_attributes=True
        from_attributes = True

# --- GET: Super filtro de Containers ---
@router.get("/containers/", response_model=List[ContainerViajeRead])
async def super_filtro_containers(
    codigo_container: Optional[str] = Query(None),
    bl_code: Optional[str] = Query(None),
    size: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    contenido: Optional[str] = Query(None),
    order_by: Optional[str] = Query(
        None, regex="^(c\\.code|b\\.code|c\\.size|c\\.type|c\\.contenido|c\\.id)$"
    ),
    order: str = Query("ASC", regex="^(ASC|DESC|asc|desc)$"),
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    # Usamos select() para obtener los campos deseados
    stmt = (
        select(
            ContainerViaje.id.label("id_container_viaje"),
            Container.code.label("container_code"),
            BL.code.label("bl_code"),
            Container.size,
            Container.type,
            Container.contenido
        )
        .join(Container, Container.id == ContainerViaje.id_container)
        .join(BL, BL.id == ContainerViaje.id_bl)
    )
    if codigo_container:
        stmt = stmt.where(Container.code.ilike(f"{codigo_container}%"))
    if bl_code:
        stmt = stmt.where(BL.code.ilike(f"{bl_code}%"))
    if size:
        stmt = stmt.where(Container.size.ilike(f"{size}%"))
    if type:
        stmt = stmt.where(Container.type.ilike(f"{type}%"))
    if contenido:
        stmt = stmt.where(Container.contenido.ilike(f"{contenido}%"))
    
    # Mapeo de order_by a columnas
    order_by_mapping = {
        "c.code": Container.code,
        "b.code": BL.code,
        "c.size": Container.size,
        "c.type": Container.type,
        "c.contenido": Container.contenido,
        "c.id": Container.id,
    }
    if order_by and order_by in order_by_mapping:
        col = order_by_mapping[order_by]
        if order.lower() == "desc":
            stmt = stmt.order_by(desc(col))
        else:
            stmt = stmt.order_by(asc(col))
    stmt = stmt.limit(limit).offset(offset)
    try:
        result = await db.execute(stmt)
        rows = result.mappings().all()
        if not rows:
            raise HTTPException(status_code=404, detail="Containers no encontrados")
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al ejecutar la consulta containers: {str(e)}")

# --- GET: Containers por código (filtrado sobre containers.code) ---
@router.get("/containers/code/{code}", response_model=List[ContainerViajeRead])
async def ver_container_by_code(
    code: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    stmt = (
        select(
            ContainerViaje.id.label("id_container_viaje"),
            Container.code.label("container_code"),
            BL.code.label("bl_code"),
            Container.size,
            Container.type,
            Container.contenido
        )
        .join(Container, Container.id == ContainerViaje.id_container)
        .join(BL, BL.id == ContainerViaje.id_bl)
        .where(Container.code.ilike(f"{code}%"))
        .limit(limit).offset(offset)
    )
    try:
        result = await db.execute(stmt)
        rows = result.mappings().all()
        if not rows:
            raise HTTPException(status_code=404, detail="Containers no encontrados")
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al ejecutar la consulta containers: {str(e)}")

# --- GET: Containers por BL code (filtrado sobre bls.code) ---
@router.get("/containers/bl_code/{code}", response_model=List[ContainerViajeRead])
async def ver_container_by_bl_code(
    code: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    stmt = (
        select(
            ContainerViaje.id.label("id_container_viaje"),
            Container.code.label("container_code"),
            BL.code.label("bl_code"),
            Container.size,
            Container.type,
            Container.contenido
        )
        .join(Container, Container.id == ContainerViaje.id_container)
        .join(BL, BL.id == ContainerViaje.id_bl)
        .where(BL.code.ilike(f"{code}%"))
        .limit(limit).offset(offset)
    )
    try:
        result = await db.execute(stmt)
        rows = result.mappings().all()
        if not rows:
            raise HTTPException(status_code=404, detail="Containers no encontrados")
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al ejecutar la consulta containers: {str(e)}")

# --- GET: Containers por código alternativo (similar al anterior) ---
@router.get("/containers/{code}", response_model=List[ContainerViajeRead])
async def ver_container_by_code_alt(
    code: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    # Reutilizamos la consulta basada en Container.code
    stmt = (
        select(
            ContainerViaje.id.label("id_container_viaje"),
            Container.code.label("container_code"),
            BL.code.label("bl_code"),
            Container.size,
            Container.type,
            Container.contenido
        )
        .join(Container, Container.id == ContainerViaje.id_container)
        .join(BL, BL.id == ContainerViaje.id_bl)
        .where(Container.code.ilike(f"{code}%"))
        .limit(limit).offset(offset)
    )
    try:
        result = await db.execute(stmt)
        rows = result.mappings().all()
        if not rows:
            raise HTTPException(status_code=404, detail="Containers no encontrados")
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al ejecutar la consulta containers: {str(e)}")

# --- PATCH: Actualización parcial de un Container ---
@router.patch("/containers/{container_code}")
async def actualizar_parcial_container(
    container_code: str,
    size: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    contenido: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """
    Actualiza la tabla containers. Se pueden actualizar los campos size, type y contenido.
    Se asume que existe una tabla 'dict_containers' en la base de datos para validar size y type.
    """
    if not container_code:
        raise HTTPException(status_code=400, detail="Debe proporcionar un container_code")
    container_code = container_code.upper()
    stmt_check = select(Container).where(Container.code == container_code)
    result_check = await db.execute(stmt_check)
    container_instance = result_check.scalars().first()
    if not container_instance:
        raise HTTPException(status_code=400, detail="El código de container no existe.")
    # Se construirán los cambios; en este ejemplo, usaremos text queries para consultar el diccionario
    update_fields = {}
    if size is not None:
        size_upper = size.upper()
        # Consulta al diccionario (asumiendo que existe la tabla dict_containers)
        stmt_size = text("SELECT nombre_size FROM dict_containers WHERE size = :size LIMIT 1")
        result_size = await db.execute(stmt_size, {"size": size_upper})
        nombre_size = result_size.scalar()
        if nombre_size is None:
            raise HTTPException(status_code=400, detail="El size no existe en el diccionario de containers.")
        update_fields["size"] = nombre_size
    if type is not None:
        type_upper = type.upper()
        stmt_type = text("SELECT nombre_type FROM dict_containers WHERE type = :type LIMIT 1")
        result_type = await db.execute(stmt_type, {"type": type_upper})
        nombre_type = result_type.scalar()
        if nombre_type is None:
            raise HTTPException(status_code=400, detail="El type no existe en el diccionario de containers.")
        update_fields["type"] = nombre_type
    if contenido is not None:
        contenido_upper = contenido.upper()
        if contenido_upper not in ["DRY", "REEFER"]:
            raise HTTPException(status_code=400, detail="El tipo de contenido debe ser DRY o REEFER.")
        update_fields["contenido"] = contenido_upper
    if not update_fields:
        raise HTTPException(status_code=400, detail="No se proporcionaron campos para actualizar")
    try:
        # Actualización usando ORM
        for key, value in update_fields.items():
            setattr(container_instance, key, value)
        await db.commit()
        await db.refresh(container_instance)
        return {"message": "Actualización realizada con éxito"}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al actualizar container: {str(e)}")

# --- PATCH: Actualización parcial de container_viaje y containers ---
@router.patch("/containers_viaje/{id_container_viaje}")
async def actualizar_parcial_container_viaje(
    id_container_viaje: int,
    container_code: Optional[str] = Query(None),
    bl_code: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """
    Actualiza la tabla container_viaje. Si se actualiza el container_code o bl_code,
    se actualizan los campos asociados.
    """
    stmt_cv = select(ContainerViaje).where(ContainerViaje.id == id_container_viaje)
    result_cv = await db.execute(stmt_cv)
    cv_instance = result_cv.scalars().first()
    if not cv_instance:
        raise HTTPException(status_code=404, detail="Registro de container_viaje no encontrado")
    if container_code is not None:
        container_code = container_code.upper()
        stmt_container = select(Container).where(Container.code == container_code)
        result_container = await db.execute(stmt_container)
        container_instance = result_container.scalars().first()
        if not container_instance:
            raise HTTPException(status_code=400, detail="El container_code no existe en containers")
        cv_instance.id_container = container_instance.id
    if bl_code is not None:
        bl_code = bl_code.upper()
        stmt_bl = select(BL).where(BL.code == bl_code)
        result_bl = await db.execute(stmt_bl)
        bl_instance = result_bl.scalars().first()
        if not bl_instance:
            raise HTTPException(status_code=400, detail="El bl_code no existe en BLs")
        cv_instance.id_bl = bl_instance.id
    try:
        await db.commit()
        await db.refresh(cv_instance)
        return {"message": "Actualización realizada con éxito"}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al actualizar container_viaje: {str(e)}")

# --- POST: Insertar un nuevo Container ---
@router.post("/containers/", response_model=dict)
async def insertar_container(
    code: str,
    size: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    contenido: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """
    Inserta un nuevo registro en la tabla containers.
    """
    if not code:
        raise HTTPException(status_code=400, detail="El campo 'code' es obligatorio.")
    code = code.upper()
    stmt_check = select(Container).where(Container.code == code)
    result_check = await db.execute(stmt_check)
    if result_check.scalars().first():
        raise HTTPException(status_code=400, detail="El code ya existe")
    # Valores por defecto para campos opcionales
    size = size.upper() if size else "DESCONOCIDO"
    type = type.upper() if type else "DESCONOCIDO"
    contenido = contenido.upper() if contenido else "DESCONOCIDO"
    new_container = Container(code=code, size=size, type=type, contenido=contenido)
    db.add(new_container)
    try:
        await db.commit()
        await db.refresh(new_container)
        return {"message": "Registro creado exitosamente en containers", "id_container": new_container.id}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al insertar container: {str(e)}")

# --- POST: Insertar un nuevo Container Viaje ---
@router.post("/containers_viaje/", response_model=dict)
async def insertar_container_viaje(
    container_code: str,
    bl_code: str,
    db: AsyncSession = Depends(get_db)
):
    """
    Inserta un nuevo registro en la tabla container_viaje.
    """
    if not container_code or not bl_code:
        raise HTTPException(status_code=400, detail="Los campos 'container_code' y 'bl_code' son obligatorios.")
    container_code = container_code.upper()
    bl_code = bl_code.upper()
    stmt_container = select(Container).where(Container.code == container_code)
    result_container = await db.execute(stmt_container)
    container_instance = result_container.scalars().first()
    if not container_instance:
        raise HTTPException(status_code=400, detail="El container_code no existe")
    stmt_bl = select(BL).where(BL.code == bl_code)
    result_bl = await db.execute(stmt_bl)
    bl_instance = result_bl.scalars().first()
    if not bl_instance:
        raise HTTPException(status_code=400, detail="El bl_code no existe")
    new_cv = ContainerViaje(id_container=container_instance.id, id_bl=bl_instance.id)
    db.add(new_cv)
    try:
        await db.commit()
        await db.refresh(new_cv)
        return {"message": "Registro creado exitosamente en container_viaje", "id_container_viaje": new_cv.id}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al insertar container_viaje: {str(e)}")
