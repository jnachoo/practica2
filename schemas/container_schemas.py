from pydantic import BaseModel
from typing import Optional

class ContainerBase(BaseModel):
    code: str
    size: Optional[str] = "DESCONOCIDO"
    type: Optional[str] = "DESCONOCIDO"
    contenido: Optional[str] = "DESCONOCIDO"

class ContainerCreate(ContainerBase):
    pass

class ContainerUpdate(BaseModel):
    size: Optional[str] = None
    type: Optional[str] = None
    contenido: Optional[str] = None

class ContainerViajeCreate(BaseModel):
    container_code: str
    bl_code: str

class ContainerViajeUpdate(BaseModel):
    container_code: Optional[str] = None
    bl_code: Optional[str] = None

class ContainerResponse(ContainerBase):
    id_container_viaje: int
    container_code: str
    bl_code: str