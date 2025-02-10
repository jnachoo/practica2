from pydantic import BaseModel, Field

class LoginRequest(BaseModel):
    username: str
    password: str

class UserCreate(BaseModel):
    """
    Expected incoming JSON:
    {
      "nombre": "Juan Pérez",
      "nombre_usuario": "juan123",
      "clave": "PassSegura123",
      "rol": 1
    }
    """
    nombre: str = Field(..., min_length=3, max_length=100)
    nombre_usuario: str = Field(..., min_length=3, max_length=50)
    clave: str = Field(..., min_length=8, description="Must be at least 8 characters")
    rol: int = Field(..., description="1.-Admin, 2.-EDITAR, 3.-VER")
    
class UserRead(BaseModel):
    id: int
    nombre: str
    nombre_usuario: str
    id_rol: int

    class Config:
        orm_mode = True