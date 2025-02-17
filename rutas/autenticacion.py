# rutas/autenticacion.py
from datetime import datetime, timedelta
from typing import Optional, List

from fastapi import Depends, HTTPException, status, APIRouter
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db  # get_db retorna un AsyncSession
from models import User

router = APIRouter()

# Configuración de JWT
SECRET_KEY = "clave_secreta_segura"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Contexto para el hash de contraseñas
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)

async def authenticate_user(db: AsyncSession, username: str, password: str) -> Optional[User]:
    stmt = select(User).filter(User.nombre_usuario == username)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()
    if not user or not verify_password(password, user.clave):
        return None
    return user

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db)
) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Token inválido o expirado",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="No se encontró el usuario en el token")
    except JWTError:
        raise credentials_exception

    stmt = select(User).filter(User.nombre_usuario == username)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()
    if user is None:
        raise HTTPException(status_code=401, detail="El usuario del token no existe en la base de datos")
    return user

def check_rol(user: User, roles: List[int]):
    if user.id_rol not in roles:
        raise HTTPException(
            status_code=403,
            detail="No tienes permisos para realizar esta acción"
        )

# Modelo Pydantic para login (opcional)
class LoginRequest(BaseModel):
    username: str
    password: str

@router.post("/login")
async def login(
    request: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db)
):
    user = await authenticate_user(db, request.username, request.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario o contraseña incorrectos",
        )
    access_token = create_access_token(data={"sub": user.nombre_usuario})
    return {"mensaje": "Has iniciado sesión", "access_token": access_token, "token_type": "bearer"}

# Modelo Pydantic para crear un usuario
class UserCreate(BaseModel):
    """
    El frontend debe enviar:
    {
      "nombre": "Juan Pérez",
      "nombre_usuario": "juan123",
      "clave": "PassSegura123",
      "rol": 1
    }
    """
    nombre: str = Field(..., min_length=3, max_length=100)
    nombre_usuario: str = Field(..., min_length=3, max_length=50)
    clave: str = Field(..., min_length=8, description="Debe tener al menos 8 caracteres")
    rol: int = Field(..., description="1.-Admin, 2.-EDITAR, 3.-VER")

@router.post("/registrar_usuario")
async def registrar_usuario(
    user_data: UserCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # Solo usuarios con rol Admin (por ejemplo, rol 1) pueden registrar nuevos usuarios
    check_rol(current_user, [1])
    
    # Verificar si el usuario ya existe
    stmt = select(User).filter(User.nombre_usuario == user_data.nombre_usuario)
    result = await db.execute(stmt)
    existing_user = result.scalar_one_or_none()
    if existing_user:
        raise HTTPException(status_code=400, detail="El usuario ya existe")

    # Hashear la contraseña y crear el nuevo usuario
    hashed_password = get_password_hash(user_data.clave)
    new_user = User(
        nombre=user_data.nombre,
        nombre_usuario=user_data.nombre_usuario,
        clave=hashed_password,
        id_rol=user_data.rol
    )
    db.add(new_user)
    await db.commit()
    await db.refresh(new_user)
    return {"message": "Usuario creado exitosamente"}

# Modelo Pydantic para la salida de usuarios
class UserRead(BaseModel):
    id: int
    nombre: str
    nombre_usuario: str
    id_rol: int

    class Config:
        orm_mode = True

@router.get("/users", response_model=List[UserRead])
async def get_all_users(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # Solo Admins pueden listar todos los usuarios
    check_rol(current_user, [1])
    stmt = select(User)
    result = await db.execute(stmt)
    users = result.scalars().all()
    return users
