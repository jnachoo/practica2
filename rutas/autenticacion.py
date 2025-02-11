from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, status, APIRouter
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from database import get_bd  # Asegúrate de que `database` está importado si usas `fetch_all`
from models import User  # Asegúrate de que `User` está correctamente definido en models.py

router = APIRouter()

# Clave secreta para firmar los tokens
SECRET_KEY = "clave_secreta_segura"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Contexto de hash para almacenar contraseñas de manera segura
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def authenticate_user(db: Session, username: str, password: str):
    user = db.query(User).filter(User.nombre_usuario == username).first()
    if not user or not verify_password(password, user.clave):
        return None
    return user

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_bd)):
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
    
    user = db.query(User).filter(User.nombre_usuario == username).first()
    if user is None:
        raise HTTPException(status_code=401, detail="El usuario del token no existe en la base de datos")
    
    return user

from typing import List

def check_rol(user: User, roles: List[int]):
    if user.id_rol not in roles:
        raise HTTPException(
            status_code=403,
            detail="No tienes permisos para realizar esta acción"
        )

    
# Pydantic model para recibir las credenciales
class LoginRequest(BaseModel):
    username: str
    password: str

@router.post("/login")
async def login(request: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_bd)):
    user = authenticate_user(db, request.username, request.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario o contraseña incorrectos",
        )
    access_token = create_access_token(data={"sub": user.nombre_usuario})
    return {"mensaje":"Has iniciado sesión","access_token": access_token, "token_type": "bearer"}

# Modelo de entrada con validaciones
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
    nombre :str = Field(...,min_length=3,max_length=100)
    nombre_usuario: str = Field(..., min_length=3, max_length=50)
    clave: str = Field(..., min_length=8, description="Debe tener al menos 8 caracteres")
    rol: int = Field(...,description="1.-Admin, 2.-EDITAR, 3.-VER")

@router.post("/registrar_usuario")
async def registrar_usuario(
    user_data: UserCreate,
    db: Session = Depends(get_bd),
    current_user: User = Depends(get_current_user)
    ):

    # Verificar si el rol del usuario registrado puede acceder a la función
    check_rol(current_user, 1)
              
    # Verificar si el usuario ya existe
    existing_user = db.query(User).filter(User.nombre_usuario == user_data.nombre_usuario).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="El usuario ya existe")

    # Hashear la contraseña
    hashed_password = get_password_hash(user_data.clave)

    # Crear nuevo usuario
    new_user = User(nombre=user_data.nombre,nombre_usuario=user_data.nombre_usuario, clave=hashed_password, id_rol=user_data.rol)
    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    return {"message": "Usuario creado exitosamente"}



class UserRead(BaseModel):
    id: int
    nombre: str
    nombre_usuario: str
    id_rol: int

@router.get("/users", response_model=List[UserRead])
async def get_all_users(
    db: Session = Depends(get_bd),
    current_user: User = Depends(get_current_user)
):
    """
    Retorna todos los usuarios de la base de datos.
    Opcional: Verificar si solo Admin puede verlos => check_rol(current_user, [1])
    """
    # Si deseas restringir a solo admin:
    check_rol(current_user, [1])
    users = db.query(User).all()
    return users
