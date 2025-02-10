from datetime import timedelta
from typing import List

from fastapi import Depends, HTTPException, status, APIRouter
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from sqlalchemy.orm import Session

from db.database import get_bd
from models.models import User
from schemas.auth_schemas import LoginRequest, UserCreate, UserRead
from services.auth_service import (
    authenticate_user,
    create_access_token,
    register_user,
)
from repositories.user_repository import get_user_by_username, get_all_users

router = APIRouter()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_bd)) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, "clave_secreta_segura", algorithms=["HS256"])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = get_user_by_username(db, username)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    return user

def check_role(user: User, roles: List[int]):
    if user.id_rol not in roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to perform this action",
        )

@router.post("/login")
async def login(request: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_bd)):
    user = authenticate_user(db, request.username, request.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
        )
    access_token = create_access_token(data={"sub": user.nombre_usuario})
    return {"mensaje": "Logged in successfully", "access_token": access_token, "token_type": "bearer"}

@router.post("/registrar_usuario")
async def registrar_usuario(
    user_data: UserCreate,
    db: Session = Depends(get_bd),
    current_user: User = Depends(get_current_user)
):
    # Allow only admins (role 1) to register new users
    check_role(current_user, [1])
    
    if get_user_by_username(db, user_data.nombre_usuario):
        raise HTTPException(status_code=400, detail="User already exists")
    
    new_user = register_user(db, user_data.nombre, user_data.nombre_usuario, user_data.clave, user_data.rol)
    return {"message": "Usuario creado exitosamente"}

@router.get("/users", response_model=List[UserRead])
async def get_all_users_route(
    db: Session = Depends(get_bd),
    current_user: User = Depends(get_current_user)
):
    check_role(current_user, [1])
    users = get_all_users(db)
    return users