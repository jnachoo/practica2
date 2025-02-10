from sqlalchemy.orm import Session
from models.models import User

def get_user_by_username(db: Session, username: str):
    return db.query(User).filter(User.nombre_usuario == username).first()

def create_user(db: Session, user: User):
    db.add(user)
    db.commit()
    db.refresh(user)
    return user

def get_all_users(db: Session):
    return db.query(User).all()