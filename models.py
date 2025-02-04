from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from database import Base  # Importa la base de SQLAlchemy

class User(Base):
    __tablename__ = "usuarios"  # Debe coincidir con el nombre de la tabla en PostgreSQL

    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(255), nullable=False)
    nombre_usuario = Column(String(70), nullable=False, unique=True)
    clave = Column(String(255), nullable=False)
    id_rol = Column(Integer, ForeignKey("roles.id"), nullable=True)

    # Relación opcional con la tabla roles (si tienes un modelo Role)
    rol = relationship("Role", back_populates="usuarios")

class Role(Base):
    __tablename__ = "roles"

    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String, unique=True, index=True)
    permiso = Column(String, nullable=False)

    usuarios = relationship("User", back_populates="rol")
