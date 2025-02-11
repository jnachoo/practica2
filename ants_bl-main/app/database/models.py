from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Boolean, Date, TIMESTAMP
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

class Naviera(Base):
    __tablename__ = 'navieras'
    
    id = Column(Integer, primary_key=True)
    nombre = Column(String(255), nullable=False)
    url = Column(String(255), nullable=False)
    url_2 = Column(String(255))
    
    # Relación inversa
    bls = relationship("BL", back_populates="naviera")

class BL(Base):
    __tablename__ = 'bls'
    
    id = Column(Integer, primary_key=True)
    bl_code = Column(String(255), nullable=False)
    naviera_id = Column(Integer, ForeignKey('navieras.id'), nullable=False)
    fecha_bl = Column(Date, nullable=False)
    revisado_con_exito = Column(Boolean, nullable=False, default=False)
    revisado_hoy = Column(Boolean, nullable=False, default=False)
    manual_pendiente = Column(Boolean, nullable=False, default=False)
    etapa = Column(Integer, nullable=False)
    nave = Column(String(100))
    mercado = Column(String(100))
    no_revisar = Column(Boolean, nullable=False, default=False)
    html_descargado = Column(Boolean, nullable=False, default=False)
    state_code = Column(Integer)
    proxima_revision = Column(Date)
    id_carga = Column(Integer, ForeignKey('cargas.id'))
    
    # Relación
    naviera = relationship("Naviera", back_populates="bls")
    containers = relationship("Container", back_populates="bl")
    paradas = relationship("Paradas", back_populates="bl")
    html_descargados = relationship("HTMLDescargado", back_populates="bl")

class Container(Base):
    __tablename__ = 'containers'
    
    id_container = Column(Integer, primary_key=True)
    code = Column(String(255), nullable=False)
    size = Column(String(50))
    type = Column(String(50))
    pol = Column(String(255))
    pod = Column(String(255))
    bl_id = Column(Integer, ForeignKey('bls.id'), nullable=False)
    peso_kg = Column(Integer)
    service = Column(String(255))
    pol_locode = Column(String(255))
    pod_locode = Column(String(255))
    pol_pais = Column(String(255))
    pod_pais = Column(String(255))
    pol_port = Column(String(255))
    pod_port = Column(String(255))
    pol_limpio = Column(String(255))
    pod_limpio = Column(String(255))
    
    # Relación
    bl = relationship("BL", back_populates="containers")

class Proxy(Base):
    __tablename__ = 'proxies'
    
    id = Column(Integer, primary_key=True)
    ip_address = Column(String(15), nullable=False)
    port = Column(Integer, nullable=False)
    user_proxy = Column(String(255))
    pass_proxy = Column(String(255))
    auth_type = Column(Integer)                                     #ip_auth: 1 - user_pass_auth: 2
    provider = Column(String(255))
    country = Column(String(255))
    is_active = Column(Boolean, nullable=False, default=True)
    is_residential = Column(Boolean, nullable=False, default=False)

class Request(Base):
    __tablename__ = 'requests'
    
    id = Column(Integer, primary_key=True)
    url = Column(String(255), nullable=False)
    proxy_id = Column(Integer, ForeignKey('proxies.id'))
    bl_id = Column(Integer, ForeignKey('bls.id'), nullable=False)
    timestamp = Column(TIMESTAMP, nullable=False)
    success = Column(Boolean, nullable=False)
    response_code = Column(Integer)
    error = Column(String)
    agente = Column(String(255))
    tipo = Column(Integer)                              # 1: descarga y parseo de BL - 2: descarga de HTML - 3: Solo lectura de archivos
    uso_de_proxy = Column(Boolean, nullable=False, default=True)
    
class Cargas(Base):
    __tablename__ = 'cargas'
    
    id = Column(Integer, primary_key=True)
    nbls = Column(Integer)
    timestamp = Column(TIMESTAMP)
    manual = Column(Boolean, nullable=False, default=False)
    msg = Column(String)

class Cruce(Base):
    __tablename__ = 'cruce_aux'
    id = Column(Integer, primary_key=True)
    fecha = Column(Date, nullable=False)
    etapa =  Column(Integer, nullable=False)
    blmaster = Column(String(255))
    pol = Column(String(255))
    pod = Column(String(255))

class Paradas(Base):
    __tablename__ = 'paradas'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    lugar = Column(String, nullable=True)
    pais = Column(String, nullable=True)
    codigo_pais = Column(String, nullable=True)
    locode = Column(String, nullable=True)
    terminal = Column(String, nullable=True)
    status = Column(String, nullable=True)
    nave = Column(String, nullable=True)
    viaje = Column(String, nullable=True)
    fecha = Column(TIMESTAMP, nullable=True)
    orden = Column(Integer, nullable=True)
    nave_imo = Column(String, nullable=True)
    bl_id = Column(Integer, ForeignKey('bls.id'), nullable=True)
    is_pol = Column(Boolean, nullable=False, default=False)
    is_pod = Column(Boolean, nullable=False, default=False)
    us_state_code = Column(String, nullable=True)
    bl = relationship("BL", back_populates="paradas")

class HTMLDescargado(Base):
    __tablename__ = 'html_descargados'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ruta_full = Column(String)
    nombre = Column(String)
    ruta_s3 = Column(String)
    info = Column(Integer)
    fecha_descarga = Column(TIMESTAMP)
    ruta_relativa = Column(String)
    bl_id = Column(Integer, ForeignKey('bls.id'))
    tipo_archivo = Column(Integer)
    en_s3 = Column(Boolean, nullable=False, default=False)
    en_pabrego = Column(Boolean, nullable=False, default=True)

    bl = relationship("BL", back_populates="html_descargados")


