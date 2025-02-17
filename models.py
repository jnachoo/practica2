from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Boolean, Date, TIMESTAMP
from sqlalchemy.orm import relationship, declarative_base
from database import Base

class User(Base):
    __tablename__ = "usuarios"
    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(255), nullable=False)
    nombre_usuario = Column(String(70), nullable=False, unique=True)
    clave = Column(String(255), nullable=False)
    id_rol = Column(Integer, ForeignKey("roles.id"), nullable=True)
    
    rol = relationship("Role", back_populates="usuarios")
    ordenes_descarga = relationship("OrdenDescarga", back_populates="usuario")

class Role(Base):
    __tablename__ = "roles"
    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(255), unique=True, index=True)
    permiso = Column(String(200))
    
    usuarios = relationship("User", back_populates="rol")

class StatusBL(Base):
    __tablename__ = 'status_bl'
    id = Column(Integer, primary_key=True)
    descripcion_status = Column(String, nullable=False)
    
    bls = relationship("BL", back_populates="status")
    
class Etapa(Base):
    __tablename__ = 'etapa'
    id = Column(Integer, primary_key=True)
    nombre = Column(String, nullable=False)
    
    bls = relationship("BL", back_populates="etapa")

class Naviera(Base):
    __tablename__ = 'navieras'
    
    id = Column(Integer, primary_key=True)
    nombre = Column(String(255), nullable=False)
    url = Column(String(500))
    url2 = Column(String(500))
    id_maestro = Column(Integer)
    
    # Relación inversa
    bls = relationship("BL", back_populates="naviera")

class BL(Base):
    __tablename__ = 'bls'
    
    id = Column(Integer, primary_key=True)
    code = Column(String(255), nullable=False)
    id_naviera = Column(Integer, ForeignKey('navieras.id'), nullable=False)
    nave = Column(String(255))
    id_etapa = Column(Integer, ForeignKey('etapa.id'), nullable=False)
    mercado = Column(String(255))
    pol = Column(String(100))  # Añadido
    pod = Column(String(100))  # Añadido
    fecha = Column(Date, nullable=False)
    proxima_revision = Column(Date)
    id_status = Column(Integer, ForeignKey('status_bl.id'), nullable=False)  # Changed from state_code
    id_carga = Column(Integer, ForeignKey('carga_bls.id'))
    revisado_con_exito = Column(Boolean, nullable=False, default=False)
    manual_pendiente = Column(Boolean, nullable=False, default=False)
    no_revisar = Column(Boolean, nullable=False, default=False)
    revisado_hoy = Column(Boolean, nullable=False, default=False)
    html_descargado = Column(Boolean, nullable=False, default=False)
    
    # Relación
    naviera = relationship("Naviera", back_populates="bls")
    etapa = relationship("Etapa", back_populates="bls")
    status = relationship("StatusBL", back_populates="bls")
    tracking = relationship("Tracking", back_populates="bl")
    container_viajes = relationship("ContainerViaje", back_populates="bl")
    carga = relationship("CargaBls", back_populates="bl")
    orden_detalles = relationship("OrdenDetalle", back_populates="bl")
    #paradas = relationship("Paradas", back_populates="bl")

class Tracking(Base):
    __tablename__ = 'tracking'
    
    id = Column(Integer, primary_key=True)
    id_bl = Column(Integer, ForeignKey('bls.id'), nullable=False)
    fecha = Column(TIMESTAMP, nullable=False)
    status = Column(String(255))
    orden = Column(Integer)
    id_parada = Column(Integer, ForeignKey('paradas.id'))
    terminal = Column(String(255))
    is_pol = Column(Boolean)
    is_pod = Column(Boolean)
    
    # Relación
    bl = relationship("BL", back_populates="tracking")
    parada = relationship("Paradas", back_populates="tracking")

class CargaBls(Base):
    __tablename__ = 'carga_bls'
    
    id = Column(Integer, primary_key=True)
    numero_bls_descargados = Column(Integer, nullable=False)
    fecha_descarga = Column(TIMESTAMP, nullable=False)
    carga_manual = Column(Boolean)
    mensaje = Column(String(500))
    
    # Relación
    bl = relationship("BL", back_populates="carga")

class Container(Base):
    __tablename__ = 'containers'
    
    id = Column(Integer, primary_key=True)  # Changed from id_container
    code = Column(String(200), nullable=False)
    size = Column(String(15))
    type = Column(String(50))
    contenido = Column(String(15))  # Added missing field
    
    container_viajes = relationship("ContainerViaje", back_populates="container")

class ContainerViaje(Base):
    __tablename__ = 'container_viaje'
    
    id = Column(Integer, primary_key=True)
    id_container = Column(Integer, ForeignKey('containers.id'))
    id_bl = Column(Integer, ForeignKey('bls.id'))
    peso_kg = Column(String(50))
    
    # Relación
    container = relationship("Container", back_populates="container_viajes")
    bl = relationship("BL", back_populates="container_viajes")

# class Proxy(Base):
#     __tablename__ = 'proxies'
    
#     id = Column(Integer, primary_key=True)
#     ip_address = Column(String(15), nullable=False)
#     port = Column(Integer, nullable=False)
#     user_proxy = Column(String(255))
#     pass_proxy = Column(String(255))
#     auth_type = Column(Integer)                                     #ip_auth: 1 - user_pass_auth: 2
#     provider = Column(String(255))
#     country = Column(String(255))
#     is_active = Column(Boolean, nullable=False, default=True)
#     is_residential = Column(Boolean, nullable=False, default=False)

class Request(Base):
    __tablename__ = 'requests'
    
    id = Column(Integer, primary_key=True)
    id_bl = Column(Integer)
    url = Column(String(255), nullable=False)
    fecha = Column(TIMESTAMP, nullable=False)
    mensaje = Column(String(255))
    sucess = Column(Boolean)  # Note: it's "sucess" in DB, not "success"
    id_html = Column(Integer, ForeignKey('html_descargados.id'))
    id_respuesta = Column(Integer, ForeignKey('respuesta_requests.id'))
    
    # Relaciones
    html_descargado = relationship("HTMLDescargado", back_populates="requests")
    respuesta = relationship("RespuestaRequest", back_populates="requests")
    orden_detalles = relationship("OrdenDetalle", back_populates="request")

class RespuestaRequest(Base):
    __tablename__ = 'respuesta_requests'  # Note: singular
    
    id = Column(Integer, primary_key=True)
    descripcion = Column(String(255))
    
    # Relación
    requests = relationship("Request", back_populates="respuesta")

# class Cargas(Base):
#     __tablename__ = 'cargas'
    
#     id = Column(Integer, primary_key=True)
#     nbls = Column(Integer)
#     timestamp = Column(TIMESTAMP)
#     manual = Column(Boolean, nullable=False, default=False)
#     msg = Column(String)

# class Cruce(Base):
#     __tablename__ = 'cruce_aux'
#     id = Column(Integer, primary_key=True)
#     fecha = Column(Date, nullable=False)
#     etapa =  Column(Integer, nullable=False)
#     blmaster = Column(String(255))
#     pol = Column(String(255))
#     pod = Column(String(255))

# class Paradas(Base):
#     __tablename__ = 'paradas'
    
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     lugar = Column(String, nullable=True)
#     pais = Column(String, nullable=True)
#     codigo_pais = Column(String, nullable=True)
#     locode = Column(String, nullable=True)
#     terminal = Column(String, nullable=True)
#     status = Column(String, nullable=True)
#     nave = Column(String, nullable=True)
#     viaje = Column(String, nullable=True)
#     fecha = Column(TIMESTAMP, nullable=True)
#     orden = Column(Integer, nullable=True)
#     nave_imo = Column(String, nullable=True)
#     bl_id = Column(Integer, ForeignKey('bls.id'), nullable=True)
#     is_pol = Column(Boolean, nullable=False, default=False)
#     is_pod = Column(Boolean, nullable=False, default=False)
#     us_state_code = Column(String, nullable=True)
#     bl = relationship("BL", back_populates="paradas")

class Paradas(Base):
    __tablename__ = 'paradas'
    
    id = Column(Integer, primary_key=True)
    locode = Column(String(50), nullable=False)
    pais = Column(String(255))
    lugar = Column(String(255))
    
    # Relación
    tracking = relationship("Tracking", back_populates="parada")

class HTMLDescargado(Base):
    __tablename__ = 'html_descargados'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ruta_full = Column(String(255))
    nombre = Column(String(255))
    ruta_s3 = Column(String(255))
    info = Column(Integer)
    fecha_descarga = Column(TIMESTAMP)
    ruta_relativa = Column(String(255))
    tipo_archivo = Column(Integer)
    en_s3 = Column(Boolean, nullable=False, default=False)
    en_pabrego = Column(Boolean, nullable=False, default=True)

    requests = relationship("Request", back_populates="html_descargado")

class OrdenDescarga(Base):
    __tablename__ = "orden_descargas"
    
    id = Column(Integer, primary_key=True)
    id_usuario = Column(Integer, ForeignKey('usuarios.id'), nullable=False)
    fecha_creacion = Column(TIMESTAMP, nullable=False)
    fecha_programacion = Column(TIMESTAMP)
    descripcion = Column(String)
    enviar_correo = Column(Boolean, nullable=False, default=False)
    
    usuario = relationship("User", back_populates="ordenes_descarga")
    # Usamos el mismo nombre en ambos lados para la relación:
    orden_detalles = relationship("OrdenDetalle", back_populates="orden_descarga")

class OrdenDetalle(Base):
    __tablename__ = 'orden_detalle'
    
    id = Column(Integer, primary_key=True)
    id_cabecera = Column(Integer, ForeignKey('orden_descargas.id'))
    id_request = Column(Integer, ForeignKey('requests.id'))
    id_bls = Column(Integer, ForeignKey('bls.id'))
    
    # El back_populates debe coincidir con el de OrdenDescarga:
    orden_descarga = relationship("OrdenDescarga", back_populates="orden_detalles")
    request = relationship("Request", back_populates="orden_detalles")
    bl = relationship("BL", back_populates="orden_detalles")

# class DictContainer(Base):
#     __tablename__ = "dict_containers"
    
#     # Create a surrogate primary key that won't affect the actual database
#     id = Column(Integer, primary_key=True, autoincrement=True)
    
#     size = Column(String(50), nullable=True)
#     type = Column(String(50), nullable=True)
#     nombre_size = Column(String, nullable=True)
#     nombre_type = Column(String(50), nullable=True)
#     dryreef = Column(String(50), nullable=True)
    
#     # Tell SQLAlchemy this is a view or read-only table
#     __mapper_args__ = {
#         'primary_key': [id]
#     }

# class DictLocode(Base):
#     __tablename__ = "dict_locode"
    
#     # Create a surrogate primary key
#     id = Column(Integer, primary_key=True, autoincrement=True)
    
#     locode = Column(String(50), nullable=True)
#     pais = Column(String(50), nullable=True)
#     ciudad_lugar = Column(String(150), nullable=True)
#     coordenadas = Column(String(70), nullable=True)
    
#     __mapper_args__ = {
#         'primary_key': [id]
#     }


# class BLTemp(Base):
#     __tablename__ = 'bls_temp'
    
#     # Surrogate primary key for SQLAlchemy
#     _id = Column('_id', Integer, primary_key=True, autoincrement=True)
    
#     # Regular fields
#     id = Column(Integer, nullable=True)
#     bl_code = Column(String(50))
#     naviera_id = Column(Integer)
#     fecha_bl = Column(String(50))  
#     revisado_con_exito = Column(Boolean)
#     etapa = Column(Integer)
#     nave = Column(String(50))
#     manual_pendiente = Column(Boolean)
#     id_carga = Column(Integer)
#     no_revisar = Column(Boolean)
#     state_code = Column(Integer)
#     html_descargado = Column(Boolean)
#     proxima_revision = Column(String(50))  
#     revisado_hoy = Column(Boolean)
#     mercado = Column(String(50))
#     nuevo_id = Column(Integer)
    
#     __mapper_args__ = {
#         'primary_key': [_id]
#     }

# class ContainerTemp(Base):
#     __tablename__ = 'containers_temp'
    
#         # Surrogate primary key for SQLAlchemy
#     _id = Column('_id', Integer, primary_key=True, autoincrement=True)
    
#     id_container = Column(Integer, nullable=True) 
#     code = Column(String(50))
#     size = Column(String(50))
#     type = Column(String(50))
#     pol = Column(String(50))
#     pod = Column(String(50))
#     bl_id = Column(Integer)
#     peso_kg = Column(String(50))  
#     service = Column(String(50))
#     pol_locode = Column(String(250))
#     pol_port = Column(String(250))
#     pol_pais = Column(String(250))
#     pod_locode = Column(String(250))
#     pod_port = Column(String(250))
#     pod_pais = Column(String(50))
#     pol_limpio = Column(String(50))
#     pod_limpio = Column(String(50))
    
#     __mapper_args__ = {
#         'primary_key': [_id]
#     }
    
# class HTMLDescargadoTemp(Base):
#     __tablename__ = 'html_descargados_temp'

#         # Surrogate primary key for SQLAlchemy
#     _id = Column('_id', Integer, primary_key=True, autoincrement=True)

#     id = Column(Integer, nullable=True)
#     ruta_full = Column(String(250))
#     nombre = Column(String(100))
#     ruta_s3 = Column(String(104))
#     info = Column(Integer)
#     fecha_descarga = Column(TIMESTAMP)  
#     ruta_relativa = Column(String(100))
#     bl_id = Column(Integer)
#     tipo_archivo = Column(Integer)
#     en_s3 = Column(Boolean)
#     en_pabrego = Column(Boolean)
    
#     __mapper_args__ = {
#         'primary_key': [_id]
#     }

# class ParadasTemp(Base):
#     __tablename__ = 'paradas_temp'

#     # Surrogate primary key for SQLAlchemy
#     _id = Column('_id', Integer, primary_key=True, autoincrement=True)

#     id = Column(Integer, nullable=True)
#     lugar = Column(String(250))
#     pais = Column(String(50))
#     codigo_pais = Column(String(50))
#     locode = Column(String(50))
#     terminal = Column(String(250))
#     status = Column(String(250))
#     nave = Column(String(250))
#     viaje = Column(String(50))
#     fecha = Column(String(50)) 
#     orden = Column(Integer)
#     nave_imo = Column(String(50))
#     bl_id = Column(Integer)
#     is_pol = Column(Boolean)
#     is_pod = Column(Boolean)
#     tipo = Column(String(50))
#     us_state_code = Column(String(50))
    
#     __mapper_args__ = {
#         'primary_key': [_id]
#     }
    
# class RequestTemp(Base):
#     __tablename__ = 'requests_temp'

#     # Surrogate primary key for SQLAlchemy
#     _id = Column('_id', Integer, primary_key=True, autoincrement=True)

#     id = Column(Integer, nullable=True)
#     url = Column(String(250))
#     proxy_id = Column(Integer)
#     bl_id = Column(Integer)
#     timestamp = Column(String(50))  
#     success = Column(Boolean)
#     response_code = Column(Integer)
#     error = Column(String(50))
#     agente = Column(String(50))
#     tipo = Column(String(50))
#     uso_de_proxy = Column(Boolean)
    
#     __mapper_args__ = {
#         'primary_key': [_id]
#     }
