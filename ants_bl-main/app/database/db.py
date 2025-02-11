from contextlib import contextmanager
from sqlalchemy import create_engine, exists
from sqlalchemy.orm import sessionmaker, scoped_session
from app.database.models import BL, Naviera, Proxy, Request, Container, Cargas, Cruce, Paradas, HTMLDescargado  # Asegúrate de ajustar las rutas de importación según tu estructura de proyecto
from sqlalchemy.sql.expression import func as sql_func
from datetime import datetime
from sqlalchemy import text, cast, Date, select
#from config.settings import DATABASE_URL  # Asegúrate de que esta es la cadena de conexión correcta
from config.logger import logger

#from database.clases import BL, Container

class DatabaseManager:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)
        self.Session = scoped_session(sessionmaker(bind=self.engine))

    @contextmanager
    def session_scope(self):
        """Proporciona un contexto transaccional."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def add_bl(self, bl_code, naviera_id, fecha_bl, revisado_con_exito=False):
        with self.session_scope() as session:
            bl = BL(bl_code=bl_code, naviera_id=naviera_id, fecha_bl=fecha_bl, revisado_con_exito=revisado_con_exito)
            session.add(bl)
            return bl
        
    def add_html_descargados_batch(self, lista_datos, batch_size=1000):
        with self.session_scope() as session:
            for i in range(0, len(lista_datos), batch_size):
                batch = lista_datos[i:i + batch_size]
                for datos in batch:
                    ruta_full = datos.get('ruta_full')
                    fecha_descarga = datos.get('fecha_descarga')

                    # Verificar si ya existe un registro con ruta_full y fecha_descarga
                    if not session.query(exists().where(HTMLDescargado.ruta_full == ruta_full)
                                                 .where(HTMLDescargado.fecha_descarga == fecha_descarga)).scalar():
                        # Crear y agregar el nuevo registro
                        html_descargado = HTMLDescargado(
                            ruta_full=ruta_full,
                            nombre=datos.get('nombre'),
                            ruta_s3=datos.get('ruta_s3'),
                            info=datos.get('info'),
                            fecha_descarga=fecha_descarga,
                            ruta_relativa=datos.get('ruta_relativa'),
                            bl_id=datos.get('bl_id'),
                            tipo_archivo=datos.get('tipo_archivo'),
                            en_s3 = True,
                            en_pabrego = False
                        )
                        session.add(html_descargado)
                session.commit()  # Confirmar el lote actual
                print(f"Guardados {i + len(batch)} registros de {len(lista_datos)}", end="\r")
        
    def add_bl_cruce(self, df):
        with self.session_scope() as session:
            # Crea una lista de diccionarios con los datos de los BLs
            bls = df.to_dict(orient='records')
            
            # Utiliza bulk_insert_mappings para insertar masivamente los BLs
            session.bulk_insert_mappings(Cruce, bls)
            
        return True
    
    def add_proxima_revision(self, bl_id, fecha):
        with self.session_scope() as session:
            bl = session.query(BL).filter(BL.id == bl_id).first()
            if bl:
                bl.proxima_revision = fecha
                session.commit()
        return True
    
    def add_bls(self, df):
        unique_bl_codes = df['bl_code'].unique()

        with self.session_scope() as session:
            # 1. Consulta optimizada para obtener BLs existentes:
            bls_actuales = session.query(BL.bl_code).all()

            # 1.1 trar ultima carga
            ultima_carga = session.query(Cargas).order_by(Cargas.timestamp.desc()).first()
            id_carga = ultima_carga.id + 1
            existing_bl_codes = set(bl.bl_code for bl in bls_actuales)

            # 2. Filtrar el DataFrame para mantener solo los nuevos BLs:
            new_df = df[~df['bl_code'].isin(existing_bl_codes)]

            # 3. Precarga de navieras (solo para los nuevos BLs):
            unique_navieras = new_df['nombre_naviera'].unique()
            navieras_actuales = session.query(Naviera).all()
            existing_navieras = {nav.nombre: nav for nav in navieras_actuales}

            # 4. Crear nuevos BLs:
            new_bls = []
            for _, row in new_df.iterrows():
                no_revisar = False
                naviera = existing_navieras.get(row['nombre_naviera'])
                if 'OOLU' in row['bl_code']:
                    naviera = existing_navieras.get('OOCL')
                if '-' in row['bl_code']:
                    no_revisar = True
                if '/' in row['bl_code']:
                    no_revisar = True
                if '.' in row['bl_code']:
                    no_revisar = True  
                if ' ' in row['bl_code']:
                    no_revisar = True  
                if len(row['bl_code']) < 5:
                    no_revisar = True  
                if naviera:
                    new_bls.append({
                        "bl_code": row['bl_code'],
                        "naviera_id": naviera.id,
                        "fecha_bl": row['fecha_bl'],
                        "etapa": row['etapa'],
                        "nave": row['nave'],
                        "no_revisar": no_revisar,
                        "id_carga": id_carga
                    })

            # 5. Inserción masiva:
            if new_bls:
                id_carga = self.nueva_carga(len(new_bls))
                session.bulk_insert_mappings(BL, new_bls)

        return new_bls

        
    """def add_bls(self, df):
        # Obtén todos los códigos BL y nombres de naviera únicos del DataFrame
        unique_bl_codes = df['bl_code'].unique()
        unique_navieras = df['nombre_naviera'].unique()
        
        with self.session_scope() as session:
            # Pre-carga BLs y Navieras existentes en memoria
            existing_bls = {bl.bl_code: bl for bl in session.query(BL).filter(BL.bl_code.in_(unique_bl_codes)).all()}
            existing_navieras = {nav.nombre: nav for nav in session.query(Naviera).filter(Naviera.nombre.in_(unique_navieras)).all()}
            
            # Lista para guardar nuevos BLs a insertar
            new_bls = []
            
            for index, row in df.iterrows():
                # Verifica si el BL ya existe
                bl = existing_bls.get(row['bl_code'])
                if bl:
                    # BL existe, actualiza sus valores
                    naviera = existing_navieras.get(row['nombre_naviera'])
                    if naviera:
                        bl.naviera_id = naviera.id
                        bl.fecha_bl = row['fecha_bl']
                        bl.etapa = row['etapa']
                        bl.nave = row['nave']
                        # Aquí se supone que la sesión se encarga de rastrear y actualizar los cambios automáticamente
                else:
                    # BL no existe, crea uno nuevo
                    naviera = existing_navieras.get(row['nombre_naviera'])
                    if naviera:
                        new_bls.append({
                            "bl_code": row['bl_code'],
                            "naviera_id": naviera.id,
                            "fecha_bl": row['fecha_bl'],
                            "etapa": row['etapa'],
                            "nave": row['nave']
                        })
            
            # Utiliza bulk_insert_mappings para inserción masiva de los nuevos BLs
            if len(new_bls) > 0:
                session.bulk_insert_mappings(BL, new_bls)
                self.nueva_carga(len(new_bls))

        return new_bls"""

    def nueva_carga(self, cantidad, manual=False):
        id_carga = None
        with self.session_scope() as session:
            ultima_carga = session.query(Cargas).order_by(Cargas.timestamp.desc()).first()
            if ultima_carga:
                ultima_carga = ultima_carga.timestamp
            else:
                ultima_carga = "2021-01-01 00:00:00"
            nueva_carga = Cargas(nbls=cantidad, timestamp=sql_func.now(), manual=manual, msg="Carga por rutina")
            session.add(nueva_carga)
        with self.session_scope() as session:
            nueva_carga = session.query(Cargas).order_by(Cargas.timestamp.desc()).first()
            id_carga = nueva_carga.id

        return id_carga

    def add_navieras(self, df):
        # Asegúrate de que 'nombre' y 'url' sean los nombres correctos de las columnas en tu DataFrame
        unique_navieras = df.drop_duplicates(subset=['nombre'])
        
        with self.session_scope() as session:
            # Pre-carga las Navieras existentes en memoria para chequeo rápido
            existing_navieras = {nav.nombre: nav for nav in session.query(Naviera).all()}
            
            # Prepara una lista para las nuevas navieras que necesitan ser insertadas
            new_navieras = []
            
            for _, row in unique_navieras.iterrows():
                if row['nombre'] not in existing_navieras:
                    new_navieras.append({
                        "nombre": row['nombre'],
                        "url": row['url']
                    })
            
            # Utiliza bulk_insert_mappings para inserción masiva de las nuevas Navieras
            if new_navieras:
                session.bulk_insert_mappings(Naviera, new_navieras)
                
        return True
    
    def get_localidades(self):
        with self.session_scope() as session:
            # paradas de bls con naviera_id = 2
            localidades = session.query(Paradas).join(BL, Paradas.bl_id == BL.id).filter(BL.naviera_id == 2).with_entities(Paradas.lugar).distinct().all()
            localidades = [loc[0] for loc in localidades]
            return localidades
    
    def save_request(self, bl_id, url, proxy_id, caso, msg, tipo=1, agente=None):
        if caso in [1,2,3,4,5,6,7]:
            success = True
        else:  
            success = False
        with self.session_scope() as session:
            request = Request(bl_id=bl_id, url=url, proxy_id=proxy_id, success=success, response_code=caso, error=msg, timestamp=sql_func.now(), tipo=tipo, agente=agente)
            session.add(request)
            return request
        
    def no_revisar_bl(self, bl):
        with self.session_scope() as session:
            bl_a_editar = session.query(BL).filter(BL.id==bl["id"]).first()
            if bl_a_editar:
                bl_a_editar.no_revisar = True
                session.commit()
        return True

    def get_bls_manuales(self):
        with self.session_scope() as session:
            # Consulta los BLs que necesitan revisión manual
            # bls = session.query(BL).filter(BL.manual_pendiente == True).all()
            bls = session.query(
                            BL.id,
                            BL.bl_code,
                            BL.naviera_id,
                            BL.fecha_bl,
                            BL.revisado_con_exito,
                            BL.etapa,
                            BL.nave,
                            BL.manual_pendiente,
                            sql_func.count(Container.id_container).label('ncontainers')
                        ).outerjoin(Container, Container.bl_id == BL.id)\
                        .filter(BL.manual_pendiente == True)\
                        .having(sql_func.count(Container.id_container) == 50)\
                        .group_by(
                            BL.id,
                            BL.bl_code,
                            BL.naviera_id,
                            BL.fecha_bl,
                            BL.revisado_con_exito,
                            BL.etapa,
                            BL.nave,
                            BL.manual_pendiente,
                        ).all()    
            # Convierte las instancias de BL a diccionarios
            bls_list = [
                {
                    "id": bl.id,
                    "bl_code": bl.bl_code,
                    "naviera_id": bl.naviera_id,
                    "fecha_bl": bl.fecha_bl,
                    "revisado_con_exito": bl.revisado_con_exito,
                    "etapa": bl.etapa,
                    "nave": bl.nave
                }
                for bl in bls
            ]
            return bls_list
        
    def get_bl_mas_reciente(self, naviera=None):
        with self.session_scope() as session:
            # Consulta el BL más reciente
            bl = session.query(BL)
            if naviera:
                bl = bl.join(Naviera).filter(Naviera.nombre == naviera)
            
            bl = bl.order_by(BL.fecha_bl.desc()).first()
            if bl:
                return {
                    "id": bl.id,
                    "bl_code": bl.bl_code,
                    "naviera_id": bl.naviera_id,
                    "fecha_bl": bl.fecha_bl,
                    "revisado_con_exito": bl.revisado_con_exito,
                    "etapa": bl.etapa,
                    "nave": bl.nave
                }
            else:
                return None
        
    def get_bl_by_id(self, bl_id):
        with self.session_scope() as session:
            bl = session.query(BL).filter(BL.id == bl_id).first()
            
            if bl:
                naviera = session.query(Naviera.nombre).filter(Naviera.id == bl.naviera_id).first()[0]
                return [{
                    "id": bl.id,
                    "bl_code": bl.bl_code,
                    "nombre_naviera": naviera,
                    "fecha_bl": bl.fecha_bl,
                    "revisado_con_exito": bl.revisado_con_exito,
                    "etapa": bl.etapa,
                    "nave": bl.nave
                    # Añade aquí otros campos necesarios
                }]
            else:
                return None

    def get_containers(self, bl_id):
        with self.session_scope() as session:
            containers = session.query(Container).filter(Container.bl_id == bl_id).all()
            containers_list = [
                {
                    "id": container.id_container,
                    "code": container.code,
                    "size": container.size,
                    "type": container.type,
                    "pol": container.pol,
                    "pod": container.pod,
                    "bl_id": container.bl_id,
                    "peso_kg": container.peso_kg,
                    "service": container.service
                    # Añade aquí otros campos necesarios
                }
                for container in containers
            ]
            return containers_list

    def descargar_html(self, bl):
        with self.session_scope() as session:
            bl_a_editar = session.query(BL).filter(BL.id==bl.id).first()
            if bl_a_editar:
                bl_a_editar.html_descargado = True
                session.commit()
        return True
    
    def buscar_locode(self, lugar, pais=None, codigo_pais=None, us_state_code=None):
        with self.session_scope() as session:
            # Lista de combinaciones de búsqueda en orden de prioridad
            combinaciones = [
                {'lugar': lugar, 'codigo_pais': codigo_pais},
                {'lugar': lugar, 'us_state_code': us_state_code},
                {'lugar': lugar, 'pais': pais}
            ]
            
            for comb in combinaciones:
                query = session.query(Paradas).filter(Paradas.lugar.ilike(comb['lugar'])).filter(Paradas.locode != None)
                if comb.get('codigo_pais'):
                    query = query.filter(Paradas.codigo_pais.ilike(comb['codigo_pais']))
                if comb.get('us_state_code'):
                    query = query.filter(Paradas.us_state_code.ilike(comb['us_state_code']))
                if comb.get('pais'):
                    query = query.filter(Paradas.pais.ilike(comb['pais']))
                
                locode = query.first()
                if locode:
                    return locode.locode
            
            # Retorna None si no se encuentra ninguna coincidencia
            return None
        
    def get_bls_a_leer(self, naviera=None, limit=1, random=False, month=None, year=None, day=None, state=None):
        with self.session_scope() as session:
            # Consulta los BLs que necesitan revisión manual
            if state:
                estado = state
            else:
                estado = 12
            bls = session.query(BL).filter(BL.state_code == estado).filter(BL.no_revisar == False)
            if naviera:
                bls = bls.join(Naviera).filter(Naviera.nombre == naviera)
            if month:
                bls = bls.filter(sql_func.extract('month', BL.fecha_bl) == month)
            if year:
                bls = bls.filter(sql_func.extract('year', BL.fecha_bl) == year)
            if day:
                bls = bls.filter(sql_func.extract('day', BL.fecha_bl) == day)
            if random:
                bls = bls.order_by(sql_func.random())
            bls = bls.limit(limit).all()
            # Convierte las instancias de BL a diccionarios
            bls_list = [
                {
                    "id": bl.id,
                    "bl_code": bl.bl_code,
                    "nombre_naviera": naviera,
                    "fecha_bl": bl.fecha_bl,
                    "revisado_con_exito": bl.revisado_con_exito,
                    "etapa": bl.etapa,
                    "nave": bl.nave,
                    "estado": bl.state_code
                }
                for bl in bls
            ]
            return bls_list

    def get_bls_reporte_rutina(self):
        # bls con request con fechas mayor igual a hoy
        with self.session_scope() as session:
            #bls = session.query(BL).filter(BL.id_carga == 63).all()
            bls = session.query(BL).join(Request, BL.id == Request.bl_id).filter(Request.timestamp >= datetime.now().date()).all()
            bls_list = [
                {
                    "id": bl.id,
                    "bl_code": bl.bl_code,
                    "naviera_id": bl.naviera_id,
                    "fecha_bl": bl.fecha_bl,
                    "revisado_con_exito": bl.revisado_con_exito,
                    "id_carga": bl.id_carga,
                    "state_code": bl.state_code,
                    "etapa": bl.etapa,
                    "nave": bl.nave
                }
                for bl in bls
            ]
            return bls_list

    def get_bl(self, id=None, bl_code=None, naviera=None, revisado_con_exito=None, random=False, limit=1, full=False):
        with self.session_scope() as session:
            query = session.query(BL)
            
            if revisado_con_exito:
                query = query.filter(BL.revisado_con_exito == revisado_con_exito)
            if naviera:
                query = query.join(Naviera).filter(Naviera.nombre == naviera)
            if id:
                query = query.filter(BL.id == id)
            elif bl_code:
                if isinstance(bl_code, list):
                    query = query.filter(BL.bl_code.in_(bl_code))
                else:
                    query = query.filter(BL.bl_code == bl_code)
            elif random:
                query = query.order_by(sql_func.random())

            if full:
                bls = query.all()
            else:
                bls = query.limit(limit).all()

            #naviera = session.query(Naviera).filter(Naviera.id == bls[0].naviera_id).first()
            if bls and len(bls) > 0:
                    return [
                        {
                            "id": bl.id,
                            "bl_code": bl.bl_code,
                            "nombre_naviera": naviera,
                            "fecha_bl": bl.fecha_bl,
                            "revisado_con_exito": bl.revisado_con_exito,
                            "etapa": bl.etapa,
                            "nave": bl.nave,
                            "manual_pendiente": bl.manual_pendiente,
                            "estado": bl.state_code,
                            # Añade aquí otros campos necesarios
                        }
                        for bl in bls
                    ]
            else:
                return []


    def get_lista_bls_no_revisados(self, naviera=None, limit=1, month=None, year=None):
        """
        Obtiene una lista de BLs que no han sido revisados con éxito.
        """
        with self.session_scope() as session:
            bls = session.query(BL).filter(BL.revisado_con_exito == False).filter(BL.no_revisar == False)
            if naviera:
                bls = bls.join(Naviera).filter(Naviera.nombre == naviera)
            # filtrar solo bls del mes month
            if month:
                bls = bls.filter(sql_func.extract('month', BL.fecha_bl) == month)
            
            # filtrar solo bls del año year
            if year:
                bls = bls.filter(sql_func.extract('year', BL.fecha_bl) == year)

            bls = bls.limit(limit).all()

            # Convierte las instancias de BL a diccionarios
            bls = [
                {
                    "id": bl.id,
                    "bl_code": bl.bl_code,
                    "nombre_naviera": naviera,
                    "fecha_bl": bl.fecha_bl,
                    "revisado_con_exito": bl.revisado_con_exito,
                    "etapa": bl.etapa,
                    "nave": bl.nave
                    # Añade otros campos relevantes
                }
                for bl in bls
            ]

            return bls

    def get_url(self, bl_code=None, naviera=None, aux=False):
        with self.session_scope() as session:
            if naviera and aux:
                query = session.query(Naviera.url_2).filter(Naviera.nombre == naviera)
                url = query.first()[0]
                return url
            elif naviera:
                
                query = session.query(Naviera.url).filter(Naviera.nombre == naviera)
                url = query.first()[0]
                return url
            elif bl_code:
                query = session.query(Naviera.url, BL.bl_code).join(BL, Naviera.id == BL.naviera_id).filter(BL.bl_code == bl_code)
                resultado = query.first()
            
                if resultado:
                    url_naviera, bl_code = resultado
                    return f"{url_naviera}{bl_code}"
                else:
                    return None
            
    def get_requests(self):
        with self.session_scope() as session:
            requests = session.query(Request).all()
            # Convierte las instancias de Request a diccionarios
            requests_list = [
                {
                    "id": request.id,
                    "url": request.url,
                    "proxy_id": request.proxy_id,
                    "success": request.success,
                    # Añade otros campos relevantes
                }
                for request in requests
            ]
            return requests_list
        
    def get_bls_rutina(self, naviera, limit=1, estados=[1,3,5,8], month=None, year=None, day=None, random=False):
        # traer bls con status_code in 1,3,5,8; que tengan no_revisar false, 
        with self.session_scope() as session:
            n = session.query(Naviera).filter(Naviera.nombre == naviera).first()
            bls = session.query(BL).filter(BL.naviera_id == n.id).filter(BL.no_revisar == False).filter(BL.state_code.in_(estados)).filter(BL.revisado_hoy == False)
            if month:
                bls = bls.filter(sql_func.extract('month', BL.fecha_bl) == month)
            if year:
                bls = bls.filter(sql_func.extract('year', BL.fecha_bl) == year)
            if day:
                bls = bls.filter(sql_func.extract('day', BL.fecha_bl) == day)
            if random:
                bls = bls.order_by(sql_func.random())
            bls = bls.limit(limit).all()
            bls_list = [
                {
                    "id": bl.id,
                    "bl_code": bl.bl_code,
                    "nombre_naviera": n.nombre,
                    "fecha_bl": bl.fecha_bl,
                    "revisado_con_exito": bl.revisado_con_exito,
                    "etapa": bl.etapa,
                    "nave": bl.nave,
                    "estado": bl.state_code
                }
                for bl in bls
            ]
            return bls_list

    # obtener cantiad de bls en estado 1 de la naviera con id 8
    def get_pendientes_oocl(self,estados=[1]):
        with self.session_scope() as session:
            n = session.query(Naviera).filter(Naviera.nombre == 'OOCL').first()
            bls = session.query(BL).filter(BL.naviera_id == n.id).filter(BL.state_code.in_(estados)).count()
            return bls
    
    def get_navieras(self):
        with self.session_scope() as session:
            navieras = session.query(Naviera).all()
            navieras_list = [
                {
                    "id": naviera.id,
                    "nombre": naviera.nombre
                    # Añade otros campos relevantes
                }
                for naviera in navieras
            ]
            return navieras_list
        
    def get_dict_bl_state(self):
        with self.session_scope() as session:
            # select * from bl_state_dictionary
            bls_dict = session.execute(text("select * from bl_state_dictionary")).fetchall()
            bls_dict = [
                {
                    "state_code": bl.state_code,
                    "state_description": bl.state_description
                    # Añade otros campos relevantes
                }
                for bl in bls_dict
            ]

            return bls_dict
        
    def get_bls_sin_html(self, naviera=None, limit=1, month=None, year=None, day=None):
        """
        BLs que nunca se han revisado.
        """
        with self.session_scope() as session:
            query = session.query(BL).filter(BL.html_descargado == False).filter(BL.no_revisar == False)
            if naviera:
                bls = query.join(Naviera).filter(Naviera.nombre == naviera)
            # filtrar solo bls del mes month
            if month:
                bls = bls.filter(sql_func.extract('month', BL.fecha_bl) == month)

            # filtrar solo bls del año year
            if year:
                bls = bls.filter(sql_func.extract('year', BL.fecha_bl) == year)
            if day:
                bls = bls.filter(sql_func.extract('day', BL.fecha_bl) == day)

            bls = bls.limit(limit).all()
            # Convierte las instancias de BL a diccionarios
            bls_list = [
                {
                    "id": bl.id,
                    "bl_code": bl.bl_code,
                    "nombre_naviera": naviera,
                    "fecha_bl": bl.fecha_bl,
                    "revisado_con_exito": bl.revisado_con_exito,
                    "etapa": bl.etapa,
                    "nave": bl.nave,
                    "estado": bl.state_code
                    # Añade otros campos relevantes
                }
                for bl in bls
            ]
            return bls_list
            
        
    def get_bls_sin_request(self, naviera=None, revisado_con_exito=None, limit=1, month=None, year=None):
        """
        BLs que nunca se han revisado.
        """
        with self.session_scope() as session:
            query = session.query(BL)
            if revisado_con_exito is not None:
                query = query.filter(BL.revisado_con_exito == revisado_con_exito)
            if naviera:
                query = query.join(Naviera).filter(Naviera.nombre == naviera)
            # Consulta los BLs que no tienen requests asociados
            bls = query.outerjoin(Request).filter(Request.id == None)
            # filtrar solo bls del mes month
            if month:
                bls = bls.filter(sql_func.extract('month', BL.fecha_bl) == month)

            # filtrar solo bls del año year
            if year:
                bls = bls.filter(sql_func.extract('year', BL.fecha_bl) == year)

            bls = bls.limit(limit).all()
            # Convierte las instancias de BL a diccionarios
            bls_list = [
                {
                    "id": bl.id,
                    "bl_code": bl.bl_code,
                    "nombre_naviera": naviera,
                    "fecha_bl": bl.fecha_bl,
                    "revisado_con_exito": bl.revisado_con_exito,
                    "etapa": bl.etapa,
                    "nave": bl.nave
                    # Añade otros campos relevantes
                }
                for bl in bls
            ]
            return bls_list
        
    # get bls que no han sido revisados con éxito
    def get_bls_request_fallida(self, naviera=None, limit=1, month=None, year=None):
        with self.session_scope() as session:
            # Subquery to count failed requests for each bl
            """
            subquery = session.query(Request.bl_id, sql_func.count(Request.id).label('requests')).\
                join(Proxy, Request.proxy_id == Proxy.id).\
                filter(Proxy.is_residential == True).\
                filter(Request.error.like('%éxito%')).\
                group_by(Request.bl_id).\
                subquery()

                join(subquery, BL.id == subquery.c.bl_id).\
                filter(subquery.c.requests < 6).\
            """
            #print(subquery)
            
            # Query bls with more than 3 failed requests
            query = session.query(BL).\
                outerjoin(Container, BL.id == Container.bl_id, ).\
                outerjoin(Request, BL.id == Request.bl_id, ).\
                filter(Container.id_container == None).\
                filter(sql_func.length(BL.bl_code) < 14).\
                filter(Request.error.like('%éxito%'))
                # BLs sin contenedores
            

            query = query.filter(BL.revisado_con_exito == False)
            if naviera:
                query = query.join(Naviera).filter(Naviera.nombre == naviera)

            # filtrar solo bls del mes month
            if month:
                query = query.filter(sql_func.extract('month', BL.fecha_bl) == month)
            
            # filtrar solo bls del año year
            if year:
                query = query.filter(sql_func.extract('year', BL.fecha_bl) == year)
            
            bls = query.limit(limit).all()
            # Convierte las instancias de BL a diccionarios
            bls_list = [
                {
                    "id": bl.id,
                    "bl_code": bl.bl_code,
                    "nombre_naviera": naviera,
                    "fecha_bl": bl.fecha_bl,
                    "revisado_con_exito": bl.revisado_con_exito,
                    "etapa": bl.etapa,
                    "nave": bl.nave
                    # Añade otros campos relevantes
                }
                for bl in bls
            ]
            return bls_list
    
    def get_proxy_performance(self, residential=False, exclude=None):
        with self.session_scope() as session:
            # Consulta la vista proxy_performance
            base_query = """
                SELECT * FROM proxy_performance
                INNER JOIN proxies ON proxy_performance.proxy_id = proxies.id
                WHERE proxies.is_active = True
            """

            # Prepara los parámetros de la consulta
            params = {}

            # Agrega el filtro para "es residencial" si es especificado
            if residential is not None:
                base_query += " AND proxies.is_residential = :residential"
                params['residential'] = residential

            # Ejecuta la consulta con los parámetros
            query = text(base_query)

            result = session.execute(query, params).mappings().all()
                    # Transforma el resultado en una lista de diccionarios
            performance_list = [
                {
                    "proxy_id": row['proxy_id'],
                    "successful_requests": row['successful_requests'],
                    "failed_requests": row['failed_requests'],
                    "total_requests": row['total_requests'],
                    "success_rate": row['success_rate']
                } for row in result if exclude if int(row['proxy_id']) != int(exclude)
            ]
            return performance_list

    def get_proxy_by_id(self, proxy_id):
        with self.session_scope() as session:
            proxy = session.query(Proxy).filter(Proxy.id == proxy_id).first()
            if proxy:
                return {
                    "id": proxy.id,
                    "ip_address": proxy.ip_address,
                    "port": proxy.port,
                    "user_proxy": proxy.user_proxy,
                    "pass_proxy": proxy.pass_proxy,
                    "auth_type": proxy.auth_type,
                    "provider": proxy.provider,
                    "country": proxy.country,
                    "is_active": proxy.is_active,
                    "is_residential": proxy.is_residential
                    # Añade aquí otros campos que necesites.
                }
            else:
                return None
    
    def get_proxy(self, residential=False):
        with self.session_scope() as session:
            query = session.query(Proxy).filter(Proxy.is_active == True)
            if residential:
                query = query.filter(Proxy.is_residential == True)
            proxy = query.order_by(sql_func.random()).first()
            if proxy:
                return {
                    "id": proxy.id,
                    "ip_address": proxy.ip_address,
                    "port": proxy.port,
                    "user_proxy": proxy.user_proxy,
                    "pass_proxy": proxy.pass_proxy,
                    "auth_type": proxy.auth_type,
                    "provider": proxy.provider,
                    "country": proxy.country,
                    "is_active": proxy.is_active,
                    "is_residential": proxy.is_residential
                    # Añade aquí otros campos que necesites.
                }
            else:
                return proxy

    def get_proxies(self):
        with self.session_scope() as session:
            # Obtiene todos los proxies activos
            proxy_objects = session.query(Proxy).filter(Proxy.is_active == True).all()
            
            # Convierte cada objeto Proxy en un diccionario, incluyendo solo los campos relevantes
            proxies_list = [
                {
                    "id": proxy.id,
                    "ip_address": proxy.ip_address,
                    "port": proxy.port,
                    "user_proxy": proxy.user_proxy,
                    "pass_proxy": proxy.pass_proxy,
                    "auth_type": proxy.auth_type,
                    "provider": proxy.provider,
                    "country": proxy.country,
                    "is_active": proxy.is_active,
                    "is_residential": proxy.is_residential
                    # Añade aquí otros campos que necesites.
                }
                for proxy in proxy_objects
            ]
            
            return proxies_list
        
    def add_proxies(self, df):
        with self.session_scope() as session:
            # Crea una lista de diccionarios con los datos de los proxies
            proxies = df.to_dict(orient='records')

            # validar si existe en la bd
            existing_proxies = {proxy.ip_address: proxy for proxy in session.query(Proxy).all()}
            new_proxies = []
            for proxy in proxies:
                if proxy['ip_address'] not in existing_proxies:
                    new_proxies.append(proxy)
            
            # Utiliza bulk_insert_mappings para insertar masivamente los proxies
            session.bulk_insert_mappings(Proxy, proxies)
            
        return True
    
    def revisar_manualmente(self, bl):
        with self.session_scope() as session:
            bl_a_editar = session.query(BL).filter(BL.id==bl["id"]).first()
            if bl_a_editar:
                bl_a_editar.manual_pendiente = True
                session.commit()
        return True
    
    def update_bl(self, bl, html=False):
        with self.session_scope() as session:
            bl_a_editar = session.query(BL).filter(BL.id==bl.id).first()
            if bl_a_editar and len(bl.containers) > 0:
                if html:
                    bl_a_editar.html_descargado = True
                contenedores = session.query(Container).filter(Container.bl_id==bl.id).all()
                for c in contenedores:
                    c.pod = bl.pod
                    c.pol = bl.pol
                    c.pol_locode = bl.pol_locode
                    c.pod_locode = bl.pod_locode
                    c.pol_pais = bl.pol_pais
                    c.pod_pais = bl.pod_pais
                    c.pol_port = bl.pol_port
                    c.pod_port = bl.pod_port
                    c.pol_limpio = bl.pol_limpio
                    c.pod_limpio = bl.pod_limpio    
                session.commit()
        return True
    
    def buscar_archivo_a_leer(self, bl):
        with self.session_scope() as session:
            ultima_request_con_exito = session.query(Request).filter(Request.bl_id==bl.id).filter(Request.response_code==1).filter(Request.tipo==1).order_by(Request.timestamp.desc()).first()
            archivo = None
            if ultima_request_con_exito:
                archivo = ultima_request_con_exito.url.split(",")[-1].strip()
            if archivo and 'http' in archivo:
                archivo = None
        return archivo
    
    def add_containers(self, bl, manual=False):
        #print(bl)
        cont = bl.containers
        with self.session_scope() as session:
            # revisar que el bl exista en la base de datos
            bl_aux = session.query(BL).filter(BL.bl_code==bl.bl_code).first()
            if not bl_aux:
                raise ValueError(f"El BL {bl.bl_code} no existe en la base de datos")
            
            # revisar que el contenedor no esté repetido en el mismo bl.
            contenedores = session.query(Container).filter(Container.bl_id==bl_aux.id).all()
            contenedores_code = [c.code for c in contenedores]
            msg = []

            for c in cont:
                if c.code in contenedores_code:
                    msg.append(f"Contentenedor {c.code} ya existe en el BL {bl_aux.bl_code}")
                    print(f"Contentenedor {c.code} ya existe en el BL {bl_aux.bl_code}")
                    contenedor = session.query(Container).filter(Container.bl_id==bl_aux.id).filter(Container.code==c.code).first()
                    contenedor.pod = bl.pod
                    contenedor.pol = bl.pol
                    contenedor.service = c.service
                    contenedor.size = c.size
                    contenedor.type = c.type
                    session.commit()
                    continue
                try:
                    container = Container(
                        code=c.code, 
                        bl_id=bl_aux.id, 
                        pod=bl.pod, 
                        pol=bl.pol, 
                        size=c.size, 
                        type=c.type, 
                        peso_kg=c.peso_kg, 
                        service=c.service
                    )
                except KeyError:
                    container = Container(code=c.code, bl_id=bl_aux.id, pod=bl.pod, pol=bl.pol, size=c.size, type=c.type)
                session.add(container)
                session.commit()
                msg.append(f"Contentenedor {c.code} agregado")
                #print(f"Contentenedor {c.code} agregado")
            logger.info(f"Se agregaron {len(cont)} contenedores al BL {bl.bl_code}")
            bl_a_editar = session.query(BL).filter(BL.id==bl_aux.id).first()
            if bl_a_editar:
                bl_a_editar.revisado_con_exito = True
                session.commit()
                #print(f"BL {bl.bl_code} revisado con éxito")
            if bl.manual_pendiente:
                bl_a_editar.manual_pendiente = True
                session.commit()
            if manual and bl_a_editar:
                bl_a_editar.manual_pendiente = False
                session.commit()

        if manual:
            return msg
        return True
    
    """def add_paradas(self, bl):
        paradas = bl.paradas
        with self.session_scope() as session:
            # revisar que el bl exista en la base de datos
            bl_aux = session.query(BL).filter(BL.bl_code==bl.bl_code).first()
            if not bl_aux:
                raise ValueError(f"El BL {bl.bl_code} no existe en la base de datos")
            pol = None
            pod = None
            for p in paradas:
                parada_aux = session.query(Paradas).filter(
                    Paradas.bl_id == bl_aux.id,
                    Paradas.orden == p.orden
                ).first()
                if p.is_pol:
                    pol = p.lugar
                if p.is_pod:
                    pod = p.lugar
                if parada_aux:
                    # Actualizar los valores de la parada existente
                    parada_aux.lugar = p.lugar
                    parada_aux.terminal = p.terminal
                    parada_aux.pais = p.pais
                    parada_aux.codigo_pais = p.codigo_pais
                    parada_aux.fecha = p.fecha
                    parada_aux.status = p.status
                    parada_aux.nave = p.nave
                    parada_aux.viaje = p.viaje
                    parada_aux.nave_imo = p.nave_imo
                    parada_aux.locode = p.locode
                    parada_aux.is_pol = p.is_pol
                    parada_aux.is_pod = p.is_pod
                    parada_aux.us_state_code = p.us_state_code
                else:
                    parada = Paradas(
                        bl_id=bl_aux.id,
                        lugar=p.lugar,
                        terminal=p.terminal,
                        pais=p.pais,
                        codigo_pais=p.codigo_pais,
                        fecha=p.fecha,
                        status=p.status,
                        nave=p.nave,
                        viaje=p.viaje,
                        orden=p.orden,
                        nave_imo=p.nave_imo,
                        locode=p.locode,
                        is_pol=p.is_pol,
                        is_pod=p.is_pod,
                        us_state_code=p.us_state_code
                    )
                    session.add(parada)
                session.commit()
            logger.info(f"Se agregaron {len(paradas)} paradas al BL {bl.bl_code}. POL: {pol}, POD: {pod}")
        return True"""
    
    def add_paradas(self, bl):
        paradas = bl.paradas
        with self.session_scope() as session:
            # Verificar que el BL exista en la base de datos
            bl_aux = session.query(BL).filter(BL.bl_code == bl.bl_code).first()
            if not bl_aux:
                raise ValueError(f"El BL {bl.bl_code} no existe en la base de datos")

            # Obtener paradas existentes para este BL
            paradas_existentes = [(p.lugar, p.status) for p in session.query(Paradas).filter(Paradas.bl_id == bl_aux.id).all()]
            #import pdb; pdb.set_trace()

            nuevas_paradas = []
            for p in paradas:
                #fecha = datetime.strptime(p.fecha, '%Y-%m-%d %H:%M:%S')
                par = (p.lugar, p.status)
                # Si la parada no existe en la base de datos, agregarla
                if par not in paradas_existentes:
                    nuevas_paradas.append(
                        Paradas(
                            bl_id=bl_aux.id,
                            lugar=p.lugar,
                            terminal=p.terminal,
                            pais=p.pais,
                            codigo_pais=p.codigo_pais,
                            fecha=p.fecha,
                            status=p.status,
                            nave=p.nave,
                            viaje=p.viaje,
                            nave_imo=p.nave_imo,
                            locode=p.locode,
                            is_pol=p.is_pol,
                            is_pod=p.is_pod,
                            us_state_code=p.us_state_code
                        )
                    )
            
            # Agregar nuevas paradas a la base de datos
            session.add_all(nuevas_paradas)
            session.commit()

            # Reordenar las paradas por fecha y actualizar el campo orden
            paradas_ordenadas = session.query(Paradas).filter(Paradas.bl_id == bl_aux.id).order_by(Paradas.fecha).all()
            for idx, parada in enumerate(paradas_ordenadas):
                parada.orden = idx
            session.commit()

            logger.info(
                f"Se agregaron {len(nuevas_paradas)} nuevas paradas al BL {bl.bl_code}. Total paradas: {len(paradas_ordenadas)}"
            )
        return True

    
    def add_paradas_bulk(self, df_paradas):
        # Convertir 1/0 a True/False para is_pol y is_pod
        df_paradas['is_pol'] = df_paradas['is_pol'].astype(bool)
        df_paradas['is_pod'] = df_paradas['is_pod'].astype(bool)

        with self.session_scope() as session:
            # Obtener todos los BLs necesarios de una vez para evitar múltiples consultas
            bl_ids = df_paradas['bl_id'].unique()
            bls = session.query(BL).filter(BL.id.in_(bl_ids)).all()
            bl_dict = {bl.id: bl for bl in bls}

            if len(bl_dict) != len(bl_ids):
                missing_bl_ids = set(bl_ids) - set(bl_dict.keys())
                raise ValueError(f"Los siguientes BLs no existen en la base de datos: {missing_bl_ids}")

            # Preparar las filas para bulk insert y update
            new_paradas = []
            update_paradas = []

            for index, row in df_paradas.iterrows():
                bl_id = row['bl_id']
                bl_aux = bl_dict[bl_id]

                parada_aux = session.query(Paradas).filter(
                    Paradas.bl_id == bl_aux.id,
                    Paradas.orden == row['orden']
                ).first()

                parada_data = {
                    'bl_id': bl_aux.id,
                    'lugar': row['lugar'],
                    'locode': row['locode'],
                    'is_pol': row['is_pol'],
                    'is_pod': row['is_pod'],
                    'orden': row['orden']
                }

                if parada_aux:
                    parada_data['id'] = parada_aux.id
                    update_paradas.append(parada_data)
                else:
                    new_paradas.append(parada_data)

            if new_paradas:
                session.bulk_insert_mappings(Paradas, new_paradas)
            if update_paradas:
                session.bulk_update_mappings(Paradas, update_paradas)

            logger.info(f"Se agregaron {len(new_paradas)} nuevas paradas y se actualizaron {len(update_paradas)} paradas existentes.")
        return True
    
    def add_revision_exitosa(self,bl, manual=False):
        with self.session_scope() as session:
            bl_a_editar = session.query(BL).filter(BL.id==bl["id"]).first()
            if bl_a_editar and not manual:
                bl_a_editar.revisado_con_exito = True
                session.commit()
                print(f"BL {bl} revisado con éxito")
            elif bl_a_editar and manual:
                pass
        return True
