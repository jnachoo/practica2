
from app.agents.maersk import AgenteMaersk
from app.lector.lector_maersk import LectorMaersk
from app.database.db import DatabaseManager
from app.browser.seleniumdriver import SeleniumWebDriver
from app.validar_descarga import generar_dict_bl

from config.exceptions import BLCancelled, BLNotFound, NoContainer, FormatoErroneoBL, HTMLChanged
from config.logger import logger

from database.clases import BL, Container, Parada

from zenrows import ZenRowsClient 
import asyncio 
import json
from botocore.exceptions import NoCredentialsError, ClientError
import datetime
import requests

class AgenteMaerskZen(AgenteMaersk):
    def __init__(self,  web_driver: SeleniumWebDriver, data: DatabaseManager):
        super().__init__( web_driver, data)
        """
        [
        {"wait_for": "#maersk-app > div > div > div.track__error > h4, #maersk-app > div > div > div:nth-child(3) > div > dl" },
        {"click": "#maersk-app > div > div > div.track__user-feedback > div > div > div > mc-button"},
        {"wait_for":  "#maersk-app > div > div > div.track__error > h4, #maersk-app > div > div > div:nth-child(3) > div > dl"}
        ]
        """  
        self.instructions = "%5B%7B%22wait_for%22%3A%22%23maersk-app%20%3E%20div%20%3E%20div%20%3E%20div.track__error%20%3E%20h4%2C%20%23maersk-app%20%3E%20div%20%3E%20div%20%3E%20div%3Anth-child(3)%20%3E%20div%20%3E%20dl%22%7D%2C%7B%22click%22%3A%22%23maersk-app%20%3E%20div%20%3E%20div%20%3E%20div.track__user-feedback%20%3E%20div%20%3E%20div%20%3E%20div%20%3E%20mc-button%22%7D%2C%7B%22wait_for%22%3A%22%23maersk-app%20%3E%20div%20%3E%20div%20%3E%20div.track__error%20%3E%20h4%2C%20%23maersk-app%20%3E%20div%20%3E%20div%20%3E%20div%3Anth-child(3)%20%3E%20div%20%3E%20dl%22%7D%5D"
        self.params = {
            "js_render":"true",
            "json_response":"true",
            "js_instructions":self.instructions
            }

    def descargar_html(self, bls):
        urls = []
        for bl in bls:
            u = self.generar_url(bl)
            urls.append(u)
            bl.url = u
        
        responses = asyncio.run(self.request_zenrows(self.params, urls))

        for i, response in enumerate(responses): 
            bl = bls[i]
            bls[i] = self.guardar_html(response, bl)
        return bls

    def scrape_rutina(self, bls):
        urls = []
        for bl in bls:
            u = self.generar_url(bl)
            urls.append(u)
            bl.url = u
        
        responses = asyncio.run(self.request_zenrows(self.params, urls))

        for i, response in enumerate(responses): 
            bl = bls[i]
            bls[i] = self.guardar_html(response, bl)
            bls[i] = self.scrape_json(response, bl)
        return bls

    def guardar_html(self,response,bl):
        files = []
        if response.status_code == 200:
            data = json.loads(response.text)  # Carga el JSON completo en 'data'
            html = data.pop('html')  # Extrae y elimina la clave 'html' del diccionario
            todo_menos_html = data  # 'data' ahora contiene el diccionario sin la clave 'html'
            lector = LectorMaersk(html)
            tabla = lector.extraer_tabla_html()
            fecha = str(datetime.datetime.now()).replace(" ","_").replace(":","_").split(".",)[0]

            nombre_html = f'html_maersk/{bl.bl_code}_{fecha}.html'
            nombre_json = f'html_maersk/{bl.bl_code}_{fecha}_zenrows.json'
            try:
                # Crear lista de archivos a subir, con el nombre de archivo y contenido
                archivos = [
                    (nombre_html, tabla),
                    (nombre_json, json.dumps(todo_menos_html, indent=4))
                ]
                
                for nombre, contenido in archivos:
                    # Cargar el archivo a S3
                    self.s3_client.put_object(
                        Bucket=self.bucket_name,
                        Key=nombre,
                        Body=contenido,
                    )
                    dict_bl = generar_dict_bl(nombre, bl.id)
                    files.append(dict_bl)

                # Agregar la información cargada a la base de datos
                self.data.add_html_descargados_batch(files)
                bl.html_descargado = True
                
            except (NoCredentialsError, ClientError) as e:
                logger.error(f"Error al cargar el archivo {nombre_html} a S3: {e}")
                bl.html_descargado = False
                bl.request_case = 9
        else:
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}, {bl.bl_code}")
            bl.html_descargado = False
            bl.request_case = 9
        return bl

    
    def scrape_bl(self,response,bl):

        if response.status_code == 200:
            html = json.loads(response.text)['html']
            lector = LectorMaersk(html)
            try:
                bl_data, caso = lector.extraer_informacion()
                if len(bl_data) == 0:
                    raise NoContainer
                bl.pod = bl_data[0]['pod']
                bl.pol = bl_data[0]['pol']
                for container in bl_data:
                    c = Container(code=container['cont_id'], size=container['cont_size'], type=container['cont_type'])
                    bl.containers.append(c)
                bl.request_case = 1
            except BLNotFound:
                logger.info(f"BL {bl.bl_code} no encontrado")
                bl.request_case = 3
            except NoContainer:
                logger.info(f"BL {bl.bl_code} sin contenedor asignado")
                bl.request_case = 4
            except BLCancelled:
                logger.info(f"BL {bl.bl_code} cancelado")
                bl.request_case = 6
            except FormatoErroneoBL:
                logger.info(f"Error de formato en el BL {bl.bl_code}")
                bl.request_case = 7
        else:
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}")
            bl.request_case = 9
        return bl
    
    def leer_html(self, bl, archivo = None):
        if not archivo:
            carpeta = 'html_maersk/'
            archivos = self.buscar_archivos(carpeta, bl.bl_code)
            lista_filtrada = [s for s in archivos if "zenrows" in s]
            archivo = max(lista_filtrada)
        else:
            archivo = archivo
        bl.url = archivo
        with open(f'{archivo}', 'r') as f:
            json = f.read()
        bl = self.parse_json(json, bl)
        return bl
    
    def scrape_json(self, response, bl):
        if response.status_code == 200:
            json_data = response.text
            bl = self.parse_json(json_data, bl)
        else:
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}, {bl.bl_code}")
            bl.request_case = response.status_code
        return bl

    def parse_json(self, json_data, bl):
        lector = LectorMaersk(json=json_data)
        try:
            containers, paradas = lector.extraer_json()
            for container in containers:
                c = Container(
                    code=container['cont_id'],
                    size=container['cont_size'],
                    type=container['cont_type'],
                    pol=container['pol'],
                    pod=container['pod']
                )
                bl.containers.append(c)
            for parada in paradas:
                p = Parada(
                    lugar=parada['lugar'],
                    fecha=parada['fecha'],
                    pais=parada['pais'],
                    terminal=parada['terminal'],
                    codigo_pais=parada['codigo_pais'],
                    locode=parada['locode'],
                    status=parada['status'],
                    orden=parada['orden'],
                    nave=parada['nave'],
                    viaje=parada['viaje'],
                    is_pol=parada['is_pol'],
                    is_pod=parada['is_pod']
                )
                bl.paradas.append(p)
            if len(bl.containers) > 0 and len(bl.paradas) > 0:
                bl.request_case = 1
            else:
                bl.request_case = 4
        except BLNotFound:
            logger.info(f"BL {bl.bl_code} no encontrado")
            bl.request_case = 3
        except NoContainer:
            logger.info(f"BL {bl.bl_code} sin contenedor asignado")
            bl.request_case = 4
        except BLCancelled:
            logger.info(f"BL {bl.bl_code} cancelado")
            bl.request_case = 6
        except FormatoErroneoBL:
            logger.info(f"Error de formato en el BL {bl.bl_code}")
            bl.request_case = 7
        except HTMLChanged as e:
            logger.warning(e)
            logger.info(f"Naviera {bl.naviera} cambio la estructura del HTML")
            bl.request_case = 11
        return bl
    
    def parse_html(self, html, bl):
        lector = LectorMaersk(html)
        try:
            bl_data, caso = lector.extraer_informacion()
            if len(bl_data) == 0:
                raise NoContainer
            bl.pol = bl_data[0]['pol']
            bl.pod = bl_data[0]['pod']
            bl.pol_port = bl_data[0]['pol_port']
            bl.pod_port = bl_data[0]['pod_port']
            bl.pol_limpio = bl_data[0]['pol_limpio']
            bl.pod_limpio = bl_data[0]['pod_limpio']
            for container in bl_data:
                c = Container(code=container['cont_id'], size=container['cont_size'], type=container['cont_type'])
                bl.containers.append(c)
            bl.request_case = 1
        except BLNotFound:
            logger.info(f"BL {bl.bl_code} no encontrado")
            bl.request_case = 3
        except NoContainer:
            logger.info(f"BL {bl.bl_code} sin contenedor asignado")
            bl.request_case = 4
        except BLCancelled:
            logger.info(f"BL {bl.bl_code} cancelado")
            bl.request_case = 6
        except FormatoErroneoBL:
            logger.info(f"Error de formato en el BL {bl.bl_code}")
            bl.request_case = 7
        
        return bl