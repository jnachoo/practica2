
from app.agents.agent import Agente
from app.lector.lector_msc import LectorMSC
from app.database.db import DatabaseManager
from app.browser.seleniumdriver import SeleniumWebDriver
from app.validar_descarga import generar_dict_bl

from config.exceptions import BLCancelled, BLNotFound, NoContainer, FormatoErroneoBL, Bloqueado, HTMLChanged
from config.logger import logger
from database.clases import BL, Container, Parada

from zenrows import ZenRowsClient 
import asyncio 
import json
from botocore.exceptions import NoCredentialsError, ClientError
import datetime

import requests

"""
[
  {"wait": 100},
  {"fill": ["#trackingNumber", "MEDUZT938335"]},
  {"click": "#main > div.msc-flow-tracking.separator--bottom-medium > div > div.grid-x.no-print > div > form > div > div.msc-search-autocomplete.msc-search-autocomplete--focused > div > button.msc-cta-icon-simple.msc-search-autocomplete__search"},
  {"wait_for": ".msc-flow-tracking__subtitle"},
  {"evaluate": "document.body.innerHTML = ''"},
  {"evaluate": "document.head.innerHTML = ''"}
]
"""

class AgenteMSC(Agente):
    def __init__(self,  web_driver: SeleniumWebDriver, data: DatabaseManager):
        super().__init__( web_driver, data)
        self.instructions = lambda bl: f"%5B%7B%22wait%22%3A100%7D%2C%7B%22fill%22%3A%5B%22%23trackingNumber%22%2C%22{bl.bl_code}%22%5D%7D%2C%7B%22click%22%3A%22%23main%20%3E%20div.msc-flow-tracking.separator--bottom-medium%20%3E%20div%20%3E%20div.grid-x.no-print%20%3E%20div%20%3E%20form%20%3E%20div%20%3E%20div.msc-search-autocomplete.msc-search-autocomplete--focused%20%3E%20div%20%3E%20button.msc-cta-icon-simple.msc-search-autocomplete__search%22%7D%2C%7B%22wait_for%22%3A%22.msc-flow-tracking__subtitle%22%7D%2C%7B%22evaluate%22%3A%22document.body.innerHTML%20%3D%20''%22%7D%2C%7B%22evaluate%22%3A%22document.head.innerHTML%20%3D%20''%22%7D%5D"
        self.params = lambda bl: {
                "js_render":"true",
                "js_instructions":self.instructions(bl),
                "json_response": "true"
                }
        self.url = "https://www.msc.com/en/track-a-shipment"

    def descargar_html(self, bls):
        paramss = []
        for bl in bls:
            paramss.append(self.params(bl))
            bl.url = self.url
        
        responses = asyncio.run(self.request_zenrows(paramss, self.url, paramss=True))

        for i, response in enumerate(responses): 
            bl = bls[i]
            bls[i] = self.guardar_html(response, bl)
        return bls

    def scrape_rutina(self, bls):
        paramss = []
        for bl in bls:
            paramss.append(self.params(bl))
            bl.url = self.url
        
        responses = asyncio.run(self.request_zenrows(paramss, self.url, paramss=True))

        for i, response in enumerate(responses): 
            bl = bls[i]
            bls[i] = self.guardar_html(response, bl)
            archivo = bl.url.split(",")[1]
            if archivo != 'None':
                bls[i] = self.scrape_json(response, bl)

        return bls

    def guardar_html(self,response,bl):
        files = []
        fecha = str(datetime.datetime.now()).replace(" ","_").replace(":","_").split(".",)[0]
        data = json.loads(response.text)  # Carga el JSON completo en 'data'
        if response.status_code == 200:
            html = data.pop('html')  # Extrae y elimina la clave 'html' del diccionario
            todo_menos_html = data  # 'data' ahora contiene el diccionario sin la clave 'html'
            lector = LectorMSC(html)
            tabla = lector.extraer_tabla_html()

            # Nombre del archivo en S3
            nombre_html = f'html_msc/{bl.bl_code}_{fecha}.html'
            nombre_json = f'html_msc/{bl.bl_code}_{fecha}_zenrows.json'
            # Cargar el contenido de `tabla` como JSON a S3
            try:
                # Crear lista de archivos a subir, con el nombre de archivo y contenido
                archivos = [
                    (nombre_html, tabla),
                    (nombre_json, json.dumps(todo_menos_html))
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
                bl.url = bl.url + ',' + nombre_json

            except (NoCredentialsError, ClientError) as e:
                logger.error(f"Error al cargar el archivo {nombre_html} a S3: {e}")
                bl.html_descargado = False
                bl.request_case = 9
                bl.url = bl.url+',None'
        else:
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}, {bl.bl_code}")
            
            # Nombre del archivo en S3
            nombre_error = f'html_msc/error_{bl.bl_code}_{fecha}_zenrows.json'
            
            # Cargar el contenido de `tabla` como JSON a S3
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=nombre_error,
                    Body=data
                )
            except (NoCredentialsError, ClientError) as e:
                logger.error(f"Error al cargar el archivo {nombre_html} a S3: {e}")
            
            bl.html_descargado = False
            bl.request_case = response.status_code
            bl.url = bl.url+',None'
        return bl
    
    def leer_html(self,bl, archivo = None):
        if not archivo:
            carpeta = 'html_msc/'
            archivos = self.buscar_archivos(carpeta, bl.bl_code)
            logger.debug(f"Archivos encontrados: {archivos}")
            lista_filtrada = [s for s in archivos if "zenrows" in s]
            logger.debug(f"Archivos filtrados: {lista_filtrada}")
            archivo = max(lista_filtrada)
            bl.url = archivo
        else:
            archivo = archivo
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
        lector = LectorMSC(json=json_data)
        try:
            containers, paradas, pr = lector.extraer_json()
            bl.proxima_revision = pr
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
            bl.request_case = 1
        except Bloqueado:
            logger.info(f"BL {bl.bl_code} bloqueado")
            bl.request_case = 5
        except BLNotFound:
            logger.info(f"BL {bl.bl_code} no encontrado")
            bl.request_case = 3
        except HTMLChanged as e:
            logger.warning(e)
            logger.info(f"Naviera {bl.naviera} cambio la estructura del HTML")
            bl.request_case = 11
        return bl
    
    def scrape_bl(self,response,bl):

        if response.status_code == 200:
            html = json.loads(response.text)['html']
            lector = LectorMSC(html)
            try:
                bl_data, caso = lector.extraer_informacion()
                if caso == 1:
                    bl.pod = bl_data[0]['pod']
                    bl.pol = bl_data[0]['pol']
                    bl.pol_pais = bl_data[0]['pol_pais']
                    bl.pod_pais = bl_data[0]['pod_pais']
                    bl.pol_limpio = bl_data[0]['pol_limpio']
                    bl.pod_limpio = bl_data[0]['pod_limpio']
                    for container in bl_data:
                        c = Container(code=container['cont_id'], size=container['cont_size'], type=container['cont_type'])
                        bl.containers.append(c)
                    
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
        else:
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}, {bl.bl_code}")
            bl.request_case = 9
            print(response.text)
        return bl