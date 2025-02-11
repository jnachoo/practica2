
from app.agents.agent import Agente
from app.lector.lector_zim import LectorZIM
from app.database.db import DatabaseManager
from app.browser.seleniumdriver import SeleniumWebDriver
from app.validar_descarga import generar_dict_bl

from config.exceptions import BLNotFound, Bloqueado, HTMLChanged
from config.logger import logger
from database.clases import BL, Container, Parada

import asyncio 
import json
from botocore.exceptions import NoCredentialsError, ClientError
import datetime

import requests

"""
[
  {"wait_for": "#qa-shipment-search"},
  {"fill": ["#qa-shipment-search", "ZIMUMTL930146"]},
  {"click": "#quickActionsTabs-tabpane-tracing > form > div > div.input-group-btn.col-12 > input"},
  {"wait_for": "#trackShipment > div > div.progress-block"}
]
"""

class AgenteZIM(Agente):
    def __init__(self,  web_driver: SeleniumWebDriver, data: DatabaseManager):
        super().__init__( web_driver, data)
        self.instructions = lambda bl: f"%5B%7B%22wait_for%22%3A%22%23qa-shipment-search%22%7D%2C%7B%22wait%22%3A1534%7D%2C%7B%22fill%22%3A%5B%22%23qa-shipment-search%22%2C%22{bl.bl_code}%22%5D%7D%2C%7B%22wait%22%3A1083%7D%2C%7B%22click%22%3A%22%23quickActionsTabs-tabpane-tracing%20%3E%20form%20%3E%20div%20%3E%20div.input-group-btn.col-12%20%3E%20input%22%7D%2C%7B%22wait_for%22%3A%22%23trackShipment%20%3E%20div%20%3E%20div.progress-block%22%7D%5D"
        self.params = lambda bl: {
                "js_render":"true",
                "json_response":"true",
                "js_instructions":self.instructions(bl),
                "premium_proxy":"true"
                }
        self.url = "https://www.zim.com/"

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
        data = json.loads(response.text)
        if response.status_code == 200:
            html = data.pop('html')  # Extrae y elimina la clave 'html' del diccionario
            todo_menos_html = data  # 'data' ahora contiene el diccionario sin la clave 'html'
            lector = LectorZIM(html)
            tabla = lector.extraer_tabla_html()

            # Nombre del archivo en S3
            nombre_html = f'html_zim/{bl.bl_code}_{fecha}.html'
            nombre_json = f'html_zim/{bl.bl_code}_{fecha}_zenrows.json'
            
            # Cargar el contenido de `tabla` como JSON a S3
            try:
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
            logger.error(f"Error en la petición HTTP: header {response.headers}, {bl.bl_code}")
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}, {bl.bl_code}")

            # Nombre del archivo en S3
            nombre_error = f'html_zim/error_{bl.bl_code}_{fecha}_zenrows.json'
            
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
            carpeta = 'html_zim/'
            archivos = self.buscar_archivos(carpeta, bl.bl_code, mayor=True)
            logger.debug(f"Archivos encontrados: {archivos}")
            lista_filtrada = [s for s in archivos if "zenrows" in s and "error" not in s]
            logger.debug(f"Archivos filtrados: {lista_filtrada}")
            archivo = max(lista_filtrada)
            logger.debug(f"Archivo seleccionado: {archivo}")
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
            logger.error(f"Error en la petición HTTP: header {response.headers}, {bl.bl_code}")
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}, {bl.bl_code}")
            bl.request_case = response.status_code
        return bl

    def parse_json(self, json_data, bl):
        lector = LectorZIM(json=json_data)
        try:
            containers, paradas = lector.extraer_json()
            for container in containers:
                c = Container(
                    code=container['cont_id'],
                    size=container['cont_size'],
                    type=container['cont_type'],
                    pol=container['pol'],
                    pod=container['pod'],
                    service=container['service']
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
                    us_state_code=parada['us_state_code'],
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
    