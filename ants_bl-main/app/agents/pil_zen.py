
from app.agents.agent import Agente
from app.lector.lector_pil import LectorPIL
from app.database.db import DatabaseManager
from app.browser.seleniumdriver import SeleniumWebDriver
from app.validar_descarga import generar_dict_bl

from config.exceptions import BLCancelled, BLNotFound, NoContainer, FormatoErroneoBL, Bloqueado, HTMLChanged
from config.logger import logger
from database.clases import BL, Container, Parada

import asyncio 
import json
from botocore.exceptions import NoCredentialsError, ClientError
import datetime

"""
[
  {"wait": 10000},
  {"evaluate": "document.querySelector('.custom-dropdown-selected').textContent = 'B/L Number'; document.getElementById('module').value = 'TrackTraceBL'; document.getElementById('refNo').value = 'NGCN40344000'; document.querySelector('.tt-submit').click();"},
  {"wait": 1000}
]

"%5B%7B%22wait%22%3A10000%7D%2C%7B%22evaluate%22%3A%22document.querySelector('.custom-dropdown-selected').textContent%20%3D%20'B%2FL%20Number'%3B%20document.getElementById('module').value%20%3D%20'TrackTraceBL'%3B%20document.getElementById('refNo').value%20%3D%20'NGCN40344000'%3B%20document.querySelector('.tt-submit').click()%3B%22%7D%2C%7B%22wait%22%3A1000%7D%5D"
"""



class AgentePIL(Agente):
    def __init__(self,  web_driver: SeleniumWebDriver, data: DatabaseManager):
        super().__init__( web_driver, data)
        self.instructions = lambda bl: f"%5B%7B%22wait%22%3A10000%7D%2C%7B%22evaluate%22%3A%22document.querySelector('.custom-dropdown-selected').textContent%20%3D%20'B%2FL%20Number'%3B%20document.getElementById('module').value%20%3D%20'TrackTraceBL'%3B%20document.getElementById('refNo').value%20%3D%20'{bl.bl_code}'%3B%20document.querySelector('.tt-submit').click()%3B%22%7D%2C%7B%22wait%22%3A10000%7D%5D"
        self.params = lambda bl: {
                "js_render":"true",
                "json_response":"true",
                "js_instructions":self.instructions(bl),
                "premium_proxy":"true"
                }
        self.url = "https://www.pilship.com/digital-solutions/"

    def descargar_html(self, bls):
        paramss = []
        """
        urls = []
        for bl in bls:
            u = url+bl.bl_code
            urls.append(u)
            bl.url = u

        params = {"js_render":"true","json_response":"true", "premium_proxy":"true","js_instructions":"%5B%7B%22wait%22%3A5000%7D%5D"}
        
        responses = asyncio.run(self.request_zenrows(params, urls))"""

        for bl in bls:
            paramss.append(self.params)
            bl.url = self.url
        
        responses = asyncio.run(self.request_zenrows(paramss, self.url, paramss=True))

        for i, response in enumerate(responses): 
            bl = bls[i]
            bls[i] = self.guardar_html(response, bl)
        return bls

    def scrape_rutina(self, bls):
        paramss = []
        """
        urls = []
        for bl in bls:
            u = url+bl.bl_code
            urls.append(u)
            bl.url = u

        params = {"js_render":"true","json_response":"true", "premium_proxy":"true","js_instructions":"%5B%7B%22wait%22%3A5000%7D%5D"}
        
        responses = asyncio.run(self.request_zenrows(params, urls))"""

        for bl in bls:
            paramss.append(self.params(bl))
            bl.url = self.url
        
        responses = asyncio.run(self.request_zenrows(paramss, self.url, paramss=True))

        for i, response in enumerate(responses): 
            bl = bls[i]
            bls[i] = self.guardar_html(response, bl)
            archivo = bl.url.split(",")[1]
            if archivo != 'None':
                bls[i] = self.scrape_html(response, bl)

        return bls

    def guardar_html(self,response,bl):
        files = []
        fecha = str(datetime.datetime.now()).replace(" ","_").replace(":","_").split(".",)[0]
        data = json.loads(response.text)
        if response.status_code == 200:
            html = data.pop('html')  # Extrae y elimina la clave 'html' del diccionario
            lector = LectorPIL(html, bl)
            tabla = lector.extraer_tabla_html()

            # Nombre del archivo en S3
            nombre_html = f'html_pil/{bl.bl_code}_{fecha}.html'
            
            # Cargar el contenido de `tabla` como JSON a S3
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=nombre_html,
                    Body=tabla
                )
                dict_bl = generar_dict_bl(nombre_html, bl.id)
                files.append(dict_bl)

                # Agregar la información cargada a la base de datos
                self.data.add_html_descargados_batch(files)
                bl.html_descargado = True
                bl.url = bl.url + ',' + nombre_html

            except (NoCredentialsError, ClientError) as e:
                logger.error(f"Error al cargar el archivo {nombre_html} a S3: {e}")
                bl.html_descargado = False
                bl.request_case = 9
                bl.url = bl.url+',None'
        else:
            logger.error(f"Error en la petición HTTP: header {response.headers}, {bl.bl_code}")
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}, {bl.bl_code}")
            
            # Nombre del archivo en S3
            nombre_error = f'html_pil/error_{bl.bl_code}_{fecha}_zenrows.json'
            
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
            carpeta = 'html_pil/'
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
        bl = self.parse_html(json, bl)
        return bl
    
    def scrape_html(self, response, bl):
        if response.status_code == 200:
            json_data = response.text
            html = json.loads(json_data)['html']
            bl = self.parse_html(html, bl)
        else:
            logger.error(f"Error en la petición HTTP: header {response.headers}, {bl.bl_code}")
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}, {bl.bl_code}")
            bl.request_case = response.status_code
        return bl
    
    def parse_html(self, html, bl):
        lector = LectorPIL(html, bl)
        try:
            containers, paradas = lector.extraer_informacion()
            for container in containers:
                c = Container(
                    code=container['cont_id'],
                    size=container['cont_size'],
                    type=container['cont_type']
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
            bl.request_case = 5
        except BLNotFound:
            logger.info(f"BL {bl.bl_code} no encontrado")
            bl.request_case = 3
        except BLCancelled:
            bl.request_case = 6
        except HTMLChanged as e:
            logger.warning(e)
            logger.info(f"Naviera {bl.naviera} cambio la estructura del HTML")
            bl.request_case = 11
        return bl
    

