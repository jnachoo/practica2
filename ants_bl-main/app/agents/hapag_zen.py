
from app.agents.hapag import AgenteHapag
from app.lector.lector_hapag import LectorHapag
from app.database.db import DatabaseManager
from app.browser.seleniumdriver import SeleniumWebDriver
from app.validar_descarga import generar_dict_bl

from config.exceptions import BLCancelled, BLNotFound, NoContainer, FormatoErroneoBL, ContainerNotBL, HTMLChanged
from config.logger import logger

from database.clases import Container, BL, Parada

import datetime
from botocore.exceptions import NoCredentialsError, ClientError
from zenrows import ZenRowsClient 
import asyncio 
from urllib.parse import urlparse, parse_qs
import json

import requests



class AgenteHapagZen(AgenteHapag):
    def __init__(self,  web_driver: SeleniumWebDriver, data: DatabaseManager):
        super().__init__( web_driver, data)
        """
        [
            {"wait": 5000},
            {"click": "#accept-recommended-btn-handler"},
            {"wait": 1000},
            {"click":"#tracing_by_booking_f\\:hl27 > tbody > tr.odd > td:nth-child(1) > div"},
            {"click":"#tracing_by_booking_f\\:hl27\\:hl53"},
            {"wait": 5000}
        ]
        """
        self.instructions = "%5B%7B%22wait%22%3A5000%7D%2C%7B%22click%22%3A%22%23accept-recommended-btn-handler%22%7D%2C%7B%22wait%22%3A1000%7D%2C%7B%22click%22%3A%22%23tracing_by_booking_f%5C%5C%3Ahl27%20%3E%20tbody%20%3E%20tr.odd%20%3E%20td%3Anth-child(1)%20%3E%20div%22%7D%2C%7B%22click%22%3A%22%23tracing_by_booking_f%5C%5C%3Ahl27%5C%5C%3Ahl53%22%7D%2C%7B%22wait%22%3A5000%7D%5D"

    """
    FUNCIONES DE LECTURA: Reciben un objeto BL y devuelven un objeto BL con la información actualizada
    """
    def descargar_html(self, bls):
        urls = []
        for bl in bls:
            u = self.generar_url(bl)
            urls.append(u)
            bl.url = u
        
        params = {
                "js_render":"true",
                "json_response":"true",
                "js_instructions":self.instructions,
                "premium_proxy":"true"
                }
        
        responses = asyncio.run(self.request_zenrows(params, urls))

        for i, response in enumerate(responses): 
            bl = bls[i]
            bl = self.guardar_html(response, bl, container=False)

        return bls

    def leer_html(self, bl):
        carpeta = 'html_hapag/paradas/'
        archivos = self.buscar_archivos(carpeta, bl.bl_code)
        lista_filtrada = [s for s in archivos if "zenrows" not in s]
        try:
            archivo = max(lista_filtrada)
            bl.url = archivo
            with open(f'{archivo}', 'r') as f:
                html = f.read()
            bl = self.parse_html_paradas(html, bl)
        except ValueError:
            bls = self.scrape_rutina([bl])
            bl = bls[0]
        return bl


    def scrape_rutina(self, bls):
        urls = []
        for bl in bls:
            u = self.generar_url(bl)
            urls.append(u)
            bl.url = u
        
        params = {
                "js_render":"true",
                "json_response":"true",
                "js_instructions":self.instructions,
                "premium_proxy":"true"
                }
        logger.info("Descargando paradas")
        responses = asyncio.run(self.request_zenrows(params, urls))
        logger.info("Paradas descargadas. Comenzando análisis")
        for i, response in enumerate(responses): 
            bl = bls[i]
            bl = self.guardar_html(response, bl, container=False)
            bls[i] = self.scrape_paradas(response,bl)

        logger.info("Parseo de paradas lista")

        params = {
                "js_render":"true",
                "json_response":"true",
                "premium_proxy":"true"
                }
        
        logger.info("Descargando contenedores")
        responses = asyncio.run(self.request_zenrows(params, urls))
        logger.info("Contenedores descargados. Comenzando análisis")
        
        for i, response in enumerate(responses):
            bl = bls[i]
            bl = self.guardar_html(response, bl, container=True)
            if len(bl.containers) > 0:
                continue
            bls[i] = self.scarpe_containers(response, bl)
        logger.info("Descarga de contenedores lista")
        return bls
    
   
    def guardar_html(self,response,bl, container=False):
        files = []
        if response.status_code == 200:
            data = json.loads(response.text)  # Carga el JSON completo en 'data'
            html = data.pop('html')  # Extrae y elimina la clave 'html' del diccionario
            todo_menos_html = data  # 'data' ahora contiene el diccionario sin la clave 'html'
            lector = LectorHapag(html)
            tabla = lector.extraer_tabla()
            fecha = str(datetime.datetime.now()).replace(" ","_").replace(":","_").split(".",)[0]
            
            # Nombre del archivo en S3
            if container:
                nombre_html = f'html_hapag/containers/{bl.bl_code}_{fecha}.html'
                nombre_json = f'html_hapag/containers/{bl.bl_code}_{fecha}_zenrows.json' 
            else:
                nombre_html = f'html_hapag/paradas/{bl.bl_code}_{fecha}.html'
                nombre_json = f'html_hapag/paradas/{bl.bl_code}_{fecha}_zenrows.json'

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

            except (NoCredentialsError, ClientError) as e:
                logger.error(f"Error al cargar el archivo {nombre} a S3: {e}")
                bl.html_descargado = False
                bl.request_case = 9
        else:
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}, {bl.bl_code}")
            bl.html_descargado = False
            bl.request_case = response.status_code
        return bl
        
    """
    FUNCIONES DE PARSEO: Reciben un response o texto y parsean
    """
    def scrape_bl(self,response,bl):
        if response.status_code == 200:
            html = json.loads(response.text)['html']
            lector = LectorHapag(html)
            revision_manual = False
            try:
                bl_data, revision_manual = lector.extraer_informacion_bl()
                if revision_manual:
                    bl.revision_manual = True
                if bl_data:
                    for container in bl_data:
                        c = Container(code=container['cont_id'], size=container['cont_size'], type=container['cont_type'])
                        bl.containers.append(c)
                    if len(bl.containers) >= 50:
                        bl.manual_pendiente = True
                else:
                    bl.request_case = 3
            except BLNotFound:
                logger.info(f"BL {bl.bl_code} no encontrado")
                bl.request_case = 3
            except NoContainer:
                bl.request_case = 4
                logger.info(f"BL {bl.bl_code} sin contenedor asignado")
        else:
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}")
            bl.request_case = 9
        return bl
    
    def scarpe_containers(self, response, bl):
        if response.status_code == 200:
            html = json.loads(response.text)['html']
            bl = self.parse_html_containers(html, bl)
        else:
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}")
            bl.request_case = response.status_code
        return bl
        
    def scrape_container(self, response, bl):
        if response.status_code == 200:
            html = json.loads(response.text)['html']
            js_instructions_report = json.loads(response.text)['js_instructions_report']
            instructions = js_instructions_report["instructions"]
            if not instructions[4]["success"] or not instructions[5]["success"]:
                bl.request_case = 5
                return bl
            if "tracing_by_booking_f:hl66" in html:
                print("Encontrado")
            lector_container = LectorHapag(html)
            
            container_data = lector_container.extraer_informacion_container()
            if container_data:
                #bl.pol = container_data['pol']
                #bl.pod = container_data['pod']
                if bl.pod == "Pendiente" or bl.pol == "Pendiente":
                    bl.request_case = 2
                else:
                    bl.request_case = 1
            else:
                bl.request_case = 4
        else:
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}")
            bl.request_case = 9
        return bl
    
    def scrape_paradas(self, response, bl):
        if response.status_code == 200:
            html = json.loads(response.text)['html']
            bl = self.parse_html_paradas(html, bl)
        else:
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}")
            bl.request_case = response.status_code
        return bl
    

 
    def parse_html_paradas(self, html, bl):

        lector_container = LectorHapag(html)
        
        try:
            paradas = lector_container.extraer_informacion_container()
            if paradas:
                pendiente = True
                for parada in paradas:
                    p = Parada(lugar=parada["lugar"])
                    p.fecha = parada["fecha"]
                    p.pais = parada["pais"]
                    p.locode = parada["locode"]
                    p.status = parada["status"]
                    p.nave = parada["nave"]
                    p.orden = parada["orden"]
                    p.is_pol = parada["is_pol"]
                    p.is_pod = parada["is_pod"]
                    if p.is_pod:
                        pendiente = False
                    p.us_state_code = parada["us_state_code"]
                    bl.paradas.append(p)
                if pendiente:
                    bl.request_case = 2
                else:
                    bl.request_case = 1
            else:
                bl.request_case = 3
        except HTMLChanged as e:
            logger.warning(e)
            logger.info(f"Naviera {bl.naviera} cambio la estructura del HTML")
            bl.request_case = 11
        return bl
    
    def parse_html_containers(self, html, bl):
        lector = LectorHapag(html)
        revision_manual = False
        try:
            bl_data, revision_manual = lector.extraer_informacion_bl()
            if revision_manual:
                bl.revision_manual = True
            if bl_data:
                for container in bl_data:
                    c = Container(code=container['cont_id'], size=container['cont_size'], type=container['cont_type'])
                    bl.containers.append(c)
            else:
                bl.request_case = 3
        except BLNotFound:
            logger.info(f"BL {bl.bl_code} no encontrado")
            bl.request_case = 3
        except NoContainer:
            bl.request_case = 4
            logger.info(f"BL {bl.bl_code} sin contenedor asignado")
        except HTMLChanged as e:
            logger.warning(e)
            logger.info(f"Naviera {bl.naviera} cambio la estructura del HTML")
            bl.request_case = 11
        return bl
    
        
    def dict_localidades(self, inputs):
        url = "https://www.hapag-lloyd.com/en/online-business/quotation/tariffs/ocean-tariff.html"
        """
        [
            {"wait": 5000},
            {"click": "#accept-recommended-btn-handler"},
            {"wait": 1000},
            {"click": "#tariffs_ocean_rates_f\\:hl23"},
            {"wait": 1000},
            {"fill": ["#tariffs_ocean_rates_f\\:hl16", "CARTAGENA"]},
            {"click": "#tariffs_ocean_rates_f\\:hl18"},
            {"wait_for": "#tariffs_ocean_rates_f\\:hl29"}
        ]
        """
        paramss = []
        for input in inputs:
            params = {
                "js_render":"true",
                "json_response":"true",
                "js_instructions":f"%5B%7B%22wait%22%3A5000%7D%2C%7B%22click%22%3A%22%23accept-recommended-btn-handler%22%7D%2C%7B%22wait%22%3A1000%7D%2C%7B%22click%22%3A%22%23tariffs_ocean_rates_f%5C%5C%3Ahl23%22%7D%2C%7B%22wait%22%3A1000%7D%2C%7B%22fill%22%3A%5B%22%23tariffs_ocean_rates_f%5C%5C%3Ahl16%22%2C%22{input}%22%5D%7D%2C%7B%22click%22%3A%22%23tariffs_ocean_rates_f%5C%5C%3Ahl18%22%7D%2C%7B%22wait_for%22%3A%22%23tariffs_ocean_rates_f%5C%5C%3Ahl29%22%7D%5D",
                "premium_proxy":"true",
                "proxy_country":"us"
            }
            paramss.append(params)
        responses = asyncio.run(self.request_zenrows(paramss, url, paramss=True))
        lista = []
        for i, response in enumerate(responses):
            with open(f'localidades_hapag/{inputs[i].replace("/", "-").replace(".", "")}.html', 'w', encoding='utf-8') as file:
                file.write(json.loads(response.text)['html'])
            localidades = self.parse_localidad(response)
            if localidades:
                lista.append(localidades)
        return lista

    def parse_localidad(self, response):
        if response.status_code == 200:
            html = json.loads(response.text)['html']
            lector = LectorHapag(html)
            localidades = lector.extraer_localidades()
            return localidades
        else:
            return None
        
    def leer_localidad(self, input):
        with open(f'localidades_hapag/{input.replace("/", "-").replace(".", "")}.html', 'r') as f:
            html = f.read()
        localidades = self.parse_localidad_html(html)
        return localidades
        
    def parse_localidad_html(self, html):
        lector = LectorHapag(html)
        localidades = lector.extraer_localidades()
        return localidades