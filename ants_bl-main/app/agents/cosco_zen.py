
from app.agents.agent import Agente
from app.lector.lector_cosco import LectorCOSCO
from app.database.db import DatabaseManager
from app.browser.seleniumdriver import SeleniumWebDriver
from app.validar_descarga import generar_dict_bl

from config.exceptions import BLCancelled, BLNotFound, NoContainer, FormatoErroneoBL, ContainerNotBL, CarpetaErronea, HTMLChanged
from config.logger import logger

from database.clases import BL, Container, Parada

import os
import shutil

from zenrows import ZenRowsClient 
import asyncio 
from urllib.parse import urlparse, parse_qs

import datetime
import requests
import json
from botocore.exceptions import NoCredentialsError, ClientError

class AgenteCOSCO(Agente):

    def descargar_html(self, bls):
        urls = []
        for bl in bls:
            url = self.generar_url(bl, url2=True)
            urls.append(url)
            bl.url = url
        params = None
        responses = asyncio.run(self.request_zenrows(params, urls))

        for i, response in enumerate(responses): 
            bl = bls[i]
            bl = self.guardar_html(response, bl, container=True)
            urls[i] = self.generar_url(bl)

        responses = asyncio.run(self.request_zenrows(params, urls))
        for i, response in enumerate(responses):
            bl = bls[i]
            bl = self.guardar_html(response, bl, container=False)


        return bls

    def scrape_rutina(self, bls):
        urls = []
        for bl in bls:
            url = self.generar_url(bl, url2=True)
            urls.append(url)
            bl.url = url
        params = None
        responses = asyncio.run(self.request_zenrows(params, urls))

        for i, response in enumerate(responses): 
            bl = bls[i]
            bl = self.guardar_html(response, bl, container=True)
            bls[i] = self.scrape_containers(response,bl)
            urls[i] = self.generar_url(bl)

        responses = asyncio.run(self.request_zenrows(params, urls))
        for i, response in enumerate(responses):
            bl = bls[i]
            bl = self.guardar_html(response, bl)
            bls[i] = self.scrape_paradas(response,bl)


        return bls

    def guardar_html(self,response,bl, container=False):
        if response.status_code == 200:
            tabla = json.loads(response.text)
            fecha = str(datetime.datetime.now()).replace(" ","_").replace(":","_").split(".",)[0]
            
            # Nombre del archivo en S3
            if container:
                nombre_json = f'json_cosco/containers/{bl.bl_code}_{fecha}.json'
            else:
                nombre_json = f'json_cosco/paradas/{bl.bl_code}_{fecha}.json'
            
            # Cargar el contenido de `tabla` como JSON a S3
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=nombre_json,
                    Body=json.dumps(tabla, indent=4),
                )
                dict_bl = generar_dict_bl(nombre_json, bl.id)

                # Agregar la información cargada a la base de datos
                self.data.add_html_descargados_batch([dict_bl])
                bl.html_descargado = True

            except (NoCredentialsError, ClientError) as e:
                logger.error(f"Error al cargar el archivo {nombre_json} a S3: {e}")
                bl.html_descargado = False
                bl.request_case = 9
        else:
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}, {bl.bl_code}")
            bl.html_descargado = False
            bl.request_case = response.status_code
        return bl

    def scrape_containers(self,response,bl):
        if response.status_code == 200:
            html = response.text    
            bl = self.parse_html_containers(html, bl)
        else:
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}")
            bl.request_case = response.status_code
        return bl
        
    def scrape_paradas(self, response, bl):

        if response.status_code == 200:
            html = response.text
            bl = self.parse_html_paradas(html, bl)
        else:
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}")
            bl.request_case = response.status_code
        return bl
    
    def leer_html(self, bl, archivo=None):
        carpeta = 'json_cosco/containers'
        archivos = self.buscar_archivos(carpeta, bl.bl_code)
        lista_filtrada = [s for s in archivos if "zenrows" not in s]
        archivo = max(lista_filtrada)
        bl.url = archivo
        with open(f'{archivo}', 'r') as f:
            html = json.loads(f.read())
        
        bl = self.parse_html_containers(html, bl)

        carpeta = 'json_cosco/paradas'
        archivos = self.buscar_archivos(carpeta, bl.bl_code)
        lista_filtrada = [s for s in archivos if "zenrows" not in s]
        archivo = max(lista_filtrada)
        bl.url = bl.url + " " + archivo
        with open(f'{archivo}', 'r') as f:
            html = json.loads(f.read())
        bl = self.parse_html_paradas(html, bl)
        return bl
    
    def parse_html_containers(self,html,bl):
        lector = LectorCOSCO(html)
        try:
            bl_data = lector.extraer_informacion_bl()
            if len(bl_data) > 0:
                bl.pol = bl_data[0]['pol']
                bl.pod = bl_data[0]['pod']
                for container in bl_data:
                    c = Container(code=container['cont_id'], size=container['cont_size'], type=container['cont_type'])
                    bl.containers.append(c)
                bl.request_case = 1
            else:
                bl.request_case = 5
        except CarpetaErronea:
            self.intercambiar_carpeta(bl)
            bl = self.leer_html(bl)
        except BLNotFound:
            logger.info(f"BL {bl.bl_code} no encontrado")
            bl.request_case = 3
        except NoContainer:
            logger.info(f"BL {bl.bl_code} sin contenedor asignado")
            bl.request_case = 4
        except HTMLChanged as e:
            logger.warning(e)
            logger.info(f"Naviera {bl.naviera} cambio la estructura del HTML")
            bl.request_case = 11
        return bl
    
    def parse_html_paradas(self,html,bl):
        lector = LectorCOSCO(html)
        try:
            paradas = lector.extraer_informacion_podpol()
            for parada in paradas:
                p = Parada(
                    fecha=parada['fecha'],
                    lugar=parada['lugar'],
                    pais=parada['pais'],
                    locode=parada['locode'],
                    terminal=parada['terminal'],
                    status=parada['status'],
                    orden=parada['orden'],
                    nave=parada['nave'],
                    is_pol=parada['is_pol'],
                    is_pod=parada['is_pod']
                )
                bl.paradas.append(p)
            bl.request_case = 1
        except BLNotFound:
            logger.info(f"BL {bl.bl_code} no encontrado")
            bl.request_case = 3
        except NoContainer:
            logger.info(f"BL {bl.bl_code} sin contenedor asignado")
            bl.request_case = 4
        except HTMLChanged as e:
            logger.warning(e)
            logger.info(f"Naviera {bl.naviera} cambio la estructura del HTML")
            bl.request_case = 11
        return bl

    def intercambiar_carpeta(self, bl):
        pythonpath = os.getenv('PYTHONPATH', '')

        containers_path = os.path.join(pythonpath,bl.url.replace('\\', '/'))
        carpeta = 'json_cosco/paradas'
        archivos = self.buscar_archivos(carpeta, bl.bl_code)
        lista_filtrada = [s for s in archivos if "zenrows" not in s]
        paradas_path = max(lista_filtrada)
        temp_path = containers_path.replace('containers', 'temp')
        
        logger.info(f"containers_path {containers_path}")
        logger.info(f"paradas_path {paradas_path}")
        logger.info(f"temp_path {temp_path}")

        # Verificar si ambos archivos existen
        if not os.path.exists(paradas_path) or not os.path.exists(containers_path):
            print("Uno o ambos archivos no existen.")
            return
        
        # Mover archivo de paradas a una ubicación temporal
        shutil.move(paradas_path, temp_path)
        
        # Mover archivo de containers a paradas
        shutil.move(containers_path, paradas_path)
        
        # Mover archivo de la ubicación temporal a containers
        shutil.move(temp_path, containers_path)

        logger.info(f"Se intercambiaron los archivos de {bl.bl_code}")
            