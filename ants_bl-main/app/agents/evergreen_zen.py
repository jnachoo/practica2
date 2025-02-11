
from app.agents.agent import Agente
from app.lector.lector_evergreen import LectorEVERGREEN
from app.database.db import DatabaseManager
from app.browser.seleniumdriver import SeleniumWebDriver
from app.validar_descarga import generar_dict_bl

from config.exceptions import BLCancelled, BLNotFound, NoContainer, FormatoErroneoBL, HTMLChanged
from config.logger import logger

from database.clases import BL, Container, Parada

import datetime
import json
from zenrows import ZenRowsClient 
import asyncio
from botocore.exceptions import NoCredentialsError, ClientError
import requests


class AgenteEVERGREEN(Agente):
    def __init__(self,  web_driver: SeleniumWebDriver, data: DatabaseManager):
        super().__init__( web_driver, data)
        self.url = "https://ct.shipmentlink.com/servlet/TDB1_CargoTracking.do"
    
    def descargar_html(self, bls):
        paramss = []
        for bl in bls:
            bl_code = bl.bl_code[4:]
            data = {
                                "TYPE": "BL",
                                "PRINT": "YES",
                                "BL": bl_code,
                            }

            paramss.append(data)
            bl.url = self.url
        paramss
        responses = asyncio.run(self.request_zenrows(paramss, self.url, post=True))

        for i, response in enumerate(responses): 
            bl = bls[i]
            bls[i] = self.guardar_html(response, bl)

        return bls
    
    def leer_html(self, bl, archivo=None):
        # Buscar archivos en la carpeta html_evergreen
        carpeta = 'html_evergreen'
        archivos = self.buscar_archivos(carpeta, bl.bl_code)
        try:
            archivo = max(archivos)
            with open(f'{archivo}', 'r', encoding='utf-8') as file:
                html = file.read()
            bl = self.parse_html(html, bl)
        except ValueError:
            bls = self.scrape_rutina([bl])
            bl = bls[0]

        #print(archivos)
        return bl


    def scrape_rutina(self, bls):
        paramss = []
        for bl in bls:
            bl_code = bl.bl_code[4:]
            data = {
                                "TYPE": "BL",
                                "PRINT": "YES",
                                "BL": bl_code,
                            }

            paramss.append(data)
            bl.url = self.url

        responses = asyncio.run(self.request_zenrows(paramss, self.url, post=True))

        for i, response in enumerate(responses): 
            bl = bls[i]
            bls[i] = self.guardar_html(response, bl)
            bls[i] = self.scrape_bl(response,bl)


        return bls

    def guardar_html(self,response,bl):
        if response.status_code == 200:
            html = (response.text)
            lector = LectorEVERGREEN(html)
            tabla = lector.extraer_tabla_html()
            fecha = str(datetime.datetime.now()).replace(" ","_").replace(":","_").split(".",)[0]
            
            # Nombre del archivo en S3
            nombre_html = f'html_evergreen/{bl.bl_code}_{fecha}.html'
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=nombre_html,
                    Body=tabla
                )
                dict_bl = generar_dict_bl(nombre_html, bl.id)

                # Agregar la información cargada a la base de datos
                self.data.add_html_descargados_batch([dict_bl])
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

    async def scrape(self, bls):
        paramss = []
        client = ZenRowsClient("fe625ce84f8d2e2d04a8ec5177710814141fdac8", concurrency=len(bls), retries=1)
        for bl in bls:
            bl_code = bl['bl_code'][4:]
            data = {
                                "TYPE": "BL",
                                "PRINT": "YES",
                                "BL": bl_code,
                            }

            paramss.append(data)
        paramss

        responses = await asyncio.gather(*[client.post_async(self.url,data=data) for data in paramss]) 
        
        for i, response in enumerate(responses): 
            bl = bls[i]
            #print(url)
            #import pdb; pdb.set_trace()
            exito, caso, bl_data = self.scrape_bl(response,bl)
            if caso == 1:
                self.guardar_datos(bl, self.url, bl_data, "Exito. Container agregado.", exito)
            elif caso == 0:
                self.guardar_datos(bl, self.url, None, "BL no encontrado", exito)
            else:
                self.guardar_datos(bl, self.url, None, "Error. Caso desconocido", exito)
        
        return True
    
    def scrape_bl(self,response,bl):

        #with open(f'pagina_web_{bl["bl_code"]}.html', 'w', encoding='utf-8') as file:
        #    file.write(response.text)
        if response.status_code == 200:
            html = response.text
            bl = self.parse_html(html, bl)
        else:
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}")
            bl.request_case = response.status_code
        return bl
    
    def parse_html(self, html, bl):
        lector = LectorEVERGREEN(html)
        try:
            bl_data, paradas = lector.extraer_informacion()
            if bl_data:
                for container in bl_data:
                    bl.containers.append(Container(code=container['cont_id'], size=container['cont_size'], type=container['cont_type'], peso_kg=container['peso']))
                for para in paradas:
                    bl.paradas.append(Parada(lugar=para['lugar'], fecha=para['fecha'], pais=para['pais'], terminal=para['terminal'], codigo_pais=para['codigo_pais'], locode=para['locode'], status=para['status'], orden=para['orden'], nave=para['nave'], viaje=para['viaje'], is_pol=para['is_pol'], is_pod=para['is_pod']))
                bl.request_case = 1
            else:
                bl.request_case = 2
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