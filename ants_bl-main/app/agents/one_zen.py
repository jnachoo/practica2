
from app.agents.agent import Agente
from app.lector.lector_one import LectorONE
from app.database.db import DatabaseManager
from app.browser.seleniumdriver import SeleniumWebDriver
from app.validar_descarga import generar_dict_bl

from config.exceptions import BLCancelled, BLNotFound, NoContainer, FormatoErroneoBL, ContainerNotBL, HTMLChanged
from config.logger import logger

from database.clases import BL, Container, Parada

from zenrows import ZenRowsClient 
import asyncio 
from urllib.parse import urlparse, parse_qs

import datetime
import requests
import json
from botocore.exceptions import NoCredentialsError, ClientError


class AgenteONE(Agente):
    def __init__(self,  web_driver: SeleniumWebDriver, data: DatabaseManager):
        super().__init__( web_driver, data)
        self.url = "https://ecomm.one-line.com/ecom/CUP_HOM_3301GS.do"
    
    def descargar_html_containers(self, bls):
        pass

    def descargar_html_paradas(self, bls):
        pass

    def descargar_html_puertos(self, bls):
        pass

    def descargar_html(self, bls):
        urls = []
        for bl in bls:
            u = self.generar_url(bl)
            urls.append(u)
            bl.url = u
        params = []
        responses = asyncio.run(self.request_zenrows(None, urls))
        descargado = True
        for i, response in enumerate(responses): 
            bl = bls[i]
            bl = self.guardar_html(response, bl, container=True)
            if descargado:
                descargado = bl.html_descargado
            bls[i] = self.scrape_containers(response,bl)
            if 'ONEY' in bl.bl_code:
                bl_code = bl.bl_code.replace("ONEY", "")
            else:
                bl_code = bl.bl_code
            
            if len(bl.containers) > 0:
                #print(bl.containers[0].cop_no)
                data = {
                            "f_cmd": "125",
                            "bkg_no": bl_code,
                            "cntr_no": bl.containers[0].code,
                            "cop_no": bl.containers[0].cop_no
                        }
            else:    
                data = {
                            "f_cmd": "125",
                            "bkg_no": bl_code,
                            "cntr_no": "",
                            "cop_no": ""
                        }
            params.append(data)

        responses = asyncio.run(self.request_zenrows(params, self.url, post=True))
        params = []
        for i, response in enumerate(responses):
            bl = bls[i]
            bl = self.guardar_html(response, bl, container=False)
            if descargado:
                descargado = bl.html_descargado
            if 'ONEY' in bl.bl_code:
                bl_code = bl.bl_code.replace("ONEY", "")
            else:
                bl_code = bl.bl_code
            data = {
                        "f_cmd": "124",
                        "bkg_no": bl_code
                    }
            params.append(data)

        responses = asyncio.run(self.request_zenrows(params, self.url, post=True))
        for i, response in enumerate(responses):
            bl = bls[i]
            bl = self.guardar_html(response, bl, puertos=True)
            if descargado:
                descargado = bl.html_descargado
            bls[i].html_descargado = descargado
        return bls

    def scrape_rutina(self, bls):
        urls = []
        for bl in bls:
            u = self.generar_url(bl)
            urls.append(u)
            bl.url = u
        params = []
        responses = asyncio.run(self.request_zenrows(None, urls))

        for i, response in enumerate(responses): 
            bl = bls[i]
            self.guardar_html(response, bl, container=True)
            bls[i] = self.scrape_containers(response,bl)
            if 'ONEY' in bl.bl_code:
                bl_code = bl.bl_code.replace("ONEY", "")
            else:
                bl_code = bl.bl_code

            if len(bl.containers) > 0:
                data = {
                            "f_cmd": "125",
                            "bkg_no": bl_code,
                            "cntr_no": bl.containers[0].code,
                            "cop_no": bl.containers[0].cop_no,
                        }
            else:
                data = {
                            "f_cmd": "125",
                            "bkg_no": bl_code,
                            "cntr_no": "",
                            "cop_no": "",
                        }
            params.append(data)
        responses = asyncio.run(self.request_zenrows(params, self.url, post=True))

        for i, response in enumerate(responses):
            bl = bls[i]
            self.guardar_html(response, bl, container=False)
            bls[i] = self.scrape_paradas(response,bl)

        return bls

    def guardar_html(self,response,bl, container=False, puertos=False):
        files = []
        if response.status_code == 200:
            html = response.text
            fecha = str(datetime.datetime.now()).replace(" ","_").replace(":","_").split(".",)[0]
            
            # Nombre del archivo en S3
            if container:
                nombre_json = f'json_one/containers/{bl.bl_code}_{fecha}.json'
            else:
                nombre_json = f'json_one/paradas/{bl.bl_code}_{fecha}.json'
            # Cargar el contenido de `tabla` como JSON a S3
            try:
                tabla = json.loads(html)
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=nombre_json,
                    Body=json.dumps(tabla, indent=4),
                )
                dict_bl = generar_dict_bl(nombre_json, bl.id)
                files.append(dict_bl)

                # Agregar la información cargada a la base de datos
                self.data.add_html_descargados_batch(files)
                bl.html_descargado = True

            except (NoCredentialsError, ClientError, json.JSONDecodeError) as e:
                logger.error(f"Error al cargar el archivo {nombre_json} a S3: {e}")
                bl.html_descargado = False
                bl.request_case = 9
            
        else:
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}, {bl.bl_code}")
            bl.html_descargado = False
            bl.request_case = 9
        return bl
    
    async def scrape(self, bls):
        urls = []
        client = ZenRowsClient("fe625ce84f8d2e2d04a8ec5177710814141fdac8", concurrency=len(bls), retries=1)
        for bl in bls:
            urls.append(self.generar_url(bl))
            #print(bl['bl_code'])
        responses = await asyncio.gather(*[client.get_async(url) for url in urls])

        exitos = []
        casos = []
        bl_datas = []
        container_urls = []
        datas = []
 
        for i, response in enumerate(responses): 
            url = urls[i]
            bl = bls[i]
            #print(url)
            #import pdb; pdb.set_trace()
            exito, caso, bl_data = self.scrape_bl(response,bl)
            exitos.append(exito)
            casos.append(caso)
            bl_datas.append(bl_data)
            if bl_data:
                if 'ONEY' in bl['bl_code']:
                    bl_code = bl["bl_code"].replace("ONEY", "")
                data = {
                                "f_cmd": "124",
                                "bkg_no": bl_code,
                            }
                datas.append(data)

        logger.info(datas)
        url = "https://ecomm.one-line.com/ecom/CUP_HOM_3301GS.do"
        podpol_responses = await asyncio.gather(*[client.post_async(url, data=data) for data in datas])
        aux = 0
        for i, bl_data in enumerate(bl_datas):
            url = urls[i]
            bl = bls[i]
            caso = casos[i]
            exito = exitos[i]
            if bl_data:
                response = podpol_responses[aux]
                pod, pol = self.scrape_container(bl, response)
                if pod or pol:
                    for container in bl_data:
                        container['pol'] = pol
                        container['pod'] = pod
                aux += 1

            if caso == 1:
                self.guardar_datos(bl, url, bl_data, "Exito. Container agregado.", exito)
            elif caso == 0:
                self.guardar_datos(bl, url, None, "BL No encontrado.", exito)
            elif caso == 3:
                self.guardar_datos(bl, url, None, "Bl sin contenedor asignado (tabla vacía).", exito)

        
        return True
    
    # Trae lista de contenedores de un BL
    def scrape_containers(self,response,bl):
        if response.status_code == 200:
            bl = self.parse_html_containers(response.text, bl)
        else:
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}")
            bl.request_case = 9
        return bl
        
    # Trae lista de paradas de un BL
    def scrape_paradas(self,response, bl):
        if response.status_code == 200:
            bl = self.parse_html_paradas(response.text, bl)
        else:
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}")
            bl.request_case = 9
        return bl
    
    def leer_html(self,bl,archivo=None):
        carpeta = 'json_one/containers/'
        archivos = self.buscar_archivos(carpeta, bl.bl_code)
        lista_filtrada = [s for s in archivos if "zenrows" not in s]
        archivo = max(lista_filtrada)
        bl.url = archivo
        with open(f'{archivo}', 'r') as f:
            html = f.read()
        bl = self.parse_html_containers(html, bl)

        carpeta = 'json_one/paradas/'
        archivos = self.buscar_archivos(carpeta, bl.bl_code)
        lista_filtrada = [s for s in archivos if "zenrows" not in s]
        archivo = max(lista_filtrada)
        bl.url = bl.url + " " + archivo
        with open(f'{archivo}', 'r') as f:
            html = f.read()
        bl = self.parse_html_paradas(html, bl)

        carpeta = 'json_one/puertos/'
        archivos = self.buscar_archivos(carpeta, bl.bl_code)
        lista_filtrada = [s for s in archivos if "zenrows" not in s]
        archivo = max(lista_filtrada)
        bl.url = bl.url + " " + archivo
        with open(f'{archivo}', 'r') as f:
            html = f.read()
        bl = self.parse_html_puertos(html, bl)
        return bl
    
    def parse_html_paradas(self, html, bl):
        lector_container = LectorONE(html)
        try:
            paradas, bl.pol, bl.pod = lector_container.extraer_informacion_paradas()
            for parada in paradas:
                bl.paradas.append(
                    Parada(
                        lugar=parada['lugar'],
                        fecha=parada['fecha'], 
                        nave=parada['nave'], 
                        viaje=parada['viaje'],
                        nave_imo=parada['nave_imo'],
                        orden=parada['orden'],
                        pais=parada['pais'],
                        locode=parada['locode'],
                        status=parada['status'],
                        terminal=parada['terminal'],
                        is_pol=parada['is_pol'],
                        is_pod=parada['is_pod']
                        )
                    )
            if len(bl.containers) > 0 and bl.pol and bl.pod:
                bl.request_case = 1
            elif len(bl.containers) == 0 and bl.pol and bl.pod:
                bl.containers.append(Container(code="Empty", size="Unknown", type="Unknown", peso_kg=None ))
                bl.request_case = 1
            elif len(bl.containers) == 0:
                bl.request_case = 3
            else:
                bl.request_case = 2
        except ContainerNotBL:
            logger.info(f"BL {bl.bl_code} no encuentra container")
            bl.request_case = 2
        except HTMLChanged as e:
            logger.warning(e)
            logger.info(f"Naviera {bl.naviera} cambio la estructura del HTML")
            bl.request_case = 11
        return bl
    
    def parse_html_containers(self, html, bl):
        lector_container = LectorONE(html)
        try:
            containers = lector_container.extraer_informacion_bl()
            for container in containers:
                cont = Container(code=container['cont_id'], size=container['cont_size'], type=container['cont_type'], peso_kg=container['peso'], cop_no=container['cop_no'])
                bl.containers.append(cont)
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

