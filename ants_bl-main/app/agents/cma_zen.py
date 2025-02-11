
from app.agents.agent import Agente
from app.lector.lector_cma import LectorCMA
from app.database.db import DatabaseManager
from app.browser.seleniumdriver import SeleniumWebDriver

from config.exceptions import BLCancelled, BLNotFound, NoContainer, FormatoErroneoBL, Bloqueado, HTMLChanged
from config.logger import logger

from database.clases import BL, Container, Parada

from zenrows import ZenRowsClient 
import asyncio 
import json
import datetime
from botocore.exceptions import NoCredentialsError, ClientError

from app.validar_descarga import generar_dict_bl

class AgenteCMA(Agente):
    def __init__(self,  web_driver: SeleniumWebDriver, data: DatabaseManager):
        super().__init__( web_driver, data)
        """
        [
            {"wait_for": "#btnTracking"},
            {"click": "#btnTracking"},
            {"wait_for": "#multiresultssection, #trackingsearchsection, script[data-cfasync="false"]"}
        ]
        """   
        self.instructions = "%5B%7B%22wait_for%22%3A%22%23btnTracking%22%7D%2C%7B%22click%22%3A%22%23btnTracking%22%7D%2C%7B%22wait_for%22%3A%22%23multiresultssection%2C%20%23trackingsearchsection%2C%20script%5Bdata-cfasync%3D'false'%5D%22%7D%5D"
        self.params = {
                "js_render":"true",
                "js_instructions":self.instructions,
                "json_response": "true",
                "premium_proxy":"true",
                "proxy_country":"us"
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
            self.guardar_html(response, bl)
            bls[i] = self.scrape_bl(response,bl)
        return bls

    def guardar_html(self,response,bl):
        files = []
        if response.status_code == 200:
            data = json.loads(response.text)  # Carga el JSON completo en 'data'
            html = data.pop('html')  # Extrae y elimina la clave 'html' del diccionario
            todo_menos_html = data  # 'data' ahora contiene el diccionario sin la clave 'html'
            lector = LectorCMA(html)
            tabla = lector.extraer_tabla_html()
            fecha = str(datetime.datetime.now()).replace(" ","_").replace(":","_").split(".",)[0]

            # Nombre del archivo en S3
            nombre_html = f'html_cma/{bl.bl_code}_{fecha}.html'
            nombre_json = f'html_cma/{bl.bl_code}_{fecha}_zenrows.json'
            try:
                # Crear lista de archivos a subir, con el nombre de archivo y contenido
                archivos = [
                    (nombre_html, json.dumps(tabla, indent=4)),
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
                logger.error(f"Error al cargar el archivo {nombre_html} a S3: {e}")
                bl.html_descargado = False
                bl.request_case = 9
        else:
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}, {bl.bl_code}")
            bl.html_descargado = False
            bl.request_case = response.status_code
        return bl
            

    async def scrape(self, bls):
        urls = []
        client = ZenRowsClient("fe625ce84f8d2e2d04a8ec5177710814141fdac8", concurrency=len(bls), retries=1)
        for bl in bls:
            if '-' in bl['bl_code']:
                self.no_revisar(bl)
                continue
            urls.append(self.generar_url(bl))
        print(urls)

        responses = await asyncio.gather(*[client.get_async(url,self.params) for url in urls]) 
        
        for i, response in enumerate(responses): 
            url = urls[i]
            bl = bls[i]
            #print(url)
            #import pdb; pdb.set_trace()
            exito, caso, bl_data = self.scrape_bl(response,bl)
            if caso == 1:
                self.guardar_datos(bl, url, bl_data, "Exito. Container agregado.", exito)
            elif caso == 2:
                self.guardar_datos(bl, url, None, "BL Cancelado.", exito)
            elif caso == 3:
                self.guardar_datos(bl, url, None, "Intento bloqueado.", True)
            elif caso == 4:
                self.guardar_datos(bl, url, None, "Error en la peticion.", exito)
            elif caso == 0:
                self.guardar_datos(bl, url, None, "BL no encontrado", exito)
            else:
                self.guardar_datos(bl, url, None, "Error. Caso desconocido", exito)
        
        return True
    
    def scrape_bl(self,response,bl):

        #with open(f'pagina_web_{bl["bl_code"]}.json', 'w', encoding='utf-8') as file:
        #    file.write(response.text)
        if response.status_code == 200:
            html = json.loads(response.text)['html']
            bl = self.parse_html(html, bl)
        else:
            logger.error(f"Error en la petición HTTP: Status code {response.status_code}")
            bl.request_case = response.status_code
        return bl
    
    def leer_html(self,bl, archivo = None):
        carpeta = 'html_cma/'
        archivos = self.buscar_archivos(carpeta, bl.bl_code)
        #logger.debug(f"Archivos encontrados: {archivos}")
        lista_filtrada = [s for s in archivos if "zenrows" not in s]
        #logger.debug(f"Archivos filtrados: {lista_filtrada}")
        archivo = max(lista_filtrada)
        bl.url = archivo
        with open(f'{archivo}', 'r', encoding='utf-8') as f:
            html = f.read()
        bl = self.parse_html(html, bl)
        return bl
    

    
    def parse_html(self,html,bl):
        lector = LectorCMA(html)
        try:
            bl_data, paradas = lector.extraer_informacion()
            if len(bl_data) > 0:
                bl.pol = bl_data[0]['pol']
                bl.pod = bl_data[0]['pod']
                for container in bl_data:
                    c = Container(code=container['cont_id'], size=container['cont_size'], type=container['cont_type'])
                    bl.containers.append(c)
                for parada in paradas:
                    p = Parada(
                        fecha=parada['fecha'],
                        lugar=parada['lugar'],
                        pais=parada['pais'],
                        codigo_pais=parada['codigo_pais'],
                        terminal=parada['terminal'],
                        locode=parada['locode'],
                        status=parada['status'],
                        orden=parada['orden'],
                        nave=parada['nave'],
                        nave_imo=parada['nave_imo'],
                        viaje=parada['viaje'],
                        is_pol=parada['is_pol'],
                        is_pod=parada['is_pod']
                    )
                    bl.paradas.append(p)
                bl.request_case = 1
            else:
                bl.request_case = 5
        except BLNotFound:
            logger.info(f"BL {bl.bl_code} no encontrado")
            bl.request_case = 3
        except Bloqueado:
            logger.info(f"BL {bl.bl_code} bloqueado")
            bl.request_case = 5
        except NoContainer:
            logger.info(f"BL {bl.bl_code} sin contenedor asignado")
            bl.request_case = 3
        except HTMLChanged as e:
            logger.warning(e)
            logger.info(f"Naviera {bl.naviera} cambio la estructura del HTML")
            bl.request_case = 11
        return bl