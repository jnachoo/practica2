
import datetime

from config.exceptions import BLCancelled, BLNotFound, NoContainer, FormatoErroneoBL, HTMLChanged
from config.logger import logger

from app.database.clases import BL, Container, Parada

from app.lector.lector_oocl import LectorOOCL
from app.agents.agent import Agente
from app.validar_descarga import generar_dict_bl
#from app.carga_container_manual import process_containers, get_bls_manuales

from app.database.db import DatabaseManager
from config.settings import DATABASE_URL
data = DatabaseManager(DATABASE_URL)

from botocore.exceptions import NoCredentialsError, ClientError
import json

class AgenteOOCL(Agente):

    def leer_html(self, bl, archivo=None):
        # Buscar archivos en la carpeta html_oocl
        carpeta = 'html_oocl'
        archivos = self.buscar_archivos(carpeta, bl.bl_code)
        try:
            archivo = max(archivos)
            with open(f'{archivo}', 'r', encoding='utf-8') as file:
                html = file.read()
            bl = self.parse_html(html, bl)
        except ValueError:
            logger.info(f"No se encontraron archivos para el BL {bl.bl_code}")

        #print(archivos)
        return bl

    def guardar_html(self,bl,html,nombre_html):
        files = []
        # Cargar el contenido de `tabla` como JSON a S3
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=nombre_html,
                Body=str(html)
            )
            dict_bl = generar_dict_bl(nombre_html, bl.id)
            files.append(dict_bl)

            # Agregar la información cargada a la base de datos
            data.add_html_descargados_batch(files)
            bl.html_descargado = True

        except (NoCredentialsError, ClientError) as e:
            logger.error(f"Error al cargar el archivo {nombre_html} a S3: {e}")
            bl.html_descargado = False
            bl.request_case = 9
        return bl
    
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
        lector = LectorOOCL(html)
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