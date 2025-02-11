from config.settings import DATABASE_URL_TEST
from app.database.db import DatabaseManager
from app.browser.seleniumdriver import SeleniumWebDriver

from selenium.common.exceptions import InvalidSessionIdException, TimeoutException, ElementClickInterceptedException, NoSuchElementException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import TimeoutException
from config.logger import logger

import random
import time
import os
import glob
import boto3

from zenrows import ZenRowsClient 
import asyncio 

class Agente():

    instructions = None
    params = None
    url = None
    browser = None
    proxy = None
    # Configuración de boto3 para S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )
    bucket_name = "ants-bl"  # Reemplaza con el nombre de tu bucket de S3

    def __init__(self, web_driver: SeleniumWebDriver, data: DatabaseManager):
        self.web_driver = web_driver
        self.data = data

    def delay(self, min_delay=1, max_delay=10, seconds=None):
        """Genera un retraso aleatorio."""
        sec = seconds or random.uniform(min_delay, max_delay)
        print(f"Delay de {sec} segundos...")
        time.sleep(sec)

    async def request_zenrows(self, params, urls, post=False, paramss=False):
        if post:
            client = ZenRowsClient("fe625ce84f8d2e2d04a8ec5177710814141fdac8", concurrency=len(params), retries=1)
            responses = await asyncio.gather(*[client.post_async(urls, data=param) for param in params])
        elif paramss:
            client = ZenRowsClient("fe625ce84f8d2e2d04a8ec5177710814141fdac8", concurrency=len(params), retries=1)
            responses = await asyncio.gather(*[client.get_async(urls,param) for param in params])
        elif params is None:
            client = ZenRowsClient("fe625ce84f8d2e2d04a8ec5177710814141fdac8", concurrency=len(urls), retries=1)
            responses = await asyncio.gather(*[client.get_async(url) for url in urls])
        else:
            client = ZenRowsClient("fe625ce84f8d2e2d04a8ec5177710814141fdac8", concurrency=len(urls), retries=1)
            responses = await asyncio.gather(*[client.get_async(url,params) for url in urls])

        return responses
        


    def guardar_datos(self, bl, url, bl_data, msg, exito):
        # existen containers
        if exito and bl_data:
            self.data.add_containers(bl_data,bl)

        if exito and bl_data == None:
            self.data.add_revision_exitosa(bl)

        if self.proxy:
            self.data.save_request(bl["id"], url, "202", exito, msg)
        else:
            self.data.save_request(bl["id"], url, 202, exito, msg)

    def no_revisar(self, bl):
        self.data.no_revisar_bl(bl)

    def navegar_a(self, url):
        """Navega a una URL específica."""
        try:
            self.browser.get(url)
            print(f"Navegando a {url}")
            return self.browser.current_url == url
        except (InvalidSessionIdException, TimeoutException, WebDriverException) as e:
            print(f"Error: {e}")
            return False
        
    def generar_url(self, bl=None, url2=False):
        if bl:
            if url2:
                url = self.data.get_url(naviera=bl.naviera, aux=True)
            else:
                url = self.data.get_url(naviera=bl.naviera)

            if 'ONEY' in bl.bl_code or 'OOLU' in bl.bl_code or 'COSU' in bl.bl_code:
                url_bl = url + bl.bl_code[4:]
            else:
                url_bl = url + bl.bl_code
            return url_bl
        else:
            return None
        
    def buscar_archivos(self, carpeta, texto, mayor=False):
        # buscar archivo con bl.bl_code
        patron = os.path.join(carpeta, f'*{texto}*')
        #print(patron)
        archivos_encontrados = glob.glob(patron)
        #print(archivos_encontrados)

        if not archivos_encontrados:
            return None
        if mayor:
            archivos_encontrados = [archivo for archivo in archivos_encontrados if 'zenrows' in archivo]
            archivo_mas_pesado = max(archivos_encontrados, key=os.path.getsize)
            return [archivo_mas_pesado]
        return archivos_encontrados
        
    def elemento_visible(elemento, driver, esperar_hasta=0):
        """
        Verifica si un elemento está visible en la pantalla.

        :param elemento: El WebElement a verificar.
        :param driver: La instancia del navegador WebDriver.
        :param esperar_hasta: El tiempo máximo en segundos para esperar que el elemento sea visible. Si es 0, se verifica la visibilidad inmediata.
        :return: bool - True si el elemento está visible, False en caso contrario.
        """
        if esperar_hasta > 0:
            try:
                # Espera hasta que el elemento sea visible
                WebDriverWait(driver, esperar_hasta).until(EC.visibility_of(elemento))
                return True
            except TimeoutException:
                # Si se agota el tiempo de espera, el elemento no está visible
                return False
        else:
            # Verifica la visibilidad inmediata sin esperar
            return elemento.is_displayed()

    def get_best_proxy(self, residential, exclude=None): # TODO: random umbral
        # Obtiene el rendimiento de todos los proxies
        proxy_performance = self.data.get_proxy_performance(residential, exclude)

        # Selecciona el proxy con la tasa de éxito más alta, preferiblemente con más solicitudes
        best_proxy = max(proxy_performance, key=lambda x: (x['success_rate'], -x['total_requests']+2), default=None)

        if best_proxy:
            proxy = self.data.get_proxy_by_id(best_proxy['proxy_id'])
            # Aquí podrías hacer algo adicional con el mejor proxy, como loguearlo o actualizar su estado
            print(f"Proxy seleccionado: {proxy['ip_address']}. Tasa de éxito{best_proxy['success_rate']}. Residencial: {proxy['is_residential']}")
        else:
            proxy = self.data.get_proxy(residential=residential)
        # Retorna el ID del mejor proxy, o la instancia completa del proxy, según lo que sea más útil para tu aplicación
        return proxy