
from bs4 import BeautifulSoup

import json
import re
import os
#from html_comparator import detectar_cambios_dom

from config.exceptions import BLNotFound, NoContainer, BLCancelled, Bloqueado, HTMLChanged

class LectorCMA():
    def __init__(self, html):
        self.html = html

    def extraer_informacion(self):
        resultados = []
        soup = BeautifulSoup(self.html, 'html.parser')
        df = self.extract_container_id_and_type(soup)
        return df
    
    def extraer_tabla_html(self):
        soup = BeautifulSoup(self.html, 'html.parser')
        script_tag = soup.find('script', text=re.compile(r'var model ='))
        if script_tag:
            script_content = script_tag.string
            model_json_text = re.search(r'var model =(\[.*\]);', script_content, re.DOTALL)
            if model_json_text is None:
                return "No existe la tabla"
            else:
                model_json_text = model_json_text.group(1)

            mjs = model_json_text[:-25].strip()[:-1]

            # Paso 5: Cargar el JSON en un diccionario de Python
            model_data = json.loads(mjs)
            return model_data
        else:
            return "No existe la tabla"
    
    def extraer_contenedores(self, soup):        
        # Lista para almacenar los datos de id del contenedor y el tipo
        container_info = []
        list_paradas = []

        # Paso 1: Encontrar y extraer el contenido del script que contiene la variable 'model'
        script_tag = soup.find('script', text=re.compile(r'var model ='))
        if not script_tag:
            raise Bloqueado()
        script_content = script_tag.string
        if not script_content:
            raise NoContainer()

        # Paso 2: Usar expresiones regulares para extraer el JSON de 'model'
        match = re.search(r'var model =(\[.*\]);', script_content, re.DOTALL)
        if not match:
            raise NoContainer()
        model_json_text = match.group(1)
        mjs = model_json_text[:-25].strip()[:-1]

        # Paso 3: Cargar el JSON en un diccionario de Python
        model_data = json.loads(mjs)
        
        # Paso 4: Verificar si el JSON está vacío
        if not model_data:
            raise NoContainer()

        # Paso 5: Extraer información de los movimientos
        try:
            paradas = model_data[0]['ContainerMoveDetails']['pastMoves']
            paradas.extend(model_data[0]['ContainerMoveDetails']['currentMoves'])
            paradas.extend(model_data[0]['ContainerMoveDetails']['futureMoves'])
        except KeyError as e:
            raise HTMLChanged(f"Formato inesperado en {e}.")
        
        # Paso 6: Extraer la información requerida de cada contenedor
        for container in model_data:
            # Verificaciones de campos críticos
            container_reference = container.get('ContainerReference')
            container_type = container.get('Type')
            container_size = container.get('Size')

            # Si algún campo crítico falta, lanzar excepción
            if not container_reference or not container_type or not container_size:
                raise HTMLChanged("Faltan campos clave en los datos del contenedor.")
            try:
                # Verificar si existen los puertos y locodes, si no, lanzar excepción
                port_of_loading = container['ContainerMoveDetails']['routingInformation']['portOfLoading']['name']
                port_of_discharge = container['ContainerMoveDetails']['routingInformation']['portOfDischarge']['name']
                pol_locode = container['ContainerMoveDetails']['routingInformation']['portOfLoading']['code']
                pod_locode = container['ContainerMoveDetails']['routingInformation']['portOfDischarge']['code']

            except KeyError as e:
                raise HTMLChanged(f"Formato inesperado en {e}.")

            # Procesar los países
            pol_pais = port_of_loading.split('(')[1].strip()[:-1]
            pod_pais = port_of_discharge.split('(')[1].strip()[:-1]
            
            
            # Verificar y corregir países si es necesario
            if pol_pais == 'CI':
                pol_pais = 'CL'
            if pod_pais == 'CI':
                pod_pais = 'CL'

            container_info.append({
                            'cont_id': container_reference,
                            'cont_type': container_type,
                            'cont_size':container_size,
                            'pol': port_of_loading,
                            'pod': port_of_discharge,
                            'service': None,
                            'peso': None,
                            'pol_locode': pol_locode,
                            'pod_locode': pod_locode,
                            'pol_pais': pol_pais,
                            'pod_pais': pod_pais,
                            'pol_limpio': port_of_loading.split('(')[0].strip(),
                            'pod_limpio': port_of_discharge.split('(')[0].strip()
                        })
        is_pol = False
        is_pod = False
        first_orden = None
        last_orden = None
        for i, parada in enumerate(paradas):
            container_status = parada.get('containerStatus')
            if not container_status:
                raise HTMLChanged("No se encontro el 'containerStatus' en una parada.") 
            if ('Loadedonboard' in container_status or 'ActualVesselDeparture' in container_status) and is_pol == False:
                is_pol = True
                first_orden = i
            if 'ActualVesselArrival' in container_status or 'PlannedVesselArrival' in container_status or 'Discharged' in container_status:
                last_orden = i

            lugar = parada.get('location').get('name') if parada.get('location') else None
            locode = parada.get('location').get('code') if parada.get('location') else None
            if not lugar and not locode:
                raise HTMLChanged("Error critico en las llaves de 'locode' y 'lugar'.")

            list_paradas.append({
                'fecha': parada.get('containerStatusDate').replace('T', ' ') if parada.get('containerStatusDate') else None,
                'lugar': lugar,
                'pais': None,
                'codigo_pais': None,
                'locode': locode,
                'terminal': parada.get('locationTerminal'),
                'status': parada.get('containerStatus'),
                'nave': parada.get('vesselName'),
                'viaje': parada.get('voyageReference'),
                'nave_imo': parada.get('vesselId'),
                'orden': i,
                'is_pol': False,
                'is_pod': False,
            })
            pass
        if first_orden:
            list_paradas[first_orden]['is_pol'] = True
            if list_paradas[first_orden]['lugar'] == list_paradas[-1]['lugar']:
                list_paradas = list_paradas[:-1]
        if last_orden:
            list_paradas[last_orden]['is_pod'] = True
        return container_info, list_paradas

    def extract_container_id_and_type(self, soup):
        try: 
            if self.verifica_caso(soup):
                return self.extraer_contenedores(soup)
        except BLNotFound:
            raise BLNotFound()
        except Bloqueado:
            raise Bloqueado()
        except HTMLChanged as e:
            raise HTMLChanged(e)
        return [], 5

    
    def verifica_caso(self, soup):
        if self.revisar_existe_container(soup):
            return True
        elif self.revisar_nor_found(soup):
            raise BLNotFound()
        elif self.revisar_bloqueo(soup):
            raise Bloqueado()
        return True
    
    def revisar_existe_container(self, soup):
        container_solo = soup.find('section', attrs={"id": "trackingsearchsection"})
        multi_conainer = soup.find('section', attrs={"id": "multiresultssection"})
        if container_solo or multi_conainer:
            return True
        return False
    
    def revisar_nor_found(self, soup):
        error_message = soup.select_one('#noresultssection > div.main-wrapper > div')
        if error_message:
            return True
        return False
    
    def revisar_bloqueo(self, soup):
        cancellation_notice = soup.find('script', attrs={"data-cfasync": "false"})
        script_tag = soup.find('script', text=re.compile(r'var model ='))
        if cancellation_notice or not script_tag:
            return True
        return False