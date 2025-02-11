
from bs4 import BeautifulSoup
import json

from config.logger import logger

from config.exceptions import BLNotFound, NoContainer, BLCancelled, FormatoErroneoBL, HTMLChanged

class LectorMaersk():
    def __init__(self, html=None, json=None):
        self.html = html
        self.json = json

    def extraer_informacion(self):
        resultados = []
        df = self.extract_container_id_and_type()
        return df

    def extract_location_name(self, location_div):
        #print(location_div)
        strong_tag = location_div.find('strong')
        #print(strong_tag)
        #print(strong_tag.next_sibling.next_sibling)
        strong_text = strong_tag.text.strip() if strong_tag else None
        #print(strong_text)
        # Obtener el texto restante después de <strong> si existe, de lo contrario obtener todo el texto del div
        remaining_text = strong_tag.next_sibling.next_sibling.text.strip() if strong_tag and strong_tag.next_sibling.next_sibling else None
        return strong_text, remaining_text

    def extract_container_id_and_type(self):
        html = self.html
        soup = BeautifulSoup(html, 'html.parser')
        
        # Lista para almacenar los datos de id del contenedor y el tipo
        container_info = []

        try:
            if self.verifica_caso():
                pol_value = soup.find('dd', {'data-test': 'track-from-value'})
                if pol_value:
                    pol_value = pol_value.text.strip()
            
                pod_value = soup.find('dd', {'data-test': 'track-to-value'})
                if pod_value:
                    pod_value = pod_value.text.strip()
                
                # Encontrar todos los divs que representan contenedores
                containers = soup.find_all('div', attrs={'data-test': 'container'})

                detalle = soup.find('div', {'id': 'transport-plan__container__0'})

                lista = detalle.find('ul', class_='transport-plan__list')

                # Encontrar todos los elementos de la lista
                items = lista.find_all('li', class_='transport-plan__list__item')

                #pol_limpio, pol_port = self.extract_location_name(items[0].find('div', {'data-test': 'location-name'}))
                pol_limpio = None
                pol_port = None
                pod_limpio = None
                pod_port = None
                for item in items:
                    lugar = item.find('div', {'data-test': 'location-name'})
                    if lugar:
                        lugar_nombre, port = self.extract_location_name(lugar)
                    milestone = item.find('div', {'data-test': 'milestone'}).find('span').text.strip()
                    if milestone == 'Load' and not pol_limpio:
                        pol_limpio = lugar_nombre
                        pol_port = port
                    if milestone == 'Discharge':
                        pod_limpio = lugar_nombre
                        pod_port = port
                
                
                
                for container in containers:
                    # Extraer el ID del contenedor
                    id_contenedor = container.find('span', {'data-test': 'container-details-value'}).text.strip()
                    # Extraer el tipo de contenedor

                    if container.find('span', {'data-test': 'container-type-value'}):
                        tipo = container.find('span', {'data-test': 'container-type-value'}).text.strip()
                    else:
                        return container_info, 3 # bl sin container asignado
                
                    # Agregar la información al listado
                    container_info.append({
                            'cont_id': id_contenedor,
                            'cont_type': tipo[3:],
                            'cont_size': tipo[:2],
                            'pod': pod_value,
                            'pol': pol_value,
                            'service': None,
                            'peso': None,
                            'pol_port': pol_port,
                            'pod_port': pod_port,
                            'pol_limpio': pol_limpio,
                            'pod_limpio': pod_limpio,
                        })
                
                #import pdb; pdb.set_trace()
                return container_info, 1
        except BLNotFound:
            raise BLNotFound()
        except NoContainer:
            raise NoContainer()
        except BLCancelled:
            raise BLCancelled()
        except FormatoErroneoBL:
            raise FormatoErroneoBL()
        return [], 5
        
    def extraer_json(self):
        if self.json:
            list_containers = []
            list_paradas = []
            j = json.loads(self.json)
            xhr = j.get('xhr')
            if not xhr:
                return [], []
            data = None
            hay_data = False
            for i in xhr:
                if 'api.maersk.com/synergy/tracking/' in i.get('url') :
                    data = i.get('body')
                    if 'tpdoc_num' in data:
                        hay_data = True
                        break
            if not data or not hay_data:
                return [], []
            data = json.loads(data)
            origen = data.get('origin')
            destino = data.get('destination')
            containers = data.get('containers')
            paradas = self.get_paradas(containers)
            if not origen or not destino or not containers or not paradas:
                raise HTMLChanged("Error critico en las llaves de 'origin', 'destination', 'containers' o 'locations'.")
            for container in containers:
                iso = container.get('iso_code')
                cont_id = container.get('container_num')
                if not cont_id or not iso:
                    raise HTMLChanged("Error critico en las llaves de 'iso_code' o 'container_num'.")
                if iso:
                    size = iso[:2]
                    type = iso[2:]
                else:
                    size = container.get('container_size')
                    type = container.get('container_type')

                list_containers.append({
                    'cont_id': cont_id,
                    'cont_type': type,
                    'cont_size': size,
                    'pod': destino.get('city'),
                    'pol': origen.get('city'),
                    'service': None,
                    'peso': None,
                    'pol_port': None,
                    'pod_port': None,
                    'pol_limpio': None,
                    'pod_limpio': None,
                })

            orden = 0
            is_pol = False
            first_orden = None
            last_orden = None
            for parada in paradas:
                try:
                    eventos = parada['events']
                except KeyError as e:
                        raise HTMLChanged(f"Formato inesperado en {e}.")
                for evento in eventos:
                    if ('LOAD' in evento.get('activity') or 'GATE-IN' in evento.get('activity')) and is_pol == False:
                        is_pol = True
                        first_orden = orden
                    if 'DISCHARG' in evento.get('activity') or 'CONTAINER ARRIVAL' in evento.get('activity') or 'ARRIVECU' in evento.get('activity'):
                        last_orden = orden
                    lugar = parada.get('city')
                    locode = parada.get('location_code')[:5] if parada.get('location_code') else None
                    if not lugar and not locode:
                        raise HTMLChanged("Error critico en las llaves de 'origen_locode' y 'origen_lugar'.")
                    list_paradas.append({
                        'fecha': evento.get('event_time'),
                        'lugar': lugar,
                        'pais': parada.get('country'),
                        'codigo_pais': parada.get('country_code'),
                        'locode': locode,
                        'tipo': parada.get('site_type'),
                        'terminal': parada.get('terminal'),
                        'status': evento.get('activity'),
                        'nave': evento.get('vessel_name'),
                        'viaje': evento.get('voyage_num'),
                        'nave_imo': None,
                        'orden': orden,
                        'is_pol': False,
                        'is_pod': False,
                    })
                    orden += 1
            if last_orden:
                list_paradas[last_orden]['is_pod'] = True
            if first_orden:
                list_paradas[first_orden]['is_pol'] = True
                if list_paradas[first_orden]['lugar'] == list_paradas[-1]['lugar']:
                    list_paradas = list_paradas[:-1]
            if len(list_paradas) < 3:
                existe_pol = False
                existe_pod = False
                for p in list_paradas:
                    if existe_pol == False and p['is_pol']:
                        existe_pol = True
                    if existe_pod == False and p['is_pod']:
                        existe_pod = True
                    p['orden'] += 1 
                origen_lugar = origen.get('city')
                origen_locode = origen.get('location_code')[:5] if origen.get('location_code') else None
                if not origen_lugar and not origen_locode:
                    raise HTMLChanged("Error critico en las llaves de 'origen_locode' y 'origen_lugar'.")
                list_paradas.append({
                    'fecha': None,
                    'lugar': origen_lugar,
                    'pais': origen.get('country'),
                    'codigo_pais': origen.get('country_code'),
                    'locode': origen_locode,
                    'tipo': origen.get('site_type'),
                    'terminal': origen.get('terminal'),
                    'status': origen.get('activity'),
                    'nave': None,
                    'viaje': None,
                    'nave_imo': None,
                    'orden': 0,
                    'is_pol': not existe_pol,
                    'is_pod': False,
                })

                destino_lugar = destino.get('city')
                destino_locode = destino.get('location_code')[:5] if destino.get('location_code') else None
                if not origen_lugar and not origen_locode:
                    raise HTMLChanged("Error critico en las llaves de 'origen_locode' y 'origen_lugar'.")
                list_paradas.append({
                    'fecha': None,
                    'lugar': destino_lugar,
                    'pais': destino.get('country'),
                    'codigo_pais': destino.get('country_code'),
                    'locode': destino_locode,
                    'tipo': destino.get('site_type'),
                    'terminal': destino.get('terminal'),
                    'status': destino.get('activity'),
                    'nave': None,
                    'viaje': None,
                    'nave_imo': None,
                    'orden': orden+1,
                    'is_pol': False,
                    'is_pod': not existe_pod,
                })
            return list_containers, list_paradas
    
    def get_paradas(self,containers):
        #  buscar el container que tenga mas locations y eventos
        paradas = 0
        max_paradas = 0
        max_container = None
        for container in containers:
            if container.get('locations'):
                for location in container.get('locations'):
                    if location.get('events'):
                        for event in location.get('events'):
                            paradas += 1
            if paradas > max_paradas:
                max_paradas = paradas
                max_container = container
                #logger.debug(f"Container con mas paradas: {max_container.get('container_num')}")
            paradas = 0
        return max_container.get('locations') if max_container else []

    def validar_paradas(self, paradas):
        # si no hay pol, el que tenga orden 0 es pol
        if not any([i['is_pol'] for i in paradas]):
            paradas[0]['is_pol'] = True
        # si no hay pod, el que tenga orden maximo es pod
        if not any([i['is_pod'] for i in paradas]):
            paradas[-1]['is_pod'] = True
        return paradas
       
    
    def verifica_caso(self):
        soup = BeautifulSoup(self.html, 'html.parser')
        if self.revisar_existe_container(soup):
            return True
        elif self.revisar_nor_found(soup):
            raise BLNotFound()
        elif self.revisar_sin_contenedor(soup):
            raise NoContainer()
        elif self.revisar_cancelado(soup):
            raise BLCancelled()
        elif self.revisar_formato_erroneo(soup):
            raise FormatoErroneoBL()
        return False
        
    
    def revisar_nor_found(self, soup):
        error_message = soup.find('div', attrs={"class": "track__error"})
        if error_message and "No results found" in error_message.text:
            return True
        return False
    
    def revisar_sin_contenedor(self, soup):
        if soup.find('div', attrs={"data-test": "container"}):
            container_details = soup.find('span', attrs={"data-test": "container-details-value"})
            if container_details and "not yet assigned" in container_details.text:
                return True
        return False
    
    def revisar_cancelado(self, soup):
        cancellation_notice = soup.find('mc-notification')
        if cancellation_notice and "cancelled" in cancellation_notice.text:
            return True
        return False

    def revisar_existe_container(self, soup):
        if soup.find('div', attrs={"data-test": "container"}):
            container_details = soup.find('span', attrs={"data-test": "container-details-value"})
            if container_details and "not yet assigned" not in container_details.text:
                return True
        return False
    
    def revisar_formato_erroneo(self, soup):
        # Caso 5: Formato de entrada incorrecto
        invalid_input = soup.find('input', {'aria-invalid': 'Incorrect format, please check your reference is in one of the formats described below.'})
        if invalid_input:
            return True
        return False

    def extraer_tabla_html(self):
        soup = BeautifulSoup(self.html, 'html.parser')
        table = soup.select_one("#maersk-app > div > div > div:nth-child(3)")
        if table:
            return str(table)
        else:
            return "No existe la tabla"
