
from bs4 import BeautifulSoup
import json
from config.exceptions import BLNotFound, NoContainer, BLCancelled, Bloqueado, HTMLChanged
from datetime import datetime, timedelta
from config.settings import DATABASE_URL_TEST, DATABASE_URL
from app.database.db import DatabaseManager

basededatos = DatabaseManager(DATABASE_URL)

class LectorMSC():
    def __init__(self, html=None, json=None):
        self.html = html
        self.json = json

    def extraer_tabla_html(self):
        soup = BeautifulSoup(self.html, 'html.parser')
        table = soup.select_one('#main > div.msc-flow-tracking.separator--bottom-medium > div > div:nth-child(3) > div > div')
        if table:
            return str(table)
        else:
            return "No existe la tabla"

    def extraer_informacion(self):
        resultados = []
        df = self.extract_container_id_and_type()
        return df


    def extract_container_id_and_type(self):
        html = self.html
        soup = BeautifulSoup(html, 'html.parser')
        
        # Lista para almacenar los datos de id del contenedor y el tipo
        container_info = []

        try:
            if self.verifica_caso():
                css_select_pol = "#main > div.msc-flow-tracking.separator--bottom-medium > div > div:nth-child(3) > div > div > div > div.msc-flow-tracking__results > div > div > div.msc-flow-tracking__details > ul > li:nth-child(3) > span.msc-flow-tracking__details-value > span:nth-child(1)"
                pol_value = soup.select_one(css_select_pol)
                if pol_value:
                    pol_value = pol_value.text.strip()

                css_select_pod = "#main > div.msc-flow-tracking.separator--bottom-medium > div > div:nth-child(3) > div > div > div > div.msc-flow-tracking__results > div > div > div.msc-flow-tracking__details > ul > li:nth-child(4) > span.msc-flow-tracking__details-value > span:nth-child(1)"
                pod_value = soup.select_one(css_select_pod)
                if pod_value:
                    pod_value = pod_value.text.strip()
                
                # Encontrar todos los divs que representan contenedores
                containers = soup.find_all('div', attrs={'class': 'msc-flow-tracking__content'})
                
                
                for container in containers:
                    # Extraer el ID del contenedor
                    id_contenedor = container.find('span', {'x-text': 'container.ContainerNumber'}).text.strip()
                    if len(id_contenedor) == 0:
                        continue

                    # Extraer el tipo de contenedor
                    tipo = container.find('span', {'x-text': 'container.ContainerType'}).text.strip()
                
                    # Agregar la información al listado
                    container_info.append({
                            'cont_id': id_contenedor,
                            'cont_type': tipo[3:].strip(),
                            'cont_size': tipo[:2],
                            'pol': pol_value,
                            'pod': pod_value,
                            'service': None,
                            'peso': None,
                            'pol_limpio': pol_value.split(',')[0].strip(),
                            'pod_limpio': pod_value.split(',')[0].strip(),
                            'pol_pais': pol_value.split(',')[1].strip(),
                            'pod_pais': pod_value.split(',')[1].strip()
                        })
                
                #import pdb; pdb.set_trace()
                if len(container_info) == 0:
                    raise NoContainer()
                return container_info, 1
        except BLNotFound:
            raise BLNotFound()
        except NoContainer:
            raise NoContainer()
        except BLCancelled:
            raise BLCancelled()
        return [], 5
        
    def extraer_json(self):
        if self.json:
            try :
                data = json.loads(self.json)
            except:
                print("Error decodificando JSON")
                return [], [], None
            xhr = data.get('xhr')
            if not xhr:
                print("No hay xhr")
                return [], [], None
            data = None
            for i in xhr:
                if '/api/feature/tools/TrackingInfo' in i.get('url') :
                    data = i.get('body')
                    break
            if not data:
                raise BLNotFound()
            try:
                data = json.loads(data)
            except:
                raise Bloqueado()
            
            list_containers = []
            list_paradas = []
            data = data.get('Data')
            if 'Service is temporarily unavailable' in data:
                raise Bloqueado()
            if 'No results found' in data:
                raise BLNotFound()
            data = data.get('BillOfLadings')[0] if data.get('BillOfLadings') else None
            if not data:
                raise HTMLChanged("No se encontraro 'BillOfLadings' en el JSON.")  # Lanza una excepción si el dato no está presente
            try:
                inicio = data['GeneralTrackingInfo']['PriceCalculationDate']
                origen = data['GeneralTrackingInfo']['ShippedFrom']
                destino = data['GeneralTrackingInfo']['ShippedTo']
                pod = data['GeneralTrackingInfo']['PortOfDischarge']
                pr = data['GeneralTrackingInfo']['FinalPodEtaDate']
            except KeyError as e:
                raise HTMLChanged(f"Formato inesperado en {e}.")
            pr = datetime.strptime(pr, '%d/%m/%Y') + timedelta(days=1) if pr else None
            pr = pr.strftime('%Y-%m-%d') if pr else None
            try:
                containers = data['ContainersInfo']
            except KeyError as e:
                raise HTMLChanged(f"Formato inesperado en {e}.")
            paradas = self.get_paradas(containers)
            paradas.reverse()
            for container in containers:
                cont_id = container.get('ContainerNumber')
                type = container.get('ContainerType')[3:]
                size = container.get('ContainerType')[:2]
                if not size or not type or not cont_id:
                    raise HTMLChanged("Error critico en las llaves de 'ContainerNumber' y 'ContainerType'.")
                list_containers.append({
                    'cont_id': cont_id,
                    'cont_type': type,
                    'cont_size': size,
                    'pod': destino,
                    'pol': origen,
                    'service': None,
                    'peso': None,
                    'pol_port': None,
                    'pod_port': None,
                    'pol_limpio': None,
                    'pod_limpio': None,
                })
            is_pol = False
            last_orden = 0
            first_orden = 0

            for i, parada in enumerate(paradas):
                if ('Loaded' in parada.get('Description') or 'ActualVesselDeparture' in parada.get('Description') or 'Export received at CY' in parada.get('Description')) and is_pol == False:
                    is_pol = True
                    first_orden = i
                if ('Discharged' in parada.get('Description') and 'Transshipment' not in parada.get('Description')):
                    last_orden = i
                
                codigo_pais = parada.get('Location').split(',')[-1].strip() if parada.get('Location') else None
                locode = None
                if parada.get('UnLocationCode'):
                    if parada.get('UnLocationCode') != "":
                        locode = parada.get('UnLocationCode')
                    else:
                        locode = basededatos.buscar_locode(parada.get('Location'), codigo_pais=codigo_pais)
                lugar = parada.get('Location')
                impo = 0
                if i == 0 and lugar != origen:
                    impo = 1
                    list_paradas.append({
                        'fecha': datetime.strptime(inicio, '%d/%m/%Y').strftime('%Y-%m-%d'),
                        'lugar': origen,
                        'pais': None,
                        'codigo_pais': origen.split(',')[-1].strip(),
                        'locode': basededatos.buscar_locode(origen),
                        'tipo': None,
                        'terminal': None,
                        'status': 'ShippedFrom',
                        'nave': None,
                        'viaje': None,
                        'nave_imo': None,
                        'orden': 0,
                        'is_pol': False,
                        'is_pod': False,
                    })
                lugar = parada.get('Location')
                if not lugar and not locode:
                    raise HTMLChanged("Error critico en las llaves de 'UnLocationCode' y 'Location'.")

                list_paradas.append({
                    'fecha': datetime.strptime(parada.get('Date'), '%d/%m/%Y').strftime('%Y-%m-%d') if parada.get('Date') else None,
                    'lugar': lugar,
                    'pais': None,
                    'codigo_pais': codigo_pais,
                    'locode': locode,
                    'tipo': parada.get('site_type'),
                    'terminal': parada.get('EquipmentHandling')["Name"] if parada.get('EquipmentHandling') else None,
                    'status': parada.get('Description'),
                    'nave': parada.get('Detail')[0] if parada.get('Detail') else None,
                    'viaje': parada.get('voyage_num'),
                    'nave_imo': None,
                    'orden': parada.get('Order') + impo,
                    'is_pol': False,
                    'is_pod': False,
                })
            tiene_pod = False
            if len(list_paradas) > 0:
                if first_orden:
                    list_paradas[first_orden]['is_pol'] = True
                    if list_paradas[first_orden]['lugar'] == list_paradas[-1]['lugar']:
                        list_paradas = list_paradas[:-1]
                if last_orden:
                    list_paradas[last_orden]['is_pod'] = True
                    tiene_pod = True

            if len(list_paradas) > 0:
                if not tiene_pod:    #if (pr and expo and datetime.strptime(pr, "%Y-%m-%d") - hoy > timedelta(days=2)) or list_paradas[0]['lugar'] == list_paradas[-1]['lugar'] :
                    if last_orden and last_orden < len(list_paradas):
                        list_paradas[last_orden]['is_pod'] = False
                    list_paradas.append({
                        'fecha': pr,
                        'lugar': pod,
                        'pais': None,
                        'codigo_pais': pod.split(',')[-1].strip(),
                        'locode': basededatos.buscar_locode(pod),
                        'tipo': None,
                        'terminal': None,
                        'status': 'PortOfDischarge',
                        'nave': None,
                        'viaje': None,
                        'nave_imo': None,
                        'orden': len(list_paradas),
                        'is_pol': False,
                        'is_pod': True,
                    })
                    list_paradas.append({
                        'fecha': pr,
                        'lugar': destino,
                        'pais': None,
                        'codigo_pais': destino.split(',')[-1].strip(),
                        'locode': basededatos.buscar_locode(destino),
                        'tipo': None,
                        'terminal': None,
                        'status': 'ShippedTo',
                        'nave': None,
                        'viaje': None,
                        'nave_imo': None,
                        'orden': len(list_paradas),
                        'is_pol': False,
                        'is_pod': False,
                    })
            return list_containers, list_paradas, pr
        
    def get_paradas(self,containers):
        #  buscar el container que tenga mas locations y eventos
        paradas = 0
        max_paradas = 0
        max_container = None
        lugares = set()
        for container in containers:
            if container.get('Events'):
                for location in container.get('Events'):
                    paradas += 1
                    lugares.add(location.get('Location'))
            if len(lugares) > max_paradas:
                max_paradas = len(lugares)
                max_container = container
                #logger.debug(f"Container con mas paradas: {max_container.get('container_num')}")
            paradas = 0
            lugares = set()
        return list(max_container.get('Events')) if max_container else []

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
        
    
    def revisar_nor_found(self, soup):
        error_message = soup.find('div', attrs={"class": "msc-flow-tracking__error"})
        if error_message:
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
        container = soup.find('div', attrs={"class": "msc-flow-tracking__container"})
        if container:
            container_details = soup.find('div', attrs={"class": "msc-flow-tracking__data"})
            if container_details:
                return True
        return False
    
    
