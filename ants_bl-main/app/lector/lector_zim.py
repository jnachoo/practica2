
from bs4 import BeautifulSoup
import json
from config.exceptions import BLNotFound, NoContainer, BLCancelled, Bloqueado, HTMLChanged
from datetime import datetime, timedelta
from config.settings import DATABASE_URL_TEST, DATABASE_URL
from app.database.db import DatabaseManager

basededatos = DatabaseManager(DATABASE_URL)

class LectorZIM():
    def __init__(self, html=None, json=None):
        self.html = html
        self.json = json

    def extraer_tabla_html(self):
        soup = BeautifulSoup(self.html, 'html.parser')
        table = soup.select_one('#trackShipment > div > div.progress-block')
        if table:
            return str(table)
        else:
            return str(soup)

    def extraer_informacion(self):
        resultados = []
        df = self.extract_container_id_and_type()
        return df
        
    def extraer_json(self):
        if self.json:
            try :
                data = json.loads(self.json)
            except:
                print("Error decodificando JSON")
                return [], []
            xhr = data.get('xhr')
            if not xhr:
                print("No hay xhr")
                return [], []
            data = None
            tiene_info = False
            for i in xhr:
                url = i.get('url')
                if '/api/v2/GetZimComBanners/global-banner?pagePath=/tools/track-a-shipment' in url:
                    tiene_info = True
                if '/api/v2/trackShipment/GetTracing?consnumber' in url:
                    data = i.get('body')
                    break
            if not data and tiene_info:
                raise Bloqueado()
                return [], []
            if not data and not tiene_info:
                raise BLNotFound()
                return [], []
            try:
                data = json.loads(data)
            except:
                raise Bloqueado()
            
            if not data.get('isSuccess'):
                raise BLNotFound()
                        
            list_containers = []
            list_paradas = []
            data = data.get('data')
            if 'Service is temporarily unavailable' in data:
                raise Bloqueado()
            if 'No results found' in data:
                raise BLNotFound()
            tramos = data.get('blRouteLegs')
            if not tramos:
                raise HTMLChanged("Error critico en la llave 'blRouteLegs'.")
            data = data.get('consignmentDetails')
            if not data:
                raise HTMLChanged("Error critico en la llave 'consignmentDetails'.")
            try:
                origen = data['consPolDesc']+', '+data['consPolCountryName']
                destino = data['consPodDesc']+ ', '+data['consPodCountryName']
                containers = data['consContainerList']
            except KeyError as e:
                raise HTMLChanged(f"Error critico en la llave {e}.")
            paradas, vacio = self.get_paradas(containers, tramos)
            paradas.reverse()
            for container in containers:
                cont_id = container.get('unitPrefix')+container.get('unitNo')
                if not cont_id:
                    raise HTMLChanged("Error critico en las llaves de 'unitPrefix' y 'unitNo'.")
                list_containers.append({
                    'cont_id': cont_id,
                    'cont_type': container.get('cargoType')[:2] if container.get('cargoType') else None,
                    'cont_size': container.get('cargoType')[2:] if container.get('cargoType') else None,
                    'pod': destino,
                    'pol': origen,
                    'service': 'VACIO' if vacio == True else None,
                    'peso': None,
                    'pol_port': None,
                    'pod_port': None,
                    'pol_limpio': None,
                    'pod_limpio': None,
                })
            is_pol = False
            is_pod = False
            last_orden = None
            first_orden = None
            for i,parada in enumerate(paradas):
                #print('Port of Loading' in parada.get('activityDesc'))
                if ('Port of Loading' in parada.get('activityDesc') or 'POL' in parada.get('activityDesc')) and is_pol == False:
                    is_pol = True
                    first_orden = i
                    #print('POL')
                if 'discharged at Port of Destination' in parada.get('activityDesc') or 'POD' in parada.get('activityDesc'):
                    last_orden = i

                lugar = parada.get('placeFromDesc')
                us_state_code = None
                if '(' in lugar:
                    # Suponiendo que la intención era extraer el país entre paréntesis
                    lugar, us_state_code = lugar.split('(')[0].strip(), lugar.split('(')[-1].split(')')[0].strip()

                locode = basededatos.buscar_locode(lugar, pais = parada.get('countryFromName'))

                if not lugar and not locode:
                    raise HTMLChanged("Error critico en las llaves de 'placeFromDesc' y 'countryFromName'.")

                list_paradas.append({
                    'fecha': parada.get('activityDateTz'),
                    'lugar': lugar,
                    'pais': parada.get('countryFromName'),
                    'codigo_pais': None,
                    'locode': locode,
                    'tipo': None,
                    'terminal':None,
                    'us_state_code': us_state_code,
                    'status': parada.get('activityDesc'),
                    'nave': parada.get('vesselName'),
                    'viaje': parada.get('voyage'),
                    'nave_imo': None,
                    'orden': i,
                    'is_pol': False,
                    'is_pod': False,
                })
            if first_orden is not None:
                #print('first orden', first_orden)
                list_paradas[first_orden]['is_pol'] = True
                if list_paradas[first_orden]['lugar'] == list_paradas[-1]['lugar']:
                    list_paradas = list_paradas[:-1]
            if last_orden is not None:
                list_paradas[last_orden]['is_pod'] = True
        
            return list_containers, list_paradas
        
    def get_paradas(self,containers, tramos):
        #  buscar el container que tenga mas locations y eventos
        paradas = 0
        max_paradas = 0
        max_container = None
        lugares = set()
        vacio = False
        existe_pod = False
        for container in containers:
            locales = container.get('unitActivityList')
            if not locales:
                raise HTMLChanged("Error critico en la llave 'unitActivityList'.")
            if locales:
                locales.reverse()
                for location in locales:
                    location_activity = location.get('activityDesc')
                    if not location_activity:
                        raise HTMLChanged("Error critico en la llave 'activityDesc'.")
                    if 'Port of Discharge' in location_activity:
                        existe_pod = True
                    if 'Empty container gate in' in location_activity and not existe_pod:
                        vacio = True
                    paradas += 1
                    location_place_from = location.get('placeFromDesc')
                    if not location_place_from:
                        raise HTMLChanged("Error critico en la llave 'placeFromDesc'.")
                    lugares.add(location_place_from)
                existe_pod = False
            if len(lugares) > max_paradas:
                max_paradas = len(lugares)
                max_container = container
                #logger.debug(f"Container con mas paradas: {max_container.get('container_num')}")
            paradas = 0
            lugares = set()
        
        if len(tramos)+1 > max_paradas and not vacio:
            tramos_parada = []
            max_paradas = len(tramos)+1
            max_container = None
            country_from_name = tramos[0].get('countryNameFrom')
            place_from = tramos[0].get('portNameFrom')
            if not country_from_name and not place_from:
                raise HTMLChanged("Error critico en las llaves 'countryNameFrom' y 'portNameFrom'.")
            t1 = {
              "activityDateTz": tramos[0].get('actualArrivalDateTZ'),
              "activityDesc": tramos[0].get('portFromType'),
              "countryFromName": country_from_name,
              "leg": tramos[0].get('leg'),
              "vessel": tramos[0].get('vessel'),
              "vesselName": tramos[0].get('vesselName'),
              "voyage": tramos[0].get('voyage'),
              "placeFromDesc": place_from
            }
            tramos_parada.append(t1)
            for i,tramo in enumerate(tramos):
                country_from_name = tramo.get('countryNameTo')
                place_from = tramo.get('portNameTo')
                if not country_from_name and not place_from:
                    raise HTMLChanged("Error critico en las llaves 'countryNameTo' y 'portNameTo'.")
                t = {
                  "activityDateTz": tramo.get('actualDepartureDateTZ'),
                  "activityDesc": tramo.get('portToType'),
                  "countryFromName": country_from_name,
                  "leg": tramo.get('leg'),
                  "vessel": tramo.get('vessel'),
                  "vesselName": tramo.get('vesselName'),
                  "voyage": tramo.get('voyage'),
                  "placeFromDesc": place_from
                }
                tramos_parada.append(t)
            lista_paradas = tramos_parada
            lista_paradas.reverse()
        else:
            lista_paradas = list(max_container.get('unitActivityList')) if max_container.get('unitActivityList') else []
            lista_paradas.reverse()
        return lista_paradas, vacio

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
    
    
