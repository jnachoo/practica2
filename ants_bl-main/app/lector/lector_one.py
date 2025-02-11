
from bs4 import BeautifulSoup

from config.exceptions import BLNotFound, NoContainer, BLCancelled, ContainerNotBL, HTMLChanged

import json

class LectorONE():
    def __init__(self, json):
        self.json = json   

    def extraer_tabla_html(self):
        return self.json

    def extraer_informacion_podpol(self):
        # Convertir el string JSON a un diccionario
        try:
            data = json.loads(self.json)
            polNm = data['list'][0]['polNm']
            podNm = data['list'][-1]['podNm']
            return polNm, podNm
        except json.JSONDecodeError as e:
            return None, None
        except KeyError as e:
            return None, None
        
    def extraer_informacion_paradas(self):
        paradas = []
        try:
            data = json.loads(self.json)
            pol = None
            pod = None
            pol_flag = False
            pod_flag = False
            data = data.get('list') 
            if data is None:
                raise ContainerNotBL()
            for parada in data:
                fecha = parada.get('eventDt') if parada.get('eventDt') != '' else None
                lugar = parada.get('placeNm')
                pais = lugar.split(",")[-1].strip()
                locode = parada.get('nodCd')[:5] if parada.get('nodCd') else None
                status = parada.get('statusNm')
                terminal = parada.get('yardNm')
                viaje = parada.get('skdVoyNo')
                nave = parada.get('vslEngNm')
                nave_imo = parada.get('lloydNo')
                order = int(parada.get('no')) - 1
                if not locode and not lugar:
                    raise HTMLChanged("Error critico en las llaves de 'locode' y 'lugar'.")
                # Validar `pol` y `pod` en función del status
                if 'Loaded' in status and 'Loading' in status and not pol:
                    pol = lugar
                    pol_flag = True
                if 'Unloaded' in status and 'Discharging' in status:
                    pod = lugar
                    pod_flag = True
                paradas.append({
                    'fecha': fecha,
                    'lugar': lugar,
                    'pais': pais,
                    'locode': locode,
                    'status': status,
                    'terminal': terminal,
                    'viaje': viaje,
                    'nave': nave,
                    'nave_imo': nave_imo,
                    'orden': order,
                    'is_pol': pol_flag,
                    'is_pod': pod_flag
                })
                pol_flag = False
                pod_flag = False
            return paradas, pol, pod
        except json.JSONDecodeError as e:
            return [], None, None

    def extraer_informacion_bl(self):
        try:
            data = json.loads(self.json)
        except json.JSONDecodeError as e:
            with open('error.json', 'w') as f:
                f.write(self.json)
            raise BLNotFound()
        container_info = []
        #import pdb; pdb.set_trace()
        try:
            if self.verifica_caso():
                # Extraer los valores
                data = data.get('list') 
                if data is None:
                    raise ContainerNotBL()
                for d in data:
                    try:
                        id_contenedor = d['cntrNo']
                        cop = d['copNo']
                        tipo = d['cntrTpszNm']
                        peso = self.peso_a_int(d['weight'])
                    except KeyError as e:
                        raise HTMLChanged(f"Formato inesperado en {e}.")
                    container_info.append({
                            'cont_id': id_contenedor,
                            'cont_type': tipo[2:].strip(),
                            'cont_size': tipo[:2],
                            'peso': peso,
                            'cop_no': cop,
                            'pod': "",
                            'pol': "", # Necesitarías especificar dónde encontrar el POL
                        })
                return container_info
        except BLNotFound:
            raise BLNotFound()
    
    def peso_a_int(self, peso):
        if peso == "":
            return 0
        p = peso.replace("KGS", "").replace(",", "").replace(".", "").strip()[:-3]
        if p == "":
            return 0
        p = int(p)
        #import pdb; pdb.set_trace()
        return p
    
    def verifica_caso(self):
        try:
            data = json.loads(self.json)
        except json.JSONDecodeError as e:
            with open('error.json', 'w') as f:
                f.write(self.json)
            print("Error decodificando JSON:", e)
        
        if self.revisar_existe_container(data):
            return True
        elif self.revisar_nor_found(data):
            raise BLNotFound()
        elif self.revisar_sin_contenedor(data):
            raise NoContainer()
        elif self.revisar_cancelado(data):
            raise BLCancelled()
        
    
        
    
    def revisar_nor_found(self, data):
        if data['count'] == '0':
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

    def revisar_existe_container(self, data):
        if data['count'] != '0':
            return True
        return False
    
    
