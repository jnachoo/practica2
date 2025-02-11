
from bs4 import BeautifulSoup

from config.logger import logger
from database.db import DatabaseManager
from config.settings import DATABASE_URL


from config.exceptions import BLNotFound, NoContainer, BLCancelled, CarpetaErronea, HTMLChanged

import json

basededatos = DatabaseManager(DATABASE_URL)

"""
OOCL
https://www.oocl.com/Pages/ExpressLink.aspx?eltype=ct&bl_no=OOLU2302216930&cont_no=&booking_no=
[{"wait": 500},
  {"click": "#allowAll"},
 {"wait": 500},
 {"evaluate": "document.querySelector('#aspnetForm > div.mix-hp.scrollify-section > div > a.mobile-panel-tab.mobile-cargo-tracking').click();"},
 {"wait": 1000},
 {"fill": ["#search > div.bd > input[type=text]:nth-child(3)", "OOLU2734108350"]},
 {"click": "#search > div.bd > input:nth-child(4)"},
 {"solve_captcha": {"type": "cloudflare_turnstile"}},
 {"wait": 1000}
]
"""

class LectorCOSCO():
    def __init__(self, jsona):
        if isinstance(jsona, dict):
            self.json = jsona
        else:
            self.json = json.loads(jsona) 

    def extraer_tabla_html(self):
        data = json.loads(self.json)
        return self.json

    """
    CASOS
    1. UN SOLO TRAMO: POL Y POD
        - orden 0: Pol
            - lugar: trackingPath, fromCity
            - pais:  trackingPath, fromCity
            - fecha: trackingPath, cgoAvailTm
            - locode: base de datos
            - nave:  actualShipmentPic[0], vesselName
            - terminal: trackingPath, pol
            - viaje: actualShipmentPic[0], service
        - orden 1: Pod
            - lugar: trackingPath, toCity
            - pais:  trackingPath, toCity
            - fecha: actualShipmentPic[0], estimatedDateOfArrival
            - locode: base de datos
            - nave:  actualShipmentPic[0], vesselName
            - terminal: trackingPath, pod
            - viaje: actualShipmentPic[0], service
    
    2. DOS TRAMOS: POL Y POD
        - orden 0: Pol
            - lugar: trackingPath, fromCity
            - pais:  trackingPath, fromCity
            - fecha: trackingPath, cgoAvailTm
            - locode: base de datos
            - nave:  actualShipmentPic[0], vesselName
            - terminal: trackingPath, pol
            - viaje: actualShipmentPic[0], service
        - orden 1: INTERMEDIO
            - lugar: actualShipmentPic[i], portOfDischarge
            - pais:  None
            - fecha: actualShipmentPic[i], estimatedDateOfArrival
            - locode: base de datos
            - nave:  actualShipmentPic[i], vesselName
            - terminal: None
            - viaje: actualShipmentPic[i], service
        - orden 2: Pod
            - lugar: trackingPath, toCity
            - pais:  trackingPath, toCity
            - fecha: actualShipmentPic[len-1], estimatedDateOfArrival
            - locode: base de datos
            - nave:  actualShipmentPic[len-1], vesselName
            - terminal: trackingPath, pod
            - viaje: actualShipmentPic[len-1], service
    """

    def extraer_informacion_podpol(self, etapa=None):
        # Convertir el string JSON a un diccionario
        data = self.json

        if not data['data']['content']['cargoTrackingContainer']:
            raise BLNotFound()

        try:
            # Verificar que el contenido del JSON exista
            tramos = data['data']['content']['actualShipmentPic']
            paradas = []

            # Extraer información para el POL
            fecha_pol = data['data']['content']['trackingPath']['cgoAvailTm']
            
            pol = data['data']['content']['trackingPath']['fromCity']

            lugar_pol = pol.split(",")[0].strip()
            pais_pol = pol.split(",")[-1].strip()
            locode_pol = basededatos.buscar_locode(lugar_pol, pais_pol)

            pol_dict = {
                    'fecha': fecha_pol,
                    'lugar': lugar_pol,
                    'pais': pais_pol,
                    'locode': locode_pol,
                    'status': "portOfLoading",
                    'terminal': data['data']['content']['trackingPath']['pol'],
                    'nave': tramos[0]['vesselName'],
                    'orden': 0,
                    'is_pol': True,
                    'is_pod': False
                }
            
            paradas.append(pol_dict)

            # TRAMOS INTERMEDIOS
            if len(tramos) > 1:
                for i,tramo in enumerate(tramos[:-1]):
                    lugar = tramo.get('portOfDischarge')
                    if not lugar:
                        raise HTMLChanged(f"No se encontró 'portOfDischarge' en el tramo {i+1}.")
                    
                    fecha = tramo.get('estimatedDateOfArrival')
                    
                    nave = tramo.get('vesselName')

                    locode = basededatos.buscar_locode(lugar)
                    paradas.append({
                        'fecha': fecha,
                        'lugar': lugar,
                        'pais': None,
                        'locode': locode,
                        'terminal': None,
                        'status': "Intermedio",
                        'nave': nave,
                        'orden': i+1,
                        'is_pol': False,
                        'is_pod': False
                    })

            # POD
            fecha_pod = tramos[-1]['estimatedDateOfArrival']
            pod = data['data']['content']['trackingPath']['toCity']
            lugar_pod = pod.split(",")[0].strip()
            pais_pod = pod.split(",")[-1].strip()
            locode_pod = basededatos.buscar_locode(lugar_pod, pais_pod)

            pod_dict = {
                    'fecha': fecha_pod,
                    'lugar': lugar_pod,
                    'pais': pais_pod,
                    'locode': locode_pod,
                    'status': "portOfDischarge",
                    'terminal': data['data']['content']['trackingPath']['pod'],
                    'nave': tramos[-1]['vesselName'],
                    'orden': len(tramos),
                    'is_pol': False,
                    'is_pod': True
                }
            paradas.append(pod_dict)

            if etapa == 1:
                service = data['data']['content']['actualShipmentPic'][-1]['service']
            if etapa == 2:
                service = data['data']['content']['actualShipmentPic'][0]['service']
            else:
                service = None

            return paradas
        except KeyError as e:
            raise HTMLChanged(f"Formato inesperado en {e}.")


    def extraer_informacion_bl(self):
        data = self.json
        container_info = []
        try:
            if self.verifica_caso():
                content = data['data']['content']
                if not isinstance(content, list):
                    raise CarpetaErronea()
                for data in content:
                    try:
                        id_contenedor = data['containerNumber']
                        tipo = data['containerType']
                    except KeyError as e:
                        raise HTMLChanged(f"Formato inesperado en {e}.")
                    peso = self.peso_a_int(data['grossWeight'])
                    container_info.append({
                            'cont_id': id_contenedor,
                            'cont_type': tipo[2:].strip(),
                            'cont_size': tipo[:2],
                            'peso': peso,
                            'service': "",
                            'pod': "",
                            'pol': "", 
                        })
                return container_info
            else:
                raise BLNotFound()
        except BLNotFound:
            raise BLNotFound()
        except HTMLChanged as e:
            raise HTMLChanged(e)
    
    def peso_a_int(self, peso):
        if not peso or peso == "":
            return 0
        return float(peso.replace("KG", "").strip())

    
    def verifica_caso(self):
        data = self.json
        if self.revisar_existe_container(data):
            return True
        elif self.revisar_nor_found(data):
            raise BLNotFound()

        
    
    def revisar_nor_found(self, data):
        if data['data']['content']:
            return False
        return True


    def revisar_existe_container(self, data):        
        if data['data']['content'] and len(data['data']['content']) > 0:
            return True
        return False
    
    
