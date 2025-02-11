
from bs4 import BeautifulSoup
import json
from config.exceptions import BLNotFound, NoContainer, BLCancelled, Bloqueado, HTMLChanged
from datetime import datetime, timedelta
from config.settings import DATABASE_URL_TEST, DATABASE_URL
from app.database.db import DatabaseManager
import pandas as pd
from io import StringIO
from config.logger import logger

import re

basededatos = DatabaseManager(DATABASE_URL)

class LectorHMM():

    def __init__(self, html, bl=None):
        self.html = html
        self.bl = bl

    def extraer_tabla_html(self):
        soup = BeautifulSoup(self.html, 'html.parser')
        tabla = soup.find('div', id='trackingInfomationDateResultTable')

        return str(tabla)

    def extraer_informacion(self):
        # Crear el objeto BeautifulSoup
        soup = BeautifulSoup(self.html, 'html.parser')

        div_containers = soup.find(id='containerStatus')
        # import pdb; pdb.set_trace()
        try:
            df = pd.read_html(StringIO(str(div_containers)))
        except ValueError:
            raise BLNotFound()
        # Asumimos que la tabla que te interesa es la primera
        containers = df[0]

        div_paradas = soup.find(id='cntrChangeArea')
        if not div_paradas:
            raise HTMLChanged("Error critico en el id 'cntrChangeArea'.")
        df = pd.read_html(StringIO(str(div_paradas)))
        paradas = df[0]
        # import pdb; pdb.set_trace()
        

        # Extraer información de los contenedores
        container_info = []
        for i, row in containers.iterrows():
            try:
                id_contenedor = row['Container No.']
                tipo = row['Type / Size']
            except KeyError as e:
                raise HTMLChanged(f"Error critico en columna {e}.")
            partes = tipo.split('/')
            size = partes[1]

            type = partes[0]

            container_info.append({
                'cont_id': id_contenedor,
                'cont_type': type.strip(),
                'cont_size': size.strip(),
                'peso': None,
            })
            #import pdb; pdb.set_trace()
            

        # Extraer información de las paradas realizadas en el viaje
        stops = []

        try:
            origen = self.get_origen(paradas)
            pol = self.get_pol(paradas)
            pod = self.get_pod(paradas)
            destino = self.get_destino(paradas)
            if 'T/S Port' in paradas.columns:
                ts = self.get_ts(paradas)
                stops = [origen, pol, ts, pod, destino]
            else:
                stops = [origen, pol, pod, destino]
            # import pdb; pdb.set_trace()
        except KeyError as e:
            raise HTMLChanged(f"Error critico en columna {e}.")
        return container_info, stops

    def get_origen(self, paradas):
        if ',' in paradas['Origin'][0]:
            lugar = paradas['Origin'][0].split(',')[0].strip()
            pais = paradas['Origin'][0].split(',')[-1].strip()
        else:
            lugar = paradas['Origin'][0]
            pais = None
        terminal = paradas['Origin'][1]
        fecha = paradas['Origin'][3]
        orden = 0
        locode = basededatos.buscar_locode(lugar, pais=pais)
        return {
            'fecha': fecha,
            'lugar': lugar,
            'pais': pais,
            'locode': locode,
            'status': "Origin",
            'terminal': terminal,
            'codigo_pais': None,
            'viaje': None,
            'nave': None,
            'orden': orden,
            'is_pol': False,
            'is_pod': False
        }
        

    def get_pol(self, paradas):
        if ',' in paradas['Loading Port'][0]:
            lugar = paradas['Loading Port'][0].split(',')[0].strip()
            pais = paradas['Loading Port'][0].split(',')[-1].strip()
        else:
            lugar = paradas['Loading Port'][0]
            pais = None
        terminal = paradas['Loading Port'][1]
        fecha = paradas['Loading Port'][3]
        orden = 1
        locode = basededatos.buscar_locode(lugar, pais=pais)
        return {
            'fecha': fecha,
            'lugar': lugar,
            'pais': pais,
            'locode': locode,
            'status': "Loading Port",
            'terminal': terminal,
            'codigo_pais': None,
            'viaje': None,
            'nave': None,
            'orden': orden,
            'is_pol': True,
            'is_pod': False
        }
    
    def get_ts(self, paradas):
        if ',' in paradas['T/S Port'][0]:
            lugar = paradas['T/S Port'][0].split(',')[0].strip()
            pais = paradas['T/S Port'][0].split(',')[-1].strip()
        else:
            lugar = paradas['T/S Port'][0]
            pais = None
        terminal = paradas['T/S Port'][1]
        fecha = paradas['T/S Port'][3]
        orden = 2
        locode = basededatos.buscar_locode(lugar, pais=pais)
        return {
            'fecha': fecha,
            'lugar': lugar,
            'pais': pais,
            'locode': locode,
            'status': "T/S Port",
            'terminal': terminal,
            'codigo_pais': None,
            'viaje': None,
            'nave': None,
            'orden': orden,
            'is_pol': False,
            'is_pod': False
        }
    
    def get_pod(self, paradas):
        if ',' in paradas['Discharging Port'][0]:
            lugar = paradas['Discharging Port'][0].split(',')[0].strip()
            pais = paradas['Discharging Port'][0].split(',')[-1].strip()
        else:
            lugar = paradas['Discharging Port'][0]
            pais = None
        terminal = paradas['Discharging Port'][1]
        fecha = paradas['Discharging Port'][2]
        orden = len(paradas.columns) - 3
        locode = basededatos.buscar_locode(lugar, pais=pais)
        return {
            'fecha': fecha,
            'lugar': lugar,
            'pais': pais,
            'locode': locode,
            'status': "Discharging Port",
            'terminal': terminal,
            'codigo_pais': None,
            'viaje': None,
            'nave': None,
            'orden': orden,
            'is_pol': False,
            'is_pod': True
        }

    def get_destino(self, paradas):
        if ',' in paradas['Destination'][0]:
            lugar = paradas['Destination'][0].split(',')[0].strip()
            pais = paradas['Destination'][0].split(',')[-1].strip()
        else:
            lugar = paradas['Destination'][0]
            pais = None
        terminal = paradas['Destination'][1]
        fecha = paradas['Destination'][2]
        orden = len(paradas.columns) - 2
        locode = basededatos.buscar_locode(lugar, pais=pais)
        return {
            'fecha': fecha,
            'lugar': lugar,
            'pais': pais,
            'locode': locode,
            'status': "Destination",
            'terminal': terminal,
            'codigo_pais': None,
            'viaje': None,
            'nave': None,
            'orden': orden,
            'is_pol': False,
            'is_pod': False
        }