
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

class LectorPIL():

    def __init__(self, html, bl=None):
        self.html = html
        self.bl = bl

    def extraer_tabla_html(self):
        soup = BeautifulSoup(self.html, 'html.parser')
        tabla = soup.find('div', id='job_buttons')

        return str(tabla)

    def extraer_informacion(self):
        # Crear el objeto BeautifulSoup
        soup = BeautifulSoup(self.html, 'html.parser')
        
        div_containers = soup.find(id='results')
        #import pdb; pdb.set_trace()
        try:
            df = pd.read_html(StringIO(str(div_containers)))
        except ValueError:
            raise BLNotFound()
        # Asumimos que la tabla que te interesa es la primera
        containers = df[1]
        paradas = df[0]
        
        #import pdb; pdb.set_trace()
        
        # Extraer información de los contenedores
        container_info = []
        for i, row in containers.iterrows():
            try:
                id_contenedor = row['Container #'].split()[0]
                tipo = row['Size/Type']
            except KeyError as e:
                raise HTMLChanged(f"Error critico en columna {e}.")
            

            size = tipo[:2]

            type = tipo[2:]

            container_info.append({
                'cont_id': id_contenedor,
                'cont_type': type.strip(),
                'cont_size': size.strip(),
                'peso': None,
            })
            #import pdb; pdb.set_trace()
            

        # Extraer información de las paradas realizadas en el viaje
        try:
            origen = []#self.get_origen(origen_destino)
            #pod = self.get_pod(paradas)
            destino = self.get_destino(paradas, len(paradas)+1)
            ts = self.get_ts(paradas)
            origen.extend(ts)
            origen.extend(destino)
        except KeyError as e:
            raise HTMLChanged(f"Error critico en columna {e}.")
        return container_info, origen

    """    def get_origen(self, paradas):
        pp = paradas['Location'][0].split()
        lugar = pp[2:-1]
        lugar = ' '.join(lugar)
        locode = pp[-1]
        terminal = None
        fecha = None
        nave = None
        viaje = None
        # formato fecha 22-May-2024
        fecha = None
        orden = 0
        return [{
            'fecha': fecha,
            'lugar': lugar,
            'pais': None,
            'locode': locode,
            'status': "Place of Receipt",
            'terminal': terminal,
            'codigo_pais': None,
            'viaje': viaje,
            'nave': nave,
            'orden': orden,
            'is_pol': False,
            'is_pod': False
        }]
    """
    def get_ts(self, paradas):
        ts = []
        for i, row in paradas.iterrows():

            pp = row['Location'].split()
            lugar = pp[2:-1]
            lugar = ' '.join(lugar)
            locode = pp[-1]
            nave = row['Vessel/Voyage'].split()[:-1]
            nave = ' '.join(nave)
            viaje = row['Vessel/Voyage'].split()[-1]
            # formato fecha 22-May-2024
            #fecha = datetime.strptime(fecha, '%d-%b-%Y').strftime('%Y-%m-%d')
            if i == 0:
                is_pol = True
            else:
                is_pol = False
            ts.append({
                'fecha': row['Arrival/Delivery'].split()[0],
                'lugar': lugar,
                'pais': None,
                'locode': locode,
                'status': "Port of Loading" if is_pol else "Transshipment Port",
                'terminal': None,
                'codigo_pais': None,
                'viaje': viaje,
                'nave': nave,
                'orden': i + 1,
                'is_pol': is_pol,
                'is_pod': False
            })
        return ts

    def get_destino(self, paradas, len):
        pp = paradas['Next Location'][len-2].split()
        locode = pp[0]
        fecha = pp[1]
        # formato fecha 22-May-2024
        orden = len
        return [{
            'fecha': fecha,
            'lugar': None,
            'pais': None,
            'locode': locode,
            'status': "Place of Delivery",
            'terminal': None,
            'codigo_pais': None,
            'viaje': None,
            'nave': None,
            'orden': orden,
            'is_pol': False,
            'is_pod': True
        }]