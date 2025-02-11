
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

class LectorYangMing():

    def __init__(self, html, bl=None):
        self.html = html
        self.bl = bl

    def extraer_tabla_html(self):
        soup = BeautifulSoup(self.html, 'html.parser')
        tabla = soup.find('div', id='ContentPlaceHolder1_UpdatePanel')

        return str(tabla)

    def extraer_informacion(self):
        # Crear el objeto BeautifulSoup
        soup = BeautifulSoup(self.html, 'html.parser')

        tablas = soup.find_all('table', class_='responsiveTable2')

        #import pdb; pdb.set_trace()
        try: 
            tabla_container = tablas[1]
            tabla_paradas = tablas[0]
        except IndexError:
            raise BLNotFound()
        try:
            df = pd.read_html(StringIO(str(tabla_container)))
        except ValueError:
            raise BLNotFound()
        # Asumimos que la tabla que te interesa es la primera
        containers = df[0]

        df = pd.read_html(StringIO(str(tabla_paradas)))
        paradas = df[0]
        #import pdb; pdb.set_trace()
        

        # Extraer información de los contenedores
        container_info = []
        for i, row in containers.iterrows():
            try:
                id_contenedor = row['Container No.']
                size = row['Size']
                type = row['Type'].split('-')[1]
            except KeyError as e:
                raise HTMLChanged(f"Error critico en columna {e}.")
            peso = None

            container_info.append({
                'cont_id': id_contenedor,
                'cont_type': type.strip(),
                'cont_size': size,
                'peso': peso
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
        except KeyError as e:
            raise HTMLChanged(f"Error critico en columna {e}.")
        # import pdb; pdb.set_trace()
        return container_info, stops

    def get_origen(self, paradas):
        lugar_locode = paradas['Receipt'][0].split('(')
        locode = lugar_locode[1][:-1]
        lugar = lugar_locode[0].strip()
        if ',' in paradas['Receipt'][0]:
            lugar = paradas['Receipt'][0].split(',')[0].strip()
            us_state_code = paradas['Receipt'][0].split(',')[-1].strip()
        else:
            us_state_code = None
        pais = None
        terminal = None
        fecha = None
        orden = 0
        return {
            'fecha': fecha,
            'lugar': lugar,
            'pais': pais,
            'locode': locode,
            'status': "Receipt",
            'terminal': terminal,
            'codigo_pais': None,
            'viaje': None,
            'nave': None,
            'us_state_code': us_state_code,
            'orden': orden,
            'is_pol': False,
            'is_pod': False
        }

    def get_pol(self, paradas):
        lugar_locode = paradas['Loading'][0].split('(')
        locode = lugar_locode[1][:-1]
        lugar = lugar_locode[0].strip()
        if ',' in paradas['Loading'][0]:
            lugar = paradas['Loading'][0].split(',')[0].strip()
            us_state_code = paradas['Loading'][0].split(',')[-1].strip()
        else:
            us_state_code = None
        pais = None
        terminal = None
        fecha = None
        orden = 1
        return {
            'fecha': fecha,
            'lugar': lugar,
            'pais': pais,
            'locode': locode,
            'status': "Loading",
            'terminal': terminal,
            'codigo_pais': None,
            'us_state_code': us_state_code,
            'nave': None,
            'orden': orden,
            'is_pol': True,
            'is_pod': False
        }
    
    def get_ts(self, paradas):
        lugar_locode = paradas['T/S Port'][0].split('(')
        locode = lugar_locode[1][:-1]
        lugar = lugar_locode[0].strip()
        if ',' in paradas['T/S Port'][0]:
            lugar = paradas['T/S Port'][0].split(',')[0].strip()
            us_state_code = paradas['T/S Port'][0].split(',')[-1].strip()
        else:
            us_state_code = None
        pais = None
        terminal = None
        fecha = None
        orden = 2
        return {
            'fecha': fecha,
            'lugar': lugar,
            'pais': pais,
            'locode': locode,
            'status': "T/S Port",
            'terminal': terminal,
            'codigo_pais': None,
            'us_state_code': us_state_code,
            'viaje': None,
            'nave': None,
            'orden': orden,
            'is_pol': False,
            'is_pod': False
        }
    
    def get_pod(self, paradas):
        lugar_locode = paradas['Discharge'][0].split('(')
        locode = lugar_locode[1][:-1]
        lugar = lugar_locode[0].strip()
        if ',' in paradas['Discharge'][0]:
            lugar = paradas['Discharge'][0].split(',')[0].strip()
            us_state_code = paradas['Discharge'][0].split(',')[-1].strip()
        else:
            us_state_code = None
        pais = None
        terminal = None
        fecha = None
        orden = len(paradas.columns) - 2
        return {
            'fecha': fecha,
            'lugar': lugar,
            'pais': pais,
            'locode': locode,
            'status': "Discharge",
            'terminal': terminal,
            'codigo_pais': None,
            'us_state_code': us_state_code,
            'viaje': None,
            'nave': None,
            'orden': orden,
            'is_pol': False,
            'is_pod': True
        }

    def get_destino(self, paradas):
        lugar_locode = paradas['Delivery'][0].split('(')
        locode = lugar_locode[1][:-1]
        lugar = lugar_locode[0].strip()
        if ',' in paradas['Delivery'][0]:
            lugar = paradas['Delivery'][0].split(',')[0].strip()
            us_state_code = paradas['Delivery'][0].split(',')[-1].strip()
        else:
            us_state_code = None
        pais = None
        terminal = None
        fecha = None
        orden = len(paradas.columns) - 1
        return {
            'fecha': fecha,
            'lugar': lugar,
            'pais': pais,
            'locode': locode,
            'status': "Delivery",
            'terminal': terminal,
            'codigo_pais': None,
            'us_state_code': us_state_code,
            'viaje': None,
            'nave': None,
            'orden': orden,
            'is_pol': False,
            'is_pod': False
        }