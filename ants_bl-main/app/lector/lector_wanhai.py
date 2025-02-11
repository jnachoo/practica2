
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

class LectorWanHai():

    def __init__(self, html, bl=None):
        self.html = html
        self.bl = bl

    def extraer_tabla_html(self):
        soup = BeautifulSoup(self.html, 'html.parser')
        return str(soup)

    def extraer_informacion(self):
        # Crear el objeto BeautifulSoup
        soup = BeautifulSoup(self.html, 'html.parser')

        tables = soup.find_all('table', class_='tbl-list')

        #import pdb; pdb.set_trace()

        if '90 days, data is not available' in str(soup):
            raise BLCancelled()

        if self.bl.bl_code not in str(soup):
            logger.info(f'BL {self.bl.bl_code} no encontrada en el HTML')
            raise Bloqueado()

        if len(tables) < 4:
            raise BLNotFound()

        try:
            cabecera = pd.read_html(StringIO(str(tables[0])))[0]
            viaje = pd.read_html(StringIO(str(tables[1])))[0]
            paradas = pd.read_html(StringIO(str(tables[3])))[0]
            containers = pd.read_html(StringIO(str(tables[4])))[0]

        except IndexError:
            raise BLNotFound()
        
        

        # Extraer información de los contenedores
        container_info = []
        for i, row in containers.iterrows():
            try:
                id_contenedor = row['Ctnr No.']
                tipo = row['Container Type Container Size Ctnr Height']
            except KeyError as e:
                raise HTMLChanged(f"Error critico en columna {e}.")
            if not isinstance(tipo, str):
                continue
            partes = tipo.split()
            size = partes[-2]

            type = partes[:-2]
            type = ' '.join(type)
            if "9'6" in partes[-1]:
                type = 'HIGH CUBE ' + type
            else:
                type = 'STANDARD ' + type
            #

            container_info.append({
                'cont_id': id_contenedor,
                'cont_type': type.strip(),
                'cont_size': size[:2].strip(),
                'peso': None,
            })
            #import pdb; pdb.set_trace()
            
        if len(paradas.columns) != 5:
            raise HTMLChanged("Formato erroneo en la tabla de paradas.")
        if not 'Place of Receipt' == paradas[0][0]:
            raise HTMLChanged("Formato erroneo en la tabla de paradas, no se encontro 'Place of Receipt'.")
        if not 'Port of Loading' == paradas[0][1]:
            raise HTMLChanged("Formato erroneo en la tabla de paradas, no se encontro 'Port of Loading'.")
        if not 'Final Destination' == paradas[0].iloc[-1]:
            raise HTMLChanged("Formato erroneo en la tabla de paradas, no se encontro 'Final Destination'.")
        # Extraer información de las paradas realizadas en el viaje
        stops = []
        date_format = '%Y/%m/%d'

        # Convertir la cadena en un objeto datetime
        pol_flag = False  # Basado en lógica personalizada
        pod_flag = False  # Basado en lógica personalizada
        for i, row in paradas.iterrows():
            # Extraer la información necesaria
            if isinstance(row, float):
                logger.info('Fila vacía')
                continue   
            try:
                date = row[4]
                if isinstance(date, str):
                    fecha = datetime.strptime(date, date_format)
                else:
                    fecha = None
            except ValueError:
                fecha = None
            lugar = row[1]
            if isinstance(lugar, str):
                lugar = row[1].split(' ')[:-1]
                lugar = ' '.join(lugar)
                codigo_pais = row[1].split('(')[1].strip()[:-1]
                locode = basededatos.buscar_locode(lugar, codigo_pais=codigo_pais)  # No disponible en el HTML dado
            else:
                lugar = None
                codigo_pais = None
                locode = None
                
            terminal = None
            status = row[0] 

            try:
                if '/' in row[2]:
                    nave = row[2].split('/')[0].strip()
                    viaje = row[2].split('/')[1].strip()
                else:
                    nave = row[2]
                    viaje = None
            except TypeError:
                nave = None
                viaje = None
            
            #import pdb; pdb.set_trace()
            if nave:
                nave = row[2].split('/')[0].strip()  # Información no disponible en este contexto
            #import pdb; pdb.set_trace()
            if 'Final Destination' in status:  # Buscar el primer puerto de carga
                break
            # LOCODE y otros detalles se pueden extraer de manera más específica si están disponibles en el HTML
            

            # Determinar POL y POD
            if "Port of Loading" in status:  # Buscar el primer puerto de carga
                pol_flag = True
            else:
                pol_flag = False
            
            if "Port of Discharging" in status:  # Actualizar el POD cada vez que haya un evento de descarga
                pod_flag = True
            else:
                pod_flag = False

            stops.append({
                'fecha': fecha,
                'lugar': lugar,
                'pais': None,
                'codigo_pais': codigo_pais,
                'locode': locode,
                'status': status,
                'terminal': terminal,
                'viaje': viaje,
                'nave': nave,
                'nave_imo': None,
                'orden': i,
                'is_pol': pol_flag,
                'is_pod': pod_flag,
            })
    

        return container_info, stops
    
