
from bs4 import BeautifulSoup
import json
from config.exceptions import BLNotFound, NoContainer, BLCancelled, Bloqueado, HTMLChanged
from datetime import datetime, timedelta
from config.settings import DATABASE_URL_TEST, DATABASE_URL
from app.database.db import DatabaseManager
import pandas as pd
from io import StringIO

import re

basededatos = DatabaseManager(DATABASE_URL)

class LectorOOCL():

    def __init__(self, html):
        self.html = html



    def extraer_tabla_html(self):
        pass

    def extraer_informacion(self):
        # Crear el objeto BeautifulSoup
        soup = BeautifulSoup(self.html, 'html.parser')

        # Extraer información de los contenedores
        container_info = []
        container_rows = soup.select('#summaryTable tr[class]')
        for row in container_rows:
            id_contenedor = row.find('a')
            tipo = row.select_one("span[id^='form:cntSizeType']")
            peso = row.select_one("span[id^='form:gross']")
            if not id_contenedor:
                raise HTMLChanged("Formato erroneo en la tabla de contenedores, no se encontro id_contenedor.")
            if not tipo:
                raise HTMLChanged("Formato erroneo en la tabla de contenedores, no se encontro tipo.")
            if not peso:
                raise HTMLChanged("Formato erroneo en la tabla de contenedores, no se encontro peso.")
            id_contenedor = id_contenedor.text.strip()
            tipo = tipo.text.strip()
            peso = peso.text.strip().split()[0]  # Solo peso numérico
            container_info.append({
                'cont_id': id_contenedor,
                'cont_type': tipo[2:].strip(),
                'cont_size': tipo[:2],
                'peso': peso.split('.')[0],
            })

        # Extraer información de las paradas realizadas en el viaje
        stops = []
        tabla_paradas = soup.find_all('table', {'id': 'eventListTable'})
        if not tabla_paradas:
            raise BLNotFound()
        tramos = tabla_paradas[0]
        table = tabla_paradas[1]


        # Leer la tabla en un DataFrame de pandas
        df = pd.read_html(StringIO(str(table)))[0]
        df_tramos = pd.read_html(StringIO(str(tramos)))[0]
        try:
            paradas = self.tramos_paradas(df_tramos)
        except KeyError as e:
            raise HTMLChanged(f"Error critico en columna {e}.")
        
        df = df.iloc[::-1].reset_index(drop=True)
        order = 0
        date_format = "%d %b %Y, %H:%M"

        # Convertir la cadena en un objeto datetime
        pol_flag = False  # Basado en lógica personalizada
        pod_flag = False  # Basado en lógica personalizada
        for _, row in df.iterrows():
            # Extraer la información necesaria
            date = row['Time'].split()[:-1]
            fecha = datetime.strptime(' '.join(date), date_format)  # La fecha se extrae dependiendo del campo específico
            lugar = row['Location'].split(',')[-3].strip() if pd.notnull(row['Location']) else None
            pais = row['Location'].split(',')[-1].strip() if pd.notnull(row['Location']) else None
            terminal = row['Location'].split(',')[0].strip() if row['Location'].split(',')[0].strip() != lugar else None # row['Empty Pickup Location'] if pd.notnull(row['Empty Pickup Location']) else None
            status = row['Event']  # Dependerá de la interpretación del contexto del evento
            viaje = None #row['Vessel Voyage'] if pd.notnull(row['Vessel Voyage']) else None
            nave = None  # Información no disponible en este contexto
            nave_imo = None  # Información no disponible en este contexto
            
            # LOCODE y otros detalles se pueden extraer de manera más específica si están disponibles en el HTML
            locode = basededatos.buscar_locode(lugar, pais=pais)  # No disponible en el HTML dado
            viaje = None  # No disponible en el HTML dado
            nave = None  # No disponible en el HTML dado
            nave_imo = None  # No disponible en el HTML dado
            

            # Determinar POL y POD
            if not pol_flag and "Loaded" in status:  # Buscar el primer puerto de carga
                pol_flag = True
                pol_order = order
            
            if "Discharge" in status:  # Actualizar el POD cada vez que haya un evento de descarga
                pod_flag = True
                pod_order = order

            stops.append({
                'fecha': fecha,
                'lugar': lugar,
                'pais': pais,
                'codigo_pais': None,
                'locode': locode,
                'status': status,
                'terminal': terminal,
                'viaje': viaje,
                'nave': nave,
                'nave_imo': nave_imo,
                'orden': order,
                'is_pol': False,
                'is_pod': False,
            })
            
            order += 1
        
        # Marcar POL y POD
        if pol_flag:
            stops[pol_order]['is_pol'] = True
        if pod_flag:
            stops[pod_order]['is_pod'] = True
        
        return container_info, paradas#stops
    
    def borrar_parentesis(self, texto):
        return texto.split('(')[0].strip()
    
    def tramos_paradas(self,df):

        paradas = []
        
        # Origen
        origen = df['Origin'][0]
        terminal_origen = df['Empty Pickup Location'][0]
        pais = origen.split(',')[-1].strip()
        lugar = origen.split(',')[0].strip()
        locode = basededatos.buscar_locode(lugar, pais=pais)
        status = 'Origen'
        terminal_pol, fecha = self.extract_and_format_date(df['Full Return Location'][0])
        terminal_pol = str(terminal_pol).strip()

        paradas.append({
            'fecha': fecha,
            'lugar': lugar,
            'pais': pais,
            'codigo_pais': None,
            'locode': locode,
            'status': status,
            'terminal': terminal_origen,
            'viaje': None,
            'nave': None,
            'nave_imo': None,
            'orden': len(paradas),
            'is_pol': False,
            'is_pod': False,
        })
        #import pdb; pdb.set_trace()


        for i, row in df.iterrows():

            if isinstance(row['Port of Load'], str):
                pol, fecha = self.extract_and_format_date(row['Port of Load'])
            else:
                pol, fecha = None, None
            pais = pol.split(',')[-1].strip()
            lugar = pol.split(',')[0].strip()
            locode = basededatos.buscar_locode(lugar, pais=pais)
            status = 'Load'

            paradas.append({
                'fecha': fecha,
                'lugar': lugar,
                'pais': pais,
                'codigo_pais': None,
                'locode': locode,
                'status': status,
                'terminal': terminal_pol,
                'viaje': None,
                'nave': None,
                'nave_imo': None,
                'orden': len(paradas),
                'is_pol': False,
                'is_pod': False,
            })
            terminal_pol = None

            pod, fecha = self.extract_and_format_date(row['Port of Discharge'])

            if isinstance(row['Final Destination Hub'], str):
                terminal_pod, fecha_destino = self.extract_and_format_date(row['Final Destination Hub'])
            else:
                terminal_pod, fecha_destino = None, None
            pais = pod.split(',')[-1].strip()
            lugar = pod.split(',')[0].strip()
            locode = basededatos.buscar_locode(lugar, pais=pais)
            status = 'Discharge'

            print("termnial pod ", terminal_pod)
            terminal_pod = str(terminal_pod).strip()

            paradas.append({
                'fecha': fecha,
                'lugar': lugar,
                'pais': pais,
                'codigo_pais': None,
                'locode': locode,
                'status': status,
                'terminal': terminal_pod,
                'viaje': None,
                'nave': None,
                'nave_imo': None,
                'orden': len(paradas),
                'is_pol': False,
                'is_pod': False,
            })
        
        paradas[1]['is_pol'] = True
        paradas[-1]['is_pod'] = True
        

        destino = df.iloc[-1]['Destination']
        terminal_destino = df.iloc[-1]['Empty Return Location']
        pais = destino.split(',')[-1].strip()
        lugar = destino.split(',')[0].strip()
        locode = basededatos.buscar_locode(lugar, pais=pais)
        status = 'Destino'

        paradas.append({
            'fecha': fecha_destino,
            'lugar': lugar,
            'pais': pais,
            'codigo_pais': None,
            'locode': locode,
            'status': status,
            'terminal': terminal_destino,
            'viaje': None,
            'nave': None,
            'nave_imo': None,
            'orden': len(paradas),
            'is_pol': False,
            'is_pod': False,
        })
        return paradas

    def extract_and_format_date(self,date_string):
        if not isinstance(date_string, str):
            return None, None
        # Expresión regular para encontrar la fecha en formato "dd MMM yyyy, HH:mm"
        date_pattern = r'(\d{1,2}\s\w{3}\s\d{4},\s\d{1,2}:\d{2})'
        
        # Buscar la fecha en el string
        match = re.search(date_pattern, date_string)
        
        if match:
            # Capturar la parte que es la fecha
            date_part = match.group(1)
            
            # Separar la parte antes de la fecha y después de la fecha
            before_date = date_string[:match.start()].strip()
            after_date = date_string[match.end():].strip()
            
            return str(before_date), date_part#, after_date
        else:
            return date_string, None#, None

