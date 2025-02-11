from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO

from config.exceptions import BLNotFound, NoContainer, BLCancelled, FormatoErroneoBL, ContainerNotBL, HTMLChanged
from config.logger import logger
from config.settings import DATABASE_URL_TEST, DATABASE_URL
from app.database.db import DatabaseManager

basededatos = DatabaseManager(DATABASE_URL)

class LectorHapag():
    def __init__(self, html):
        self.html = html

    def extraer_tabla(self):
        soup = BeautifulSoup(self.html, 'html.parser')
        table = soup.find('table', id='tracing_by_booking_f:hl28')
        if table:
            return str(table)
        else:
            return "No existe la tabla"
        

    def extraer_informacion_bl(self):
        # Parsea el HTML con Beautiful Soup
        soup = BeautifulSoup(self.html, 'html.parser')

        # Revisar que no diga "DOCUMENT does not exist". 
        try:
            self.check_not_found()
        except BLNotFound:
            raise BLNotFound()
            # guardar requen no success y bl revisado con extio
            # return

        # Encuentra la tabla que deseas extraer por ID o clase
        table = soup.find('table', {'id': 'tracing_by_booking_f:hl27'})

        tiene_next = False
        # Opcional: verifica que la tabla no sea None
        if table:
            container_info = []
            # Utiliza pandas para leer la tabla HTML directamente
            df = pd.read_html(StringIO(str(table)))[0]  # Convertimos la tabla a string y leemos con pandas
            #print(df)
            # revisar si la ultima fila dice "Next" y si es asi, lo registra en la bd para evisar a mano
            df.dropna(how='all', inplace=True)
            if len(df) == 1:
                raise NoContainer()
            for index, row in df.iterrows():
                if index == len(df) - 1:
                    if 'Next' in row['Type']:
                        tiene_next = True
                    break
                # Extraer los datos de la tabla
                id_contenedor = row['Container No.']
                tipo = row['Type']
                if not id_contenedor or not tipo:
                    raise HTMLChanged("Error critico en llaves 'Container No.' y 'Type'.")
                # Agregar los datos a la lista
                if id_contenedor  and tipo:
                    container_info.append({
                        'cont_id': str(id_contenedor).replace(' ', ''),
                        'cont_type': tipo[2:],
                        'cont_size': tipo[:2],
                        'pod': None,
                        'pol': None,
                        'service': None,
                        'peso': None
                    })

            return container_info, tiene_next
        else:
            print("No se encontró la tabla especificada.")
            raise NoContainer()
        
    def check_not_found(self):
        soup = BeautifulSoup(self.html, 'html.parser')
        if soup.find('span', {'class': 'error'}):
            raise BLNotFound()
        
    def extraer_informacion_container(self, nave=None):
        soup = BeautifulSoup(self.html, 'html.parser')
        table = soup.find('table', {'id': 'tracing_by_booking_f:hl66'})
        paradas = []
        if table:
            container_info = []
            # Utiliza pandas para leer la tabla HTML directamente
            df = pd.read_html(StringIO(str(table)))[0]  # Convertimos la tabla a string y leemos con pandas
            #print(df)

            # revisar si existe la nave en la tabla, columna Transport
            #if not df['Transport'].str.contains(nave).any():
            #    raise ContainerNotBL()

            
            #obtener el nombre de la primera columna
            col_name = df.columns[0]
            #logger.debug(f"Columna 0: {col_name}")
            # traer la primera fila que diga Loaded en la columna col_name
            loaded = False
            discharged = False
            is_pol = False

            # Lista de valores a buscar en la columna 0
            pols = ['Loaded', 'Loading', 'Vessel departure', 'Vessel departed']
            pods = ['Vessel arrival', 'Discharged', 'Discharge', 'Vessel arrived']

            lista_de_pols = df[df.iloc[:, 0].isin(pols)]
            try:
                index_pol = min(lista_de_pols.index.tolist())
            except ValueError:
                index_pol = 0

            lista_de_pods = df[df.iloc[:, 0].isin(pods)]
            try:
                index_pod = max(lista_de_pods.index.tolist())
            except ValueError:
                index_pod = len(df) - 1

            indice_ultima_fila = df.index[-1]

            # Paso 3: Eliminar la última fila
            df.drop(indice_ultima_fila, inplace=True)

            for i,row in df.iterrows():
                lugar = str(row[df.columns[1]])  # Convertir lugar a string explícitamente
                pais = None
                us_state_code = None

                if '(' in lugar:
                    # Suponiendo que la intención era extraer el país entre paréntesis
                    lugar, pais = lugar.split('(')[0].strip(), lugar.split('(')[-1].split(')')[0].strip()
                if ',' in lugar:
                    # Corrección en la extracción de lugar y us_state_code cuando hay una coma
                    lugar, us_state_code = lugar.split(',')[0].strip(), lugar.split(',')[-1].strip()


                nave = str(row[df.columns[4]])
                viaje = row[df.columns[5]]
                orden = i
                status = row[col_name]
                fecha = row[df.columns[2]] if not pd.isnull(row[df.columns[2]]) else "1900-01-01"
                hora = row[df.columns[3]] if not pd.isnull(row[df.columns[3]]) else "00:00"
                fecha = f"{fecha} {hora}:00"
                is_pol = False
                is_pod = False

                if i == index_pol:
                    is_pol = True
                if i == index_pod:
                    is_pod = True

                locode = basededatos.buscar_locode(lugar, pais, us_state_code=us_state_code)

                if not lugar and not locode:
                    raise HTMLChanged("Error critico en la tabla con id 'tracing_by_booking_f:hl66', error en formato de 'lugar' o 'locode'.")

                parada = {
                    'lugar': lugar,
                    'pais': pais,
                    'codigo_pais':None,
                    'locode':locode,
                    'terminal':None,
                    'status':status,
                    'fecha':fecha, 
                    'orden':orden,
                    'nave_imo':None,
                    'nave':nave,
                    'viaje':viaje,
                    'is_pol':is_pol,
                    'is_pod':is_pod,
                    'us_state_code':us_state_code
                }
                paradas.append(parada)
            return paradas
        else:
            logger.warning("No se encontró la tabla especificada.")
            return False


    def extraer_localidades(self):
        soup = BeautifulSoup(self.html, 'html.parser')
        table = soup.find('table', {'id': 'tariffs_ocean_rates_f:hl29'})
        localidades = []
        if table:
            # Utiliza pandas para leer la tabla HTML directamente
            df = pd.read_html(StringIO(str(table)))[0]  # Convertimos la tabla a string y leemos con pandas
            locode = df.columns[1]
            lugar = df.columns[2]
            codigo_pais = df.columns[4]

            # eliminar filas con Nan en columna locode
            df = df.dropna(subset=[locode])
            # edliminar ultima fila
            df.drop(df.tail(1).index,inplace=True)
            for i,row in df.iterrows():
                localidad = {
                    'locode': row[locode],
                    'lugar': row[lugar],
                    'codigo_pais': row[codigo_pais]
                }
                localidades.append(localidad)
            return localidades
        else:
            logger.warning("No se encontró la tabla especificada.")
            return None
