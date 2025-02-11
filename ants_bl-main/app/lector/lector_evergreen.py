
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from html import unescape

from config.exceptions import BLNotFound, NoContainer, BLCancelled, HTMLChanged
from config.logger import logger
from config.settings import DATABASE_URL_TEST, DATABASE_URL
from app.database.db import DatabaseManager

basededatos = DatabaseManager(DATABASE_URL)

class LectorEVERGREEN():
    def __init__(self, html):
        self.html = html

    def extraer_tabla_html(self):
        soup = BeautifulSoup(self.html, 'html.parser')
        return str(soup)

    def extraer_informacion(self):
        resultados = []
        soup = BeautifulSoup(self.html, 'html.parser')
        df = self.extract_container_id_and_type(soup)
        return df
    
    def html_to_dataframe(self, soup):
        
         # Buscar el <td> con el texto específico
        target_td = soup.find('td', text="Container(s) information on B/L and Current Status")

        if not target_td:
            raise HTMLChanged("No se encontró el texto 'Container(s) information on B/L and Current Status' en ninguna cabezera de tabla.")

        # Encontrar la tabla que contiene el texto especifico
        container_table = target_td.find_parent('table')

        if not container_table:
            raise HTMLChanged("No se encontró la tabla que contiene el texto 'Container(s) information on B/L and Current Status'.")

        # Inicializar listas para almacenar los datos
        headers = []
        rows = []

        # Extraer encabezados
        header_row = container_table.find_all('tr')[1]
        if not header_row:
            raise HTMLChanged("No se encontró la fila de encabezados en la tabla que contiene el texto 'Container(s) information on B/L and Current Status'.")

        for th in header_row.find_all('th'):
            headers.append(unescape(th.get_text(strip=True)))

        # Extraer filas de datos
        for row in container_table.find_all('tr')[2:]:  # Saltar los primeros 2 <tr> que son los encabezados
            cells = row.find_all('td')
            row_data = [unescape(cell.get_text(strip=True)) for cell in cells]
            rows.append(row_data)

        # Crear el DataFrame
        df = pd.DataFrame(rows, columns=headers)
        return df

    def extract_pol_pod(self,soup):

        # Buscar el <td> con el texto "Port of Loading"
        paradas = []

        form = soup.find('form', {'name': 'frmCntrMove'})
        if not form:
            raise HTMLChanged("No se encontró el formulario 'frmCntrMove'.")
        
        pol_locode = None
        pod_locode = None

        # Buscar el campo 'pol'
        pol_input = form.find('input', {'name': 'pol'})
        pol_locode = pol_input.get('value') if pol_input else None

        # Buscar el campo 'pod'
        pod_input = form.find('input', {'name': 'pod'})
        pod_locode = pod_input.get('value') if pod_input else None

        # Verificar que se hayan encontrado valores en tablas
        origen_td = soup.find('th', text="Place of Receipt")
        if not origen_td:
            raise HTMLChanged("No se encontró el encabezado de tabla 'Place of Receipt'.")
        origen_value = origen_td.find_next_sibling('td').get_text(strip=True)
        if not origen_value:
            raise HTMLChanged("No se encontró el cuerpo de la tabla 'Place of Receipt'.")
        origen_pais = origen_value.split('(')[1].strip()[:-1]

        # Verificar que se hayan encontrado valores en tablas
        pol_td = soup.find('th', text="Port of Loading")
        if not pol_td:
            raise HTMLChanged("No se encontró el encabezado de tabla 'Port of Loading'")
        pol_value = pol_td.find_next_sibling('td').get_text(strip=True)
        if not pol_value:
            raise HTMLChanged("No se encontró el cuerpo de la tabla 'Port of Loading'.")
        pol_pais = pol_value.replace('(', '').replace(')', '').strip().split(' ')[-1]

        # Verificar que se hayan encontrado valores en tablas
        fecha_pol_td = soup.find('th', text="Estimated On Board Date")
        fecha_pol_value = fecha_pol_td.find_next_sibling('td').get_text(strip=True)
        fecha_pol = self.transform_date(fecha_pol_value)

        if origen_value == pol_value:
            origen_locode = pol_locode
        else:
            origen_locode = basededatos.buscar_locode(origen_value, codigo_pais=origen_pais)

        parada = {
            'lugar': origen_value,
            'pais': None,
            'codigo_pais': origen_pais,
            'locode': origen_locode,
            'terminal': None,
            'status': "Place of Receipt",
            'fecha': fecha_pol,
            'orden': 0,
            'nave_imo': None,
            'nave': None,
            'viaje': None,
            'is_pol': False,
            'is_pod': False
        }
        paradas.append(parada)

        parada = {
            'lugar': pol_value,
            'pais': None,
            'codigo_pais': pol_pais,
            'locode': pol_locode,
            'terminal': None,
            'status': "Port of Loading",
            'fecha': fecha_pol,
            'orden': 1,
            'nave_imo': None,
            'nave': None,
            'viaje': None,
            'is_pol': True,
            'is_pod': False
        }
        paradas.append(parada)
            
        # Verificar que se hayan encontrado valores en tablas
        pod_td = soup.find('th', text="Port of Discharge")
        if not pod_td:
            raise HTMLChanged("No se encontró el encabezado de tabla 'Port of Discharge'.")
        pod_value = pod_td.find_next_sibling('td').get_text(strip=True)
        if not pod_value:
            raise HTMLChanged("No se encontró el cuerpo de la tabla 'Port of Discharge'.")
        pod_pais = pod_value.replace('(', '').replace(')', '').strip().split(' ')[-1]

        # Verificar que se hayan encontrado valores en tablas
        destino_td = soup.find('th', text="Place of Delivery")
        if not destino_td:
            raise HTMLChanged("No se encontró el encabezado de tabla 'Place of Delivery'.")
        destino_value = destino_td.find_next_sibling('td').get_text(strip=True)
        if not destino_value:
            raise HTMLChanged("No se encontró el cuerpo de la tabla 'Place of Delivery'.")
        destino_pais = destino_value.split('(')[1].strip()[:-1]

        # Encontrar la tabla que contiene la información de las paradas
        tables = soup.find_all('table', {'class': 'ec-table ec-table-sm'})
        if not tables:
            raise HTMLChanged("No se encontraron tablas de la clase 'ec-table ec-table-sm'.")

        # Buscar la tabla con la información de las paradas (Plan Moves)
        plan_moves_table = None
        for table in tables:
            if 'Plan Moves' in table.text:
                plan_moves_table = table
                break

        # Extraer los datos de la tabla
        fecha_pod = None
        if plan_moves_table:
            
            rows = plan_moves_table.find_all('tr')
            if not rows:
                raise HTMLChanged("No se encontraron suficientes filas en la tabla.")

            # Recorrer las filas y extraer la información
            for i, row in enumerate(rows[2:]):  # Saltar las primeras dos filas (encabezados)
                cols = row.find_all('td')
                if len(cols) < 3:
                    raise HTMLChanged(f"Fila {i+2} tiene menos de 3 columnas, posible cambio en el HTML.")

                fecha = self.transform_date(cols[0].text.strip())
                
                lugar = cols[1].text.strip()
                if '(' not in lugar:
                    raise HTMLChanged(f"No se pudo extraer el país en la fila {i+2}, posible cambio en el HTML.")
                lugar_pais = lugar.split('(')[1].strip()[:-1]
                
                nave_viaje = cols[2].text.strip().split()
                nave = " ".join(nave_viaje[:-1])  # Nave
                viaje = nave_viaje[-1]  # Viaje
                fecha_pod = fecha
                locode = basededatos.buscar_locode(lugar, codigo_pais=lugar_pais)
                
                parada = {
                    'lugar': lugar,
                    'pais': None,  # Necesita mapeo adicional
                    'codigo_pais': lugar_pais,  # Necesita mapeo adicional
                    'locode': locode,  # No disponible en el HTML
                    'terminal': None,  # No disponible en el HTML
                    'status': None,  # No disponible en el HTML
                    'fecha': fecha,
                    'orden': i + 2,  # Orden basado en la posición en la tabla
                    'nave_imo': None,  # No disponible en el HTML
                    'nave': nave,
                    'viaje': viaje,
                    'is_pol': False,  # Necesita lógica adicional
                    'is_pod': False,  # Necesita lógica adicional
                }
                paradas.append(parada)

        parada = {
            'lugar': pod_value,
            'pais': None,
            'codigo_pais': pod_pais,
            'locode': pod_locode,
            'terminal': None,
            'status': "Port of Discharge",
            'fecha': fecha_pod,
            'orden': len(paradas),
            'nave_imo': None,
            'nave': None,
            'viaje': None,
            'is_pol': False,
            'is_pod': True
        }
        paradas.append(parada)

        if destino_value == pod_value:
            destino_locode = pod_locode
        else:
            destino_locode = basededatos.buscar_locode(destino_value, codigo_pais=destino_pais)

        parada = {
            'lugar': destino_value,
            'pais': None,
            'codigo_pais': destino_pais,
            'locode': destino_locode,
            'terminal': None,
            'status': "Place of Delivery",
            'fecha': fecha_pod,
            'orden': len(paradas),
            'nave_imo': None,
            'nave': None,
            'viaje': None,
            'is_pol': False,
            'is_pod': False
        }
        paradas.append(parada)
                
        logger.debug(f"POL: {pol_value} - POD: {pod_value}. POL LOCODE: {pol_locode} - POD LOCODE: {pod_locode}")

        return paradas, pol_value, pod_value
    
    def convert_weight(self, vgm):
        if not vgm or vgm == "":  # Verificar si el string está vacío o None
            return None

        # Separar el valor y la unidad de medida
        parts = vgm.split()
        if len(parts) != 2:
            return None  # Si no hay exactamente dos partes, el formato es incorrecto

        try:
            value = float(parts[0])
            unit = parts[1].upper()
        except ValueError:
            return None  # Si no se puede convertir el valor a float, retornar None

        if unit == 'LBS':
            value = value * 0.453592  # Convertir LBS a KGS
            print("se cambio a kgs")

        return value
    
    def transform_date(self, date_str):
        # Parsear la fecha en el formato inicial
        date_obj = datetime.strptime(date_str, '%b-%d-%Y')
        # Convertir la fecha al nuevo formato
        new_date_str = date_obj.strftime('%Y-%m-%d')
        return new_date_str

    def extract_container_id_and_type(self, soup):
        
        # Lista para almacenar los datos de id del contenedor y el tipo
        container_info = []

        try:
            if self.verifica_caso():
                
                lista_paradas, pod_value, pol_value = self.extract_pol_pod(soup)
                
                # Encontrar todos los divs que representan contenedores
                containers = self.html_to_dataframe(soup)
                
                for i, row in containers.iterrows():
                    id_contenedor = row['Container No.']
                    if not id_contenedor:
                        raise HTMLChanged("Formato inesperado en 'Container No.' en contenedores.")
                    tipo = row['Size/Type']
                    if not tipo:
                        raise HTMLChanged("Formato inesperado en 'Size/Type' en contenedores.")
                    peso = self.convert_weight(row['VGM'])
                
                    # Agregar la información al listado
                    container_info.append({
                            'cont_id': id_contenedor,
                            'cont_type': tipo[3:].strip().replace('(', '').replace(')', ''),
                            'cont_size': tipo[:2],
                            'pod': pod_value,
                            'pol': pol_value,
                            'service': None,
                            'peso': peso,
                        })
                
                #import pdb; pdb.set_trace()
                if len(container_info) == 0:
                    raise NoContainer()
                return container_info, lista_paradas
        except BLNotFound:
            raise BLNotFound()
        except NoContainer:
            raise NoContainer()
        except HTMLChanged as e:
            raise HTMLChanged(e)
        return [], 5
    
    def verifica_caso(self):
        soup = BeautifulSoup(self.html, 'html.parser')
        if self.revisar_existe_container(soup):
            return True
        elif self.revisar_nor_found(soup):
            raise BLNotFound()
        
    
    def revisar_nor_found(self, soup):
        error_message = soup.find('script', text=lambda x: x and 'B/L No. is not valid' in x)
        error_table = soup.find('td', text="No information on B/L No., please enter a valid B/L No. or contact our offices for assistance.")
        if error_message or error_table:
            return True
        return False

    def revisar_existe_container(self, soup):
        container = soup.find('table', attrs={"class": "ec-table ec-table-sm"})
        if container:
            return True
        return False
    
    
