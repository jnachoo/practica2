
from config.settings import DATABASE_URL_TEST, DATABASE_URL
from config.logger import setup_logger

from app.database.db import DatabaseManager
from app.agents.maersk import AgenteMaersk
from app.agents.maersk_zen import AgenteMaerskZen
from app.agents.hapag import AgenteHapag
from app.agents.hapag_zen import AgenteHapagZen
from app.agents.msc_zen import AgenteMSC
from app.agents.cma_zen import AgenteCMA
from app.agents.one_zen import AgenteONE
from app.agents.cosco_zen import AgenteCOSCO
from app.agents.evergreen_zen import AgenteEVERGREEN
from app.agents.zim_zen import AgenteZIM
from app.agents.wanhai import AgenteWanHai
from app.agents.pil_zen import AgentePIL
from app.agents.hmm_zen import AgenteHMM
from app.agents.yangming_zen import AgenteYangMing


from database.clases import BL, Container

import random
import dotenv
import os
import asyncio

import argparse
import datetime
import pandas as pd

dotenv.load_dotenv()

# Obtiene la ruta actual del script (ants_bl-main/app)
current_dir = os.path.dirname(os.path.abspath(__file__))
# Obtiene el directorio padre (ants_bl-main)
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))

# Inserta el directorio padre en sys.path si no está ya presente
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Opcional: imprime sys.path para verificar que se agregó correctamente
print("sys.path:", sys.path)

from config.logger import logger

data = DatabaseManager(DATABASE_URL)
driver = None

"""
Dos posibles casos de uso:
1. Leer bls de un csv y hacer scraping de los bls. Esta opbion no deja rastros en la base de datos. Lee de csv, hace scraping y escribe en csv.
2. Leer bls de la rutina diaria. Trae bls de la base de datos, hace scraping y registra el resultado.

Flujo para el caso 1
1. get_bls_csv(archivo) -> lista de objetos BL
2. descargar_bls_csv(bls_vacios) -> lista de objetos BL descargados
3. guardar_csv(bls_descargados) -> guarda en csv

Flujo para el caso 2
1. seleccionar_bls(data, navieras) -> lista de objetos BL

"""


def seleccionar_bls(data, navieras, estados, limit=10, mes=None, anio=None, bl_code=None):
    bls_a_revisar = []
    naviera = random.choice(navieras)
    
    while len(bls_a_revisar) == 0:
        bls_a_revisar = get_bls_db(naviera, estados, limit=limit, mes=mes, anio=anio, bl_code=bl_code)
        if len(bls_a_revisar) == 0:
            navieras.pop(navieras.index(naviera))
            if len(navieras) == 0:
                return []
            naviera = random.choice(navieras)

    return bls_a_revisar

def get_bls_db(naviera,estados, limit=10, mes=None, anio=None, bl_code=None):
    if not bl_code:
        bls_a_revisar = data.get_bls_rutina(naviera=naviera, limit=limit, estados=estados, month=mes, year=anio, random=True)
    else:
        bls_a_revisar = data.get_bl(bl_code=bl_code, naviera=naviera)
    bls = []
    for i in bls_a_revisar:
        bl = BL(id= i['id'], bl_code=i['bl_code'], naviera=i['nombre_naviera'], fecha_bl=i['fecha_bl'], etapa=i['etapa'], estado=i['estado'])

        """containers = data.get_containers(bl.id)
        for j in containers:
            container = Container(code=j['code'], size=j['size'], type=j['type'], pol=j['pol'], pod=j['pod'], bl_id=j['bl_id'], peso_kg=j['peso_kg'], service=j['service'])
            bl.containers.append(container)

        if bl.estado in [3,4,5,6,7,9] and len(bl.containers) > 0:
            print(f"BL: {bl.bl_code}. Estado mal calculado")
            #raise Exception(f"BL: {bl.bl_code}. Estado mal calculado")
        """
        
        bls.append(bl)

    return bls


# Get bls csv. recibe nombre del archivo csv, devuelve lista de objetos BL
def get_bls_csv(archivo):
    bls_a_revisar = pd.read_csv(archivo, sep=';')
    if len(bls_a_revisar) == 0:
        return False
    bls = []
    for i, row in bls_a_revisar.iterrows():
        bl = BL(id=i, bl_code=row['bl_code'], naviera=row['naviera'])

        bls.append(bl)

    return bls

# Descargar bls csv. recibe lista de objetos BL, agrupa en listas de 10, descarga de 10 en 10 y devuelve una lista de objetows BLS cargados de info
def descargar_bls_csv(bls):
    navieras = list(set([bl.naviera for bl in bls]))
    for naviera in navieras:
        bls_naviera = [bl for bl in bls if bl.naviera == naviera]
        grupos_de_10 = [bls_naviera[i:i + 10] for i in range(0, len(bls_naviera), 10)]
      #  print(grupos_de_10)
       # exit()
        bls_descargados = []
        for bls in grupos_de_10:
            bls_descargados.extend(scrape(bls))
        
    return bls_descargados

# scrape recibe una lista de bls, los descarga y devuelve la misma lista con los bls cargados
def scrape(bls):
    bls_descargados = []
    if bls[0].naviera == "HAPAG-LLOYD":
        agente = AgenteHapagZen(driver, data)
    elif bls[0].naviera == "MAERSK":
        agente = AgenteMaerskZen(driver, data)
    elif bls[0].naviera == "MSC":
        agente = AgenteMSC(driver, data)
    elif bls[0].naviera == "CMA-CGM":
        agente = AgenteCMA(driver, data)
    elif bls[0].naviera == "ONE":
        agente = AgenteONE(driver, data)
    elif bls[0].naviera == "COSCO":
        agente = AgenteCOSCO(driver, data)
    elif bls[0].naviera == "EVERGREEN":
        agente = AgenteEVERGREEN(driver, data)
    elif bls[0].naviera == "ZIM":
        agente = AgenteZIM(driver, data)
    elif bls[0].naviera == "WAN HAI LINES":
        agente = AgenteWanHai(driver, data)
    elif bls[0].naviera == "PIL":
        agente = AgentePIL(driver, data)
    elif bls[0].naviera == "HMM":
        agente = AgenteHMM(driver, data)
    elif bls[0].naviera == "YANG MING":
        agente = AgenteYangMing(driver, data)
    else:
        logger.error("Naviera no encontrada")
        return False
    
    bls_descargados = agente.scrape_rutina(bls)

    return bls_descargados

def eliminar_contenedores_repetidos(bl):
    contenedores = []
    for container in bl.containers:
        if container not in contenedores:
            contenedores.append(container)
    bl.containers = contenedores
    return bl



def validar_casos(bl):

    if bl.request_case == 1:
        if len(bl.containers) > 0 and bl.pod and bl.pol and bl.pod != "Pendiente" and bl.pol != "Pendiente":
            return bl
        
    if bl.request_case == 2:
        if len(bl.containers) > 0 and (bl.pod == "Pendiente" or bl.pol == "Pendiente" or not bl.pod or not bl.pol):
            return bl
        
    if bl.request_case == 3:
        if len(bl.containers) == 0:
            return bl
    
    if bl.request_case == 4:
        if len(bl.containers) == 0:
            return bl
        
    if bl.request_case == 5:
        if len(bl.containers) == 0:
            return bl
    
    if bl.request_case == 6:
        if len(bl.containers) == 0:
            return bl
        
    if bl.request_case == 7:
        if len(bl.containers) == 0:
            return bl
        
    if bl.request_case == 8:
        if bl.revision_manual:
            return bl
        
    bl.request_case = 10

    return bl

# guardar a base de datos recibe una lista de bls cargados y los guarda en la base de datos
def guardar_db(bls):
    for bl in bls:
        #bl = validar_casos(bl)
        bl = eliminar_contenedores_repetidos(bl)
        if len(bl.containers) == 50 and bl.naviera == "HAPAG-LLOYD":
            bl.manual_pendiente = True

        data.add_containers(bl)
        data.add_paradas(bl)
        data.descargar_html(bl)
        data.add_proxima_revision(bl.id, bl.proxima_revision)
        
        caso = bl.request_case
        url = bl.url

        if caso == 1:
            msg = "Exito. Container agregado."
        elif caso == 2:
            msg = "Se descarga containers. Falta pol y/o pod"
        elif caso == 3:
            msg = "BL No encontrado."
        elif caso == 4:
            msg = "Bl sin contenedor asignado (tabla vacía)."
        elif caso == 5:
            msg = "Intento Bloqueado."
        elif caso == 6:
            msg = "BL Cancelado o ya no está disponible."
        elif caso == 7:
            msg = "Formato invalido."
        elif caso == 8:
            msg = "Carga manual."
        elif caso == 9:
            msg = "Error desconocido."
        elif caso == 10:
            msg = "Error en la validacion de casos."
        elif caso == 11:
            msg = "Cambio el formato del HTML"
        else:
            msg = "Request sin caso asignado."

        data.save_request(bl.id, url, 202, caso, msg, tipo=1)
    
    return True
"""
            
"""

# guardar a csv. recibe una lista de bls cargados y los guarda en un archivo csv
def guardar_csv(bls):
    datos = []
    for bl in bls:
        for container in bl.containers:
            datos.append({
                "bl_code": bl.bl_code,
                "pod": bl.pod,
                "pol": bl.pol,
                "container": container.code,
                "type": container.size,
                "size": container.type
            })
    df = pd.DataFrame(datos)
    fecha = datetime.datetime.now().strftime("%Y%m%d%H%M%S").replace(" ", "-").replace(":", "-")
    df.to_csv(f'bls_descargados_{fecha}.csv', index=False)

def descarga_de_bls_csv():
    bls_a_revisar = get_bls_csv('Libro2.csv')
    if not bls_a_revisar:
        logger.info("No hay BLs para revisar")
        return False
    bls = descargar_bls_csv(bls_a_revisar)
    guardar_csv(bls)

def main(
        max_requests_per_ip: int = 10,
        diario: bool = True,
        semana: bool = False,
        mensual: bool = False,
        manual: bool = False,
        navieras: list = None,
        mes: int = None,
        anio: int = None,
        bl_code: list = None,
        state: int = None,
        debug: bool = False):
    
    if 'WANHAI' in navieras:
        navieras.pop(navieras.index('WANHAI'))
        navieras.append('WAN HAI LINES')
    if 'YANGMING' in navieras:
        navieras.pop(navieras.index('YANGMING'))
        navieras.append('YANG MING')

    if navieras is [] or navieras is None:
        navieras = os.getenv('NAVIERA').split(',')

    if debug:
        logger.info("Modo debug")
        max_requests_per_ip = 1

    logger.info(f"Navieras: {navieras}. Chunk: {max_requests_per_ip}. Debug: {debug}.")
    

    estados = []
    if diario:
        logger.info("Iniciando rutina: Modo diario")
        estados = [1,17,3]
    elif semana:
        logger.info("Iniciando rutina: Modo semanal")
        estados = [5,3, 14]
    elif mensual:
        logger.info("Iniciando rutina: Modo mensual")
        estados = [1,3,4,5,6,7,8,9,10,11,14,17]
    elif manual:
        logger.info("Iniciando rutina: Modo manual")
        estados = [99]
    elif state:
        logger.info("Iniciando rutina: Modo estado específico")
        estados = [state]
    
    if bl_code:
        logger.info(f"BLs específicos")
        especifico = True
    else:
        especifico = False


    while True:
        bls = seleccionar_bls(data, navieras, estados, max_requests_per_ip, mes=mes, anio=anio, bl_code=bl_code)
        if len(bls) == 0:
            break
        print_bls = [bl.bl_code for bl in bls]
        logger.info(f"BLs seleccionados:\n{print_bls}")
        bls = scrape(bls)
        casos = [bl.request_case for bl in bls]
        logger.info(casos)
        guardar_db(bls)
        if especifico or debug:
            break

if __name__ == "__main__":
    #descarga_de_bls_csv()

    parser = argparse.ArgumentParser(description="Descripción de tu script")
    parser.add_argument("--diaria", action="store_true", help="Habilitar modo diario")
    parser.add_argument("--semanal", action="store_true", help="Habilitar modo semanal")
    parser.add_argument("--mensual", action="store_true", help="Habilitar modo mensual")
    parser.add_argument("--manual", action="store_true", help="Habilitar modo manual")
    parser.add_argument("--csv", action="store_true", help="Archivo csv con bls a revisar")
    parser.add_argument("--debug", action="store_true", help="Corre solo un ciclo")
    parser.add_argument("--navieras", nargs="*", help="Lista de navieras (separadas por espacios)")
    parser.add_argument("--mes", type=int, help="Mes de los BLs a revisar")
    parser.add_argument("--anio", type=int, help="Año de los BLs a revisar")
    parser.add_argument("--state", type=int, help="BLs con estado específico")
    parser.add_argument("--bls", nargs="*", help="BLs específicos a revisar")

    args = parser.parse_args()

    if args.csv:
        descarga_de_bls_csv()
    else:

        main(
            diario=args.diaria,
            semana=args.semanal,
            mensual=args.mensual,
            manual=args.manual,
            navieras=args.navieras,
            mes=args.mes,
            anio=args.anio,
            bl_code=args.bls,
            state=args.state,
            debug=args.debug
        )

