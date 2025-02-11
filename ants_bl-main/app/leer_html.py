import logging
from app.browser.seleniumdriver import SeleniumWebDriver

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
from app.agents.oocl import AgenteOOCL
from app.agents.wanhai import AgenteWanHai
from app.agents.pil_zen import AgentePIL
from app.agents.hmm_zen import AgenteHMM
from app.agents.yangming_zen import AgenteYangMing


from database.clases import BL, Container

from config.logger import logger

import random
import dotenv
import os
import glob
import argparse

import pandas as pd

dotenv.load_dotenv()

data = DatabaseManager(DATABASE_URL)
driver = None

def buscar_bl(bl_code, naviera):
    databl = data.get_bl(bl_code=bl_code, naviera=naviera)
    if len(databl) == 0:
        return None
    i = data.get_bl(bl_code=bl_code, naviera=naviera)[0]
    bl = BL(id= i['id'], bl_code=i['bl_code'], naviera=i['nombre_naviera'], fecha_bl=i['fecha_bl'], etapa=i['etapa'], estado=i['estado'])

    containers = data.get_containers(bl.id)
    for j in containers:
        container = Container(code=j['code'], size=j['size'], type=j['type'], pol=j['pol'], pod=j['pod'], bl_id=j['bl_id'], peso_kg=j['peso_kg'], service=j['service'])
        bl.containers.append(container)
    return bl

def buscar_html_hapag():
    # Recorrer archivos de directorio
    files = os.listdir('html_hapag')
    cant = len(files)
    for i,file in enumerate(files):
        bl_code = file.split('.')[0].split('_')[2]
        #print(f"Buscando BL: {bl_code}")
        # Buscar BL en la base de datos
        bl = buscar_bl(bl_code, 'HAPAG-LLOYD')
        if not bl:
            print(f"BL {bl_code} no encontrado")
            continue
        bl.url = file
        # Si esta en estado 2 (toda la informacion cargada) no hacer nada
        #if bl.estado == 2:
            #print("\rbl revisado previamente")
            #continue
        # Leer archivo
        with open(f'html_hapag/{file}', 'r') as f:
            html = f.read()
        # Leer datos de html
        agente = AgenteHapagZen(None, data)
        bl = agente.read_html(html, bl)
        # Guardar datos en la base de datos
        guardar_bl(bl)
        print(f"\r n:{i}, pendientes:{cant-i} BL: {bl.bl_code} revisado",  end="")

def buscar_html_cma():
    # Recorrer archivos de directorio
    files = os.listdir('html_cma')
    cant = len(files)
    for i,file in enumerate(files[1000:]):
        bl_code = file.split('.')[0].split('_')[2]
        #print(f"Buscando BL: {bl_code}")
        # Buscar BL en la base de datos
        bl = buscar_bl(bl_code, 'CMA-CGM')
        if not bl:
            print(f"BL {bl_code} no encontrado")
            continue
        bl.url = file
        # Si esta en estado 2 (toda la informacion cargada) no hacer nada
        #if bl.estado == 2:
            #print("\rbl revisado previamente")
            #continue
        # Leer archivo
        with open(f'html_cma/{file}', 'r') as f:
            try:
                html = f.read()
            except UnicodeDecodeError:
                print(f"Error en el archivo {file}")
                continue
        # Leer datos de html
        agente = AgenteCMA(None, data)
        bl = agente.read_html(html, bl)
        # Guardar datos en la base de datos
        guardar_bl(bl)
        print(f"\r n:{i}, pendientes:{cant-i} BL: {bl.bl_code} revisado",  end="")

def buscar_html_maersk():
    # Recorrer archivos de directorio
    files = os.listdir('html_maersk')
    cant = len(files)
    for i,file in enumerate(files[300:]):
        bl_code = file.split('.')[0].split('_')[2]
        #print(f"Buscando BL: {bl_code}")
        # Buscar BL en la base de datos
        bl = buscar_bl(bl_code, 'MAERSK')
        if not bl:
            print(f"BL {bl_code} no encontrado")
            continue
        bl.url = file
        # Si esta en estado 2 (toda la informacion cargada) no hacer nada
        #if bl.estado == 2:
            #print("\rbl revisado previamente")
            #continue
        # Leer archivo
        with open(f'html_maersk/{file}', 'r') as f:
            try:
                html = f.read()
            except UnicodeDecodeError:
                print(f"Error en el archivo {file}")
                continue
        # Leer datos de html
        agente = AgenteMaerskZen(None, data)
        bl = agente.read_html(html, bl)
        # Guardar datos en la base de datos
        guardar_bl(bl)
        print(f"\r n:{i}, pendientes:{cant-i} BL: {bl.bl_code} revisado",  end="")
        
def guardar_bl(bl):
    data.update_bl(bl)
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
        msg = "BL Cancelado."
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

    data.save_request(bl.id, url, 202, caso, msg, tipo=3)

def actualizar_bd():
    # Obtener bl_ciode de todos los archivos
    files = os.listdir('html_hapag')
    cant = len(files)
    bl_codes = [file.split('.')[0].split('_')[2] for file in files]

    # Obtener bl_code de la base de 
    for i, bl_code in enumerate(bl_codes):
        bl = buscar_bl(bl_code, 'HAPAG-LLOYD')
        if bl:
            data.descargar_html(bl)
        print(f"\r n:{i}, pendientes:{cant-i} BL: {bl.bl_code} revisado",  end="")

#buscar_html_maersk()



def leer_bl(bl):
    bls_descargados = []
    if bl.naviera == "HAPAG-LLOYD":
        agente = AgenteHapagZen(driver, data)  #   LISTO
    elif bl.naviera == "MAERSK":
        agente = AgenteMaerskZen(driver, data)  # listo
    elif bl.naviera == "MSC":
        agente = AgenteMSC(driver, data)        # listo
    elif bl.naviera == "CMA-CGM":
        agente = AgenteCMA(driver, data)        # listo
    elif bl.naviera == "ONE":
        agente = AgenteONE(driver, data)        # listo
    elif bl.naviera == "COSCO":
        agente = AgenteCOSCO(driver, data)      # TODO
    elif bl.naviera == "EVERGREEN":
        agente = AgenteEVERGREEN(driver, data)  # TODO
    elif bl.naviera == "ZIM":
        agente = AgenteZIM(driver, data)        # TODO
    elif bl.naviera == "OOCL":
        agente = AgenteOOCL(driver, data)        # TODO
    elif bl.naviera == "WAN HAI LINES":
        agente = AgenteWanHai(driver, data)        
    elif bl.naviera == "PIL":
        agente = AgentePIL(driver, data)
    elif bl.naviera == "HMM":
        agente = AgenteHMM(driver, data)
    elif bl.naviera == "YANG MING":
        agente = AgenteYangMing(driver, data)
    else:
        logger.error("Naviera no encontrada")
        return False
    
    nombre_archivo = data.buscar_archivo_a_leer(bl)
    
    archivos = agente.leer_html(bl, nombre_archivo)

    return archivos

def get_bls_a_leer(naviera, mes=None, anio=None, dia=None, bl_code=None, state=None):
    if bl_code:
        bls_a_revisar = data.get_bl(bl_code=bl_code, naviera=naviera)
    else:
        bls_a_revisar = data.get_bls_a_leer(naviera=naviera, random=True, month=mes, year=anio, day=dia, state=state)
    bls = []
    for i in bls_a_revisar:
        bl = BL(id= i['id'], bl_code=i['bl_code'], naviera=i['nombre_naviera'], fecha_bl=i['fecha_bl'], etapa=i['etapa'], estado=i['estado'])
        bls.append(bl)

    return bls

def guardar_db(bl):
    data.add_paradas(bl)
    data.add_containers(bl)

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
        msg = "BL Cancelado."
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

    data.save_request(bl.id, f"HTML descargado: {bl.url}", 202, bl.request_case, msg, tipo=3)


def main(navieras: list = None, debug: bool = False, dia: int = None, mes: int = None, anio: int = None, bl_code: list = None, state: int = None):

    if navieras is None:
        navieras = os.getenv('NAVIERAS').split(',')

    if 'WANHAI' in navieras:
        navieras.remove('WANHAI')
        navieras.append('WAN HAI LINES')
    if 'YANGMING' in navieras:
        navieras.pop(navieras.index('YANGMING'))
        navieras.append('YANG MING')

    logger.info(f"Configuración: {navieras}.")

    if bl_code:
        logger.info(f"BLs específicos")
        especifico = True
    else:
        especifico = False

    # Lista de archivos ya leidos: ver requests de tipo 3
    while True:
        naviera = random.choice(navieras)
        bl = get_bls_a_leer(naviera, mes=mes, anio=anio, dia=dia, bl_code=bl_code, state=state)
        print(bl)
        if len(bl) == 0:
            break
        for b in bl:
            bl_info = leer_bl(b)
            guardar_db(bl_info)
        if debug or especifico:
            break
        #bl_info = leer_bl(bl)
        #guardar_db(bl_info)


if __name__ == "__main__":
    #descarga_de_bls_csv()

    parser = argparse.ArgumentParser(description="Descripción de tu script")
    parser.add_argument("--navieras", nargs="*", help="Lista de navieras (separadas por espacios)")
    parser.add_argument("--debug", action="store_true", help="Modo debug")
    parser.add_argument("--dia", type=int, help="dia de los BLs a revisar")
    parser.add_argument("--mes", type=int, help="Mes de los BLs a revisar")
    parser.add_argument("--anio", type=int, help="Año de los BLs a revisar")
    parser.add_argument("--bls", nargs="*", help="BLs específicos a revisar")
    parser.add_argument("--state",type=int, help="Estado del bl")



    args = parser.parse_args()

    main(
        navieras=args.navieras,
        debug=args.debug,
        dia=args.dia,
        mes=args.mes,
        anio=args.anio,
        bl_code=args.bls,
        state=args.state
    )
