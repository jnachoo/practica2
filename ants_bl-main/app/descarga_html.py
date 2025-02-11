
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

from database.clases import BL, Container

import random
import dotenv
import os
import asyncio

import argparse

import pandas as pd

dotenv.load_dotenv()

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


def seleccionar_bls(data, navieras, limit=10, mes=None, anio=None, dia=None):
    bls_a_revisar = []
    naviera = random.choice(navieras)
    
    while len(bls_a_revisar) == 0:
        bls_a_revisar = get_bls_db(naviera, limit=limit, mes=mes, anio=anio, dia=dia)
        if len(bls_a_revisar) == 0:
            navieras.pop(navieras.index(naviera))
            if len(navieras) == 0:
                return []
            naviera = random.choice(navieras)

    return bls_a_revisar

def get_bls_db(naviera, limit=10, mes=None, anio=None, dia=None):
    bls_a_revisar = data.get_bls_sin_html(naviera=naviera, limit=limit, month=mes, year=anio, day=dia)
    bls = []
    for i in bls_a_revisar:
        bl = BL(id= i['id'], bl_code=i['bl_code'], naviera=i['nombre_naviera'], fecha_bl=i['fecha_bl'], etapa=i['etapa'], estado=i['estado'])
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
    else:
        logger.error("Naviera no encontrada")
        return False
    
    bls_descargados = agente.descargar_html(bls)

    return bls_descargados


# guardar a base de datos recibe una lista de bls cargados y los guarda en la base de datos
def guardar_db(bls):
    for bl in bls:
        
        if not bl.html_descargado:
            caso = bl.request_case
        else:
            caso = 10

        url = bl.url

        data.save_request(bl.id, url, 202, caso, "Descarga de HTML.", tipo=2)
        if bl.html_descargado:
            data.descargar_html(bl)
    
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
    df.to_csv('bls_descargados.csv', index=False)

def descarga_de_bls_csv():
    bls_a_revisar = get_bls_csv('bls_localidades_pendientes_2.csv')
    if not bls_a_revisar:
        logger.info("No hay BLs para revisar")
        return False
    bls = descargar_bls_csv(bls_a_revisar)
    guardar_csv(bls)

def main(
        max_requests_per_ip: int = 10,
        navieras: list = None,
        dia=None,
        mes=None,
        anio=None,
        debug: bool = False
        ):

    if navieras is None:
        navieras = os.getenv('NAVIERAS').split(',')

    logger.info(f"Configuración: {navieras}. Chunk: {max_requests_per_ip}.")

    while True:
        bls = seleccionar_bls(data, navieras, max_requests_per_ip, mes, anio)
        if len(bls) == 0:
            break
        print_bls = [bl.bl_code for bl in bls]
        print(print_bls)
        bls = scrape(bls)
        guardar_db(bls)
        if debug:
            break

if __name__ == "__main__":
    #descarga_de_bls_csv()

    parser = argparse.ArgumentParser(description="Descripción de tu script")
    parser.add_argument("--navieras", nargs="*", help="Lista de navieras (separadas por espacios)")
    parser.add_argument("--dia", type=int, help="dia de los BLs a revisar")
    parser.add_argument("--mes", type=int, help="Mes de los BLs a revisar")
    parser.add_argument("--anio", type=int, help="Año de los BLs a revisar")


    args = parser.parse_args()

    main(
        navieras=args.navieras,
        dia=args.dia,
        mes=args.mes,
        anio=args.anio
    )

