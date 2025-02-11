# 

from app.database.db import DatabaseManager
from config.settings import DATABASE_URL

import pandas as pd
import io

data = DatabaseManager(DATABASE_URL)
def get_bls_manuales():
    # obtener bls que deben tener una revision manual
    bls = data.get_bls_manuales()

    # generar url para cada bl
    for bl in bls:
        url = data.get_url(bl['bl_code'])
        bl['bl_url'] = url

    return bls

def process_containers(bl_code, containers_string, ip):
    data = DatabaseManager(DATABASE_URL)
    # revisar que el bl exista en la base de datos
    try:
        bl = data.get_bl(bl_code=bl_code)
    except Exception as e:
        raise ValueError(f"Error al buscar el BL {bl_code}: {str(e)}")
    

    if not bl:
        raise ValueError(f"El BL {bl_code} no existe en la base de datos")
    else:
        bl = bl[0]
    print(bl)
    url = data.get_url(bl['bl_code'])

    if bl['manual_pendiente'] == False:
        raise ValueError(f"El BL {bl_code} ya ha sido procesado")

    # revisar el formato de la lista de contenedores
    # Cada columna debe estar separada por un tabulado. Es un contenedor por fila
    try:
        df = check_format(containers_string)
        df["Container No."] = df["Container No."].str.replace(' ', '')
        print(df)
    except Exception as e:
        raise ValueError(f"<b>Error en el formato</b> de la lista de contenedores: {str(e)}")

    # Agregar contenedores
    containers = []
    for index, row in df.iterrows():
        containers.append({
                        'cont_id': str(row['Container No.']).replace(' ', ''),
                        'cont_type': str(row['Type'])[2:],
                        'cont_size': str(row['Type'])[:2],
                        'pod': None,
                        'pol': None,
                    })
    try:
        msg = data.add_containers(containers, bl, manual=True)
        request = data.save_request(bl["id"], url, 202, True, f"Carga manual - IP: {ip}")
    except Exception as e:
        raise ValueError(f"Error al cargar contenedores: {str(e)}")

    #Marcar bl como revisado como manual_pendiente False
    msg_2 = ""
    for i in msg:
        msg_2 += i + "<br>"

    return msg_2

def check_format(containers_string):
    # Revisar que la lista de contenedores tenga el formato correcto
    # pasar string a df pandas
    # Verifica que cada fila tenga exactamente 5 columnas
    rows = containers_string.split('\n')
    correct_format = all(len(row.split('\t')) == 5 for row in rows)
    if not correct_format:
        raise ValueError("Todas las filas deben tener 5 columnas separadas por tabulaciones")
    else:
        df = pd.read_csv(io.StringIO(containers_string), sep='\t', names=["Type", "Container No.", "Status", "Date", "Place of Activity"])
        return df
    

def main():
    # leer csv con contenedores de la version anterior
    df = pd.read_csv("./app/contenedores.csv")

    # lista de bls unique
    bls = df['bl_code'].unique()

    # Revisar si bl del csv existe en la base de datos
    for bl_code in bls:
        
 
        bl = data.get_bl(bl_code=bl_code)

        if not bl:
            print(f"El BL {bl_code} no existe en la base de datos")
            continue
        else:
            bl = bl[0]

        # Obtener el id del bl del contenedor
        bl_id = bl['id']

        # Lista a de contenedoires del bl bl_code
        containers = df[df['bl_code'] == bl_code]

        cont = []
        for index, row in containers.iterrows():
            cont.append({
                'cont_id': row['cont_id'],
                'cont_type': row['cont_type'],
                'cont_size': row['cont_size'],
                'pod': row['pod'],
                'pol': row['pol'],
            })
        # import pdb; pdb.set_trace()
        data.add_containers(cont, bl)

        # crear una request por cada bl leid
        
        data.save_request(bl_id, "Version anterior", None, True, "Carga manual")


    # Si no existe, continue


    # Obtener el id del bl del contenedor

    # Agregar contenedores a la base de datos

    # marcar bl revisado con exito

    # crear una request por cada bl leido
