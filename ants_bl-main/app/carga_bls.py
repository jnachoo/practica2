
import pandas as pd

import app.database.db as db
from config.settings import DATABASE_URL_TEST, DATABASE_URL

import dotenv
import os

import pyodbc
data = db.DatabaseManager(DATABASE_URL)

import datetime

dotenv.load_dotenv()

"""

# Nombre del archivo CSV con la carga
archivo_csv = 'bls_pendiente.csv'  # Reemplaza 'ruta/del/archivo/carga.csv' con la ruta real de tu archivo CSV

# Leer el archivo CSV en un DataFrame de Pandas
try:
    carga_df = pd.read_csv(archivo_csv)
except FileNotFoundError:
    print("El archivo CSV no se encontró en la ruta especificada.")
    exit()

# Imprimir el DataFrame cargado desde el archivo CSV
print(carga_df.head())

data = db.DatabaseManager(DATABASE_URL)
data.add_bls(carga_df)
"""

def get_bls_nuevos():
    # ver bl mas reciente de pg
    
    navieras = os.getenv('NAVIERA').split(',')
    #import pdb; pdb.set_trace()
    if len(navieras) == 0:
        return False

    first_where = f"'{navieras[0]}'"
    for i, naviera in enumerate(navieras[1:]):
        first_where += f", '{navieras[i+1]}'"

    print(first_where)

    #ultima_carga = data.get_ultima_carga()

    query = f"""
                select 
                    T0.BL as "bl_code", 
                    T1.navieraGrupoNombre AS "nombre_naviera", 
                    T0.fecha AS "fecha_bl", 
                    T0.etapaId as "etapa", 
                    T2.naveNombre as "nave"
                from SEMPAT.dbo.Isidora T0
                left join MERCADO.dbo.MaeNavierasGrupo T1 ON T0.navieraGrupoId = T1.navieraGrupoId
                left join MERCADO.dbo.MaeNaves T2 on T2.naveId = T0.naveId 
                WHERE T1.navieraGrupoNombre in ({first_where})
                AND T0.fecha >= '2024-01-01'
            """#+first_where
    
    #import pdb; pdb.set_trace()


    return query

def consultar_base_datos(server, username, password, query):
    # Establecer la conexión con la base de datos
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';UID='+username+';PWD='+ password+';trustServerCertificate=yes')
    # Ejecutar la consulta SQL
    df = pd.read_sql(query, conn)

    # Cerrar la conexión
    conn.close()

    return df

def consultar_csv(archivo_csv):
    # Leer el archivo CSV en un DataFrame de Pandas
    try:
        df = pd.read_csv(archivo_csv, sep=';')
    except FileNotFoundError:
        print("El archivo CSV no se encontró en la ruta especificada.")
        return None

    return df

def validar_formato_dataframe(df):
    # Verificar que el DataFrame tenga las columnas requeridas
    required_columns = ['bl_code', 'nombre_naviera', 'fecha_bl']
    if all(col in df.columns for col in required_columns):
        return True
    else:
        return False
    
def cargar_bls_csv(archivo_csv):
    # Leer el archivo CSV en un DataFrame de Pandas
    resultado_df = consultar_csv(archivo_csv)
    if resultado_df is None:
        return
    print(resultado_df.head())
    # cargar bls a pg
    if len(resultado_df) > 0:
        print("cargando bls")
        inicio = datetime.datetime.now()
        new_bls = data.add_bls(resultado_df)
        fin = datetime.datetime.now()
        print(f"Se han cargado {len(new_bls)} nuevos BLs en la base de datos. Tiempo: {fin-inicio}")
    else:
        print("No se encontraron nuevos BLs para cargar.")
    
def cargar_nuevos_bls():
    # generar query de bls nuevos en isidora.
    query = get_bls_nuevos()

    # ejecutar query
    inicio = datetime.datetime.now()
    resultado_df = consultar_base_datos(server, username, password, query)
    fin = datetime.datetime.now()

    print(f"query leida: {len(resultado_df)}. Tiempo: {fin-inicio}")

    # cargar bls a pg
    if len(resultado_df) > 0:
        print("cargando bls")
        inicio = datetime.datetime.now()
        new_bls = data.add_bls(resultado_df)
        fin = datetime.datetime.now()
        print(f"Se han cargado {len(new_bls)} nuevos BLs en la base de datos. Tiempo: {fin-inicio}")
    else:
        print("No se encontraron nuevos BLs para cargar.")


# Parámetros de conexión a la base de datos
server = '192.168.74.96'
#database = 'tu_base_de_datos'
username = 'sa'
password = 'brains.2019'

# Consulta SQL
query = """
select T0.BL as "bl_code", T1.navieraGrupoNombre AS "nombre_naviera",  T0.fecha AS "fecha_bl", T0.etapaId as "etapa", T2.naveNombre as "nave"
	from SEMPAT.dbo.Isidora T0
	left join MERCADO.dbo.MaeNavierasGrupo T1 ON T0.navieraGrupoId = T1.navieraGrupoId
	left join MERCADO.dbo.MaeNaves T2 on T2.naveId = T0.naveId 
	WHERE T1.navieraGrupoNombre in ('MAERSK')
	AND T0.fecha >= '2024-03-01' and T0.fecha < '2024-04-01'
"""

def main():
    cargar_nuevos_bls()
#cargar_bls_csv('bls_hapag_Peru_2024_04.csv')

if __name__ == '__main__':
    main()