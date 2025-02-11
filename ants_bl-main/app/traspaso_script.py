import psycopg2
import pyodbc
import pandas as pd
from datetime import datetime, timedelta

from config.settings import DATABASE_URL_TEST, DATABASE_URL
from config.logger import logger

from app.traspaso_consultas import QUERYS


# Conexión a la base de datos de origen (Postgres)
conn_src = psycopg2.connect(DATABASE_URL)

# Conexión a la base de datos de destino (SQL Server)
server = '192.168.74.96'
#database = 'tu_base_de_datos'
username = 'sa'
password = 'brains.2019'
conn_dest = pyodbc.connect(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};UID={username};PWD={password};trustServerCertificate=yes'
)

# Crear cursores
cur_src = conn_src.cursor()

for nombre_naviera, consulta in QUERYS.items():
	print(f"Ejecutando consulta para naviera {nombre_naviera}...")
	cur_src.execute(consulta)
	columnas = [desc[0] for desc in cur_src.description]  # Obtener nombres de columnas
	resultados = cur_src.fetchall()
	
	# eliminar yyyy y mm de columnas
	columnas_id = [x for x in columnas if x not in ['yyyy', 'mm']]
	columnas_sql = ', '.join(columnas_id)

	# Configurar la conexión y fast_executemany
	with conn_dest.cursor() as cursor_dest:
		cursor_dest.fast_executemany = True
		
		# Extraer los datos existentes de la tabla de destino
		cursor_dest.execute(f"SELECT distinct {columnas_sql} FROM BL.dbo.{nombre_naviera} where yyyy > 2023")
		existing_rows = cursor_dest.fetchall()
		import pdb; pdb.set_trace()
		existing_df = pd.DataFrame([tuple(str(element) for element in row) for row in existing_rows], columns=columnas_id)



	# Convertir resultados a DataFrame
	df = pd.DataFrame(resultados, columns=columnas)

	# Identificar registros nuevos que no están en la tabla de destino
	new_df = df[~df[columnas_id].apply(tuple, axis=1).isin(
		existing_df.apply(tuple, axis=1)
	)]
	# Insertar solo los registros nuevos
	if not new_df.empty:
		with conn_dest.cursor() as cursor_dest:
			cursor_dest.fast_executemany = True
			columnas = ', '.join(new_df.columns)
			placeholders = ', '.join(['?'] * len(new_df.columns))
			query = f"INSERT INTO BL.dbo.{nombre_naviera} ({columnas}) VALUES ({placeholders})"
			
			data = [tuple(row) for row in new_df.itertuples(index=False, name=None)]
			cursor_dest.executemany(query, data)
		
		conn_dest.commit()
		print(f"Se insertaron {len(new_df)} registros nuevos.")
	else:
		print("No se encontraron registros nuevos para insertar.")


# Confirmar cambios y cerrar conexiones

cur_src.close()
conn_src.close()
conn_dest.close()







