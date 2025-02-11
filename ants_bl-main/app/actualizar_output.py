import psycopg2
from psycopg2 import sql
import sys

from config.settings import DATABASE_URL
from config.logger import logger


def refresh_materialized_view():
    try:
        # Conecta a la base de datos usando la cadena de conexión URI
        connection = psycopg2.connect(DATABASE_URL)
        
        # Crea un cursor
        cursor = connection.cursor()

                # Corrección de LOCODE
        logger.info("Corrigiendo LOCODEs")
        cursor.execute("update paradas set locode = locode_correcto from locode_corecto where locode = locode_leido")
        connection.commit()

        # Inicializar la variable para almacenar el total de paradas agregadas
        total_paradas_agregadas = 0

        # Agregar paradas identificadas por código BL y acumular el total
        logger.info("Agregando paradas identificadas por código BL")

        paradas_agregadas = cursor.execute("select insert_paradas('ONEYPLBE%','ORURO', 'BOORU', 'BOLIVIA', 'Agregada por codigo BL')")
        total_paradas_agregadas += cursor.fetchone()[0]
        connection.commit()

        paradas_agregadas = cursor.execute("select insert_paradas('ONEYPLBD%','SANTA CRUZ', 'BOSRZ', 'BOLIVIA', 'Agregada por codigo BL')")
        total_paradas_agregadas += cursor.fetchone()[0]
        connection.commit()

        paradas_agregadas = cursor.execute("select insert_paradas('ONEYBUEE%','BUENOS AIRES', 'ARBUE', 'ARGENTINA', 'Agregada por codigo BL')")
        total_paradas_agregadas += cursor.fetchone()[0]
        connection.commit()

        paradas_agregadas = cursor.execute("select insert_paradas('ONEYBUED%','MENDOZA', 'ARMDZ', 'ARGENTINA', 'Agregada por codigo BL')")
        total_paradas_agregadas += cursor.fetchone()[0]
        connection.commit()

        paradas_agregadas = cursor.execute("select insert_paradas('HLCUSRZ%','SANTA CRUZ', 'BOSRZ', 'BOLIVIA', 'Agregada por codigo BL')")
        total_paradas_agregadas += cursor.fetchone()[0]
        connection.commit()

        paradas_agregadas = cursor.execute("select insert_paradas('MEDUBV%','ORURO', 'BOORU', 'BOLIVIA', 'Agregada por codigo BL')")
        total_paradas_agregadas += cursor.fetchone()[0]
        connection.commit()

        paradas_agregadas = cursor.execute("select insert_paradas('MEDUX8%','LA PAZ', 'BOLPB', 'BOLIVIA', 'Agregada por codigo BL')")
        total_paradas_agregadas += cursor.fetchone()[0]
        connection.commit()

        paradas_agregadas = cursor.execute("select insert_paradas('CBV%','LA PAZ', 'BOLPB', 'BOLIVIA', 'Agregada por codigo BL')")
        total_paradas_agregadas += cursor.fetchone()[0]
        connection.commit()

        # Confirmar la transacción
        connection.commit()

        # Registrar el total de paradas agregadas
        logger.info(f"Total de paradas agregadas: {total_paradas_agregadas}")

        # Eliminar contenedoires repetidos
        logger.info("Eliminando contenedores repetidos")
        con_repetidos = cursor.execute("select update_duplicate_containers()")
        cantidad_contenedores_repetidos = cursor.fetchone()[0]
        connection.commit()
        logger.info(f"Total de contenedores eliminados: {cantidad_contenedores_repetidos}")
        
        # Ejecuta el comando REFRESH MATERIALIZED VIEW
        logger.info("Refreshing materialized view output_containers")
        cursor.execute(sql.SQL("REFRESH MATERIALIZED VIEW {}").format(sql.Identifier('output_containers')))

        
        # Confirma la transacción
        connection.commit()

        # Cierra el cursor y la conexión
        cursor.close()
        connection.close()

        logger.info("Materialized view output_containers refreshed successfully.")
    
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error while refreshing materialized view: {error}")

if __name__ == '__main__':
     refresh_materialized_view()