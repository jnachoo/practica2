# TODO: CREAR TABLA CON METADATOS DE DESCARGA

# TODO: CREAR FUNCION QUE RECORRA DIRECTORIO Y AGREGUE A LA TABLA LA INFO DE LOS ARCHIVOS
def get_files():
    pass


# TODO: AGREGAR ID DEL ARCHIVO DESCARGADO Y LEIDO EN TABLA REQUESTS

# TODO: AGREGAR ESTADO DE REQUEST Y BL CON ESTA INFORMACION NUEVA

import os
from datetime import datetime
import boto3

from config.settings import DATABASE_URL_TEST, DATABASE_URL
from config.logger import logger

from app.database.db import DatabaseManager

import argparse

data = DatabaseManager(DATABASE_URL)
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)
bucket_name = "ants-bl"

def generar_dict_bl(ruta, bl_id=None):
    nombre_archivo = os.path.basename(ruta)
    ruta_directorio = os.path.dirname(ruta)
    ruta_base = ruta.split('/')[0]
    ruta_completa = os.path.join(ruta_directorio, nombre_archivo)
    #print(ruta_completa, ruta_base, ruta_directorio, nombre_archivo)
    if 'hapag' in ruta_base:
        if 'pagina' in nombre_archivo:
            bl_code = nombre_archivo.split('_')[2]
        elif 'container' in nombre_archivo:
            bl_code = nombre_archivo.split('_')[1]
        else:
            bl_code = nombre_archivo.split('_')[0]
        if '.' in bl_code:
                bl_code = bl_code.split('.')[0]

        if 'paradas' in ruta_directorio:
            info = 2
        elif 'contenedores' in ruta_directorio:
            info = 1
        else:
            info = 0
        if 'zenrows' in nombre_archivo:
            info = 0
        
            
    elif 'cma' in ruta_base:
        if 'pagina' in nombre_archivo:
            bl_code = nombre_archivo.split('_')[2]
        else:
            bl_code = nombre_archivo.split('_')[0]

        if '.' in bl_code:
                bl_code = bl_code.split('.')[0]

        info = 4
        if 'zenrows' in nombre_archivo:
            info = 0
    elif 'msc' in ruta_base:
        if 'pagina' in nombre_archivo:
            bl_code = nombre_archivo.split('_')[2]
        else:
            bl_code = nombre_archivo.split('_')[0]
        
        if '.' in bl_code:
            bl_code = bl_code.split('.')[0]
        info = 4
        if 'zenrows' in nombre_archivo:
            info = 4
        
    elif 'maersk' in ruta_base:
        if 'pagina' in nombre_archivo:
            bl_code = nombre_archivo.split('_')[2]
        else:
            bl_code = nombre_archivo.split('_')[0]
        
        if '.' in bl_code:
            bl_code = bl_code.split('.')[0]
        info = 4
        if 'zenrows' in nombre_archivo:
            info = 4
    elif 'one' in ruta_base:
        if 'pagina' in nombre_archivo:
            bl_code = nombre_archivo.split('_')[2]
        if 'container' in nombre_archivo:
            bl_code = nombre_archivo.split('_')[1]
        else:
            bl_code = nombre_archivo.split('_')[0]
        
        if '.' in bl_code:
            bl_code = bl_code.split('.')[0]
        if 'paradas' in ruta_directorio:
            info = 2
        elif 'container' in ruta_directorio or 'pagina' in nombre_archivo:
            info = 1
        elif 'puertos' in ruta_directorio or 'container' in nombre_archivo:
            info = 3
        if 'zenrows' in nombre_archivo:
            info = 0
    elif 'cosco' in ruta_base:
        if 'pagina' in nombre_archivo:
            bl_code = nombre_archivo.split('_')[2]
        if 'container' in nombre_archivo:
            bl_code = nombre_archivo.split('_')[1]
        else:
            bl_code = nombre_archivo.split('_')[0]
        
        if '.' in bl_code:
            bl_code = bl_code.split('.')[0]
        if 'paradas' in ruta_directorio or 'container' in nombre_archivo:
            info = 2
        elif 'container' in ruta_directorio or 'pagina' in nombre_archivo:
            info = 1
        if 'zenrows' in nombre_archivo:
            info = 0
        pass
    elif 'evergreen' in ruta_base:
        if 'pagina' in nombre_archivo:
            bl_code = nombre_archivo.split('_')[2]
        else:
            bl_code = nombre_archivo.split('_')[0]
        
        if '.' in bl_code:
            bl_code = bl_code.split('.')[0]
        info = 4
        if 'zenrows' in nombre_archivo:
            info = 0
        pass
    elif 'zim' in ruta_base:
        bl_code = nombre_archivo.split('_')[0]

        if '.' in bl_code:
            bl_code = bl_code.split('.')[0]
        info = 4
        if 'zenrows' in nombre_archivo:
            info = 0
    elif 'wanhai' in ruta_base:
        bl_code = nombre_archivo.split('_')[0]
        
        if '.' in bl_code:
            bl_code = bl_code.split('.')[0]
        info = 4
        if 'zenrows' in nombre_archivo:
            info = 0
    elif 'hmm' in ruta_base:
        bl_code = nombre_archivo.split('_')[0]
        
        if '.' in bl_code:
            bl_code = bl_code.split('.')[0]
        info = 4
        if 'zenrows' in nombre_archivo:
            info = 0
    elif 'pil' in ruta_base:
        bl_code = nombre_archivo.split('_')[0]
        
        if '.' in bl_code:
            bl_code = bl_code.split('.')[0]
        info = 4
        if 'zenrows' in nombre_archivo:
            info = 0
    elif 'yangming' in ruta_base:
        bl_code = nombre_archivo.split('_')[0]
        
        if '.' in bl_code:
            bl_code = bl_code.split('.')[0]
        info = 4
        if 'zenrows' in nombre_archivo:
            info = 0
    else:
        print('No se reconoce la naviera')
    
    if bl_id is None:
        bl = data.get_bl(bl_code=bl_code)
        if len(bl) > 0:
            bl_id = data.get_bl(bl_code=bl_code)[0]['id']
        else:
            bl_id = None
        
    ruta_completa = os.path.join(ruta_directorio, nombre_archivo)
    ruta_relativa = os.path.relpath(ruta_completa, ruta_base)
    extension = os.path.splitext(nombre_archivo)[1][1:]
    # fecha en string
    response = s3_client.head_object(Bucket=bucket_name, Key=ruta)
    fecha_descarga = response['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
    # Fecha de creación del archivo
    tipo_archivo = 1 if extension == 'html' else 2 if extension == 'json' else 0  # Ejemplo simple para tipo_archivo
    dict_archivo = {
        'ruta_full': ruta_completa,
        'nombre': nombre_archivo,
        'ruta_s3': ruta,
        'info': info,
        'fecha_descarga': fecha_descarga,
        'ruta_relativa': ruta_relativa,
        'bl_id': bl_id,
        'tipo_archivo': tipo_archivo
    }
    return dict_archivo

def obtener_archivos(ruta_base):
    archivos = []
    for ruta_directorio, _, nombres_archivos in os.walk(ruta_base):
        for nombre_archivo in nombres_archivos:
            ruta_completa = os.path.join(ruta_directorio, nombre_archivo)
            if 'hapag' in ruta_base:
                if 'pagina' in nombre_archivo:
                    bl_code = nombre_archivo.split('_')[2]
                elif 'container' in nombre_archivo:
                    bl_code = nombre_archivo.split('_')[1]
                else:
                    bl_code = nombre_archivo.split('_')[0]
                if '.' in bl_code:
                        bl_code = bl_code.split('.')[0]

                if 'paradas' in ruta_directorio:
                    info = 2
                elif 'contenedores' in ruta_directorio:
                    info = 1
                else:
                    info = 0
                if 'zenrows' in nombre_archivo:
                    info = 0
                
                    
            elif 'cma' in ruta_base:
                if 'pagina' in nombre_archivo:
                    bl_code = nombre_archivo.split('_')[2]
                else:
                    bl_code = nombre_archivo.split('_')[0]

                if '.' in bl_code:
                        bl_code = bl_code.split('.')[0]

                info = 4
                if 'zenrows' in nombre_archivo:
                    info = 0
            elif 'msc' in ruta_base:
                if 'pagina' in nombre_archivo:
                    bl_code = nombre_archivo.split('_')[2]
                else:
                    bl_code = nombre_archivo.split('_')[0]
                
                if '.' in bl_code:
                    bl_code = bl_code.split('.')[0]
                info = 4
                if 'zenrows' in nombre_archivo:
                    info = 4
                
            elif 'maersk' in ruta_base:
                if 'pagina' in nombre_archivo:
                    bl_code = nombre_archivo.split('_')[2]
                else:
                    bl_code = nombre_archivo.split('_')[0]
                
                if '.' in bl_code:
                    bl_code = bl_code.split('.')[0]
                info = 4
                if 'zenrows' in nombre_archivo:
                    info = 4
            elif 'one' in ruta_base:
                if 'pagina' in nombre_archivo:
                    bl_code = nombre_archivo.split('_')[2]
                if 'container' in nombre_archivo:
                    bl_code = nombre_archivo.split('_')[1]
                else:
                    bl_code = nombre_archivo.split('_')[0]
                
                if '.' in bl_code:
                    bl_code = bl_code.split('.')[0]
                if 'paradas' in ruta_directorio:
                    info = 2
                elif 'container' in ruta_directorio or 'pagina' in nombre_archivo:
                    info = 1
                elif 'puertos' in ruta_directorio or 'container' in nombre_archivo:
                    info = 3
                if 'zenrows' in nombre_archivo:
                    info = 0
            elif 'cosco' in ruta_base:
                if 'pagina' in nombre_archivo:
                    bl_code = nombre_archivo.split('_')[2]
                if 'container' in nombre_archivo:
                    bl_code = nombre_archivo.split('_')[1]
                else:
                    bl_code = nombre_archivo.split('_')[0]
                
                if '.' in bl_code:
                    bl_code = bl_code.split('.')[0]
                if 'paradas' in ruta_directorio or 'container' in nombre_archivo:
                    info = 2
                elif 'container' in ruta_directorio or 'pagina' in nombre_archivo:
                    info = 1
                if 'zenrows' in nombre_archivo:
                    info = 0
                pass
            elif 'evergreen' in ruta_base:
                if 'pagina' in nombre_archivo:
                    bl_code = nombre_archivo.split('_')[2]
                else:
                    bl_code = nombre_archivo.split('_')[0]
                
                if '.' in bl_code:
                    bl_code = bl_code.split('.')[0]
                info = 4
                if 'zenrows' in nombre_archivo:
                    info = 0
                pass
            elif 'zim' in ruta_base:
                bl_code = nombre_archivo.split('_')[0]

                if '.' in bl_code:
                    bl_code = bl_code.split('.')[0]
                info = 4
                if 'zenrows' in nombre_archivo:
                    info = 0
            elif 'wanhai' in ruta_base:
                bl_code = nombre_archivo.split('_')[0]
                
                if '.' in bl_code:
                    bl_code = bl_code.split('.')[0]
                info = 4
                if 'zenrows' in nombre_archivo:
                    info = 0
            elif 'hmm' in ruta_base:
                bl_code = nombre_archivo.split('_')[0]
                
                if '.' in bl_code:
                    bl_code = bl_code.split('.')[0]
                info = 4
                if 'zenrows' in nombre_archivo:
                    info = 0
            elif 'pil' in ruta_base:
                bl_code = nombre_archivo.split('_')[0]
                
                if '.' in bl_code:
                    bl_code = bl_code.split('.')[0]
                info = 4
                if 'zenrows' in nombre_archivo:
                    info = 0
            elif 'yangming' in ruta_base:
                bl_code = nombre_archivo.split('_')[0]
                
                if '.' in bl_code:
                    bl_code = bl_code.split('.')[0]
                info = 4
                if 'zenrows' in nombre_archivo:
                    info = 0
            else:
                print('No se reconoce la naviera')
            
            bl = data.get_bl(bl_code=bl_code)
            if len(bl) > 0:
                bl_id = data.get_bl(bl_code=bl_code)[0]['id']
            else:
                bl_id = None
            
            ruta_completa = os.path.join(ruta_directorio, nombre_archivo)
            ruta_relativa = os.path.relpath(ruta_completa, ruta_base)
            extension = os.path.splitext(nombre_archivo)[1][1:]
            # fecha en string
            fecha_descarga = datetime.fromtimestamp(os.path.getctime(ruta_completa)).strftime('%Y-%m-%d %H:%M:%S')
            # Fecha de creación del archivo
            tipo_archivo = 1 if extension == 'html' else 2 if extension == 'json' else 0  # Ejemplo simple para tipo_archivo

            archivos.append({
                'ruta_full': ruta_completa,
                'nombre': nombre_archivo,
                'ruta_s3': None,  # Ajusta si tienes esta información
                'info': info,
                'fecha_descarga': fecha_descarga,
                'ruta_relativa': ruta_relativa,
                'bl_id': bl_id,
                'tipo_archivo': tipo_archivo
            })
            print(f"leidos {len(archivos)} de {len(nombres_archivos)} archivos en {ruta_directorio}", end="\r")
            if len(archivos) >= 100:
                #break
                pass
    print(f"Se leyeron {len(archivos)} archivos en {ruta_base}")
    return archivos

# Ejemplo de uso
def main(ruta_base=None):
    # Directorio base donde se encuentran los archivos
    if not ruta_base:
        ruta_base = 'html_cma'
    archivos = obtener_archivos(ruta_base)
    data.add_html_descargados_batch(archivos)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Descripción de tu script")
    parser.add_argument('--ruta', type=str, help='Ruta base de los archivos a leer')
    args = parser.parse_args()
    main(args.ruta)


"""

# Directorio donde se encuentran los archivos .json
directorio = 'html_hapag'

# Crear la subcarpeta 'contenedores' si no existe
subcarpeta = os.path.join(directorio, 'paradas')
destino = 'html_hapag'
if not os.path.exists(subcarpeta):
    os.makedirs(subcarpeta)

# Iterar sobre los archivos en el directorio
for filename in os.listdir(directorio):
    archivo_path = os.path.join(directorio, filename)
    # Verificar si es un archivo y no una carpeta
    if os.path.isfile(archivo_path):
        # Verificar si el archivo tiene extensión .json y no contiene 'zenrows' en el nombre
        #if filename.endswith('.json') and 'zenrows' not in filename:
        # Ruta completa del archivo original
        archivo_original = os.path.join(directorio, filename)
        # Nuevo nombre del archivo con extensión .html
        # nuevo_nombre = filename.replace('.json', '.html')
        # Ruta completa del archivo nuevo en la subcarpeta 'contenedores'
        archivo_nuevo = os.path.join(subcarpeta, filename)
        # Mover y renombrar el archivo

        shutil.move(archivo_original, archivo_nuevo)
|
print("Cambio de extensión y movimiento completado.")
"""