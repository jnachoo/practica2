import os
import boto3
import datetime
from dotenv import load_dotenv
from sqlalchemy import exists
from botocore.exceptions import NoCredentialsError, ClientError
from app.database.models import HTMLDescargado

import argparse

import app.database.db as db
from config.settings import DATABASE_URL_TEST, DATABASE_URL

db_manager = db.DatabaseManager(DATABASE_URL)
# Cargar las credenciales desde el archivo .env
load_dotenv()

# Configurar cliente de S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

def upload_to_s3_with_retry(file_path, bucket_name, s3_key):
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        # Verificar que el archivo existe en S3
        s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        return True
    except (NoCredentialsError, ClientError) as e:
        print(f"Error al cargar el archivo {file_path} a S3: {e}")
        # Intentar una vez más
        try:
            s3_client.upload_file(file_path, bucket_name, s3_key)
            s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            return True
        except Exception as retry_error:
            print(f"Segundo intento fallido al cargar {file_path} a S3: {retry_error}")
            return False

def backup_html_files(directory, bucket_name, naviera_folder, db_manager):
    for root, dirs, files in os.walk(directory):
        for file_name in files:
            file_path = os.path.join(root, file_name).replace("\\", "/")
            relative_path = os.path.relpath(file_path, directory).replace("\\", "/")
            s3_key = os.path.join(naviera_folder, relative_path).replace("\\", "/")

            #import pdb; pdb.set_trace()

            # Obtener la fecha de descarga
            fecha_descarga = datetime.datetime.now()

            with db_manager.session_scope() as session:
                # Verificar si el archivo ya existe en la base de datos
                existing_record = session.query(HTMLDescargado).filter_by(ruta_relativa=relative_path.replace("/", "\\")).first()
                
                if existing_record:
                    # Si el archivo ya existe en la base de datos, intentamos cargarlo a S3 si no está respaldado
                    if not existing_record.en_s3:
                        if upload_to_s3_with_retry(file_path, bucket_name, s3_key):
                            existing_record.ruta_s3 = s3_key
                            existing_record.en_s3 = True
                            session.add(existing_record)
                            print(f"Registro actualizado para {file_name} en la base de datos.")
                        else:
                            print(f"No se pudo respaldar {file_name} en S3.")
                else:
                    # Crear un nuevo registro si no existe en la base de datos
                    nuevo_registro = HTMLDescargado(
                        ruta_full=file_path,
                        nombre=file_name,
                        ruta_s3=s3_key if upload_to_s3_with_retry(file_path, bucket_name, s3_key) else None,
                        info=None,  # Cambia este campo si necesitas un valor específico
                        fecha_descarga=fecha_descarga,
                        ruta_relativa=relative_path,
                        bl_id=None,  # Asigna un valor si tienes ID
                        tipo_archivo=None,  # Asigna si necesitas este dato
                        en_s3=True if upload_to_s3_with_retry(file_path, bucket_name, s3_key) else False,
                        en_pabrego=True
                    )
                    session.add(nuevo_registro)
                    print(f"Nuevo registro creado para {file_name} en la base de datos.")
    print(f"Proceso de respaldo para {directory} completado.")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Descripción de tu script")
    parser.add_argument("--directorio", type=str, help="Carpeta de origen")
    args = parser.parse_args()

    directorio_local =  args.directorio # "html_hapag"  # Carpeta de origen
    nombre_bucket = "ants-bl"
    carpeta_naviera = args.directorio #"html_hapag"  # Carpeta en S3 para la naviera específica

    backup_html_files(directorio_local, nombre_bucket, carpeta_naviera, db_manager)