import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from app.reporte_rutina import dona_bls_por_state
from datetime import datetime

import os
from dotenv import load_dotenv
load_dotenv()
import sys
import argparse

def enviar_correo(destinatario, asunto, cuerpo):
    # Configuración del servidor y credenciales
    servidor_smtp = 'smtp.gmail.com'
    puerto = 465  # Para SSL; usa 587 para TLS
    remitente = os.getenv('EMAIL_USER')
    password = os.getenv('EMAIL_PASSWORD')

    # Crear el mensaje
    mensaje = MIMEMultipart()
    mensaje['From'] = remitente
    mensaje['To'] = destinatario
    mensaje['Subject'] = asunto
    mensaje.attach(MIMEText(cuerpo, 'html'))

    # Adjuntar el gráfico de pie
    with open('data/grafico_estado.png', 'rb') as image_file:
        img_pie = MIMEImage(image_file.read())
        img_pie.add_header('Content-ID', '<grafico_estado>')  # Usar el mismo 'cid' que en el HTML
        mensaje.attach(img_pie)
    
    # Adjuntar el gráfico de bar
    with open('data/grafico_naviera.png', 'rb') as image_file:
        img_bar = MIMEImage(image_file.read())
        img_bar.add_header('Content-ID', '<grafico_naviera>')  # Usar el mismo 'cid' que en el HTML
        mensaje.attach(img_bar)

    # Conectar al servidor y enviar el correo
    server = smtplib.SMTP_SSL(servidor_smtp, puerto)
    server.login(remitente, password)
    texto = mensaje.as_string()
    server.sendmail(remitente, destinatario, texto)
    server.sendmail(remitente, remitente,texto)
    server.quit()

def send_error_email(error_message):
    estado_counts, navieras_counts, cargo_counts, estados_navieras_counts  = dona_bls_por_state()
    fecha_actual = datetime.now().strftime('%d de %B de %Y, %H:%M')
    destinatario = "pbrain@brains.cl"#os.getenv('ADMIN_EMAIL')
    asunto = 'Rutina: '+error_message+' - '+fecha_actual
    estado_counts_html = estado_counts.to_html(index=True, index_names=False)
    navieras_counts_html = navieras_counts.to_html(index=True, index_names=False)
    cargo_counts_html = cargo_counts.to_html(index=False)
    estados_navieras_counts_html = estados_navieras_counts.to_html(index=True, index_names=False, )

    # Lee el contenido HTML desde un archivo
    with open('templates/correo_reporte.html', 'r', encoding='utf-8') as file:
        cuerpo = file.read()

    cuerpo = cuerpo.format(
        fecha=fecha_actual,
        error=error_message,
        cargo_counts=cargo_counts_html,
        estado_counts=estado_counts_html,
        navieras_counts=navieras_counts_html,
        estados_navieras_counts=estados_navieras_counts_html
    )
    
    enviar_correo(destinatario, asunto, cuerpo)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Enviar correo con mensaje de error.')
    parser.add_argument('-m', '--message', type=str, required=True, help='Mensaje de error')
    args = parser.parse_args()
    
    send_error_email(args.message)
