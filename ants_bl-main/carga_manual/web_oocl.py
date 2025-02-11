from flask import Flask, request, render_template, jsonify, url_for, redirect
from app.agents.oocl import AgenteOOCL
from app.validar_descarga import generar_dict_bl
#from app.carga_container_manual import process_containers, get_bls_manuales

from app.database.db import DatabaseManager
from config.settings import DATABASE_URL

from app.database.clases import BL, Container, Parada


app = Flask(__name__)

data = DatabaseManager(DATABASE_URL)

# Función para guardar el HTML
def save_html(content, filename):
    filename = 'html_oocl/' + filename
    with open(filename, 'w', encoding='utf-8') as file:
        file.write(content)

# Función para parsear y guardar los datos (ejemplo simple)
def parse_and_save(filename):
    # Aquí deberías agregar tu lógica de parseo
    filename = 'html_oocl/' + filename
    with open(filename, 'r', encoding='utf-8') as file:
        content = file.read()
        # Parseo y guardado en la base de datos
        print("Parsing and saving data...")

def get_bls():
    pending_bls_dict = data.get_bls_rutina(limit=1, naviera='OOCL', estados=[1, 3], random=True)
    cantidad = data.get_pendientes_oocl(estados=[1, 3])
    pending_bls = []
    for bl in pending_bls_dict:
        b = BL(id=bl['id'], bl_code=bl['bl_code'], naviera=bl['nombre_naviera'], estado=bl['estado'], etapa=bl['etapa'])
        url = f"https://www.oocl.com/Pages/ExpressLink.aspx?eltype=ct&bl_no={bl['bl_code']}&cont_no=&booking_no="
        b.url = url
        pending_bls.append(b)
    return pending_bls, cantidad

# guardar a base de datos recibe una lista de bls cargados y los guarda en la base de datos
def guardar_db(bl, ip):

    data.add_containers(bl)
    data.add_paradas(bl)
    data.descargar_html(bl)
    
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

    data.save_request(bl.id, url, 202, caso, msg, tipo=1, agente=ip)

    return True


@app.route('/', methods=['GET', 'POST'])
def index():
    agente = AgenteOOCL()
    if request.method == 'POST':
        html_content = request.form['html_content']
        filename = request.form['filename']
        
        
        b = filename.split('_')[0]
        bl_dict = data.get_bl(bl_code=b)
        if len(bl_dict) == 0:
            return redirect(url_for('index'))
        else:
            bl_dict = bl_dict[0]
        bl = BL(bl_code=bl_dict['bl_code'], id=bl_dict['id'], naviera=bl_dict['nombre_naviera'], estado=bl_dict['estado'], etapa=bl_dict['etapa'])
        url = f"https://www.oocl.com/Pages/ExpressLink.aspx?eltype=ct&bl_no={bl.bl_code}&cont_no=&booking_no="
        bl.url = url
        bl = agente.guardar_html(bl, html_content, filename)
        bl = agente.parse_html(html_content, bl)
        ip = request.remote_addr
        guardar_db(bl, ip)

        
        return redirect(url_for('index'))
    
    if request.method == 'GET':
        # Obtener la lista de bls
        pass
        # get blas de navieraid = 8y estado 1
    
        
    pending_bls, cantidad = get_bls()
    return render_template('index_oocl.html', pending_bls=pending_bls, cantidad=cantidad)

if __name__ == '__main__':
    app.run(debug=True, port=5555)
