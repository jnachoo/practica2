from flask import Flask, request, render_template, jsonify

from carga_container_manual import process_containers, get_bls_manuales

from app.database.db import DatabaseManager
from config.settings import DATABASE_URL


app = Flask(__name__)

data = DatabaseManager(DATABASE_URL)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        bl_code = request.form['bl_code']
        container_list = request.form['container_list']
        # Aquí puedes integrar la lógica para procesar el BL y los contenedores
        try:
            # Suponiendo que tienes una función que maneja esto
            msg = process_containers(bl_code, container_list, request.remote_addr)
            pending_bls = get_bls_manuales()
            return jsonify({"message": f" BL {bl_code} cargado con éxito<br> {msg} "}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    pending_bls = get_bls_manuales()  # Obtener la lista de BLs pendientes
    return render_template('index.html', pending_bls=pending_bls)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')  # Corre en todas las interfaces
