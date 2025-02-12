# rutas/scraper_service.py
import subprocess
import os

def build_scraper_command(filters: dict) -> list:
    """
    Construye la lista de argumentos para ejecutar el script legacy "rutina.py".
    Se asume que el script se encuentra en "ants_bl-main/app/rutina.py".
    
    Opciones disponibles:
      --navieras [lista de navieras]: Se espera un string o lista (separadas por coma)
      --dia [dia]
      --mes [mes]
      --anio [año]
      --bls [lista de BLs]: Se espera un string con BLs separados por coma
      --diario, --semanal, --mensual, --csv: flags (booleanos)
    """
    # Ajusta la ruta para que apunte a la carpeta "ants_bl-main"
    script_path = os.path.join("ants_bl-main", "app", "rutina.py")
    cmd = ["python", script_path]

    # Agregar opción --navieras
    if "navieras" in filters and filters["navieras"]:
        # Se espera que sea una lista de strings o un string
        navieras = ",".join(filters["navieras"]) if isinstance(filters["navieras"], list) else filters["navieras"]
        cmd.extend(["--navieras", navieras])
    # Agregar opciones numéricas si existen
    if "dia" in filters and filters["dia"] is not None:
        cmd.extend(["--dia", str(filters["dia"])])
    if "mes" in filters and filters["mes"] is not None:
        cmd.extend(["--mes", str(filters["mes"])])
    if "anio" in filters and filters["anio"] is not None:
        cmd.extend(["--anio", str(filters["anio"])])
    # Agregar opción --bls
    if "bls" in filters and filters["bls"]:
        bls = ",".join(filters["bls"]) if isinstance(filters["bls"], list) else filters["bls"]
        cmd.extend(["--bls", bls])
    # Agregar flags (solo se añade si están en True)
    if filters.get("diario", False):
        cmd.append("--diario")
    if filters.get("semanal", False):
        cmd.append("--semanal")
    if filters.get("mensual", False):
        cmd.append("--mensual")
    if filters.get("csv", False):
        cmd.append("--csv")
        
    return cmd

def run_scraper_order(order_id: int, filters: dict):
    """
    Ejecuta el script legacy "rutina.py" usando el comando construido.
    Se utiliza subprocess para lanzar el comando y se imprimen en consola
    la salida estándar y el error.
    """
    cmd = build_scraper_command(filters)
    command_str = " ".join(cmd)
    print(f"Ejecutando la orden {order_id} comando: {command_str}")

    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        print(f"Orden {order_id} ejecutada. STDOUT: {stdout.decode()}")
        if stderr:
            print(f"Orden {order_id} STDERR: {stderr.decode()}")
    except Exception as e:
        print(f"Error ejecutando la orden {order_id}: {str(e)}")