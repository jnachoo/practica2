# Ants BL

Proyecto para descargar información de BLs de las siguientes navieras:
1. CMA-CGM
2. HAPAG-LLOYD
3. ONE
4. MAERSK
5. MSC
6. COSCO
7. EVERGREEN

## Funcionalidades

### 1. Descargar HTML

Permite descargar el codigo fuente de la web de los BLs disponibles.

 Se ejecuta con el siguiente comando

 ```sh
python .\app\descargar_html.py
 ```

Por defecto descarga los bls que no tienen ningun html descargado. 

**OPCIONES**
Las siguientes opciones permiten filtrar lso bls que deseas descargar
- `--navieras [lista de navieras]`: La o las navieras que quieras descargar
- `--dia [dia]`: Día del campo fecha_bl
- `--mes [mes]`: Mes del campo fecha_bl
- `--anio [año]`: Año del campo fecha_bl
- `--bls [lista de BLs]`: Lista de BLs específicos (bl_code) #TODO

### 2. Leer HTML

Permite descargar el codigo fuente de la web de los BLs disponibles.

 Se ejecuta con el siguiente comando

 ```sh
python .\app\leer_html.py
 ```

Por defecto descarga los bls que no tienen ningun html descargado. 

**OPCIONES**
Las siguientes opciones permiten filtrar lso bls que deseas descargar
- `--navieras [lista de navieras]`: La o las navieras que quieras descargar (nombre en mayuscula)
- `--dia [dia]`: Día del campo fecha_bl
- `--mes [mes]`: Mes del campo fecha_bl
- `--anio [año]`: Año del campo fecha_bl
- `--bls [lista de BLs]`: Lista de BLs específicos (bl_code)

### 3. Rutina

Este script es el que se ejecuta a diario. Descarga y lee los archivos descargados.

 Se ejecuta con el siguiente comando

 ```sh
python .\app\rutina.py
 ```

Por defecto descarga los bls que no tienen ningun html descargado. 

**OPCIONES**
Las siguientes opciones permiten filtrar lso bls que deseas descargar
- `--navieras [lista de navieras]`: La o las navieras que quieras descargar (nombre en mayuscula)
- `--dia [dia]`: Día del campo fecha_bl
- `--mes [mes]`: Mes del campo fecha_bl
- `--anio [año]`: Año del campo fecha_bl
- `--bls [lista de BLs]`: Lista de BLs específicos (bl_code)
- `--diario`: Bls con estado 1 (no tienen requests asociadas)
- `--semanal`: Bls con estado x,x ()
- `--mensual`: Bls con estado x,x ()
- `--csv`: Se ejecuta en modo archivo. Lee bls de un archivo y entrega contenedores en otro. No interactúa con la base de datos


## Flujo del código

1. busca 10 bls de una misma naviera con los filtros dados
2. 
2. se descargan los 10 htm en simultaneo. Pueden ser mas de una request (por ejemplo, una para paradas y otra para contenedores)
3. se guardan los archivos
4. se guarda la metadata
5. se parsean los archivos
6. se guarda los containers y paradas leidas
7. se registra la request
8. se actualizan los estados de los bls



