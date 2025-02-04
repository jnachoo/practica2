from fastapi import APIRouter, HTTPException,Query,Body
from database import database
from datetime import datetime
from pydantic import BaseModel

router = APIRouter()

class Item(BaseModel):
    numero: int #solo numeros
    texto: str #cualquier cadena de texto
    booleano: bool #cualquier bool

# Ejemplo de url
# http://localhost:8000/paradas/?locode=C&pais=chil&order_by=t.orden&bl_code=SCL
@router.get("/paradas/")
async def super_filtro_paradas(
    bl_code: str = Query(None),
    locode: str = Query(None),
    pais: str = Query(None),
    lugar: str = Query(None),
    is_pol: bool = Query(None),
    is_pod: bool = Query(None),
    orden: int = Query(None),
    status: str = Query(None),
    order_by: str = Query(None, regex="^(b\\.code|t\\.orden|t\\.status|p\\.locode|p\\.pais)$"),  # Campos válidos para ordenación
    order: str = Query("ASC", regex="^(ASC|DESC|asc|desc)$"), 
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
):

    # Consulta a la base de datos ants_api
    query = """
        SELECT t.id as id_tracking,b.code AS bl_code, t.orden, t.status, p.locode, p.pais, p.lugar,
               t.is_pol, t.is_pod
        FROM tracking t
        JOIN paradas p ON p.id = t.id_parada
        JOIN bls b ON b.id = t.id_bl
        WHERE 1=1
    """
    values = {}

    # Agregar filtros dinámicos
    if bl_code:
        query += " AND b.code ILIKE :bl_code"
        values["bl_code"] = f"{bl_code}%"
    if locode:
        query += " AND p.locode ILIKE :locode"
        values["locode"] = f"{locode}%"
    if pais:
        query += " AND p.pais ILIKE :pais"
        values["pais"] = f"{pais}%"
    if lugar:
        query += " AND p.lugar ILIKE :lugar"
        values["lugar"] = f"{lugar}%"
    if is_pol is not None:
        query += " AND t.is_pol = :is_pol"
        values["is_pol"] = bool(is_pol)
    if is_pod is not None:
        query += " AND t.is_pod = :is_pod"
        values["is_pod"] = bool(is_pod)
    if orden:
        query += " AND t.orden = :orden"
        values["orden"] = orden
    if status:
        query += " AND t.status ILIKE :status"
        values["status"] = f"{status}%"

    # Ordenación dinámica
    if order_by:
        query += f" ORDER BY {order_by} {order}"

    # Agregar límites y desplazamiento
    query += " LIMIT :limit OFFSET :offset"
    values["limit"] = limit
    values["offset"] = offset

    try:
        # Ejecutar la consulta
        result = await database.fetch_all(query=query, values=values)
        if not result:
            raise HTTPException(status_code=404, detail="Datos no encontrados")
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta: {str(e)}"}


#------------------------------------------------------------
#------------------ENDPOINTS DROPDOWN------------------------
#------------------------------------------------------------

@router.get("/paradas/locode")
async def paradas_locode():
    query = "SELECT locode FROM paradas order by locode"
    resultado = await database.fetch_all(query)
    if not resultado:
        raise HTTPException(status_code=404, detail="Locode no retornados")
    return resultado

@router.get("/paradas/pais")
async def paradas_pais():
    query = "SELECT distinct pais FROM paradas order by pais"
    resultado = await database.fetch_all(query)
    if not resultado:
        raise HTTPException(status_code=404, detail="Paises no retornados")
    return resultado

@router.get("/paradas/lugar")
async def paradas_lugar():
    query = "SELECT distinct lugar FROM paradas order by lugar"
    resultado = await database.fetch_all(query)
    if not resultado:
        raise HTTPException(status_code=404, detail="Lugares no retornados")
    return resultado

@router.get("/paradas/terminal")
async def paradas_terminal():
    query = "SELECT distinct terminal FROM tracking order by terminal"
    resultado = await database.fetch_all(query)
    if not resultado:
        raise HTTPException(status_code=404, detail="Terminales no retornados")
    return resultado

@router.get("/paradas/bl_code/{bl_code}")
async def ver_paradas(
    bl_code: str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """
                SELECT t.id as id_tracking,b.code AS bl_code, t.orden, t.status, p.locode, p.pais, p.lugar,
               t.is_pol, t.is_pod
                from tracking t
                join paradas p on p.id = t.id_parada
                join bls b on b.id = t.id_bl
                where b.code like :bl_code 
                order by t.orden
                LIMIT :limit OFFSET :offset;
            """
    bl_code = bl_code.upper()
    bl_code = f"{bl_code}%"
    try:
        result = await database.fetch_all(query=query, values={"bl_code": bl_code, "limit": limit, "offset": offset})
        if not result:
            raise HTTPException(status_code=404, detail="Paradas no encontradas")
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta paradas_filtro_bl_code: {str(e)}"}
    

@router.get("/paradas/locode/{locode}")
async def ver_paradas_locode(
    locode: str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """
                SELECT t.id as id_tracking,b.code AS bl_code, t.orden, t.status, p.locode, p.pais, p.lugar,
               t.is_pol, t.is_pod 
                from tracking t
                join paradas p on p.id = t.id_parada
                join bls b on b.id = t.id_bl
                where p.locode like :locode 
                order by t.orden
                LIMIT :limit OFFSET :offset;
            """
    print(f"Valor locode enviado a la consulta: {locode}")
    locode = locode.upper()
    locode = f"{locode}%"
    try:
        print(f"Valor locode enviado a la consulta: {locode}")

        result = await database.fetch_all(query=query, values={"locode": locode, "limit": limit, "offset": offset})
        if not result:
            raise HTTPException(status_code=404, detail="Paradas no encontradas")
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta paradas_filtro_locode: {str(e)}"}
    

@router.get("/paradas/pais/{pais}")
async def ver_paradas_pais(    
    pais: str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
):
    query = """
                SELECT t.id as id_tracking,b.code AS bl_code, t.orden, t.status, p.locode, p.pais, p.lugar,
               t.is_pol, t.is_pod
                from tracking t
                join paradas p on p.id = t.id_parada
                join bls b on b.id = t.id_bl
                where p.pais like :pais 
                order by t.orden
                LIMIT :limit OFFSET :offset;
            """
    pais = pais.upper()
    pais = f"{pais}%"
    try:
        result = await database.fetch_all(query=query, values={"pais": pais, "limit": limit, "offset": offset})
        if not result:
            raise HTTPException(status_code=404, detail="Paradas no encontradas")
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta paradas_filtro_pais: {str(e)}"}
    
@router.patch("/paradas/{locode}")
async def actualizar_parcial_parada(
    locode: str,
    pais: str = Query(None),
    lugar: str = Query(None),
):
    # Construir las consultas dinámicamente
    fields_paradas = []
    values_paradas = {}

    if not locode:
        raise HTTPException(status_code=400, detail="Debe escribir un locode")
    
    if locode: values_paradas = {"locode": locode}

    if pais is not None:
        pais = pais.upper()   
        fields_paradas.append("pais = :pais")
        values_paradas["pais"] = pais

    if lugar is not None:
        lugar = lugar.upper()
        fields_paradas.append("lugar = :lugar")
        values_paradas["lugar"] = lugar

    # Validar que al menos un campo se proporcionó
    if not fields_paradas:
        raise HTTPException(status_code=400, detail="No se proporcionaron campos para actualizar")

    # Bandera para saber si algo fue actualizado
    filas_actualizadas = 0

    # Ejecutar la consulta para la tabla paradas si hay campos
    if fields_paradas:  
        query_paradas = f"""
        UPDATE paradas
        SET {', '.join(fields_paradas)}
        WHERE locode = :locode
        RETURNING id;
        """           
        # Ejecutar la consulta
        resultado_p = await database.execute(query=query_paradas, values=values_paradas)
        if resultado_p: filas_actualizadas += 1
        print(f"Resultado del update paradas: {resultado_p}")

    # Validar si se actualizaron filas
    if filas_actualizadas == 0:
        raise HTTPException(status_code=400, detail="No se pudo actualizar nada")

    return {"message": "Actualización realizada con éxito"}


@router.patch("/tracking/{id_tracking}")
async def actualizar_parcial_parada(
    id_tracking: int,
    orden: int = Query(None),
    status: str = Query(None),
    locode: str = Query(None),
    is_pol: bool = Query(None),
    is_pod: bool = Query(None),
):
    # Construir las consultas dinámicamente
    fields_tracking = []
    values_tracking = {"id_tracking": id_tracking}
    fields_paradas = []
    values_paradas = {"id_tracking": id_tracking}


    # Campos para la tabla tracking
    if orden is not None:
        if type(orden) != int:
            raise HTTPException(status_code=400, detail="La orden debe ser un numero.")
        if orden <0:
            raise HTTPException(status_code=400, detail="La orden debe ser un numero POSITIVO.")
        fields_tracking.append("orden = :orden")
        values_tracking["orden"] = orden

    if status is not None:
        status = status.upper()
        if type(status) != str:
            raise HTTPException(status_code=400, detail="El status debe ser una cadena de texto.")
        fields_tracking.append("status = :status")
        values_tracking["status"] = status
    
    if is_pol is not None:
        fields_tracking.append("is_pol = :is_pol")
        values_tracking["is_pol"] = is_pol  
    if is_pod is not None:
        fields_tracking.append("is_pod = :is_pod")
        values_tracking["is_pod"] = is_pod  

    # Campos para la tabla paradas
    if locode is not None:
        check_locode = "SELECT id FROM paradas where locode = :locode"
        ayuda_locode = await database.fetch_val(check_locode, {"locode": locode})
        if ayuda_locode == 0 or ayuda_locode is None:
            raise HTTPException(status_code=400, detail="El locode no existe en la tabla 'paradas'.")
        fields_paradas.append("id_parada = :ayuda_locode")
        values_paradas["ayuda_locode"] = ayuda_locode


    # Validar que al menos un campo se proporcionó
    if not fields_tracking and not fields_paradas:
        raise HTTPException(status_code=400, detail="No se proporcionaron campos para actualizar")

    # Bandera para saber si algo fue actualizado
    filas_actualizadas = 0

    # Ejecutar la consulta para la tabla tracking si hay campos
    if fields_tracking:
        query_tracking = f"""
            UPDATE tracking
            SET {', '.join(fields_tracking)}
            WHERE id = :id_tracking
            RETURNING id;
        """
        # Ejecutar la consulta
        resultado_t = await database.execute(query=query_tracking, values=values_tracking)
        if resultado_t : filas_actualizadas += 1
        print(f"Resultado del update tracking: {resultado_t}")
    # Ejecutar la consulta para la tabla paradas si hay campos
    if fields_paradas:
        query_paradas = f"""
            UPDATE tracking
            SET {', '.join(fields_paradas)}
            WHERE id = :id_tracking
            RETURNING id;
        """
        # Ejecutar la consulta
        resultado_p = await database.execute(query=query_paradas, values=values_paradas)
        if resultado_p: filas_actualizadas += 1
        print(f"Resultado del update paradas: {resultado_p}")

    # Validar si se actualizaron filas
    if filas_actualizadas == 0:
        raise HTTPException(status_code=400, detail="No se pudo actualizar nada")

    return {"message": "Actualización realizada con éxito"}

@router.post("/paradas/")
async def insertar_parada(
    locode: str,
    pais: str = Query(None),
    lugar: str = Query(None),
):
    """
    Endpoint para insertar un nuevo registro en la tabla paradas.
    """
    try:
        # Validar parámetros obligatorios
        if not locode :
            raise HTTPException(
                status_code=400,
                detail="El campo 'locode' es obligatorio."
            )
        locode = locode.upper()
        query_locode = "SELECT id from paradas where locode = :locode"
        verificar_locode = await database.fetch_val(query_locode, {"locode":locode})
        if verificar_locode !=0 and verificar_locode != None:
            raise HTTPException(status_code=400, detail=f"El locode ya existe, id:{verificar_locode}")
        
        if pais is None:pais = "DESCONOCIDO"
        if lugar is None:pais = "DESCONOCIDO"

        pais = pais.upper()
        lugar = lugar.upper()

        print("datos:",locode," ",pais," ",lugar)
        # Consulta SQL para insertar el registro en la tabla 'bls'
        query_paradas = """
            INSERT INTO paradas (
                locode, pais, lugar
            ) VALUES (
                :locode, :pais, :lugar
            )
            RETURNING id;
        """

        # Valores para la consulta
        values_parada = {
            "locode": locode,
            "pais": pais,
            "lugar": lugar,
        }

        # Ejecutar la consulta
        id_parada = await database.execute(query=query_paradas, values=values_parada)

        if not id_parada:
            raise HTTPException(status_code=500, detail="Error al insertar en paradas, no hay id_parada.")

        return {"message": "Registro creado exitosamente en la tabla 'paradas'.", "id_parada": id_parada}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al insertar el registro en 'paradas': {str(e)}")
    
@router.post("/tracking/")
async def insertar_tracking(
    bl_code: str,
    locode: str,
    fecha: str,
    orden: int = Query(None),
    terminal: str = Query(None),
    status: str = Query(None),
    is_pol: bool = Query(None),
    is_pod: bool = Query(None),
):
    """
    Endpoint para insertar un nuevo registro en la tabla tracking.
    """
    try:
        # Validar parámetros obligatorios
        if not locode or not bl_code or not fecha:
            raise HTTPException(
                status_code=400,
                detail="Los campos 'locode, fecha y bl_code' son obligatorios."
            )
        locode = locode.upper()
        query_locode = "SELECT id from paradas where locode = :locode"
        verificar_locode = await database.fetch_val(query_locode, {"locode":locode})
        if verificar_locode == None:
            raise HTTPException(status_code=400, detail=f"El locode no existe")
        
        bl_code = bl_code.upper()
        query_bl_code = "SELECT id from bls where code = :bl_code"
        verificar_bl_code = await database.fetch_val(query_bl_code, {"bl_code":bl_code})
        if verificar_bl_code == None:
            raise HTTPException(status_code=400, detail=f"El bl_code no existe")
        
        if orden is None:orden = 0
        if is_pod is None:is_pod = False
        if is_pol is None:is_pol = False
        if status is None:status = "DESCONOCIDO"
        if terminal is None:terminal = "DESCONOCIDO"

        # Convertir 'fecha' a tipo datetime
        try:
            fecha = datetime.strptime(fecha, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Formato de 'fecha' inválido. Use el formato YYYY-MM-DD."
            )

        # Consulta SQL para insertar el registro en la tabla 'bls'
        query_tracking = """
            INSERT INTO tracking (
                id_bl, fecha, status, orden, 
                id_parada,terminal, is_pol,is_pod
            ) VALUES (
                :verificar_bl_code, :fecha, :status, :orden,
                :verificar_locode, :terminal, :is_pol, :is_pod
            )
            RETURNING id;
        """
        

    # Ejecutar la consulta
        id_tracking = await database.fetch_val(query_tracking, {
            "verificar_bl_code": verificar_bl_code,
            "fecha": fecha,
            "status": status,
            "orden": orden,
            "verificar_locode": verificar_locode,
            "terminal": terminal,
            "is_pol": is_pol,
            "is_pod": is_pod
        })

        if not id_tracking:
            raise HTTPException(status_code=500, detail="Error al insertar en tracking.")

        return {"message": "Registro creado exitosamente en la tabla 'tracking'.", "id_tracking": id_tracking}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al insertar el registro en 'tracking': {str(e)}")