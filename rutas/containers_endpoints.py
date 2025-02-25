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
# http://localhost:8000/containers/?size=40&type=High&bl_code=238&order_by=c.code
@router.get("/containers/")
async def super_filtro_containers( 
    codigo_container: str = Query(None),
    bl_code: str = Query(None),
    size: str = Query(None), 
    type: str = Query(None),  
    contenido: str = Query(None), 
    order_by: str = Query(None, regex="^(c\\.code|b\\.code|c\\.size|c\\.type|c\\.contenido|c\\.id)$"), 
    order: str = Query("ASC", regex="^(ASC|DESC|asc|desc)$"),  
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0),  # Índice de inicio, por defecto 0
):
    # Consulta base
    query = """
        SELECT cv.id as id_container_viaje,c.code AS container_code, b.code AS bl_code, c.size, c.type, c.contenido
        FROM containers c
        JOIN container_viaje cv ON cv.id_container = c.id
        JOIN bls b ON b.id = cv.id_bl
        WHERE 1=1
    """
    values = {}

    # Agregar filtros dinámicos
    if codigo_container:
        query += " AND c.code ILIKE :codigo_container"
        values["codigo_container"] = f"{codigo_container}%"
    if bl_code:
        query += " AND b.code ILIKE :bl_code"
        values["bl_code"] = f"{bl_code}%"
    if size is not None:
        query += " AND c.size ILIKE :size"
        values["size"] = f"{size}%"
    if type:
        query += " AND c.type ILIKE :type"
        values["type"] = f"{type}%"
    if contenido:
        query += " AND c.contenido ILIKE :contenido"
        values["contenido"] = f"{contenido}%"

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
            raise HTTPException(status_code=404, detail="Containers no encontrados")
        return result
    
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta containers: {str(e)}"}

@router.get("/containers/code/{code}")
async def ver_container(
    code : str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """
                SELECT cv.id as id_container_viaje,c.code AS container_code, b.code AS bl_code, c.size, c.type, c.contenido
                FROM containers c
                JOIN container_viaje cv ON cv.id_container = c.id
                JOIN bls b ON b.id = cv.id_bl
                where c.code like :code
                LIMIT :limit OFFSET :offset;
            """
    code = f"{code}%"
    try:
        result = await database.fetch_all(query=query, values={"code":code, "limit": limit, "offset": offset})
        if not result:
            raise HTTPException(status_code=404, detail="Containers no encontrados")
        return result
    except Exception as e: return {"error": f"Error al ejecutar la consulta containers:{str(e)}"}

@router.get("/containers/bl_code/{code}")
async def ver_container(
    code : str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """
                SELECT cv.id as id_container_viaje,c.code AS container_code, b.code AS bl_code, c.size, c.type, c.contenido
                FROM containers c
                JOIN container_viaje cv ON cv.id_container = c.id
                JOIN bls b ON b.id = cv.id_bl
                where b.code like :code
                LIMIT :limit OFFSET :offset;
            """
    code = f"{code}%"
    try:
        result = await database.fetch_all(query=query, values={"code":code, "limit": limit, "offset": offset})
        if not result:
            raise HTTPException(status_code=404, detail="Containers no encontrados")
        return result
    except Exception as e: return {"error": f"Error al ejecutar la consulta containers:{str(e)}"}


@router.get("/containers/{code}")
async def ver_container(
    code : str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """
                SELECT cv.id as id_container_viaje,c.code AS container_code, b.code AS bl_code, c.size, c.type, c.contenido
                FROM containers c
                JOIN container_viaje cv ON cv.id_container = c.id
                JOIN bls b ON b.id = cv.id_bl
                where c.code like :code
                LIMIT :limit OFFSET :offset;
            """
    code = f"{code}%"
    try:
        result = await database.fetch_all(query=query, values={"code":code, "limit": limit, "offset": offset})
        if not result:
            raise HTTPException(status_code=404, detail="Containers no encontrados")
        return result
    except Exception as e: return {"error": f"Error al ejecutar la consulta containers:{str(e)}"}


@router.patch("/containers/{container_code}")
async def actualizar_parcial_container(
    container_code: str,
    size: str = Query(None),
    type: str = Query(None),
    contenido: str = Query(None)
):
    
    """En este endpoint puedes actualizar la tabla containers.
    Si quieres modificar los datos del container, debes modificar los campos size, type, contenido."""

    # Construir la consulta dinámicamente
    fields_container = []
    values_container = {}

    if not container_code:
        raise HTTPException(status_code=400, detail="Debe escribir un code")

    container_code = container_code.upper()
    query_check_container_code = "SELECT id FROM containers WHERE code = :container_code"
    id_container = await database.fetch_val(query_check_container_code, {"container_code": container_code})
    if id_container is None:
        raise HTTPException(status_code=400, detail="El codigo de container no existe en la tabla 'containers'.")
    print(id_container)
    values_container["id_container"] = id_container


    if size != None:
        size = size.upper()
        query_size = "select nombre_size FROM dict_containers WHERE size = :size limit 1;"
        nombre_size = await database.fetch_val(query_size, {"size": size})
        if nombre_size == None:
            raise HTTPException(status_code=400, detail="El size no existe en el diccionario de containers.")
        fields_container.append("size = :nombre_size")
        values_container["nombre_size"] = nombre_size
        print(nombre_size)

    if type != None:
        type = type.upper()
        query_type = "select nombre_type FROM dict_containers WHERE type = :type limit 1;"
        nombre_type = await database.fetch_val(query_type, {"type": type})
        if nombre_type == None:
            raise HTTPException(status_code=400, detail="El type no existe en el diccionario de containers.")
        fields_container.append("type = :nombre_type")
        values_container["nombre_type"] = nombre_type
        print(nombre_type)

    if contenido != None:
        contenido = contenido.upper()
        if contenido != "DRY" and contenido != "REEFER":
            raise HTTPException(status_code=400, detail="El tipo de contenido no existe en el diccionario de containers.")
        fields_container.append("contenido = :contenido")
        values_container["contenido"] = contenido
        print(contenido)

    if not fields_container :
        raise HTTPException(status_code=400, detail="No se proporcionaron campos para actualizar")

    # Bandera para saber si algo fue actualizado
    filas_actualizadas = 0

    if fields_container:
        print("ENTRA")
        query_container = f"""
            UPDATE containers
            SET {', '.join(fields_container)}
            WHERE id =:id_container
            RETURNING id;
        """
        # Ejecutar la consulta
        resultado_c = await database.execute(query=query_container, values=values_container)
        if resultado_c : filas_actualizadas += 1
        print(f"Resultado del update container: {resultado_c}")

    # Validar si se actualizaron filas
    if filas_actualizadas == 0:
        raise HTTPException(status_code=400, detail="No se pudo actualizar nada")

    return {"message": "Actualización realizada con éxito"}


@router.patch("/containers_viaje/{id_container_viaje}")
async def actualizar_parcial_container_viaje(
    id_container_viaje: int,
    container_code: str = Query(None),
    bl_code: str = Query(None),
):
    
    """En este endpoint puedes actualizar la tabla container_viaje y containers.
    Si cambias el codigo del container automaticamente se cambiaran los demás datos asociados.
    """

    # Construir la consulta dinámicamente
    fields_container_viaje = []
    fields_bls = []

    values_container_viaje = {"id_container_viaje": id_container_viaje}
    values_bls = {"id_container_viaje": id_container_viaje}

    ayuda_container_code = 0

    if bl_code is not None:
        bl_code = bl_code.upper()
        query_check_bl_code = "SELECT id FROM bls WHERE code = :bl_code"
        print(bl_code)
        id_bl = await database.fetch_val(query_check_bl_code, {"bl_code": bl_code})
        print(id_bl)
        if id_bl == None:
            raise HTTPException(status_code=400, detail="El bl_code no existe en la tabla 'bls'.")
        fields_bls.append("id_bl = :id_bl")
        values_bls["id_bl"] = id_bl

    
    if container_code is not None:
        container_code = container_code.upper()
        query_check_container_code = "SELECT id FROM containers WHERE code = :container_code"
        ayuda_container_code = await database.fetch_val(query_check_container_code, {"container_code": container_code})
        if ayuda_container_code == 0 or ayuda_container_code is None:
            raise HTTPException(status_code=400, detail="El codigo de container no existe en la tabla 'containers'.")
        fields_container_viaje.append("id_container = :ayuda_container_code")
        values_container_viaje["ayuda_container_code"] = ayuda_container_code


    if not fields_bls and not fields_container_viaje:
        raise HTTPException(status_code=400, detail="No se proporcionaron campos para actualizar")

    # Bandera para saber si algo fue actualizado
    filas_actualizadas = 0

    # Ejecutar la consulta para la tabla tracking si hay campos
    if fields_container_viaje:
        query_container_viaje = f"""
            UPDATE container_viaje
            SET {', '.join(fields_container_viaje)}
            WHERE id =:id_container_viaje
            RETURNING id;
        """
        # Ejecutar la consulta
        resultado_cv = await database.execute(query=query_container_viaje, values=values_container_viaje)
        if resultado_cv : filas_actualizadas += 1
        print(f"Resultado del update container: {resultado_cv}")

    # Ejecutar la consulta para la tabla tracking si hay campos
    if fields_bls:
        query_bls = f"""
            UPDATE container_viaje
            SET {', '.join(fields_bls)}
            WHERE id =:id_container_viaje
            RETURNING id;
        """
        # Ejecutar la consulta
        resultado_b = await database.execute(query=query_bls, values=values_bls)
        if resultado_b : filas_actualizadas += 1
        print(f"Resultado del update container: {resultado_b}")

    # Validar si se actualizaron filas
    if filas_actualizadas == 0:
        raise HTTPException(status_code=400, detail="No se pudo actualizar nada")

    return {"message": "Actualización realizada con éxito"}

@router.post("/containers/")
async def insertar_container(
    code: str,
    size: str = Query(None),
    type: str = Query(None),
    contenido: str = Query(None),
):
    """
    Endpoint para insertar un nuevo registro en la tabla containers.
    """
    try:
        # Validar parámetros obligatorios
        if not code :
            raise HTTPException(
                status_code=400,
                detail="El campo 'code' es obligatorio."
            )
        code = code.upper()
        query_code = "SELECT id from containers where code = :code"
        verificar_code = await database.fetch_val(query_code, {"code":code})
        if verificar_code !=0 and verificar_code !=None:
            raise HTTPException(status_code=400, detail=f"El code ya existe, id:{verificar_code}")
        
        if size is None: size = "DESCONOCIDO"
        if type is None: type = "DESCONOCIDO"
        if contenido is None: contenido = "DESCONOCIDO"

        size = size.upper()
        type = type.upper()
        contenido = contenido.upper()

        # Consulta SQL para insertar el registro en la tabla 'bls'
        query_container = """
            INSERT INTO containers (
                code, size, type, contenido
            ) VALUES (
                :code, :size, :type, :contenido
            )
            RETURNING id;
        """

        # Valores para la consulta
        values_container = {
            "code": code,
            "size": size,
            "type": type,
            "contenido":contenido
        }

        # Ejecutar la consulta
        id_container = await database.execute(query=query_container, values=values_container)

        if not id_container:
            raise HTTPException(status_code=500, detail="Error al insertar en containers, no hay id_container.")

        return {"message": "Registro creado exitosamente en la tabla 'containers'.", "id_container": id_container}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al insertar el registro en 'containers': {str(e)}")
    
@router.post("/containers_viaje/")
async def insertar_container_viaje(
    container_code: str,
    bl_code: str,
    ):
    """
    Endpoint para insertar un nuevo registro en la tabla containers viaje.
    """
    try:
        # Validar parámetros obligatorios
        if not bl_code or not container_code  :
            raise HTTPException(
                status_code=400,
                detail="Los campos son obligatorios."
            )
        
        container_code = container_code.upper()
        query_code = "SELECT id from containers where code = :container_code"
        id_container = await database.fetch_val(query_code, {"container_code":container_code})
        if id_container ==None:
            raise HTTPException(status_code=400, detail=f"El container_code no existe")
        
        bl_code = bl_code.upper()
        query_bl_code = "SELECT id from bls where code = :bl_code"
        id_bl = await database.fetch_val(query_bl_code, {"bl_code":bl_code})
        if id_bl == None:
            raise HTTPException(status_code=400, detail=f"El bl_code no existe")

        # Consulta SQL para insertar el registro en la tabla 'bls'
        query_container_viaje = """
            INSERT INTO container_viaje(
                id_container,id_bl
            ) VALUES (
                :id_container, :id_bl
            )
            RETURNING id;
        """

        # Valores para la consulta
        values_container_viaje = {
            "id_container": id_container,
            "id_bl": id_bl,
        }

        # Ejecutar la consulta
        id_container_viaje = await database.execute(query=query_container_viaje, values=values_container_viaje)

        if not id_container_viaje:
            raise HTTPException(status_code=500, detail="Error al insertar en containers viaje, no hay id_container_viaje.")

        return {"message": "Registro creado exitosamente en la tabla 'container_viaje'.", "id_container_viaje": id_container_viaje}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al insertar el registro en 'containers': {str(e)}")