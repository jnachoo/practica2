from fastapi import APIRouter, HTTPException,Query
from database import database
from datetime import datetime

router = APIRouter()

#---------------------------------
#-------VALIDACION EN LINEA-------
#---------------------------------

@router.get("/validacion_locode_nulo")
async def val(
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 
    ):
    
    query = """
                SELECT 
                b.code,
                b.id,
                p.locode,
                b.pod,
                b.pol
            FROM 
                bls b
            INNER JOIN tracking t ON t.id_bl = b.id
            INNER JOIN paradas p ON p.id = t.id_parada
            WHERE 
                b.pod IS NULL and b.pol IS NULL
            GROUP BY 
                b.code, b.id, p.locode, b.pod, b.pol
                LIMIT :limit OFFSET :offset;
                """
    try:
        result = await database.fetch_all(query=query, values={"limit": limit, "offset": offset})
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de locode nulo."}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de locode nulo: {str(e)}"}

@router.get("/validacion_cruce_contenedores")
async def val(
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 
    ):
    query = """
                SELECT DISTINCT ON (c.size)
                    c.code AS codigo_container,
                    b.code AS codigo_bl,
                    c.size AS container_size,
                    c.type AS container_type,
                    b.fecha
                FROM containers c
                LEFT JOIN dict_containers dc 
                    ON dc.size = c.size AND dc.type = c.type
                INNER JOIN container_viaje cv 
                    ON c.id = cv.id_container
                JOIN bls b 
                    ON b.id = cv.id_bl
                WHERE dc.size IS NULL
                ORDER BY c.size, c.code
                LIMIT :limit OFFSET :offset;
                """
    try:
        result = await database.fetch_all(query=query, values={"limit": limit, "offset": offset})
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de cruce con diccionario de contenedores."}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación del cruce con diccionario de contenedores: {str(e)}"}
    
@router.get("/validacion_containers_repetidos")
async def val(
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 
    ):
    query = """
                 SELECT * FROM container_repetido()
                 LIMIT :limit OFFSET :offset;
                """
    try:
        result = await database.fetch_all(query=query, values={"limit": limit, "offset": offset})
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de container repetido."}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de container repetido: {str(e)}"}
    
@router.get("/validacion_paradas_pol_pod")
async def val(
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 
    ):
    query = """
                SELECT * FROM obtener_paradas_pol_pod()
                LIMIT :limit OFFSET :offset;
                """
    try:
        result = await database.fetch_all(query=query, values={"limit": limit, "offset": offset})
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de paradas que sean pol y pod."}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta: {str(e)}"}
    
@router.get("/validacion_orden_repetida")
async def val(
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 
    ):
    query = """
                 SELECT * FROM obtener_paradas_con_orden_repetida()
                 LIMIT :limit OFFSET :offset;
                """
    try:
        result = await database.fetch_all(query=query, values={"limit": limit, "offset": offset})
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de orden repetida en la tabla de paradas."}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de orden repetida en tabla de paradas: {str(e)}"}
   
@router.get("/validacion_impo_distinta_CL")
async def val(
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 
    ):
    query = """
                 SELECT * FROM verificar_registros_etapa2_pais_distinto_cl()
                 LIMIT :limit OFFSET :offset;
                """
    try:
        result = await database.fetch_all(query=query, values={"limit": limit, "offset": offset})
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de parada POD distinta a Chile en importación."}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de parada POD distinta a Chile en importación: {str(e)}"}
    
@router.get("/validacion_bls_impo")
async def val(
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 
    ):
    query = """
                 SELECT * FROM validar_bls_impo(2)
                 LIMIT :limit OFFSET :offset;
                """
    try:
        result = await database.fetch_all(query=query, values={"limit": limit, "offset": offset})
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de país de destino dentro de ('CL', 'AR', 'BO', 'PY', 'UY')"}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de país de destino dentro de ('CL', 'AR', 'BO', 'PY', 'UY'): {str(e)}"}

@router.get("/validacion_expo_distinta_CL")
async def val(
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 
    ):
    query = """
                 SELECT * FROM verificar_registros_etapa1_pais_distinto_cl()
                 LIMIT :limit OFFSET :offset;
                """
    try:
        result = await database.fetch_all(query=query, values={"limit": limit, "offset": offset})
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de parada POL distinta a Chile en exportación"}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de parada POL distinta a Chile en exportación: {str(e)}"} 

@router.get("/validacion_bls_expo")
async def val(
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 
    ):
    query = """
                 SELECT * FROM validar_bls_expo(1)
                 LIMIT :limit OFFSET :offset;
                """
    try:
        result = await database.fetch_all(query=query, values={"limit": limit, "offset": offset})
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de país de origen dentro de ('CL', 'AR', 'BO')"}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de país de origen dentro de ('CL', 'AR', 'BO'): {str(e)}"}
    
@router.get("/validacion_paradas_expo")
async def val(
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 
    ):
    query = """
                SELECT * FROM obtener_paradas_con_validacion()
                LIMIT :limit OFFSET :offset;
                """
    try:
        result = await database.fetch_all(query=query, values={"limit": limit, "offset": offset})
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de país de destino y POD distinto de Chile"}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de país de destino y POD distinto de Chile: {str(e)}"}


@router.get("/validacion_dias_impo")
async def val(
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 
    ):
    query = """
                SELECT * FROM obtener_diferencia_requests_importacion()
                LIMIT :limit OFFSET :offset;
                """
    try:
        result = await database.fetch_all(query=query, values={"limit": limit, "offset": offset})
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de request en importaciones"}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de request en importaciones: {str(e)}"}
    
@router.get("/validacion_requests_expo/")
async def validacion_requests_expo(
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0)
):
    query = """
        WITH cte AS (
            SELECT 
                r.id_bl,
                t.id_parada AS id_parada,
                r.id AS id_request,
                r.mensaje,
                ROW_NUMBER() OVER (PARTITION BY r.id_bl ORDER BY t.is_pol DESC, t.orden ASC) AS rn_pol,
                ROW_NUMBER() OVER (PARTITION BY r.id_bl ORDER BY t.orden DESC) AS rn_orden_mayor
            FROM requests r
            INNER JOIN tracking t ON t.id_bl = r.id_bl
            WHERE r.id_respuesta NOT IN (1, 8)
        ),
        ids AS (
            SELECT 
                cte.id_bl,
                MAX(CASE WHEN cte.rn_pol = 1 THEN cte.id_parada ELSE NULL END) AS id_pol,
                MAX(CASE WHEN cte.rn_orden_mayor = 1 THEN cte.id_parada ELSE NULL END) AS id_destino
            FROM cte
            GROUP BY cte.id_bl
        ),
        filtered_cte AS (
            SELECT DISTINCT
                b.code AS bl_code,
                cte.id_request,
                cte.mensaje,
                cte.id_parada,
                ids.id_pol,
                p1.lugar AS lugar_pol,
                ids.id_destino,
                p2.lugar AS lugar_destino,
                ROW_NUMBER() OVER (PARTITION BY cte.id_bl ORDER BY cte.id_parada DESC) AS rn
            FROM cte
            INNER JOIN ids ON cte.id_bl = ids.id_bl
            INNER JOIN bls b ON cte.id_bl = b.id
            LEFT JOIN paradas p1 ON ids.id_pol = p1.id
            LEFT JOIN paradas p2 ON ids.id_destino = p2.id
        )
        SELECT 
            filtered_cte.bl_code,
            filtered_cte.id_request,
            filtered_cte.mensaje,
            filtered_cte.id_parada,
            filtered_cte.id_pol,
            filtered_cte.lugar_pol,
            filtered_cte.id_destino,
            filtered_cte.lugar_destino
        FROM filtered_cte
        WHERE filtered_cte.rn = 1
        LIMIT :limit OFFSET :offset;
    """
    result = await database.fetch_all(query=query, values={"limit": limit, "offset": offset})

    
    #locode = f"'{locode}'"

#-------------------------------------------
#----------VALIDACIONES TENDENCIA-----------
#-------------------------------------------

@router.get("/tendencia_navieras/{nombre}")
async def tendencia_navieras(nombre: str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 
    ):
    query = """
                SELECT 
                    n.nombre,
                    SUM(oc.c20 + oc.c40 * 2) AS teus
                FROM output_containers oc
                LEFT JOIN bls b ON b.code = oc.codigo 
                LEFT JOIN navieras n ON n.id = b.id_naviera 
                WHERE n.nombre ILIKE :nombre
                GROUP BY
                    n.nombre
                HAVING SUM(oc.c20 + oc.c40 * 2) > 0
                LIMIT :limit OFFSET :offset;
                """
    nombre = f"{nombre}%"
    try:
        # Ejecutamos la consulta pasando el parámetro 'nombre'
        result = await database.fetch_all(query=query, values={"nombre": nombre, "limit": limit, "offset": offset})
        
        # Verificamos si no hay resultados
        if not result:
            raise HTTPException(status_code=404, detail="Containers no encontrados")
            return {
                    "message": "No existen datos que cumplan con la naviera seleccionada",
                    "info":ver_info()
                }
        
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}

@router.get("/tendencia_etapa/{etapa}")
async def tendencia_etapa(etapa: str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 
    ):
    query = """
            SELECT 
                n.nombre,
                b.id_etapa,
                SUM(oc.c20 + oc.c40 * 2) AS teus
            FROM output_containers oc
            LEFT JOIN bls b ON b.code = oc.codigo
            LEFT JOIN navieras n ON n.id = b.id_naviera
            WHERE b.id_etapa = :etapa
            GROUP BY
                n.nombre,
                b.id_etapa
            HAVING SUM(oc.c20 + oc.c40 * 2) > 0
            LIMIT :limit OFFSET :offset;
            """
    etapa = f"{etapa}%"
    try:
        # Ejecutamos la consulta pasando el parámetro 'nombre'
        result = await database.fetch_all(query=query, values={"etapa": etapa, "limit": limit, "offset": offset})
        
        # Verificamos si no hay resultados
        if not result:
            return {"message": "No existen datos que cumplan con la etapa seleccionada"}
        
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}

@router.get("/tendencia_contenedor_dryreefer/{contenido}")
async def tendencia_contenedor_dryreefer(contenido: str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 
    ):
    query = """
            SELECT 
                n.nombre,
                oc."dry/reefer",
                SUM(oc.c20 + oc.c40 * 2) AS teus
            FROM output_containers oc
            JOIN bls b on b.code = oc.codigo
            LEFT JOIN navieras n ON n.id = b.id_naviera
            WHERE oc."dry/reefer" ILIKE :contenido
            GROUP BY
                n.nombre,
                oc."dry/reefer"
            HAVING SUM(oc.c20 + oc.c40 * 2) > 0
            LIMIT :limit OFFSET :offset;
                """
    contenido = f"{contenido}%"
    try:
        # Ejecutamos la consulta pasando el parámetro 'nombre'
        result = await database.fetch_all(query=query, values={"contenido": contenido, "limit": limit, "offset": offset})
        
        # Verificamos si no hay resultados
        if not result:
            return {"message": "No existen datos que cumplan con el tipo de contenido seleccionado"}
        
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}
    
@router.get("/tendencia_por_origen/{origen_locode}")
async def tendencia_por_origen(origen_locode: str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 
    ):
    query = """
            SELECT DISTINCT
                n.nombre,
                SUM(oc.c20 + oc.c40 * 2) AS teus,
                p.locode AS o
            FROM output_containers oc
            LEFT JOIN bls b ON b.code = oc.codigo
            LEFT JOIN navieras n ON n.id = b.id_naviera 
            LEFT JOIN tracking t ON t.id = oc.id_origen 
            LEFT JOIN paradas p  ON p.id = t.id_parada 
            WHERE p.locode ILIKE :origen_locode
            GROUP BY
                n.nombre,
                p.locode
            HAVING SUM(oc.c20 + oc.c40 * 2) > 0
            LIMIT :limit OFFSET :offset;
            """
    origen_locode = f"{origen_locode}%"
    try:
        # Ejecutamos la consulta pasando el parámetro 'nombre'
        result = await database.fetch_all(query=query, values={"origen_locode": origen_locode, "limit": limit, "offset": offset})
        
        # Verificamos si no hay resultados
        if not result:
            return {"message": "No existen datos que cumplan con el origen seleccionado"}
        
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}
    
        
@router.get("/tendencia_por_destino/{destino_locode}")
async def tendencia_por_destino(destino_locode: str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 
    ):
    query = """
            SELECT DISTINCT
                n.nombre,
                SUM(oc.c20 + oc.c40 * 2) AS teus,
                p.locode AS o
            FROM output_containers oc
            LEFT JOIN bls b ON b.code = oc.codigo
            LEFT JOIN navieras n ON n.id = b.id_naviera 
            LEFT JOIN tracking t ON t.id = oc.id_destino 
            LEFT JOIN paradas p  ON p.id = t.id_parada 
            WHERE p.locode ILIKE :destino_locode
            GROUP BY
                n.nombre,
                p.locode
            HAVING SUM(oc.c20 + oc.c40 * 2) > 0
            LIMIT :limit OFFSET :offset;
            """
    destino_locode = f"{destino_locode}%"
    try:
        # Ejecutamos la consulta pasando el parámetro 'nombre'
        result = await database.fetch_all(query=query, values={"destino_locode": destino_locode, "limit": limit, "offset": offset})
        
        # Verificamos si no hay resultados
        if not result:
            return {"message": "No existen datos que cumplan con el destino seleccionado"}
        
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}

#-------------------------------------------
#----------SUPERFILTRO VALIDACIONES-----------
#-------------------------------------------

@router.get("/superfiltro_validaciones/")
async def superfiltro_validaciones(
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0), # Índice de inicio, por defecto 
    bl_code: str = Query(None) 
    ):
    x = 0
    validaciones = {}
    query_locode_nulo = """
                        SELECT 
                            b.code
                        FROM 
                            bls b
                        INNER JOIN tracking t ON t.id_bl = b.id
                        INNER JOIN paradas p ON p.id = t.id_parada
                        WHERE 1=1
                        """
    values = {}
    if bl_code :
        query_locode_nulo += " AND b.code ILIKE :bl_code "
        values["bl_code"] = f"{bl_code}%"

    query_locode_nulo += """
                            AND b.pod IS NULL and b.pol IS NULL 
                            GROUP BY 
                            b.code, b.id, p.locode, b.pod, b.pol
                            LIMIT :limit OFFSET :offset;"""
    
    values["limit"] = limit
    values["offset"] = offset

    resultado_locode_nulo = await database.execute(query=query_locode_nulo, values=values)
    if resultado_locode_nulo:
        x += 1
        print("Entro: ",x)
        validaciones["locode_nulo"] = "si"
    else :
        validaciones["locode_nulo"] = "no" 
    
    query_cruce_contenedores = """
                                  SELECT DISTINCT ON (c.size)
                                    c.code AS codigo_container,
                                    b.code AS codigo_bl,
                                    c.size AS container_size,
                                    c.type AS container_type,
                                    b.fecha
                                FROM containers c
                                LEFT JOIN dict_containers dc 
                                    ON dc.size = c.size AND dc.type = c.type
                                INNER JOIN container_viaje cv 
                                    ON c.id = cv.id_container
                                JOIN bls b 
                                    ON b.id = cv.id_bl
                                WHERE 1=1
                                """
    values = {}

    if bl_code :
        query_cruce_contenedores += " AND b.code ILIKE :bl_code "
        values["bl_code"] = f"{bl_code}%"
    
    values["limit"] = limit
    values["offset"] = offset
    query_cruce_contenedores += """
                            AND dc.size IS NULL
                            ORDER BY c.size, c.code
                            LIMIT :limit OFFSET :offset;"""
    
    resultado_cruce_contenedores = await database.execute(query=query_cruce_contenedores, values=values)
    if resultado_cruce_contenedores:
        x += 1
        print("Entro: ",x)
        validaciones["cruce_contenedores"] = "si"
    else :
        validaciones["cruce_contenedores"] = "no"

    query_container_repetido = """
                                    SELECT 
                                        b.code AS codigo_bl,
                                        c.code::TEXT AS codigo_container, 
                                        b.nave::TEXT, 
                                        b.fecha,  -- Esta columna es de tipo DATE
                                        COUNT(*)::INTEGER AS cantidad_bls, 
                                        n.nombre::TEXT AS naviera_nombre
                                    FROM bls b
                                    JOIN container_viaje cv ON cv.id_bl = b.id 
                                    JOIN navieras n ON b.id_naviera = n.id
                                    JOIN containers c ON c.id = cv.id_container
                                    WHERE 1=1
                                    """
    values = {}
    if bl_code :
        
        query_container_repetido += " AND b.code ILIKE :bl_code "
        values["bl_code"] = f"{bl_code}%"
    
    values["limit"] = limit
    values["offset"] = offset

    query_container_repetido += """
                            GROUP BY c.code, b.nave, b.fecha, n.nombre, b.code
                            HAVING COUNT(*) > 1
                            ORDER BY cantidad_bls DESC
                            LIMIT :limit OFFSET :offset;
                            """
        
    resultado_container_repetido = await database.execute(query=query_container_repetido, values=values)
    if resultado_container_repetido:
            x += 1
            print("Entro: ",x)
            validaciones["container_repetido"] = "si"
    else :
            validaciones["container_repetido"] = "no"

    query_paradas_pol_y_pod = """
                            SELECT 
                                b.code, 
                                t.is_pol,
                                t.is_pod
                            FROM 
                                tracking t 
                            INNER JOIN 
                                bls b ON t.id_bl = b.id 
                            WHERE 1=1
                                """
    values = {}
    if bl_code :
        
        query_paradas_pol_y_pod += " AND b.code ILIKE :bl_code "
        values["bl_code"] = f"{bl_code}%"
    
    values["limit"] = limit
    values["offset"] = offset

    query_paradas_pol_y_pod += """ 
                            AND t.is_pol = 'true' AND t.is_pod = 'true'
                            LIMIT :limit OFFSET :offset;
                            """
        
    resultado_paradas_pol_y_pod = await database.execute(query=query_paradas_pol_y_pod, values=values)
    if resultado_paradas_pol_y_pod:
            x += 1
            print("Entro: ",x)
            validaciones["paradas_pol_y_pod"] = "si"
    else :
            validaciones["paradas_pol_y_pod"] = "no"

    query_paradas_con_validacion= """
                                SELECT DISTINCT
                                    subquery.code,
                                    subquery.status,
                                    subquery.is_pod,
                                    subquery.pais,
                                    subquery.id_etapa,
                                    subquery.orden::SMALLINT,
                                    destino_subquery.id_destino  -- Campo de destino agregado
                                FROM (
                                    -- Subconsulta principal
                                    SELECT 
                                        b.code,
                                        t.status,
                                        t.is_pod,
                                        p.pais,
                                        b.id_etapa,
                                        t.orden,
                                        RANK() OVER (PARTITION BY t.id_bl ORDER BY t.orden DESC) AS rk,
                                        t.id_bl
                                    FROM bls b
                                    INNER JOIN tracking t ON b.id = t.id_bl
                                    INNER JOIN paradas p ON p.id = t.id_parada
                                WHERE 1=1
                                """
    values = {}
    if bl_code :
        
        query_paradas_con_validacion += " AND b.code ILIKE :bl_code "
        values["bl_code"] = f"{bl_code}%"
        
        values["limit"] = limit
        values["offset"] = offset

    query_paradas_con_validacion += """ 
                                AND b.id_etapa = 1
                                ) subquery
                                -- Subconsulta para obtener el destino
                                LEFT JOIN (
                                    SELECT 
                                        cte.id_bl,
                                        MAX(
                                            CASE
                                                WHEN cte.rn_orden_mayor = 1 THEN cte.id
                                                ELSE NULL::INTEGER
                                            END
                                        ) AS id_destino
                                    FROM (
                                        SELECT 
                                            t.id_bl,
                                            t.id,
                                            ROW_NUMBER() OVER (PARTITION BY t.id_bl ORDER BY t.orden DESC) AS rn_orden_mayor
                                        FROM tracking t
                                    ) cte
                                    GROUP BY cte.id_bl
                                ) destino_subquery
                                ON subquery.id_bl = destino_subquery.id_bl
                                WHERE 
                                    -- Validaciones especificas
                                    (subquery.is_pod = TRUE AND subquery.rk > 1 AND subquery.pais = 'CHILE')
                                LIMIT :limit OFFSET :offset;
                                """
            
    resultado_paradas_con_validacion = await database.execute(query=query_paradas_con_validacion, values=values)
    if resultado_paradas_con_validacion:
            x += 1
            print("Entro: ",x)
            validaciones["paradas_con_validacion"] = "si"
    else :
            validaciones["paradas_con_validacion"] = "no"

    
    query_obtener_paradas_con_orden_repetida = """
                            SELECT 
                                b.code, 
                                t.id_bl AS bl_id, 
                                t.id AS id_parada,
                                t.orden as orden_repetido
                            FROM 
                                tracking t
                            INNER JOIN 
                                container_viaje c ON c.id_bl = t.id_bl
                            INNER JOIN
                                bls b ON b.id = c.id_bl
                            WHERE 1=1
                                """
    values = {}
    if bl_code :
        
        query_obtener_paradas_con_orden_repetida += " AND b.code ILIKE :bl_code "
        values["bl_code"] = f"{bl_code}%"
    
    values["limit"] = limit
    values["offset"] = offset

    query_obtener_paradas_con_orden_repetida += """ 
                            GROUP BY b.code, t.id_bl, t.id, t.orden, c.id_container
                            HAVING COUNT(*) > 1
                            LIMIT :limit OFFSET :offset;
                            """
        
    resultado_obtener_paradas_con_orden_repetida = await database.execute(query=query_obtener_paradas_con_orden_repetida, values=values)
    if resultado_obtener_paradas_con_orden_repetida:
            x += 1
            print("Entro: ",x)
            validaciones["paradas_con_orden_repetida"] = "si"
    else :
            validaciones["paradas_con_orden_repetida"] = "no"
            
    query_verificar_registros_etapa1_pais_distinto_cl = """
                                                SELECT DISTINCT 
                                                b.code AS codigo_bl,
                                                t.is_pol,
                                                p.pais,
                                                b.id_etapa
                                            FROM 
                                                bls b
                                            INNER JOIN 
                                                tracking t ON b.id = t.id_bl
                                            INNER JOIN 
                                                paradas p ON p.id = t.id_parada
                                            WHERE 1=1
                                            """
    values = {}
    if bl_code :
        
        query_verificar_registros_etapa1_pais_distinto_cl += " AND b.code ILIKE :bl_code "
        values["bl_code"] = f"{bl_code}%"
    
    values["limit"] = limit
    values["offset"] = offset

    query_verificar_registros_etapa1_pais_distinto_cl += """ 
                                                        AND
                                                        b.id_etapa = 1 AND 
                                                        p.pais <> 'CHILE' AND 
                                                        t.is_pol = true
                                                        GROUP BY 
                                                        b.code, t.is_pol, p.pais, b.id_etapa
                                                        LIMIT :limit OFFSET :offset;
                                                        """
        
    resultado_verificar_registros_etapa1_pais_distinto_cl = await database.execute(query=query_verificar_registros_etapa1_pais_distinto_cl, values=values)
    if resultado_verificar_registros_etapa1_pais_distinto_cl:
            x += 1
            print("Entro: ",x)
            validaciones["registros_etapa1_pais_distinto_cl"] = "si"
    else :
            validaciones["registros_etapa1_pais_distinto_cl"] = "no"

    query_verificar_registros_etapa2_pais_distinto_cl = """
                                                        SELECT DISTINCT 
                                                        b.code AS codigo_bl,
                                                        t.is_pod,
                                                        p.pais,
                                                        b.id_etapa
                                                    FROM 
                                                        bls b
                                                    INNER JOIN 
                                                        tracking t ON b.id = t.id_bl
                                                    INNER JOIN 
                                                        paradas p ON p.id = t.id_parada
                                                    WHERE 1=1
                                                    """
    values = {}
    if bl_code :
        
        query_verificar_registros_etapa2_pais_distinto_cl += " AND b.code ILIKE :bl_code "
        values["bl_code"] = f"{bl_code}%"
    
    values["limit"] = limit
    values["offset"] = offset

    query_verificar_registros_etapa2_pais_distinto_cl += """ 
                                                        AND
                                                            b.id_etapa = 2 AND 
                                                            p.pais NOT LIKE 'CHILE' AND 
                                                            t.is_pod = true
                                                        GROUP BY 
                                                            b.code, t.is_pod, p.pais, b.id_etapa
                                                        LIMIT :limit OFFSET :offset;
                                                        """
        
    resultado_verificar_registros_etapa2_pais_distinto_cl = await database.execute(query=query_verificar_registros_etapa2_pais_distinto_cl, values=values)
    if resultado_verificar_registros_etapa2_pais_distinto_cl:
            x += 1
            print("Entro: ",x)
            validaciones["registros_etapa2_pais_distinto_cl"] = "si"
    else :
            validaciones["registros_etapa2_pais_distinto_cl"] = "no" 

    query_validacion_bls_expo = """
                                SELECT 
                                subquery.code,
                                subquery.is_pol,
                                subquery.pais,
                                subquery.id_etapa,
                                CAST(subquery.orden AS INT)  -- Convertir a INTEGER
                            FROM (
                                SELECT 
                                    b.code,
                                    t.is_pol,
                                    p.pais,
                                    b.id_etapa,
                                    t.orden,
                                    ROW_NUMBER() OVER (PARTITION BY t.id_bl ORDER BY t.orden ASC) AS rn
                                FROM bls b
                                INNER JOIN tracking t ON b.id = t.id_bl
                                INNER JOIN paradas p ON p.id = t.id_parada
                                WHERE 1=1
                                """
    values = {}
    if bl_code :
        
        query_validacion_bls_expo += " AND b.code ILIKE :bl_code "
        values["bl_code"] = f"{bl_code}%"
    
    values["limit"] = limit
    values["offset"] = offset

    query_validacion_bls_expo += """ 
                                AND b.id_etapa = 1
                                ) AS subquery
                                WHERE subquery.rn = 1
                                AND subquery.pais NOT IN ('CHILE', 'ARGENTINA', 'BOLIVIA')
                                AND subquery.is_pol = TRUE
                                LIMIT :limit OFFSET :offset;
                                """
        
    resultado_validacion_bls_expo = await database.execute(query=query_validacion_bls_expo, values=values)
    if resultado_validacion_bls_expo:
            x += 1
            print("Entro: ",x)
            validaciones["validacion_bls_expo"] = "si"
    else :
            validaciones["validacion_bls_expo"] = "no" 

    query_validacion_bls_impo = """
                                SELECT 
                                subquery.code,
                                subquery.is_pod,
                                subquery.pais,
                                subquery.id_etapa,
                                CAST(subquery.orden AS INT)  -- Convertir a INTEGER
                            FROM (
                                SELECT 
                                    b.code,
                                    t.is_pod,
                                    p.pais,
                                    b.id_etapa,
                                    t.orden,
                                    ROW_NUMBER() OVER (PARTITION BY t.id_bl ORDER BY t.orden DESC) AS rn
                                FROM bls b
                                INNER JOIN tracking t ON b.id = t.id_bl
                                INNER JOIN paradas p ON p.id = t.id_parada 
                                WHERE 1=1
                                """
    values = {}
    if bl_code :
        
        query_validacion_bls_impo += " AND b.code ILIKE :bl_code "
        values["bl_code"] = f"{bl_code}%"
    
    values["limit"] = limit
    values["offset"] = offset

    query_validacion_bls_impo += """ 
                                AND b.id_etapa = 2
                                ) AS subquery
                                WHERE subquery.rn = 1
                                AND subquery.pais NOT IN ('CHILE', 'ARGENTINA', 'BOLIVIA', 'PARAGUAY', 'URUGUAY')
                                AND subquery.is_pod = TRUE
                                LIMIT :limit OFFSET :offset;
                                """
        
    resultado_validacion_bls_impo = await database.execute(query=query_validacion_bls_impo, values=values)
    if resultado_validacion_bls_impo:
            x += 1
            print("Entro: ",x)
            validaciones["validacion_bls_impo"] = "si"
    else :
            validaciones["validacion_bls_impo"] = "no" 

    query_obtener_diferencia_requests_importacion = """
                                                    SELECT 
                                                    f1.code,
                                                    f1.id AS first_request_id,
                                                    f1.fecha AS first_request_timestamp,
                                                    f2.id AS last_request_id,
                                                    f2.fecha AS last_request_timestamp,
                                                    TRUNC(EXTRACT(EPOCH FROM (f2.fecha - f1.fecha)) / 86400)::INTEGER AS difference_days
                                                    FROM (
                                                        SELECT 
                                                            b.code, 
                                                            r.id, 
                                                            r.fecha, 
                                                            ROW_NUMBER() OVER (PARTITION BY b.code ORDER BY r.fecha ASC) AS rn_asc
                                                        FROM requests r
                                                        JOIN bls b ON r.id_bl = b.id
                                                    WHERE 1=1
                                                    """
    values = {}
    if bl_code :
        print("Entro: ",x)
        query_obtener_diferencia_requests_importacion += " AND b.code ILIKE :bl_code "
        values["bl_code"] = f"{bl_code}%"
    
    values["limit"] = limit
    values["offset"] = offset

    query_obtener_diferencia_requests_importacion += """ 
                                                        AND r.id_respuesta IN (1, 2)
                                                        AND b.id_etapa = 2
                                                        AND r.fecha IS NOT NULL
                                                    ) f1
                                                    JOIN (
                                                        SELECT 
                                                            b.code, 
                                                            r.id, 
                                                            r.fecha, 
                                                            ROW_NUMBER() OVER (PARTITION BY b.code ORDER BY r.fecha DESC) AS rn_desc
                                                        FROM requests r
                                                        JOIN bls b ON r.id_bl = b.id
                                                        WHERE r.id_respuesta IN (1, 2)
                                                        AND b.id_etapa = 2
                                                        AND r.fecha IS NOT NULL
                                                    ) f2 ON f1.code = f2.code
                                                    WHERE f1.rn_asc = 1 
                                                    AND f2.rn_desc = 1
                                                    AND TRUNC(EXTRACT(EPOCH FROM (f2.fecha - f1.fecha)) / 86400)::INTEGER > 16
                                                    LIMIT :limit OFFSET :offset;
                                                    """
        
    resultado_obtener_diferencia_requests_importacion = await database.execute(query=query_obtener_diferencia_requests_importacion, values=values)
    if resultado_obtener_diferencia_requests_importacion:
            x += 1
            print("Entro: ",x)
            validaciones["obtener_diferencia_requests_importacion"] = "si"
    else :
            validaciones["obtener_diferencia_requests_importacion"] = "no" 

    query_obtener_requests_incompletos_expo = """
                                            WITH cte AS (
                                            SELECT 
                                                r.id_bl,
                                                t.id_parada AS id_parada,
                                                r.id AS id_request,
                                                r.mensaje
                                            FROM requests r
                                            INNER JOIN tracking t ON t.id_bl = r.id_bl
                                            INNER JOIN bls b ON b.id = t.id_bl
                                            WHERE 1=1
                                            """
    values = {}
    if bl_code :
        print("Entro: ",x)
        query_obtener_requests_incompletos_expo += " AND b.code ILIKE :bl_code "
        values["bl_code"] = f"{bl_code}%"
    
    values["limit"] = limit
    values["offset"] = offset

    query_obtener_requests_incompletos_expo += """ 
                                                AND r.id_respuesta NOT IN (1, 8)
                                                ),
                                                ids AS (
                                                    SELECT 
                                                        cte.id_bl,
                                                        MAX(cte.id_parada) AS id_pol, -- Mayor id_parada como id_pol
                                                        MAX(cte.id_parada) AS id_destino -- Mayor id_parada como id_destino
                                                    FROM cte
                                                    GROUP BY cte.id_bl
                                                ),
                                                filtered_cte AS (
                                                    SELECT DISTINCT
                                                        b.code AS bl_code,
                                                        cte.id_request,
                                                        cte.mensaje,
                                                        cte.id_parada,
                                                        ids.id_pol,
                                                        p1.lugar AS lugar_pol,
                                                        ids.id_destino,
                                                        p2.lugar AS lugar_destino,
                                                        ROW_NUMBER() OVER (PARTITION BY cte.id_bl ORDER BY cte.id_parada DESC) AS rn
                                                    FROM cte
                                                    INNER JOIN ids ON cte.id_bl = ids.id_bl
                                                    INNER JOIN bls b ON cte.id_bl = b.id
                                                    LEFT JOIN paradas p1 ON ids.id_pol = p1.id
                                                    LEFT JOIN paradas p2 ON ids.id_destino = p2.id
                                                )
                                                SELECT 
                                                    filtered_cte.bl_code,
                                                    filtered_cte.id_request,
                                                    filtered_cte.mensaje,
                                                    filtered_cte.id_parada,
                                                    filtered_cte.id_pol,
                                                    filtered_cte.lugar_pol,
                                                    filtered_cte.id_destino,
                                                    filtered_cte.lugar_destino
                                                FROM filtered_cte
                                                WHERE filtered_cte.rn = 1
                                                LIMIT :limit OFFSET :offset;
                                                """
        
    resultado_obtener_requests_incompletos_expo = await database.execute(query=query_obtener_requests_incompletos_expo, values=values) #porque me sale eso
    if resultado_obtener_requests_incompletos_expo:
            x += 1
            print("Entro: ",x)
            validaciones["obtener_requests_incompletos_expo"] = "si"
    else :
            validaciones["obtener_requests_incompletos_expo"] = "no" 


    return {"bl_code":bl_code,
                "validaciones":validaciones}