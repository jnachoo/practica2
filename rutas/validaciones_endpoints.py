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
                b.code, b.id, p.locode, b.pod, b.pol;
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
                SELECT * FROM consultar_contenedores_distinct()
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