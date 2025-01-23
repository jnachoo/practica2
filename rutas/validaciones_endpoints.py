from fastapi import APIRouter, HTTPException,Query
from database import database
from datetime import datetime

router = APIRouter()

#---------------------------------
#-------VALIDACION EN LINEA-------
#---------------------------------

@router.get("/validacion_locode_nulo")
async def val():
    query = """
                SELECT * FROM verificar_locode_nulo();
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de locode nulo."}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de locode nulo: {str(e)}"}

@router.get("/validacion_cruce_contenedores")
async def val():
    query = """
                SELECT * FROM consultar_contenedores_distinct();
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de cruce con diccionario de contenedores."}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación del cruce con diccionario de contenedores: {str(e)}"}
    
@router.get("/validacion_containers_repetidos")
async def val():
    query = """
                 SELECT * FROM container_repetido();
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de container repetido."}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de container repetido: {str(e)}"}
    
@router.get("/validacion_paradas_pol_pod")
async def val():
    query = """
                SELECT * FROM obtener_paradas_pol_pod();
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de paradas que sean pol y pod."}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta: {str(e)}"}
    
@router.get("/validacion_orden_repetida")
async def val():
    query = """
                 SELECT * FROM obtener_paradas_con_orden_repetida();
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de orden repetida en la tabla de paradas."}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de orden repetida en tabla de paradas: {str(e)}"}
   
@router.get("/validacion_impo_distinta_CL")
async def val():
    query = """
                 SELECT * FROM verificar_registros_etapa2_pais_distinto_cl();
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de parada POD distinta a Chile en importación."}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de parada POD distinta a Chile en importación: {str(e)}"}
    
@router.get("/validacion_bls_impo")
async def val():
    query = """
                 SELECT * FROM validar_bls_impo(2);
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de país de destino dentro de ('CL', 'AR', 'BO', 'PY', 'UY')"}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de país de destino dentro de ('CL', 'AR', 'BO', 'PY', 'UY'): {str(e)}"}

@router.get("/validacion_expo_distinta_CL")
async def val():
    query = """
                 SELECT * FROM verificar_registros_etapa1_pais_distinto_cl();
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de parada POL distinta a Chile en exportación"}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de parada POL distinta a Chile en exportación: {str(e)}"} 

@router.get("/validacion_bls_expo")
async def val():
    query = """
                 SELECT * FROM validar_bls_expo(1);
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de país de origen dentro de ('CL', 'AR', 'BO')"}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de país de origen dentro de ('CL', 'AR', 'BO'): {str(e)}"}
    
@router.get("/validacion_paradas_expo")
async def val():
    query = """
                SELECT * FROM obtener_paradas_con_validacion();
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de país de destino y POD distinto de Chile"}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de país de destino y POD distinto de Chile: {str(e)}"}


@router.get("/validacion_dias_impo")
async def val():
    query = """
                SELECT * FROM obtener_diferencia_requests_importacion();
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de request en importaciones"}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de request en importaciones: {str(e)}"}
    
@router.get("/validacion_requests_expo")
async def val():
    query = """
                SELECT * FROM obtener_requests_expo();
                """
    try:
        result = await database.fetch_all(query=query)
        if not result:
            return {"message": "No existen datos que no cumplan con la validación de request en exportaciones"}
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de request en exportaciones: {str(e)}"}
    
    #locode = f"'{locode}'"

#-------------------------------------------
#----------VALIDACIONES TENDENCIA-----------
#-------------------------------------------

@router.get("/tendencia_navieras/{nombre}")
async def tendencia_navieras(nombre: str):
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
                HAVING SUM(oc.c20 + oc.c40 * 2) > 0;
                """
    nombre = f"{nombre}%"
    try:
        # Ejecutamos la consulta pasando el parámetro 'nombre'
        result = await database.fetch_all(query=query, values={"nombre": nombre})
        
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
async def tendencia_etapa(etapa: str):
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
            HAVING SUM(oc.c20 + oc.c40 * 2) > 0;
                """
    etapa = f"{etapa}%"
    try:
        # Ejecutamos la consulta pasando el parámetro 'nombre'
        result = await database.fetch_all(query=query, values={"etapa": etapa})
        
        # Verificamos si no hay resultados
        if not result:
            return {"message": "No existen datos que cumplan con la etapa seleccionada"}
        
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}

@router.get("/tendencia_contenedor_dryreefer/{contenido}")
async def tendencia_contenedor_dryreefer(contenido: str):
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
            HAVING SUM(oc.c20 + oc.c40 * 2) > 0;;
                """
    contenido = f"{contenido}%"
    try:
        # Ejecutamos la consulta pasando el parámetro 'nombre'
        result = await database.fetch_all(query=query, values={"contenido": contenido})
        
        # Verificamos si no hay resultados
        if not result:
            return {"message": "No existen datos que cumplan con el tipo de contenido seleccionado"}
        
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}
    
@router.get("/tendencia_por_origen/{origen_locode}")
async def tendencia_por_origen(origen_locode: str):
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
            HAVING SUM(oc.c20 + oc.c40 * 2) > 0;
            """
    origen_locode = f"{origen_locode}%"
    try:
        # Ejecutamos la consulta pasando el parámetro 'nombre'
        result = await database.fetch_all(query=query, values={"origen_locode": origen_locode})
        
        # Verificamos si no hay resultados
        if not result:
            return {"message": "No existen datos que cumplan con el origen seleccionado"}
        
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}
    
        
@router.get("/tendencia_por_destino/{destino_locode}")
async def tendencia_por_destino(destino_locode: str):
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
            HAVING SUM(oc.c20 + oc.c40 * 2) > 0;
            """
    destino_locode = f"{destino_locode}%"
    try:
        # Ejecutamos la consulta pasando el parámetro 'nombre'
        result = await database.fetch_all(query=query, values={"destino_locode": destino_locode})
        
        # Verificamos si no hay resultados
        if not result:
            return {"message": "No existen datos que cumplan con el destino seleccionado"}
        
        return result
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}