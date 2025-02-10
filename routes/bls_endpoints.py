from fastapi import APIRouter, HTTPException,Query, Depends
from db.database import database
from datetime import datetime
from typing import List, Annotated, Optional
from routes.autenticacion import check_rol,get_current_user
from models.models import User


router = APIRouter()





#-----------GET----------
#-----------GET----------
#-----------GET----------

# Ejemplo de url
# http://localhost:8000/bls/?pol=CL&order_by=b.fecha&order=desc&fecha=2024-07-20&etapa=expo
@router.get("/bls/")
async def super_filtro_bls(
    bl_code: str = Query(None),  # Código del BL
    etapa: str = Query(None),  # Nombre de la etapa
    naviera: str = Query(None),  # Nombre de la naviera
    status: str = Query(None),  # Descripción del estado
    fecha: str = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$"),  # Fecha en formato YYYY-MM-DD
    fecha_proxima_revision: str = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$"),  # Próxima revisión
    order_by: str = Query(None,
        regex="^(b\\.code|e\\.nombre|n\\.nombre|sb\\.descripcion_status|b\\.fecha|b\\.proxima_revision|b\\.id)$"), # Campos válidos para ordenación
    order: str = Query("ASC", regex="^(ASC|DESC|asc|desc)$"),  # Dirección de ordenación
    limit: int = Query(500, ge=1),  # Número de resultados por página
    offset: int = Query(0, ge=0),  # Índice de inicio
):
    # Consulta base
    query = """
        SELECT b.id, b.code AS bl_code, e.nombre AS etapa, n.nombre AS naviera, 
            sb.descripcion_status AS status, 
            TO_CHAR(b.fecha, 'YYYY-MM-DD') AS fecha, 
            TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') AS fecha_proxima_revision
        FROM bls b
        JOIN etapa e ON e.id = b.id_etapa
        JOIN navieras n ON n.id = b.id_naviera
        JOIN status_bl sb ON b.id_status = sb.id
        WHERE 1=1
    """
    values = {}

    # Agregar filtros dinámicos
    if bl_code:
        query += " AND b.code ILIKE :bl_code"
        values["bl_code"] = f"{bl_code}%"
    if etapa:
        query += " AND e.nombre ILIKE :etapa"
        values["etapa"] = f"{etapa}%"
    if naviera:
        query += " AND n.nombre ILIKE :naviera"
        values["naviera"] = f"{naviera}%"
    if status:
        query += " AND sb.descripcion_status ILIKE :status"
        values["status"] = f"{status}%"
    if fecha:
        query += " AND TO_CHAR(b.fecha, 'YYYY-MM-DD') = :fecha"
        values["fecha"] = fecha
    if fecha_proxima_revision:
        query += " AND TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') = :fecha_proxima_revision"
        values["fecha_proxima_revision"] = fecha_proxima_revision

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
            raise HTTPException(status_code=404, detail="BL no encontrado")
        return result
    except Exception as e:
        return {"error": f"Error ejecutando la consulta bls: {str(e)}"}

#------------------------------------------------------------
#------------------ENDPOINTS DROPDOWN------------------------
#------------------------------------------------------------

@router.get("/bls/nombre_navieras")
async def bls_nombre_navieras():
    query = "SELECT nombre FROM navieras order by id"
    resultado = await database.fetch_all(query)
    if not resultado:
        raise HTTPException(status_code=404, detail="Navieras no retornadas")
    return resultado

@router.get("/bls/nombre_etapa")
async def bls_nombre_etapa():
    query = "SELECT nombre FROM etapa order by id"
    resultado = await database.fetch_all(query)
    if not resultado:
        raise HTTPException(status_code=404, detail="Etapas no retornadas")
    return resultado

@router.get("/bls/descripcion_status_bl")
async def bls_nombre_status_bl():
    query = "SELECT descripcion_status FROM status_bl order by id;"
    resultado = await database.fetch_all(query)
    if not resultado:
        raise HTTPException(status_code=404, detail="Status no retornadas")
    return resultado



@router.get("/bls/fecha/{fecha}")
async def bls_fecha(
    fecha: str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):

    query = """
            SELECT b.id, b.code AS bl_code, e.nombre AS etapa, n.nombre AS naviera, 
                sb.descripcion_status AS status, 
                TO_CHAR(b.fecha, 'YYYY-MM-DD') AS fecha, 
                TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') AS fecha_proxima_revision
            FROM bls b
            JOIN etapa e ON e.id = b.id_etapa
            JOIN navieras n ON n.id = b.id_naviera
            JOIN status_bl sb ON b.id_status = sb.id
            WHERE 1=1
            """
    values = {}
    
    if fecha and len(fecha)==4:
        fecha = int(fecha)
        query += " AND EXTRACT(YEAR FROM b.fecha) = :fecha"
        values["fecha"] = fecha
        mensaje = f"Los bls encontrados en el año {fecha} son:"
    elif fecha and len(fecha)==10:
        fecha = datetime.strptime(fecha, "%Y-%m-%d").date()
        query += " AND b.fecha >= :fecha"
        values["fecha"] = fecha
        mensaje = f"Los bls encontrados desde {fecha} hasta el día de hoy son:"
    elif fecha and len(fecha)==21:
        nueva_fecha = fecha.split('+')
        fecha_i = datetime.strptime(nueva_fecha[0],"%Y-%m-%d").date()
        fecha_f = datetime.strptime(nueva_fecha[1],"%Y-%m-%d").date()
        query += " AND b.fecha >= :fecha_i AND b.fecha <= :fecha_f"
        values["fecha_i"] = fecha_i
        values["fecha_f"] = fecha_f
        mensaje = f"Los bls encontrados desde {fecha_i} hasta {fecha_f} son:"
    else:
        return {"mensaje":"debes usar el formato: para año AAAA, desde: AAAA-MM-DD, desde-hasta: AAAA-MM-DD+AAAA-MM-DD"}
        
        # Agregar orden, límite y desplazamiento
    query += " ORDER BY b.code LIMIT :limit OFFSET :offset"
    values["limit"] = limit
    values["offset"] = offset

    try:
        results = await database.fetch_all(query=query, values=values)
        if not results:
              # Función adicional que mencionaste
            raise HTTPException(status_code=404, detail="BLs no encontrados")
        return {
            "mensaje": mensaje,
            "results": results
        }
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta BLs: {str(e)}"}

@router.get("/bls/id/{id}")
async def ver_bls_id(
    id:int,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """   
                select b.id,b.code as bl_code, e.nombre  as etapa, n.nombre  as naviera ,sb.descripcion_status as status, 
                TO_CHAR(b.fecha, 'YYYY-MM-DD') as fecha ,TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') as fecha_proxima_revision   
                from bls b --875.294
                join etapa e on e.id =b.id_etapa
                join navieras n on n.id =b.id_naviera
                join status_bl sb on b.id_status = sb.id
                where b.id = :id
                LIMIT :limit OFFSET :offset;
            """
    
    # Si no se encuentra la naviera, devolver el listado completo con un mensaje
    try:
        result = await database.fetch_all(query=query, values={"id": id,"limit": limit, "offset": offset})
        if not result:
            
            raise HTTPException(status_code=404, detail="ID de bl no encontrado")
        return result
    except Exception as e: return {"error": f"error al ejecutar la consulta filtro_id_bls{str(e)} "}

@router.get("/bls/code/{code}")
async def ver_bls_id(
    code:str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """                
                select b.id,b.code as bl_code, e.nombre  as etapa, n.nombre  as naviera ,sb.descripcion_status as status, 
                TO_CHAR(b.fecha, 'YYYY-MM-DD') as fecha ,TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') as fecha_proxima_revision   
                from bls b --875.294
                join etapa e on e.id =b.id_etapa
                join navieras n on n.id =b.id_naviera
                join status_bl sb on b.id_status = sb.id
                where b.code like :code
                LIMIT :limit OFFSET :offset;
            """
    code = f"{code}%"
    # Si no se encuentra la naviera, devolver el listado completo con un mensaje
    try:
        result = await database.fetch_all(query=query, values={"code": code,"limit": limit, "offset": offset})
        if not result:
            
            raise HTTPException(status_code=404, detail="Code de bl no encontrado")
        return result
    except Exception as e: return {"error": f"error al ejecutar la consulta filtro_code_bls{str(e)} "}


@router.get("/bls/naviera/{nombre}")
async def ver_bls_id(
    nombre:str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """                
                select b.id,b.code as bl_code, e.nombre  as etapa, n.nombre  as naviera ,sb.descripcion_status as status, 
                TO_CHAR(b.fecha, 'YYYY-MM-DD') as fecha ,TO_CHAR(b.proxima_revision, 'YYYY-MM-DD') as fecha_proxima_revision   
                from bls b --875.294
                join etapa e on e.id =b.id_etapa
                join navieras n on n.id =b.id_naviera
                join status_bl sb on b.id_status = sb.id
                where n.nombre like :nombre
                LIMIT :limit OFFSET :offset;
            """
    nombre = nombre.upper()
    nombre = f"{nombre}%"
    
    # Si no se encuentra la naviera, devolver el listado completo con un mensaje
    try:
        result = await database.fetch_all(query=query, values={"nombre": nombre,"limit": limit, "offset": offset})
        if not result:
            
            raise HTTPException(status_code=404, detail="Naviera de bl no encontrado")
        return result
    except Exception as e: return {"error": f"error al ejecutar la consulta filtro_navieras_bls{str(e)} "}

@router.patch("/bls/{id_bls}")
async def actualizar_parcial_bls(
    id_bl: int,
    #naviera: Optional[str] = Query(None, description="Selecciona una naviera", enum=navieras_enum),
    #naviera: Annotated[Optional[str], Depends(naviera_query)],
    bl_code: str = Query(None),
    etapa: str = Query(None),
    naviera: str = Query(None),
    status: str = Query(None),
    fecha: str = Query(None),
    fecha_proxima_revision: str = Query(None),
    current_user: User = Depends(get_current_user)
):
    # Verificar si el rol del usuario registrado puede acceder a la función
    check_rol(current_user, [1,2])

    # Construir las consultas dinámicamente
    fields_bls = []
    fields_etapa = []
    fields_navieras = []
    fields_status = []

    values_bls = {"id_bl": id_bl}
    values_etapa = {"id_bl": id_bl}
    values_navieras = {"id_bl": id_bl}
    values_status = {"id_bl": id_bl}

    # Variables de ayuda para la edición de filas
    ayuda_etapa = 0
    ayuda_naviera = 0
    ayuda_status = 0

    # Campos para la tabla `bls`
    if bl_code is not None:
        fields_bls.append("code = :bl_code")
        values_bls["bl_code"] = bl_code

    if fecha is not None:
        fecha = datetime.strptime(fecha, "%Y-%m-%d").date()
        fields_bls.append("fecha = :fecha")
        values_bls["fecha"] = fecha

    if fecha_proxima_revision is not None:
        fecha_proxima_revision = datetime.strptime(fecha_proxima_revision, "%Y-%m-%d").date()
        fields_bls.append("proxima_revision = :fecha_proxima_revision")
        values_bls["fecha_proxima_revision"] = fecha_proxima_revision

    # Campos para la tabla `etapa`
    if etapa is not None:
        etapa = etapa.upper()
        query_check_etapa = "SELECT id FROM etapa WHERE nombre = :etapa"
        ayuda_etapa = await database.fetch_val(query_check_etapa, {"etapa": etapa})
        print(ayuda_etapa)
        if ayuda_etapa == 0 or ayuda_etapa is None:
            raise HTTPException(status_code=400, detail="La etapa no existe en la tabla 'bls'.")
        fields_etapa.append("id_etapa = :ayuda_etapa")
        values_etapa["ayuda_etapa"] = ayuda_etapa

    # Campos para la tabla `navieras`
    if naviera is not None:
        naviera = naviera.upper()
        query_check_naviera = "SELECT id FROM navieras WHERE nombre = :naviera"
        ayuda_naviera = await database.fetch_val(query_check_naviera, {"naviera": naviera})
        print(ayuda_naviera)
        if ayuda_naviera == 0 or ayuda_naviera is None:
            raise HTTPException(status_code=400, detail="La naviera no existe en la tabla 'bls'.")
        fields_navieras.append("id_naviera = :ayuda_naviera")
        values_navieras["ayuda_naviera"] = ayuda_naviera

    # Campos para la tabla `status_bl`
    if status is not None:
        #status = status.upper()
        query_check_status = "SELECT id FROM status_bl WHERE descripcion_status = :status"
        ayuda_status = await database.fetch_val(query_check_status, {"status": status})
        print("Status",status)
        print("query:",ayuda_status)
        if ayuda_status == 0 or ayuda_status is None:
            raise HTTPException(status_code=400, detail="El status no existe en la tabla 'bls'.")
        fields_status.append("id_status = :ayuda_status")
        values_status["ayuda_status"] = ayuda_status

    # Validar que al menos un campo se proporcionó
    if not (fields_bls or fields_etapa or fields_navieras or fields_status):
        raise HTTPException(status_code=400, detail="No se proporcionaron campos para actualizar")

    # Bandera para saber si algo fue actualizado
    filas_actualizadas = 0

    # Ejecutar la consulta para la tabla `bls` si hay campos
    if fields_bls:
        query_bls = f"""
            UPDATE bls
            SET {', '.join(fields_bls)}
            WHERE id = :id_bl
            RETURNING id;
        """
        resultado_bls = await database.execute(query=query_bls, values=values_bls)
        if resultado_bls:
            filas_actualizadas += 1
        print(f"Resultado del update bls: {resultado_bls}")

    # Ejecutar la consulta para la tabla `etapa` si hay campos
    if fields_etapa:
        query_etapa = f"""
            UPDATE bls
            SET {', '.join(fields_etapa)}
            WHERE id = :id_bl
            RETURNING id;
        """
        resultado_etapa = await database.execute(query=query_etapa, values=values_etapa)
        if resultado_etapa:
            filas_actualizadas += 1
        print(f"Resultado del update etapa: {resultado_etapa}")

    # Ejecutar la consulta para la tabla `navieras` si hay campos
    if fields_navieras:
        query_navieras = f"""
            UPDATE bls
            SET {', '.join(fields_navieras)}
            WHERE id = :id_bl
            RETURNING id;
        """
        resultado_navieras = await database.execute(query=query_navieras, values=values_navieras)
        if resultado_navieras:
            filas_actualizadas += 1
        print(f"Resultado del update navieras: {resultado_navieras}")

    # Ejecutar la consulta para la tabla `status_bl` si hay campos
    if fields_status:
        query_status = f"""
            UPDATE bls
            SET {', '.join(fields_status)}
            WHERE id = :id_bl
            RETURNING id;
        """
        resultado_status = await database.execute(query=query_status, values=values_status)
        if resultado_status:
            filas_actualizadas += 1
        print(f"Resultado del update status_bl: {resultado_status}")

    # Validar si algo fue actualizado
    if filas_actualizadas == 0:
        raise HTTPException(status_code=404, detail="No se encontró el registro para actualizar")

    return {"mensaje": f"Se actualizaron {filas_actualizadas} tablas con éxito"}



@router.post("/bls/")
async def insertar_bls(
    code: str,
    naviera: str,
    etapa: str,
    fecha: str,
    proxima_revision: str,
    nave: str = Query(None),
    mercado: str = Query(None),
    revisado_con_exito: bool = Query(None),
    manual_pendiente: bool = Query(None),
    no_revisar: bool = Query(None),
    revisado_hoy: bool = Query(None),
    html_descargado: bool = Query(None),
    current_user: User = Depends(get_current_user)
):
    """
    Endpoint para insertar un nuevo registro en la tabla bls.
    """

    # Verificar si el rol del usuario registrado puede acceder a la función
    check_rol(current_user, [1])

    naviera = naviera.upper()
    query = "SELECT id FROM navieras WHERE nombre ILIKE :naviera"
    naviera = f"{naviera}%"
    id_naviera = await database.fetch_val(query, {"naviera":naviera})
    if id_naviera == None:
        raise HTTPException(status_code=404, detail="Id naviera no retornado")
    
    etapa = etapa.upper()
    query = "SELECT id FROM etapa WHERE nombre ILIKE :etapa"
    etapa = f"{etapa}%"
    id_etapa = await database.fetch_val(query, {"etapa":etapa})
    if id_etapa == None:
        raise HTTPException(status_code=404, detail="Id etapa no retornado") 

    try:
        # Validar parámetros obligatorios
        if not code or not id_naviera or not id_etapa or not proxima_revision or not fecha:
            raise HTTPException(
                status_code=400,
                detail="Los campos 'code', 'id_naviera', 'id_etapa', 'proxima_revision', 'fecha' son obligatorios."
            )

        # Convertir 'fecha' a tipo datetime
        try:
            fecha = datetime.strptime(fecha, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Formato de 'fecha' inválido. Use el formato YYYY-MM-DD."
            )

        # Convertir 'proxima_revision' a tipo datetime
        try:
            proxima_revision = datetime.strptime(proxima_revision, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Formato de 'proxima_revision' inválido. Use el formato YYYY-MM-DD."
            )

        # Consulta SQL para insertar el registro en la tabla 'bls'
        query_bls = """
            INSERT INTO bls (
                code, id_naviera, id_etapa, fecha, proxima_revision,
                nave,id_status,id_carga, mercado, revisado_con_exito, manual_pendiente,
                no_revisar, revisado_hoy, html_descargado
            ) VALUES (
                :code, :id_naviera, :id_etapa, :fecha, :proxima_revision,
                :nave,18,211, :mercado, :revisado_con_exito, :manual_pendiente,
                :no_revisar, :revisado_hoy, :html_descargado
            )
            RETURNING id;
        """

        # Valores para la consulta
        values_bls = {
            "code": code,
            "id_naviera": id_naviera,
            "id_etapa": id_etapa,
            "fecha": fecha,
            "proxima_revision": proxima_revision,
            "nave": nave,
            "mercado": mercado,
            "revisado_con_exito": revisado_con_exito,
            "manual_pendiente": manual_pendiente,
            "no_revisar": no_revisar,
            "revisado_hoy": revisado_hoy,
            "html_descargado": html_descargado,
        }

        # Ejecutar la consulta
        id_bls = await database.execute(query=query_bls, values=values_bls)

        if not id_bls:
            raise HTTPException(status_code=500, detail="Error al insertar en bls, no hay id.")

        return {"message": "Registro creado exitosamente en la tabla 'bls'.", "id_bls": id_bls}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al insertar el registro en 'bls': {str(e)}")
    




