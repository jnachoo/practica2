from fastapi import APIRouter, HTTPException,Query, Depends
from database import database
from datetime import datetime
from typing import List, Annotated, Optional
from rutas.autenticacion import check_rol,get_current_user
from models import User


router = APIRouter()

@router.get("/orden_descarga/")
async def superfiltro_orden_descargas(
    id :int = Query(None),
    nombre_usuario: str = Query(None),
    fecha_creacion: str = Query(None),
    fecha_programacion: str = Query(None),
    descripcion:str = Query(None),
    order_by: str = Query(None,
        regex="^(od\\.id|u\\.nombre|od\\.fecha_creacion|od\\.fecha_programacion|od\\.descripcion)$"), # Campos válidos para ordenación
    order: str = Query("ASC", regex="^(ASC|DESC|asc|desc)$"),  # Dirección de ordenación
    limit: int = Query(500, ge=1),  # Número de resultados por página
    offset: int = Query(0, ge=0),  # Índice de inicio
):
    
    """
    Este endpoint sirve para mostrar ordenes de descarga
    """

    query = """
                SELECT od.id, u.nombre_usuario,od.fecha_creacion, od.fecha_programacion, od.descripcion FROM orden_descargas od
                join usuarios u on u.id = od.id_usuario
                where 1=1
            """
    values = {}

    if id:
        query += "AND od.id = :id"
        values["id"] = id
    if nombre_usuario:
        query += "AND u.nombre_usuario =:nombre_usuario"
        values["nombre_usuario"] = nombre_usuario
    if fecha_creacion:
        fecha_creacion = datetime.strptime(fecha_creacion, "%Y-%m-%d %H:%M:%S")
        query += "AND od.fecha_creacion =:fecha_creacion"
        values["fecha_creacion"] = fecha_creacion
    if fecha_programacion:
        fecha_programacion = datetime.strptime(fecha_programacion, "%Y-%m-%d %H:%M:%S")
        query += "AND od.fecha_programacion =:fecha_programacion"
        values["fecha_programacion"] = fecha_programacion
    if descripcion:
        query += "AND od.descripcion =:descripcion"
        values["descripcion"] = descripcion
    # Ordenacion dinamica
    if order_by:
        query += f" ORDER BY {order_by} {order}"
    # Agregar limites y desplazamiento
    query += " LIMIT :limit OFFSET :offset"
    values["limit"] = limit
    values["offset"] = offset

    try:
        # Ejecutar la consulta
        result = await database.fetch_all(query=query, values=values)
        if not result:
            raise HTTPException(status_code=404, detail="Orden de descarga no encontrado")
        return result
    except Exception as e:
        return {"error": f"Error ejecutando la consulta orden descarga: {str(e)}"}

@router.patch("/orden_descarga/")
async def orden_descarga(
    id_orden: int,
    nombre_usuario: str =Query(None),
    fecha_creacion: str = Query(None),
    fecha_programacion: str = Query(None),
    descripcion:str = Query(None),
):
    """
    Este endpoint sirve para actualizar/editar una orden de descarga
    """

    fields_usuario = []
    fields_orden = []

    values_usuario = {"id_orden":id_orden}
    values_orden = {"id_orden": id_orden}
    
    filas_actualizadas = 0
    
    # Actualizar información del usuario si se proporciona
    if nombre_usuario is not None:
        query_nombre_usuario = "SELECT id FROM usuarios WHERE nombre_usuario = :nombre_usuario"
        id_usuario = await database.fetch_val(query_nombre_usuario, {"nombre_usuario": nombre_usuario})
       
        if id_usuario is None:
            raise HTTPException(status_code=400, detail=f"El usuario {nombre_usuario} no existe.")

        fields_usuario.append("id_usuario = :id_usuario")
        values_usuario["id_usuario"] = id_usuario
    
    # Actualizar información de la orden si se proporciona
    if fecha_creacion is not None:
        fecha_creacion = datetime.strptime(fecha_creacion, "%Y-%m-%d %H:%M:%S")
        fields_orden.append("fecha_creacion = :fecha_creacion")
        values_orden["fecha_creacion"] = fecha_creacion

    if fecha_programacion is not None:
        fecha_programacion = datetime.strptime(fecha_programacion, "%Y-%m-%d %H:%M:%S")
        fields_orden.append("fecha_programacion = :fecha_programacion")
        values_orden["fecha_programacion"] = fecha_programacion
    
    if descripcion is not None:
        fields_orden.append("descripcion = :descripcion")
        values_orden["descripcion"] = descripcion
    
    # Validar que al menos un campo se proporcionó
    if not (fields_usuario or fields_orden):
        raise HTTPException(status_code=400, detail="No se proporcionaron campos para actualizar")

    # Ejecutar la consulta para actualizar el usuario si hay campos
    if fields_usuario:
        query_usuario = f"""
            UPDATE orden_descargas
            SET {', '.join(fields_usuario)}
            WHERE id = :id_orden
            RETURNING id;
        """
        resultado_usuario = await database.execute(query=query_usuario, values=values_usuario)
        if resultado_usuario:
            filas_actualizadas += 1

    # Ejecutar la consulta para actualizar la orden si hay campos
    if fields_orden:
        query_orden = f"""
            UPDATE orden_descargas
            SET {', '.join(fields_orden)}
            WHERE id = :id_orden
            RETURNING id;
        """
        resultado_orden = await database.execute(query=query_orden, values=values_orden)
        if resultado_orden:
            filas_actualizadas += 1

    # Validar si algo fue actualizado
    if filas_actualizadas == 0:
        raise HTTPException(status_code=404, detail="No se encontró el registro para actualizar")

    return {"mensaje": f"Se actualizaron {filas_actualizadas} tablas con éxito"}


@router.post("/orden_descarga/")
async def orden_descarga(
    nombre_usuario: str,
    fecha_creacion: str,
    fecha_programacion: str,
    descripcion:str = Query(None),
): 
    """
    Este endpoint sirve para agregar una orden de descarga
    """
    query = "SELECT id from usuarios where nombre_usuario ILIKE :nombre_usuario"
    nombre_usuario = f"{nombre_usuario}%"
    id_usuario = await database.fetch_val(query, {"nombre_usuario":nombre_usuario})
    
    if id_usuario == None:
        raise HTTPException(status_code=404, detail=f"Usuario {nombre_usuario} no encontrado")
    
    try:
        if not nombre_usuario or not fecha_creacion or not fecha_programacion:
            raise HTTPException(
                status_code=404, 
                detail="Los campos nombre usuario, fecha creacion, fecha programacion")
                # Convertir 'fecha' a tipo datetime

        # Se formatea la fecha de creacion
        try:
            fecha_creacion = datetime.strptime(fecha_creacion, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Formato de 'fecha' inválido. Use el formato YYYY-MM-DD."
            )
        # Se formatea la fecha de programacion
        try:
            fecha_programacion = datetime.strptime(fecha_programacion, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Formato de 'fecha' inválido. Use el formato YYYY-MM-DD."
            )

        # Consulta para insertar datos
        query_orden = """
            INSERT INTO orden_descargas (
                id_usuario, fecha_creacion, fecha_programacion, descripcion
            ) VALUES (
                :id_usuario, :fecha_creacion, :fecha_programacion, :descripcion
            )
            RETURNING id;
        """

        values_orden = {
            "id_usuario" : id_usuario,
            "fecha_creacion" : fecha_creacion,
            "fecha_programacion" : fecha_programacion,
            "descripcion" : descripcion
        }
        id_orden = await database.execute(query=query_orden,values=values_orden)

        if not id_orden:
            raise HTTPException(status_code=500, detail="ERROR al insertar en orden descargas")
        return {"mensaje": f"Registro creado con exito en orden descargas, el id es:{id_orden}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al insertar el registro en 'orden descargas': {str(e)}")