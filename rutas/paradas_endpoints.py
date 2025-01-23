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
        SELECT b.code AS bl_code, t.orden, t.status, p.locode, p.pais, p.lugar,
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

@router.get("/paradas/bl_code/{bl_code}")
async def ver_paradas(
    bl_code: str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 50
    offset: int = Query(0, ge=0)  # Índice de inicio, por defecto 0
    ):
    query = """
                select b.code as bl_code,t.orden,t.status,p.locode,p.pais,p.lugar,
                t.is_pol ,t.is_pod 
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
                select b.code as bl_code,t.orden,t.status,p.locode,p.pais,p.lugar,
                t.is_pol ,t.is_pod 
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
                select b.code as bl_code,t.orden,t.status,p.locode,p.pais,p.lugar,
                t.is_pol ,t.is_pod 
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
    
    
@router.patch("/paradas/{id_parada}")
async def actualizar_parcial_parada(
    id_parada: int,
    orden: str = Body(None),
    status: int = Body(None),
    locode: str = Body(None),
    pais: str = Body(None),
    lugar: str = Body(None),
    is_pol: bool = Body(None),
    is_pod: bool = Body(None),
):
    # Construir la consulta dinámicamente
    fields = []
    values = {"id_parada": id_parada}
    if orden is not None:
        fields.append("orden = :orden")
        values["orden"] = orden
    if status is not None:
        fields.append("status = :status")
        values["status"] = status
    if locode is not None:
        fields.append("locode = :locode")
        values["locode"] = locode
    if pais is not None:
        fields.append("pais = :pais")
        values["pais"] = pais
    if lugar is not None:
        fields.append("lugar = :lugar")
        values["lugar"] = lugar
    if is_pol is not None:
        fields.append("is_pol = :is_pol")
        values["is_pol"] = is_pol
    if is_pod is not None:
        fields.append("is_pod = :is_pod")
        values["is_pod"] = is_pod


    if not fields:
        raise HTTPException(status_code=400, detail="No se proporcionaron campos para actualizar")

    query = f"UPDATE tracking t SET {', '.join(fields)} WHERE t.id = :id_parada"

    try:
        await database.execute(query=query, values=values)
        return {"message": "Container actualizado parcialmente"}
    except Exception as e:
        return {"error": f"Error al actualizar parcialmente el container: {str(e)}"}

    #en este codigo es necesario ocupar Body?, por que no solamente usar str, int, boolean, segun corresponda?.