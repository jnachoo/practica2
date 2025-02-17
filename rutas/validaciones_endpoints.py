from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from decimal import Decimal
import io
import matplotlib.pyplot as plt
import pandas as pd
from io import BytesIO
import os
import httpx
import asyncio

# Dependencia que inyecta la sesión asíncrona; asegúrate de tenerla definida en tu módulo database.
from database import get_db

router = APIRouter()

# ---------------------------------
# ------- VALIDACIÓN EN LÍNEA -------
# ---------------------------------

@router.get("/validacion_locode_nulo")
async def validacion_locode_nulo(
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
        SELECT 
            b.code,
            b.id,
            p.locode,
            b.pod,
            b.pol
        FROM bls b
        INNER JOIN tracking t ON t.id_bl = b.id
        INNER JOIN paradas p ON p.id = t.id_parada
        WHERE b.pod IS NULL AND b.pol IS NULL
        GROUP BY b.code, b.id, p.locode, b.pod, b.pol
        LIMIT :limit OFFSET :offset;
    """
    try:
        result = await db.execute(text(sql), {"limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que no cumplan con la validación de locode nulo."}
        return rows
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de locode nulo: {str(e)}"}


@router.get("/validacion_cruce_contenedores")
async def validacion_cruce_contenedores(
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
        SELECT DISTINCT ON (c.size)
            c.code AS codigo_container,
            b.code AS codigo_bl,
            c.size AS container_size,
            c.type AS container_type,
            b.fecha
        FROM containers c
        LEFT JOIN dict_containers dc ON dc.size = c.size AND dc.type = c.type
        INNER JOIN container_viaje cv ON c.id = cv.id_container
        JOIN bls b ON b.id = cv.id_bl
        WHERE dc.size IS NULL
        ORDER BY c.size, c.code
        LIMIT :limit OFFSET :offset;
    """
    try:
        result = await db.execute(text(sql), {"limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que no cumplan con la validación de cruce con diccionario de contenedores."}
        return rows
    except Exception as e:
        return {"error": f"Error al ejecutar la validación del cruce con diccionario de contenedores: {str(e)}"}


@router.get("/validacion_containers_repetidos")
async def validacion_containers_repetidos(
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
         SELECT * FROM container_repetido()
         LIMIT :limit OFFSET :offset;
    """
    try:
        result = await db.execute(text(sql), {"limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que no cumplan con la validación de container repetido."}
        return rows
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de container repetido: {str(e)}"}


@router.get("/validacion_paradas_pol_pod")
async def validacion_paradas_pol_pod(
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
         SELECT * FROM obtener_paradas_pol_pod()
         LIMIT :limit OFFSET :offset;
    """
    try:
        result = await db.execute(text(sql), {"limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que no cumplan con la validación de paradas que sean pol y pod."}
        return rows
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta: {str(e)}"}


@router.get("/validacion_orden_repetida")
async def validacion_orden_repetida(
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
         SELECT * FROM obtener_paradas_con_orden_repetida()
         LIMIT :limit OFFSET :offset;
    """
    try:
        result = await db.execute(text(sql), {"limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que no cumplan con la validación de orden repetida en la tabla de paradas."}
        return rows
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de orden repetida en tabla de paradas: {str(e)}"}


@router.get("/validacion_impo_distinta_CL")
async def validacion_impo_distinta_CL(
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
         SELECT * FROM verificar_registros_etapa2_pais_distinto_cl()
         LIMIT :limit OFFSET :offset;
    """
    try:
        result = await db.execute(text(sql), {"limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que no cumplan con la validación de parada POD distinta a Chile en importación."}
        return rows
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de parada POD distinta a Chile en importación: {str(e)}"}


@router.get("/validacion_bls_impo")
async def validacion_bls_impo(
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
         SELECT * FROM validar_bls_impo(2)
         LIMIT :limit OFFSET :offset;
    """
    try:
        result = await db.execute(text(sql), {"limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que no cumplan con la validación de país de destino dentro de ('CL', 'AR', 'BO', 'PY', 'UY')"}
        return rows
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de país de destino dentro de ('CL', 'AR', 'BO', 'PY', 'UY'): {str(e)}"}


@router.get("/validacion_expo_distinta_CL")
async def validacion_expo_distinta_CL(
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
         SELECT * FROM verificar_registros_etapa1_pais_distinto_cl()
         LIMIT :limit OFFSET :offset;
    """
    try:
        result = await db.execute(text(sql), {"limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que no cumplan con la validación de parada POL distinta a Chile en exportación"}
        return rows
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de parada POL distinta a Chile en exportación: {str(e)}"}


@router.get("/validacion_bls_expo")
async def validacion_bls_expo(
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
         SELECT * FROM validar_bls_expo(1)
         LIMIT :limit OFFSET :offset;
    """
    try:
        result = await db.execute(text(sql), {"limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que no cumplan con la validación de país de origen dentro de ('CL', 'AR', 'BO')"}
        return rows
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de país de origen dentro de ('CL', 'AR', 'BO'): {str(e)}"}


@router.get("/validacion_paradas_expo")
async def validacion_paradas_expo(
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
         SELECT * FROM obtener_paradas_con_validacion()
         LIMIT :limit OFFSET :offset;
    """
    try:
        result = await db.execute(text(sql), {"limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que no cumplan con la validación de país de destino y POD distinto de Chile"}
        return rows
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de país de destino y POD distinto de Chile: {str(e)}"}


@router.get("/validacion_dias_impo")
async def validacion_dias_impo(
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
         SELECT * FROM obtener_diferencia_requests_importacion()
         LIMIT :limit OFFSET :offset;
    """
    try:
        result = await db.execute(text(sql), {"limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que no cumplan con la validación de request en importaciones"}
        return rows
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de request en importaciones: {str(e)}"}


@router.get("/validacion_requests_expo/")
async def validacion_requests_expo(
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
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
    try:
        result = await db.execute(text(sql), {"limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que no cumplan con la validación de request en exportaciones"}
        return rows
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de request en exportaciones: {str(e)}"}


# -------------------------------------------
# ---------- ENDPOINT PARA GRÁFICOS ----------
# -------------------------------------------

@router.get("/tendencia_por_naviera/{nombre}")
async def tendencia_naviera(
    nombre: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
        SELECT 
            n.nombre,
            DATE_PART('month', b.fecha) AS mes,
            SUM(oc.c20 + oc.c40 * 2) AS teus
        FROM output_containers oc
        LEFT JOIN bls b ON b.code = oc.codigo 
        LEFT JOIN navieras n ON n.id = b.id_naviera 
        WHERE n.nombre ILIKE :nombre
        GROUP BY n.nombre, DATE_PART('month', b.fecha)
        HAVING SUM(oc.c20 + oc.c40 * 2) > 0
        ORDER BY n.nombre, mes
        LIMIT :limit OFFSET :offset;
    """
    nombre_param = f"{nombre}%"
    try:
        result = await db.execute(text(sql), {"nombre": nombre_param, "limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            raise HTTPException(status_code=404, detail="Containers no encontrados")
        # Convertir a DataFrame
        df = pd.DataFrame(rows)
        required_columns = {"nombre", "mes", "teus"}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Faltan columnas en la consulta SQL. Se encontraron: {df.columns}")
        # Calcular cambios para alertas
        df["mes_anterior"] = df.groupby("nombre")["mes"].shift(1)
        df["teus_anterior"] = df.groupby("nombre")["teus"].shift(1)
        df["cambio_teus"] = df["teus"] - df["teus_anterior"]
        alertas = []
        for _, row in df.iterrows():
            if pd.notna(row["cambio_teus"]) and abs(row["cambio_teus"]) > 5000:
                if row["cambio_teus"] > 0:
                    alertas.append(f"Alerta: Naviera {row['nombre']} tuvo un aumento de {row['cambio_teus']} TEUs en el mes {int(row['mes'])} en comparación con el mes anterior.")
                else:
                    alertas.append(f"Alerta: Naviera {row['nombre']} tuvo una disminución de {abs(row['cambio_teus'])} TEUs en el mes {int(row['mes'])} en comparación con el mes anterior.")
        # (Las alertas se pueden incluir en la respuesta o en logs)
        plt.figure(figsize=(10, 6))
        for naviera, group in df.groupby("nombre"):
            plt.plot(group["mes"], group["teus"], marker="o", label=naviera)
        plt.xlabel("Mes")
        plt.ylabel("TEUs")
        plt.title("Tendencia de TEUs por Naviera")
        plt.xticks(range(1, 13))
        plt.legend(title="Naviera")
        plt.grid(True)
        buffer = BytesIO()
        plt.savefig(buffer, format="png")
        buffer.seek(0)
        return StreamingResponse(buffer, media_type="image/png")
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta: {str(e)}"}


@router.get("/tendencia_por_naviera_json/{nombre}")
async def tendencia_por_naviera_json(
    nombre: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
        SELECT 
            n.nombre,
            DATE_PART('month', b.fecha) AS mes,
            SUM(oc.c20 + oc.c40 * 2) AS teus
        FROM output_containers oc
        LEFT JOIN bls b ON b.code = oc.codigo 
        LEFT JOIN navieras n ON n.id = b.id_naviera 
        WHERE n.nombre ILIKE :nombre
        GROUP BY n.nombre, DATE_PART('month', b.fecha)
        HAVING SUM(oc.c20 + oc.c40 * 2) > 0
        ORDER BY n.nombre, mes
        LIMIT :limit OFFSET :offset;
    """
    nombre_param = f"{nombre}%"
    try:
        result = await db.execute(text(sql), {"nombre": nombre_param, "limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            raise HTTPException(status_code=404, detail="Containers no encontrados")
        df = pd.DataFrame([dict(row) for row in rows])
        required_columns = {"nombre", "mes", "teus"}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Faltan columnas en la consulta SQL. Se encontraron: {df.columns}")
        data = {
            "naviera": nombre,
            "tendencia": [
                {"mes": mes, "teus": group["teus"].sum()}
                for mes, group in df.groupby("mes")
            ]
        }
        return data
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta: {str(e)}"}


@router.get("/tendencia_etapa/{etapa}")
async def tendencia_etapa(
    etapa: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    try:
        etapa_int = int(etapa)
    except ValueError:
        raise HTTPException(status_code=400, detail="El valor de 'etapa' debe ser un número entero.")

    sql = """
        SELECT 
            n.nombre,
            b.id_etapa,
            SUM(oc.c20 + oc.c40 * 2) AS teus
        FROM output_containers oc
        LEFT JOIN bls b ON b.code = oc.codigo
        LEFT JOIN navieras n ON n.id = b.id_naviera
        WHERE b.id_etapa = :etapa
        GROUP BY n.nombre, b.id_etapa
        HAVING SUM(oc.c20 + oc.c40 * 2) > 0
        LIMIT :limit OFFSET :offset;
    """
    try:
        result = await db.execute(text(sql), {"etapa": etapa_int, "limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            raise HTTPException(status_code=404, detail="No se encontraron resultados para la etapa")
        df = pd.DataFrame([dict(row) for row in rows])
        required_columns = {"nombre", "id_etapa", "teus"}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Faltan columnas en la consulta SQL. Se encontraron: {df.columns}")
        plt.figure(figsize=(10, 6))
        for naviera, group in df.groupby("nombre"):
            plt.plot(group["nombre"], group["teus"], marker="o", label=naviera)
        plt.xlabel("Naviera")
        plt.ylabel("TEUs")
        plt.title(f"Tendencia de TEUs por Naviera en la etapa {etapa}")
        plt.xticks(rotation=45)
        plt.legend(title="Naviera")
        plt.grid(True)
        buffer = BytesIO()
        plt.savefig(buffer, format="png")
        plt.close()
        buffer.seek(0)
        return StreamingResponse(buffer, media_type="image/png")
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta: {str(e)}"}


@router.get("/tendencia_etapa_json/{etapa}")
async def tendencia_etapa_json(
    etapa: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    try:
        etapa_int = int(etapa)
    except ValueError:
        raise HTTPException(status_code=400, detail="El valor de 'etapa' debe ser un número entero.")

    sql = """
        SELECT 
            n.nombre,
            b.id_etapa,
            SUM(oc.c20 + oc.c40 * 2) AS teus
        FROM output_containers oc
        LEFT JOIN bls b ON b.code = oc.codigo
        LEFT JOIN navieras n ON n.id = b.id_naviera
        WHERE b.id_etapa = :etapa
        GROUP BY n.nombre, b.id_etapa
        HAVING SUM(oc.c20 + oc.c40 * 2) > 0
        LIMIT :limit OFFSET :offset;
    """
    try:
        result = await db.execute(text(sql), {"etapa": etapa_int, "limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            raise HTTPException(status_code=404, detail="No se encontraron resultados para la etapa")
        df = pd.DataFrame([dict(row) for row in rows])
        required_columns = {"nombre", "id_etapa", "teus"}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Faltan columnas en la consulta SQL. Se encontraron: {df.columns}")
        data = {
            "etapa": etapa,
            "navieras": [
                {"nombre": naviera, "teus": group["teus"].sum()}
                for naviera, group in df.groupby("nombre")
            ]
        }
        return data
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta: {str(e)}"}


@router.get("/tendencia_contenedor_dryreefer/{contenido}")
async def tendencia_contenedor_dryreefer(
    contenido: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
        SELECT 
            n.nombre,
            oc."dry/reefer",
            SUM(oc.c20 + oc.c40 * 2) AS teus
        FROM output_containers oc
        JOIN bls b ON b.code = oc.codigo
        LEFT JOIN navieras n ON n.id = b.id_naviera
        WHERE oc."dry/reefer" ILIKE :contenido
        GROUP BY n.nombre, oc."dry/reefer"
        HAVING SUM(oc.c20 + oc.c40 * 2) > 0
        LIMIT :limit OFFSET :offset;
    """
    contenido_param = f"{contenido}%"
    try:
        result = await db.execute(text(sql), {"contenido": contenido_param, "limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que cumplan con el tipo de contenido seleccionado"}
        # Convertir los resultados a formato JSON
        data = [
            {"naviera": row["nombre"], "contenido": row["dry/reefer"], "teus": row["teus"]}
            for row in rows
        ]
        # Crear gráfico de barras
        plt.figure(figsize=(10, 6))
        df = pd.DataFrame(data)
        for contenido_val, group in df.groupby("contenido"):
            plt.bar(group["naviera"], group["teus"], label=contenido_val)
        plt.xlabel("Navieras")
        plt.ylabel("TEUs")
        plt.title("Tendencia de TEUs por tipo de contenedor (Dry/Reefer)")
        plt.xticks(rotation=45)
        plt.legend(title="Contenedor")
        plt.grid(True)
        buffer = BytesIO()
        plt.savefig(buffer, format="png")
        plt.close()
        buffer.seek(0)
        return StreamingResponse(buffer, media_type="image/png")
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}


@router.get("/tendencia_contenedor_dryreefer_json/{contenido}")
async def tendencia_contenedor_dryreefer_json(
    contenido: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
        SELECT 
            n.nombre,
            oc."dry/reefer",
            SUM(oc.c20 + oc.c40 * 2) AS teus
        FROM output_containers oc
        JOIN bls b ON b.code = oc.codigo
        LEFT JOIN navieras n ON n.id = b.id_naviera
        WHERE oc."dry/reefer" ILIKE :contenido
        GROUP BY n.nombre, oc."dry/reefer"
        HAVING SUM(oc.c20 + oc.c40 * 2) > 0
        LIMIT :limit OFFSET :offset;
    """
    contenido_param = f"{contenido}%"
    try:
        result = await db.execute(text(sql), {"contenido": contenido_param, "limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que cumplan con el tipo de contenido seleccionado"}
        data = [
            {"naviera": row["nombre"], "contenido": row["dry/reefer"], "teus": row["teus"]}
            for row in rows
        ]
        return {"tendencia": data}
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}


@router.get("/tendencia_por_origen/{origen_locode}")
async def tendencia_por_origen(
    origen_locode: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
        SELECT DISTINCT
            n.nombre,
            SUM(oc.c20 + oc.c40 * 2) AS teus,
            p.locode AS o
        FROM output_containers oc
        LEFT JOIN bls b ON b.code = oc.codigo
        LEFT JOIN navieras n ON n.id = b.id_naviera 
        LEFT JOIN tracking t ON t.id = oc.id_origen 
        LEFT JOIN paradas p ON p.id = t.id_parada 
        WHERE p.locode ILIKE :origen_locode
        GROUP BY n.nombre, p.locode
        HAVING SUM(oc.c20 + oc.c40 * 2) > 0
        LIMIT :limit OFFSET :offset;
    """
    origen_param = f"{origen_locode}%"
    try:
        result = await db.execute(text(sql), {"origen_locode": origen_param, "limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que cumplan con el origen seleccionado"}
        data = [
            {"naviera": row["nombre"], "origen_locode": row["o"], "teus": row["teus"]}
            for row in rows
        ]
        # Crear gráfico de barras
        plt.figure(figsize=(10, 6))
        df = pd.DataFrame(data)
        for origen_val, group in df.groupby("origen_locode"):
            plt.bar(group["naviera"], group["teus"], label=origen_val)
        plt.xlabel("Navieras")
        plt.ylabel("TEUs")
        plt.title("Tendencia de TEUs por Origen Locode")
        plt.xticks(rotation=45)
        plt.legend(title="Origen Locode")
        plt.grid(True)
        buffer = BytesIO()
        plt.savefig(buffer, format="png")
        plt.close()
        buffer.seek(0)
        return StreamingResponse(buffer, media_type="image/png")
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}


@router.get("/tendencia_por_origen_json/{origen_locode}")
async def tendencia_por_origen_json(
    origen_locode: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
        SELECT DISTINCT
            n.nombre,
            SUM(oc.c20 + oc.c40 * 2) AS teus,
            p.locode AS o
        FROM output_containers oc
        LEFT JOIN bls b ON b.code = oc.codigo
        LEFT JOIN navieras n ON n.id = b.id_naviera 
        LEFT JOIN tracking t ON t.id = oc.id_origen 
        LEFT JOIN paradas p ON p.id = t.id_parada 
        WHERE p.locode ILIKE :origen_locode
        GROUP BY n.nombre, p.locode
        HAVING SUM(oc.c20 + oc.c40 * 2) > 0
        LIMIT :limit OFFSET :offset;
    """
    origen_param = f"{origen_locode}%"
    try:
        result = await db.execute(text(sql), {"origen_locode": origen_param, "limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que cumplan con el origen seleccionado"}
        data = [
            {"naviera": row["nombre"], "origen_locode": row["o"], "teus": row["teus"]}
            for row in rows
        ]
        return {"tendencia": data}
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}


@router.get("/tendencia_por_destino/{destino_locode}")
async def tendencia_por_destino(
    destino_locode: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
        SELECT DISTINCT
            n.nombre,
            SUM(oc.c20 + oc.c40 * 2) AS teus,
            p.locode AS o
        FROM output_containers oc
        LEFT JOIN bls b ON b.code = oc.codigo
        LEFT JOIN navieras n ON n.id = b.id_naviera 
        LEFT JOIN tracking t ON t.id = oc.id_destino 
        LEFT JOIN paradas p ON p.id = t.id_parada 
        WHERE p.locode ILIKE :destino_locode
        GROUP BY n.nombre, p.locode
        HAVING SUM(oc.c20 + oc.c40 * 2) > 0
        LIMIT :limit OFFSET :offset;
    """
    destino_param = f"{destino_locode}%"
    try:
        result = await db.execute(text(sql), {"destino_locode": destino_param, "limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que cumplan con el destino seleccionado"}
        data = [
            {"naviera": row["nombre"], "destino_locode": row["o"], "teus": row["teus"]}
            for row in rows
        ]
        # Crear gráfico de barras
        plt.figure(figsize=(10, 6))
        df = pd.DataFrame(data)
        for destino_val, group in df.groupby("destino_locode"):
            plt.bar(group["naviera"], group["teus"], label=destino_val)
        plt.xlabel("Navieras")
        plt.ylabel("TEUs")
        plt.title("Tendencia de TEUs por Destino Locode")
        plt.xticks(rotation=45)
        plt.legend(title="Destino Locode")
        plt.grid(True)
        buffer = BytesIO()
        plt.savefig(buffer, format="png")
        plt.close()
        buffer.seek(0)
        return StreamingResponse(buffer, media_type="image/png")
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}


@router.get("/tendencia_por_destino_json/{destino_locode}")
async def tendencia_por_destino_json(
    destino_locode: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
        SELECT DISTINCT
            n.nombre,
            SUM(oc.c20 + oc.c40 * 2) AS teus,
            p.locode AS o
        FROM output_containers oc
        LEFT JOIN bls b ON b.code = oc.codigo
        LEFT JOIN navieras n ON n.id = b.id_naviera 
        LEFT JOIN tracking t ON t.id = oc.id_destino 
        LEFT JOIN paradas p ON p.id = t.id_parada 
        WHERE p.locode ILIKE :destino_locode
        GROUP BY n.nombre, p.locode
        HAVING SUM(oc.c20 + oc.c40 * 2) > 0
        LIMIT :limit OFFSET :offset;
    """
    destino_param = f"{destino_locode}%"
    try:
        result = await db.execute(text(sql), {"destino_locode": destino_param, "limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que cumplan con el destino seleccionado"}
        data = [
            {"naviera": row["nombre"], "destino_locode": row["o"], "teus": row["teus"]}
            for row in rows
        ]
        return {"tendencia": data}
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}


@router.get("/tendencia_completa_por_navieras")
async def tendencia_por_navieras(
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    sql = """
        SELECT 
            n.nombre,
            DATE_PART('month', b.fecha) AS mes,
            SUM(oc.c20 + oc.c40 * 2) AS teus
        FROM output_containers oc
        LEFT JOIN bls b ON b.code = oc.codigo 
        LEFT JOIN navieras n ON n.id = b.id_naviera 
        GROUP BY n.nombre, DATE_PART('month', b.fecha)
        HAVING SUM(oc.c20 + oc.c40 * 2) > 0
        ORDER BY n.nombre, mes
        LIMIT :limit OFFSET :offset;
    """
    try:
        result = await db.execute(text(sql), {"limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            raise HTTPException(status_code=404, detail="No se encontraron datos de navieras")
        df = pd.DataFrame([dict(row) for row in rows])
        required_columns = {"nombre", "mes", "teus"}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail="Error en la consulta: columnas faltantes")
        plt.figure(figsize=(12, 6))
        for naviera, group in df.groupby("nombre"):
            plt.plot(group["mes"], group["teus"], marker="o", linestyle="-", label=naviera)
            for _, row in group.iterrows():
                plt.text(row["mes"], row["teus"], f"{int(row['teus'])}", fontsize=8, ha="right")
        plt.xlabel("Mes")
        plt.ylabel("TEUs")
        plt.title("Tendencia de TEUs por Naviera")
        plt.xticks(range(1, 13))
        plt.legend(title="Naviera", bbox_to_anchor=(1.05, 1), loc="upper left")
        plt.grid(True)
        buffer = BytesIO()
        plt.savefig(buffer, format="png", bbox_inches="tight")
        plt.close()
        buffer.seek(0)
        return StreamingResponse(buffer, media_type="image/png")
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta: {str(e)}"}


@router.get("/tendencia_proporcion_naviera/{nombre}")
async def tendencia_proporcion_naviera(
    nombre: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0)
):
    sql = """
        SELECT 
            n.nombre,
            DATE_PART('month', b.fecha) AS mes,
            SUM(oc.c20 + oc.c40 * 2) AS teus,
            COUNT(b.id_naviera) AS cantidad_bls,
            SUM(oc.c20 + oc.c40 * 2) / NULLIF(COUNT(b.id_naviera), 0) AS proporcion_teus_bls
        FROM output_containers oc
        LEFT JOIN bls b ON b.code = oc.codigo 
        LEFT JOIN navieras n ON n.id = b.id_naviera 
        WHERE n.nombre ILIKE :nombre
        GROUP BY n.nombre, DATE_PART('month', b.fecha)
        HAVING SUM(oc.c20 + oc.c40 * 2) > 0
        ORDER BY n.nombre, mes
        LIMIT :limit OFFSET :offset;
    """
    nombre_param = f"{nombre}%"
    try:
        result = await db.execute(text(sql), {"nombre": nombre_param, "limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            raise HTTPException(status_code=404, detail="Datos no encontrados")
        df = pd.DataFrame([dict(row) for row in rows])
        required_columns = {"nombre", "mes", "proporcion_teus_bls"}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Faltan columnas en la consulta SQL. Se encontraron: {df.columns}")
        plt.figure(figsize=(10, 6))
        for naviera, group in df.groupby("nombre"):
            plt.plot(group["mes"], group["proporcion_teus_bls"], marker="o", label=naviera)
        plt.xlabel("Mes")
        plt.ylabel("Proporción TEUs / BLS")
        plt.title("Tendencia de la Proporción TEUs por BLS por Naviera")
        plt.xticks(range(1, 13))
        plt.legend(title="Naviera")
        plt.grid(True)
        buffer = BytesIO()
        plt.savefig(buffer, format="png")
        buffer.seek(0)
        return StreamingResponse(buffer, media_type="image/png")
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta: {str(e)}"}


@router.get("/tendencia_proporcion_completa_naviera/{nombre}")
async def tendencia_proporcion_completa_naviera(
    nombre: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0)
):
    sql = """
        SELECT 
            n.nombre,
            DATE_PART('month', b.fecha) AS mes,
            SUM(oc.c20 + oc.c40 * 2) AS teus,
            COUNT(b.id_naviera) AS cantidad_bls,
            SUM(oc.c20 + oc.c40 * 2) / NULLIF(COUNT(b.id_naviera), 0) AS proporcion_teus_bls
        FROM output_containers oc
        LEFT JOIN bls b ON b.code = oc.codigo 
        LEFT JOIN navieras n ON n.id = b.id_naviera 
        WHERE n.nombre ILIKE :nombre
        GROUP BY n.nombre, DATE_PART('month', b.fecha)
        HAVING SUM(oc.c20 + oc.c40 * 2) > 0
        ORDER BY n.nombre, mes
        LIMIT :limit OFFSET :offset;
    """
    nombre_param = f"{nombre}%"
    try:
        result = await db.execute(text(sql), {"nombre": nombre_param, "limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            raise HTTPException(status_code=404, detail="Datos no encontrados")
        df = pd.DataFrame([dict(row) for row in rows])
        required_columns = {"nombre", "mes", "teus", "cantidad_bls", "proporcion_teus_bls"}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Faltan columnas en la consulta SQL. Se encontraron: {df.columns}")
        plt.figure(figsize=(12, 6))
        for naviera, group in df.groupby("nombre"):
            plt.plot(group["mes"], group["teus"], marker="s", linestyle="-", alpha=0.7, label=f"TEUs {naviera}")
            plt.plot(group["mes"], group["cantidad_bls"], marker="^", linestyle="-", alpha=0.7, label=f"BLS {naviera}")
        plt.xlabel("Mes")
        plt.ylabel("Valores")
        plt.title("Tendencia de TEUs, BLS y Proporción TEUS/BLS por Naviera")
        plt.xticks(range(1, 13))
        plt.legend(title="Naviera")
        plt.grid(True)
        buffer = BytesIO()
        plt.savefig(buffer, format="png")
        buffer.seek(0)
        return StreamingResponse(buffer, media_type="image/png")
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta: {str(e)}"}


@router.get("/tendencia_proporcion_completa_naviera_json/{nombre}")
async def tendencia_proporcion_completa_naviera_json(
    nombre: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0)
):
    sql = """
        SELECT 
            n.nombre,
            DATE_PART('month', b.fecha) AS mes,
            SUM(oc.c20 + oc.c40 * 2) AS teus,
            COUNT(b.id_naviera) AS cantidad_bls,
            SUM(oc.c20 + oc.c40 * 2) / NULLIF(COUNT(b.id_naviera), 0) AS proporcion_teus_bls
        FROM output_containers oc
        LEFT JOIN bls b ON b.code = oc.codigo 
        LEFT JOIN navieras n ON n.id = b.id_naviera 
        WHERE n.nombre ILIKE :nombre
        GROUP BY n.nombre, DATE_PART('month', b.fecha)
        HAVING SUM(oc.c20 + oc.c40 * 2) > 0
        ORDER BY n.nombre, mes
        LIMIT :limit OFFSET :offset;
    """
    nombre_param = f"{nombre}%"
    try:
        result = await db.execute(text(sql), {"nombre": nombre_param, "limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            raise HTTPException(status_code=404, detail="Datos no encontrados")
        df = pd.DataFrame([dict(row) for row in rows])
        required_columns = {"nombre", "mes", "teus", "cantidad_bls", "proporcion_teus_bls"}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Faltan columnas en la consulta SQL. Se encontraron: {df.columns}")
        # Retornar los datos en formato JSON (lista de diccionarios)
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta: {str(e)}"}
        
        
        # -------------------------------------------
# ---------- SUPERFILTRO VALIDACIONES -----------
# -------------------------------------------

@router.get("/superfiltro_validaciones/")
async def superfiltro_validaciones(
    bl_code: str,
    limit: int = Query(500, ge=1),  # Número de resultados por página, por defecto 500
    offset: int = Query(0, ge=0),     # Índice de inicio, por defecto 0
    db: AsyncSession = Depends(get_db)
):
    validaciones = {}

    # Ejecutar todas las funciones en paralelo
    resultados = await asyncio.gather(
        superfiltro_validaciones_1_2(bl_code, limit, offset, db),
        superfiltro_validaciones_3_4(bl_code, limit, offset, db),
        superfiltro_validaciones_5_6(bl_code, limit, offset, db),
        superfiltro_validaciones_7_8(bl_code, limit, offset, db),
        superfiltro_validaciones_9_10(bl_code, limit, offset, db)
    )

    count = 0
    for resultado in resultados:
        count += 1
        print("VALIDACION:", count, "- resultado:", resultado)
        validaciones.update(resultado)

    return {"bl_code": bl_code, "validaciones": validaciones}


async def superfiltro_validaciones_1_2(
    bl_code: str,
    limit: int,
    offset: int,
    db: AsyncSession
):
    x = 0
    validaciones = {}

    # --------------------------------- 1: Locode nulo
    query_locode_nulo = """
        SELECT b.code
        FROM bls b
        INNER JOIN tracking t ON t.id_bl = b.id
        INNER JOIN paradas p ON p.id = t.id_parada
        WHERE 1=1
    """
    values = {}
    if bl_code:
        query_locode_nulo += " AND b.code ILIKE :bl_code"
        values["bl_code"] = f"{bl_code}%"
    query_locode_nulo += """
        AND b.pod IS NULL AND b.pol IS NULL 
        GROUP BY b.code, b.id, p.locode, b.pod, b.pol
        LIMIT :limit OFFSET :offset;
    """
    values["limit"] = limit
    values["offset"] = offset

    result = await db.execute(text(query_locode_nulo), values)
    rows = result.mappings().all()
    if rows:
        x += 1
        print("Entro:", x)
        validaciones["¿Este bl tiene el parámetro locode de tipo nulo?"] = "si"
    else:
        validaciones["¿Este bl tiene el parámetro locode de tipo nulo?"] = "no"

    # --------------------------------- 2: Cruce con diccionario de contenedores
    query_cruce_contenedores = """
        SELECT DISTINCT ON (c.size)
            c.code AS codigo_container,
            b.code AS codigo_bl,
            c.size AS container_size,
            c.type AS container_type,
            b.fecha
        FROM containers c
        LEFT JOIN dict_containers dc ON dc.size = c.size AND dc.type = c.type
        INNER JOIN container_viaje cv ON c.id = cv.id_container
        JOIN bls b ON b.id = cv.id_bl
        WHERE 1=1
    """
    values = {}
    if bl_code:
        query_cruce_contenedores += " AND b.code ILIKE :bl_code"
        values["bl_code"] = f"{bl_code}%"
    values["limit"] = limit
    values["offset"] = offset
    query_cruce_contenedores += """
        AND dc.size IS NULL
        ORDER BY c.size, c.code
        LIMIT :limit OFFSET :offset;
    """
    result = await db.execute(text(query_cruce_contenedores), values)
    rows = result.mappings().all()
    if rows:
        x += 1
        print("Entro:", x)
        validaciones["¿Este bl tiene el tipo de contenedor en el diccionario de contenedores?"] = "si"
    else:
        validaciones["¿Este bl tiene el tipo de contenedor en el diccionario de contenedores?"] = "no"

    return validaciones


async def superfiltro_validaciones_3_4(
    bl_code: str,
    limit: int,
    offset: int,
    db: AsyncSession
):
    x = 0
    validaciones = {}

    # --------------------------------- 3: Containers repetidos
    query_container_repetido = """
        SELECT b.code AS codigo_bl,
               c.code::TEXT AS codigo_container, 
               b.nave::TEXT, 
               b.fecha,
               COUNT(*)::INTEGER AS cantidad_bls, 
               n.nombre::TEXT AS naviera_nombre
        FROM bls b
        JOIN container_viaje cv ON cv.id_bl = b.id 
        JOIN navieras n ON b.id_naviera = n.id
        JOIN containers c ON c.id = cv.id_container
        WHERE 1=1
    """
    values = {}
    if bl_code:
        query_container_repetido += " AND b.code ILIKE :bl_code"
        values["bl_code"] = f"{bl_code}%"
    values["limit"] = limit
    values["offset"] = offset
    query_container_repetido += """
        GROUP BY c.code, b.nave, b.fecha, n.nombre, b.code
        HAVING COUNT(*) > 1
        ORDER BY cantidad_bls DESC
        LIMIT :limit OFFSET :offset;
    """
    result = await db.execute(text(query_container_repetido), values)
    rows = result.mappings().all()
    if rows:
        x += 1
        print("Entro:", x)
        validaciones["¿Este bl posee más de un container con la misma fecha y nave que lo transporta?"] = "si"
    else:
        validaciones["¿Este bl posee más de un container con la misma fecha y nave que lo transporta?"] = "no"

    # --------------------------------- 4: Paradas pol y pod
    query_paradas_pol_y_pod = """
        SELECT b.code, t.is_pol, t.is_pod
        FROM tracking t 
        INNER JOIN bls b ON t.id_bl = b.id
        WHERE 1=1
    """
    values = {}
    if bl_code:
        query_paradas_pol_y_pod += " AND b.code ILIKE :bl_code"
        values["bl_code"] = f"{bl_code}%"
    values["limit"] = limit
    values["offset"] = offset
    query_paradas_pol_y_pod += """
        AND t.is_pol = 'true' AND t.is_pod = 'true'
        LIMIT :limit OFFSET :offset;
    """
    result = await db.execute(text(query_paradas_pol_y_pod), values)
    rows = result.mappings().all()
    if rows:
        x += 1
        print("Entro:", x)
        validaciones["¿En este bl la parada pol y pod son del tipo verdadero?"] = "si"
    else:
        validaciones["¿En este bl la parada pol y pod son del tipo verdadero?"] = "no"

    return validaciones


async def superfiltro_validaciones_5_6(
    bl_code: str,
    limit: int,
    offset: int,
    db: AsyncSession
):
    x = 0
    validaciones = {}

    # --------------------------------- 5: Paradas con validación
    query_paradas_con_validacion = """
        SELECT DISTINCT
            subquery.code,
            subquery.status,
            subquery.is_pod,
            subquery.pais,
            subquery.id_etapa,
            subquery.orden::SMALLINT,
            destino_subquery.id_destino
        FROM (
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
    if bl_code:
        query_paradas_con_validacion += " AND b.code ILIKE :bl_code"
        values["bl_code"] = f"{bl_code}%"
    values["limit"] = limit
    values["offset"] = offset
    query_paradas_con_validacion += """
            AND b.id_etapa = 1
        ) subquery
        LEFT JOIN (
            SELECT 
                cte.id_bl,
                MAX(CASE WHEN cte.rn_orden_mayor = 1 THEN cte.id ELSE NULL::INTEGER END) AS id_destino
            FROM (
                SELECT 
                    t.id_bl,
                    t.id,
                    ROW_NUMBER() OVER (PARTITION BY t.id_bl ORDER BY t.orden DESC) AS rn_orden_mayor
                FROM tracking t
            ) cte
            GROUP BY cte.id_bl
        ) destino_subquery ON subquery.id_bl = destino_subquery.id_bl
        WHERE (subquery.is_pod = TRUE AND subquery.rk > 1 AND subquery.pais = 'CHILE')
        LIMIT :limit OFFSET :offset;
    """
    result = await db.execute(text(query_paradas_con_validacion), values)
    rows = result.mappings().all()
    if rows:
        x += 1
        print("Entro:", x)
        validaciones["¿Este BL contiene la parada de orden mayor y POD como Chile?"] = "si"
    else:
        validaciones["¿Este BL contiene la parada de orden mayor y POD como Chile?"] = "no"

    # ------------------------------ 6: Paradas con orden repetida
    query_obtener_paradas_con_orden_repetida = """
        SELECT b.code, t.id_bl AS bl_id, t.id AS id_parada, t.orden AS orden_repetido
        FROM tracking t
        INNER JOIN container_viaje c ON c.id_bl = t.id_bl
        INNER JOIN bls b ON b.id = c.id_bl
        WHERE 1=1
    """
    values = {}
    if bl_code:
        query_obtener_paradas_con_orden_repetida += " AND b.code ILIKE :bl_code"
        values["bl_code"] = f"{bl_code}%"
    values["limit"] = limit
    values["offset"] = offset
    query_obtener_paradas_con_orden_repetida += """
        GROUP BY b.code, t.id_bl, t.id, t.orden, c.id_container
        HAVING COUNT(*) > 1
        LIMIT :limit OFFSET :offset;
    """
    result = await db.execute(text(query_obtener_paradas_con_orden_repetida), values)
    rows = result.mappings().all()
    if rows:
        x += 1
        print("Entro:", x)
        validaciones["¿Este bl contiene paradas con el mismo número de orden?"] = "si"
    else:
        validaciones["¿Este bl contiene paradas con el mismo número de orden?"] = "no"

    return validaciones


async def superfiltro_validaciones_7_8(
    bl_code: str,
    limit: int,
    offset: int,
    db: AsyncSession
):
    x = 0
    validaciones = {}

    # --------------------------------- 7: Verificar registros etapa1 (origen distinto a Chile)
    query_verificar_etapa1 = """
        SELECT DISTINCT 
            b.code AS codigo_bl,
            t.is_pol,
            p.pais,
            b.id_etapa
        FROM bls b
        INNER JOIN tracking t ON b.id = t.id_bl
        INNER JOIN paradas p ON p.id = t.id_parada
        WHERE 1=1
    """
    values = {}
    if bl_code:
        query_verificar_etapa1 += " AND b.code ILIKE :bl_code"
        values["bl_code"] = f"{bl_code}%"
    values["limit"] = limit
    values["offset"] = offset
    query_verificar_etapa1 += """
        AND b.id_etapa = 1 AND p.pais <> 'CHILE' AND t.is_pol = true
        GROUP BY b.code, t.is_pol, p.pais, b.id_etapa
        LIMIT :limit OFFSET :offset;
    """
    result = await db.execute(text(query_verificar_etapa1), values)
    rows = result.mappings().all()
    if rows:
        x += 1
        print("Entro:", x)
        validaciones["¿Este bl contiene a Chile como su parada de origen (POD)?"] = "si"
    else:
        validaciones["¿Este bl contiene a Chile como su parada de origen (POD)?"] = "no"

    # ------------------------------ 8: Verificar registros etapa2 (destino distinto a Chile)
    query_verificar_etapa2 = """
        SELECT DISTINCT 
            b.code AS codigo_bl,
            t.is_pod,
            p.pais,
            b.id_etapa
        FROM bls b
        INNER JOIN tracking t ON b.id = t.id_bl
        INNER JOIN paradas p ON p.id = t.id_parada
        WHERE 1=1
    """
    values = {}
    if bl_code:
        query_verificar_etapa2 += " AND b.code ILIKE :bl_code"
        values["bl_code"] = f"{bl_code}%"
    values["limit"] = limit
    values["offset"] = offset
    query_verificar_etapa2 += """
        AND b.id_etapa = 2 AND p.pais NOT LIKE 'CHILE' AND t.is_pod = true
        GROUP BY b.code, t.is_pod, p.pais, b.id_etapa
        LIMIT :limit OFFSET :offset;
    """
    result = await db.execute(text(query_verificar_etapa2), values)
    rows = result.mappings().all()
    if rows:
        x += 1
        print("Entro:", x)
        validaciones["¿Este bl contiene a Chile como su parada de destino (POL)?"] = "si"
    else:
        validaciones["¿Este bl contiene a Chile como su parada de destino (POL)?"] = "no"

    return validaciones


async def superfiltro_validaciones_9_10(
    bl_code: str,
    limit: int,
    offset: int,
    db: AsyncSession
):
    x = 0
    validaciones = {}

    # ---------------------------- 9: validacion_bls_expo
    query_validacion_bls_expo = """
        SELECT 
            subquery.code,
            subquery.is_pol,
            subquery.pais,
            subquery.id_etapa,
            CAST(subquery.orden AS INT)
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
    if bl_code:
        query_validacion_bls_expo += " AND b.code ILIKE :bl_code"
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
    result = await db.execute(text(query_validacion_bls_expo), values)
    rows = result.mappings().all()
    if rows:
        x += 1
        print("Entro:", x)
        validaciones["¿Este bl NO contiene a Chile, Argentina o Bolivia como su parada de origen (orden = 1)?"] = "si"
    else:
        validaciones["¿Este bl NO contiene a Chile, Argentina o Bolivia como su parada de origen (orden = 1)?"] = "no"

    # ---------------------------- 10: validacion_bls_impo
    query_validacion_bls_impo = """
        SELECT 
            subquery.code,
            subquery.is_pod,
            subquery.pais,
            subquery.id_etapa,
            CAST(subquery.orden AS INT)
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
    if bl_code:
        query_validacion_bls_impo += " AND b.code ILIKE :bl_code"
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
    result = await db.execute(text(query_validacion_bls_impo), values)
    rows = result.mappings().all()
    if rows:
        x += 1
        print("Entro:", x)
        validaciones["¿Este bl NO contiene a Chile, Argentina, Bolivia, Paraguay o Uruguay como su parada de destino (orden más alta)?"] = "si"
    else:
        validaciones["¿Este bl NO contiene a Chile, Argentina, Bolivia, Paraguay o Uruguay como su parada de destino (orden más alta)?"] = "no"

    return validaciones

# -------------------------------------------
# ------ SUPERFILTRO VALIDACIONES  ------
# -------------------------------------------

@router.get("/superfiltro_validaciones_11")
async def superfiltro_validaciones_11(
    bl_code: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    x = 0
    validaciones = {}

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
    if bl_code:
        print("Entro:", x)
        query_obtener_diferencia_requests_importacion += " AND b.code ILIKE :bl_code"
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
    result = await db.execute(text(query_obtener_diferencia_requests_importacion), values)
    rows = result.mappings().all()
    if rows:
        x += 1
        print("Entro:", x)
        validaciones["¿Este BL contiene 2 requests exitosos con no más de 15 día de diferencia?"] = "si"
    else:
        validaciones["¿Este BL contiene 2 requests exitosos con no más de 15 día de diferencia?"] = "no"
    return validaciones


@router.get("/superfiltro_validaciones_12")
async def superfiltro_validaciones_12(
    bl_code: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    x = 0
    validaciones = {}

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
    if bl_code:
        print("Entro:", x)
        query_obtener_requests_incompletos_expo += " AND b.code ILIKE :bl_code"
        values["bl_code"] = f"{bl_code}%"
    values["limit"] = limit
    values["offset"] = offset
    query_obtener_requests_incompletos_expo += """
        AND r.id_respuesta NOT IN (1, 8)
        ),
        ids AS (
            SELECT 
                cte.id_bl,
                MAX(cte.id_parada) AS id_pol,
                MAX(cte.id_parada) AS id_destino
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
    result = await db.execute(text(query_obtener_requests_incompletos_expo), values)
    rows = result.mappings().all()
    if rows:
        x += 1
        print("Entro:", x)
        validaciones["¿Este BL contiene 2 request exitosas, una a la salida y otra a la llegada planificada de destino?"] = "si"
    else:
        validaciones["¿Este BL contiene 2 request exitosas, una a la salida y otra a la llegada planificada de destino?"] = "no"
    return validaciones


@router.get("/tendencia_por_naviera_alertas/{nombre}")
async def tendencia_por_naviera_alertas(
    nombre: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    query = """
        SELECT 
            n.nombre,
            DATE_PART('month', b.fecha) AS mes,
            SUM(oc.c20 + oc.c40 * 2) AS teus
        FROM output_containers oc
        LEFT JOIN bls b ON b.code = oc.codigo 
        LEFT JOIN navieras n ON n.id = b.id_naviera 
        WHERE n.nombre ILIKE :nombre
        GROUP BY n.nombre, DATE_PART('month', b.fecha)
        HAVING SUM(oc.c20 + oc.c40 * 2) > 0
        ORDER BY n.nombre, mes
        LIMIT :limit OFFSET :offset;
    """
    nombre_param = f"{nombre}%"
    try:
        result = await db.execute(text(query), {"nombre": nombre_param, "limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            raise HTTPException(status_code=404, detail="Containers no encontrados")
        df = pd.DataFrame([dict(row) for row in rows])
        required_columns = {"nombre", "mes", "teus"}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Faltan columnas en la consulta SQL. Se encontraron: {df.columns}")
        # Calcular cambios para generar alertas
        df["mes_anterior"] = df.groupby("nombre")["mes"].shift(1)
        df["teus_anterior"] = df.groupby("nombre")["teus"].shift(1)
        df["cambio_teus"] = df["teus"] - df["teus_anterior"]
        alertas = []
        for _, row in df.iterrows():
            if pd.notna(row["cambio_teus"]) and row["cambio_teus"] != 0:
                if row["cambio_teus"] > 0:
                    alertas.append(f"Alerta: Naviera {row['nombre']} tuvo un aumento de {row['cambio_teus']} TEUs en el mes {int(row['mes'])} en comparación con el mes anterior.")
                else:
                    alertas.append(f"Alerta: Naviera {row['nombre']} tuvo una disminución de {abs(row['cambio_teus'])} TEUs en el mes {int(row['mes'])} en comparación con el mes anterior.")
        if alertas:
            return {"alertas": alertas}
        # Si no hay alertas, se retorna el gráfico de tendencia
        import matplotlib.pyplot as plt
        from io import BytesIO
        plt.figure(figsize=(10, 6))
        for naviera, group in df.groupby("nombre"):
            plt.plot(group["mes"], group["teus"], marker="o", label=naviera)
        plt.xlabel("Mes")
        plt.ylabel("TEUs")
        plt.title("Tendencia de TEUs por Naviera")
        plt.xticks(range(1, 13))
        plt.legend(title="Naviera")
        plt.grid(True)
        buffer = BytesIO()
        plt.savefig(buffer, format="png")
        plt.close()
        buffer.seek(0)
        return StreamingResponse(buffer, media_type="image/png")
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta: {str(e)}"}


@router.get("/tendencia_etapa_alertas/{etapa}")
async def tendencia_etapa_alertas(
    etapa: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    try:
        etapa_int = int(etapa)
    except ValueError:
        raise HTTPException(status_code=400, detail="El valor de 'etapa' debe ser un número entero.")
    query = """
        SELECT 
            SUM(oc.c20 + oc.c40 * 2) AS teus
        FROM output_containers oc
        LEFT JOIN bls b ON b.code = oc.codigo
        WHERE b.id_etapa = :etapa
        GROUP BY b.id_etapa
        HAVING SUM(oc.c20 + oc.c40 * 2) > 0
        LIMIT :limit OFFSET :offset;
    """
    try:
        result = await db.execute(text(query), {"etapa": etapa_int, "limit": limit, "offset": offset})
        row = result.mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="No se encontraron resultados para la etapa")
        total_teus = row["teus"]
        alerta = None
        if total_teus > 2000000:
            alerta = f"🚨 ALERTA CRÍTICA: TEUs ({total_teus}) supera 2,000,000."
        elif total_teus > 1000000:
            alerta = f"⚠️ ALERTA: TEUs ({total_teus}) supera 1,000,000."
        elif total_teus < 1000000:
            alerta = f"⚠️ ALERTA: TEUs ({total_teus}) está por debajo de 1,000,000."
        elif total_teus < 100000:
            alerta = f"🚨 ALERTA CRÍTICA: TEUs ({total_teus}) está por debajo de 100,000."
        return {"etapa": etapa_int, "total_teus": total_teus, "alerta": alerta}
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta: {str(e)}"}


@router.get("/tendencia_contenedor_dryreefer_alertas/{contenido}")
async def tendencia_contenedor_dryreefer_alertas(
    contenido: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0)
):
    query = """
        SELECT 
            n.nombre,
            oc."dry/reefer",
            SUM(oc.c20 + oc.c40 * 2) AS teus
        FROM output_containers oc
        JOIN bls b ON b.code = oc.codigo
        LEFT JOIN navieras n ON n.id = b.id_naviera
        WHERE oc."dry/reefer" ILIKE :contenido
        GROUP BY n.nombre, oc."dry/reefer"
        HAVING SUM(oc.c20 + oc.c40 * 2) > 0
        LIMIT :limit OFFSET :offset;
    """
    contenido_param = f"{contenido}%"
    try:
        result = await db.execute(text(query), {"contenido": contenido_param, "limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que cumplan con el tipo de contenido seleccionado"}
        data = [
            {"naviera": row["nombre"], "contenido": row["dry/reefer"], "teus": row["teus"]}
            for row in rows
        ]
        total_teus = sum(row["teus"] for row in rows)
        alerta = None
        if total_teus > 2000000:
            alerta = f"🚨 ALERTA CRÍTICA: TEUs ({total_teus}) supera 2,000,000."
        elif total_teus > 1000000:
            alerta = f"⚠️ ALERTA: TEUs ({total_teus}) supera 1,000,000."
        elif total_teus < 1000000:
            alerta = f"⚠️ ALERTA: TEUs ({total_teus}) está por debajo de 1,000,000."
        elif total_teus < 100000:
            alerta = f"🚨 ALERTA CRÍTICA: TEUs ({total_teus}) está por debajo de 100,000."
        return {"tendencia": data, "total_teus": total_teus, "alerta": alerta}
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}


@router.get("/tendencia_por_origen_alertas/{origen_locode}")
async def tendencia_por_origen_alertas(
    origen_locode: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0)
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
        LEFT JOIN paradas p ON p.id = t.id_parada 
        WHERE p.locode ILIKE :origen_locode
        GROUP BY n.nombre, p.locode
        HAVING SUM(oc.c20 + oc.c40 * 2) > 0
        LIMIT :limit OFFSET :offset;
    """
    origen_param = f"{origen_locode}%"
    try:
        result = await db.execute(text(query), {"origen_locode": origen_param, "limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que cumplan con el origen seleccionado"}
        data = [
            {"naviera": row["nombre"], "origen_locode": row["o"], "teus": row["teus"]}
            for row in rows
        ]
        total_teus = sum(row["teus"] for row in rows)
        alerta = None
        if total_teus > 2000000:
            alerta = f"🚨 ALERTA CRÍTICA: TEUs ({total_teus}) supera 2,000,000."
        elif total_teus > 1000000:
            alerta = f"⚠️ ALERTA: TEUs ({total_teus}) supera 1,000,000."
        elif total_teus < 1000000:
            alerta = f"⚠️ ALERTA: TEUs ({total_teus}) está por debajo de 1,000,000."
        elif total_teus < 100000:
            alerta = f"🚨 ALERTA CRÍTICA: TEUs ({total_teus}) está por debajo de 100,000."
        return {"tendencia": data, "total_teus": total_teus, "alerta": alerta}
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}


@router.get("/tendencia_por_destino_alertas/{destino_locode}")
async def tendencia_por_destino_alertas(
    destino_locode: str,
    limit: int = Query(500, ge=1),
    offset: int = Query(0, ge=0)
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
        LEFT JOIN paradas p ON p.id = t.id_parada 
        WHERE p.locode ILIKE :destino_locode
        GROUP BY n.nombre, p.locode
        HAVING SUM(oc.c20 + oc.c40 * 2) > 0
        LIMIT :limit OFFSET :offset;
    """
    destino_param = f"{destino_locode}%"
    try:
        result = await db.execute(text(query), {"destino_locode": destino_param, "limit": limit, "offset": offset})
        rows = result.mappings().all()
        if not rows:
            return {"message": "No existen datos que cumplan con el destino seleccionado"}
        data = [
            {"naviera": row["nombre"], "destino_locode": row["o"], "teus": row["teus"]}
            for row in rows
        ]
        total_teus = sum(row["teus"] for row in rows)
        alerta = None
        if total_teus > 2000000:
            alerta = f"🚨 ALERTA CRÍTICA: TEUs ({total_teus}) supera 2,000,000."
        elif total_teus > 1000000:
            alerta = f"⚠️ ALERTA: TEUs ({total_teus}) supera 1,000,000."
        elif total_teus < 1000000:
            alerta = f"⚠️ ALERTA: TEUs ({total_teus}) está por debajo de 1,000,000."
        elif total_teus < 100000:
            alerta = f"🚨 ALERTA CRÍTICA: TEUs ({total_teus}) está por debajo de 100,000."
        return {"tendencia": data, "total_teus": total_teus, "alerta": alerta}
    except Exception as e:
        return {"error": f"Error al ejecutar la validación de tendencia: {str(e)}"}

