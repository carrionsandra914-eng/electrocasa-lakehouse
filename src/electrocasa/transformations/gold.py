"""
Capa GOLD: una materialized view por pregunta de negocio.
Período de análisis: marzo a octubre de 2026. Todo se calcula sobre Silver
(datos limpios y deduplicados; los rechazados no cuentan).
"""
from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")
MASK_DNI = f"{CATALOG}.default.mask_dni"
MASK_SALARIO = f"{CATALOG}.default.mask_salario"
INI, FIN = "DATE'2026-03-01'", "DATE'2026-10-31'"


# 1. Ventas netas por sucursal y mes -------------------------------------------------------
@dp.materialized_view(name="gold.ventas_netas_sucursal_mes",
                      comment="P1: ventas, devoluciones, ventas netas, cantidad y ticket promedio por sucursal y mes",
                      table_properties={"quality": "gold"})
def gold_ventas_netas_sucursal_mes():
    return spark.sql(f"""
        WITH v AS (
            SELECT sucursal_id, CAST(date_trunc('MONTH', fecha_venta) AS DATE) AS mes,
                   SUM(monto_total) AS ventas, COUNT(DISTINCT venta_id) AS cantidad_ventas
            FROM silver.ventas
            WHERE fecha_venta BETWEEN {INI} AND {FIN}
            GROUP BY ALL),
        d AS (
            SELECT sucursal_id, CAST(date_trunc('MONTH', fecha_devolucion) AS DATE) AS mes,
                   SUM(monto_reembolso) AS devoluciones, COUNT(*) AS cantidad_devoluciones
            FROM silver.devoluciones
            WHERE fecha_devolucion BETWEEN {INI} AND {FIN}
            GROUP BY ALL)
        SELECT COALESCE(v.sucursal_id, d.sucursal_id)          AS sucursal_id,
               s.nombre_sucursal, s.ciudad, s.region,
               COALESCE(v.mes, d.mes)                          AS mes,
               CAST(COALESCE(v.ventas, 0) AS DECIMAL(14,2))   AS ventas,
               CAST(COALESCE(d.devoluciones, 0) AS DECIMAL(14,2)) AS devoluciones,
               CAST(COALESCE(v.ventas, 0) - COALESCE(d.devoluciones, 0) AS DECIMAL(14,2)) AS ventas_netas,
               COALESCE(v.cantidad_ventas, 0)                  AS cantidad_ventas,
               COALESCE(d.cantidad_devoluciones, 0)            AS cantidad_devoluciones,
               CAST(try_divide(v.ventas, v.cantidad_ventas) AS DECIMAL(14,2)) AS ticket_promedio
        FROM v FULL OUTER JOIN d ON v.sucursal_id = d.sucursal_id AND v.mes = d.mes
        LEFT JOIN silver.sucursales s ON s.sucursal_id = COALESCE(v.sucursal_id, d.sucursal_id)
    """)


# 2a. Top 10 productos por unidades -------------------------------------------------------
@dp.materialized_view(name="gold.top10_productos_unidades",
                      comment="P2: top 10 de productos más vendidos por unidades (mar-oct 2026)",
                      table_properties={"quality": "gold"})
def gold_top10_unidades():
    return spark.sql(f"""
        WITH u AS (
            SELECT producto_id, SUM(cantidad) AS unidades, COUNT(DISTINCT venta_id) AS cantidad_ventas,
                   SUM(monto_total) AS monto
            FROM silver.ventas
            WHERE fecha_venta BETWEEN {INI} AND {FIN}
            GROUP BY producto_id)
        SELECT * FROM (
            SELECT ROW_NUMBER() OVER (ORDER BY u.unidades DESC, u.monto DESC, u.producto_id) AS ranking,
                   u.producto_id, c.nombre_producto, c.categoria, c.marca,
                   u.unidades, u.cantidad_ventas, u.monto
            FROM u LEFT JOIN silver.catalogo c USING (producto_id))
        WHERE ranking <= 10
    """)


# 2b. Top 10 productos por tasa de devolución -------------------------------------------
@dp.materialized_view(name="gold.top10_productos_tasa_devolucion",
                      comment="P2: top 10 de productos con mayor tasa de devolución = devoluciones / ventas del producto",
                      table_properties={"quality": "gold"})
def gold_top10_tasa_devolucion():
    return spark.sql(f"""
        WITH v AS (
            SELECT producto_id, COUNT(DISTINCT venta_id) AS cantidad_ventas
            FROM silver.ventas WHERE fecha_venta BETWEEN {INI} AND {FIN}
            GROUP BY producto_id),
        d AS (
            SELECT producto_id, COUNT(DISTINCT devolucion_id) AS cantidad_devoluciones
            FROM silver.devoluciones WHERE fecha_devolucion BETWEEN {INI} AND {FIN}
            GROUP BY producto_id),
        t AS (
            SELECT v.producto_id, v.cantidad_ventas, COALESCE(d.cantidad_devoluciones, 0) AS cantidad_devoluciones,
                   CAST(COALESCE(d.cantidad_devoluciones, 0) / v.cantidad_ventas AS DECIMAL(8,4)) AS tasa_devolucion
            FROM v LEFT JOIN d USING (producto_id))
        SELECT * FROM (
            SELECT ROW_NUMBER() OVER (ORDER BY t.tasa_devolucion DESC, t.cantidad_devoluciones DESC, t.producto_id) AS ranking,
                   t.producto_id, c.nombre_producto, c.categoria, c.marca,
                   t.cantidad_ventas, t.cantidad_devoluciones, t.tasa_devolucion
            FROM t LEFT JOIN silver.catalogo c USING (producto_id))
        WHERE ranking <= 10
    """)


# 3. Dotación al cierre de cada mes ------------------------------------------------------
@dp.materialized_view(name="gold.dotacion_sucursal_mes",
                      comment="P3: empleados activos por sucursal al último día de cada mes (mar-oct 2026), según la historia SCD2",
                      table_properties={"quality": "gold"})
def gold_dotacion():
    return spark.sql(f"""
        WITH meses AS (
            SELECT last_day(m) AS fecha_corte
            FROM (SELECT explode(sequence({INI}, DATE'2026-10-01', INTERVAL 1 MONTH)) AS m))
        SELECT e.sucursal_id, s.nombre_sucursal, m.fecha_corte,
               COUNT(DISTINCT e.id_empleado) AS empleados_activos
        FROM meses m
        JOIN silver.empleados_scd2 e
          ON e.vigente_desde <= m.fecha_corte
         AND (e.vigente_hasta IS NULL OR e.vigente_hasta > m.fecha_corte)
        LEFT JOIN silver.sucursales s ON s.sucursal_id = e.sucursal_id
        WHERE e.estado = 'activo'              -- sin baja hasta esa fecha
          AND e.fecha_alta <= m.fecha_corte    -- con alta en esa fecha o antes
        GROUP BY ALL
    """)


# 4. Ventas por vendedor (sucursal vigente en la fecha de la venta) ----------------------
@dp.materialized_view(name="gold.ventas_vendedor_sucursal_mes",
                      comment="P4: ventas de tienda física por vendedor, sucursal del vendedor en la fecha de venta (SCD2) y mes",
                      table_properties={"quality": "gold"})
def gold_ventas_vendedor():
    return spark.sql(f"""
        SELECT v.vendedor_id,
               COALESCE(e.sucursal_id, 'SIN_ASIGNAR')            AS sucursal_vendedor,
               CAST(date_trunc('MONTH', v.fecha_venta) AS DATE)  AS mes,
               CAST(SUM(v.monto_total) AS DECIMAL(14,2))         AS monto_ventas,
               COUNT(DISTINCT v.venta_id)                        AS cantidad_ventas
        FROM silver.ventas v
        LEFT JOIN silver.empleados_scd2 e
          ON e.id_empleado = v.vendedor_id
         AND e.vigente_desde <= v.fecha_venta
         AND (e.vigente_hasta IS NULL OR e.vigente_hasta > v.fecha_venta)
         AND e.estado = 'activo'
        WHERE v.canal = 'tienda_fisica'
          AND v.fecha_venta BETWEEN {INI} AND {FIN}
        GROUP BY ALL
    """)


# 5. Reseñas negativas por categoría y mes ---------------------------------------------
@dp.materialized_view(name="gold.resenas_negativas_categoria_mes",
                      comment="P5: % de reseñas negativas (1-2) por categoría (último snapshot) y mes",
                      table_properties={"quality": "gold"})
def gold_resenas_categoria():
    return spark.sql(f"""
        SELECT c.categoria,
               CAST(date_trunc('MONTH', r.fecha_resena) AS DATE) AS mes,
               COUNT(*)                                          AS total_resenas,
               SUM(CASE WHEN r.calificacion <= 2 THEN 1 ELSE 0 END) AS resenas_negativas,
               CAST(100 * SUM(CASE WHEN r.calificacion <= 2 THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(6,2)) AS pct_negativas
        FROM silver.resenas r
        JOIN silver.catalogo c USING (producto_id)   -- producto inexistente: no entra
        WHERE r.fecha_resena BETWEEN {INI} AND {FIN}
        GROUP BY ALL
    """)


# 5b. (Reto VARIANT) Reseñas negativas por origen y versión de la app -------------------
@dp.materialized_view(name="gold.resenas_negativas_origen",
                      comment="Reto VARIANT: % de reseñas negativas por origen (app/web/tienda) y versión de app, leído de metadata VARIANT",
                      table_properties={"quality": "gold"})
def gold_resenas_origen():
    return spark.sql(f"""
        SELECT variant_get(r.metadata, '$.origen', 'string')                  AS origen,
               COALESCE(variant_get(r.metadata, '$.app.version', 'string'), 'n/a') AS app_version,
               COUNT(*) AS total_resenas,
               SUM(CASE WHEN r.calificacion <= 2 THEN 1 ELSE 0 END) AS resenas_negativas,
               CAST(100 * SUM(CASE WHEN r.calificacion <= 2 THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(6,2)) AS pct_negativas
        FROM silver.resenas r
        JOIN silver.catalogo c USING (producto_id)
        WHERE r.fecha_resena BETWEEN {INI} AND {FIN}
        GROUP BY ALL
    """)


# 6. Despachos por courier ----------------------------------------------------------------
@dp.materialized_view(name="gold.despachos_courier",
                      comment="P6: horas promedio pendiente->entregado y % de envíos devueltos por courier",
                      table_properties={"quality": "gold"})
def gold_despachos():
    return spark.sql("""
        WITH envio AS (
            SELECT tracking_id,
                   MAX(courier) AS courier,
                   MIN(CASE WHEN estado_entrega = 'pendiente' THEN fecha_modificacion END) AS ts_pendiente,
                   MIN(CASE WHEN estado_entrega = 'entregado' THEN fecha_modificacion END) AS ts_entregado,
                   MAX(CASE WHEN estado_entrega = 'devuelto' THEN 1 ELSE 0 END)             AS fue_devuelto,
                   MAX(CASE WHEN estado_entrega IN ('entregado','devuelto') THEN 1 ELSE 0 END) AS estado_final
            FROM silver.tracking_envios
            GROUP BY tracking_id)
        SELECT courier,
               COUNT(*)                                   AS envios,
               SUM(CASE WHEN ts_entregado IS NOT NULL THEN 1 ELSE 0 END) AS envios_entregados,
               CAST(AVG(CASE WHEN ts_entregado IS NOT NULL AND ts_pendiente IS NOT NULL
                             THEN (unix_timestamp(ts_entregado) - unix_timestamp(ts_pendiente)) / 3600.0 END)
                    AS DECIMAL(10,2))                     AS horas_promedio_entrega,
               SUM(estado_final)                          AS envios_estado_final,
               SUM(fue_devuelto)                          AS envios_devueltos,
               CAST(100 * try_divide(SUM(fue_devuelto), SUM(estado_final)) AS DECIMAL(6,2)) AS pct_devueltos
        FROM envio
        GROUP BY courier
    """)


# Vista de empleados en Gold (derivada de la tabla enmascarada) ---------------------------
# Sirve como evidencia: si el dueño del pipeline está en electrocasa_ingenieria, esta tabla
# guarda los valores reales y la máscara decide qué ve cada consumidor.
@dp.materialized_view(
    name="gold.empleados_actuales",
    comment="Versión vigente de cada empleado (dni y salario enmascarados para quien no es de ingeniería)",
    table_properties={"quality": "gold"},
    schema=f"""
        id_empleado STRING, nombre STRING,
        dni STRING MASK {MASK_DNI},
        salario DECIMAL(10,2) MASK {MASK_SALARIO},
        sucursal_id STRING, cargo STRING, estado STRING, fecha_alta DATE, vigente_desde DATE
    """,
)
def gold_empleados_actuales():
    return spark.sql("""
        SELECT id_empleado, nombre, dni, salario, sucursal_id, cargo, estado, fecha_alta, vigente_desde
        FROM silver.empleados_scd2
        WHERE es_version_actual
    """)
