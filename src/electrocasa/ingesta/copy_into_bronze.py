# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesta COPY INTO → Bronze (Catálogo y Sucursales)
# MAGIC
# MAGIC **Por qué COPY INTO para estas dos fuentes**
# MAGIC - **Catálogo:** llega como *snapshot completo* (un arreglo JSON por archivo), pocas veces al año (marzo, agosto).
# MAGIC   No hay un flujo continuo de archivos que justifique un stream: una carga batch idempotente basta.
# MAGIC - **Sucursales:** tabla de referencia de 40 filas, archivo único que casi no cambia.
# MAGIC
# MAGIC COPY INTO lleva registro de los archivos ya cargados: volver a correrlo **no duplica filas** (idempotente).
# MAGIC Corre fuera del pipeline (no se puede declarar dentro de Lakeflow Declarative Pipelines) y escribe en tablas Delta
# MAGIC comunes de `bronze`, que el pipeline lee como fuente.

# COMMAND ----------

dbutils.widgets.text("catalog", "electrocasa_dev")
CATALOG = dbutils.widgets.get("catalog")
LANDING = f"/Volumes/{CATALOG}/default/vol_landing"
print(CATALOG, LANDING)

# COMMAND ----------

# Tablas Bronze (todo como STRING: Bronze guarda el dato raw) + columnas de auditoría
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.catalogo (
  producto_id STRING, nombre_producto STRING, categoria STRING, marca STRING,
  precio_lista STRING, activo STRING,
  _ingesta_ts TIMESTAMP, _archivo_origen STRING, _archivo_nombre STRING, _lote STRING
) COMMENT 'Catálogo raw (COPY INTO, snapshots completos JSON)'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.sucursales (
  sucursal_id STRING, nombre_sucursal STRING, distrito STRING, ciudad STRING,
  region STRING, fecha_apertura STRING,
  _ingesta_ts TIMESTAMP, _archivo_origen STRING, _archivo_nombre STRING, _lote STRING
) COMMENT 'Sucursales raw (COPY INTO, CSV de referencia)'
""")

# COMMAND ----------

# Catálogo: cada archivo es UN arreglo JSON -> multiLine. primitivesAsString evita
# DELTA_FAILED_TO_MERGE_FIELDS (activo llega como booleano) y guarda todo como texto.
res_cat = spark.sql(f"""
COPY INTO {CATALOG}.bronze.catalogo
FROM (
  SELECT producto_id, nombre_producto, categoria, marca, precio_lista, activo,
         current_timestamp()                                            AS _ingesta_ts,
         _metadata.file_path                                            AS _archivo_origen,
         _metadata.file_name                                            AS _archivo_nombre,
         regexp_extract(_metadata.file_name, '(\\\\d{{4}}-\\\\d{{2}}-\\\\d{{2}})', 1) AS _lote
  FROM '{LANDING}/catalogo/'
)
FILEFORMAT = JSON
FORMAT_OPTIONS ('multiLine' = 'true', 'primitivesAsString' = 'true')
COPY_OPTIONS ('mergeSchema' = 'false')
""")
display(res_cat)

# COMMAND ----------

# Sucursales: CSV con encabezado. Sin inferSchema: todas las columnas entran como STRING.
res_suc = spark.sql(f"""
COPY INTO {CATALOG}.bronze.sucursales
FROM (
  SELECT sucursal_id, nombre_sucursal, distrito, ciudad, region, fecha_apertura,
         current_timestamp()  AS _ingesta_ts,
         _metadata.file_path  AS _archivo_origen,
         _metadata.file_name  AS _archivo_nombre,
         'referencia'         AS _lote
  FROM '{LANDING}/sucursales/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
COPY_OPTIONS ('mergeSchema' = 'false')
""")
display(res_suc)

# COMMAND ----------

display(spark.sql(f"""
SELECT 'bronze.catalogo' AS tabla, _lote, COUNT(*) AS filas FROM {CATALOG}.bronze.catalogo GROUP BY _lote
UNION ALL
SELECT 'bronze.sucursales', _lote, COUNT(*) FROM {CATALOG}.bronze.sucursales GROUP BY _lote
ORDER BY tabla, _lote
"""))
