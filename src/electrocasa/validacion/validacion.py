# Databricks notebook source
# MAGIC %md
# MAGIC # Validación post-pipeline
# MAGIC 1. Conteos de cada tabla de Bronze, Silver y Gold.
# MAGIC 2. Registros rechazados (política `drop`) y advertencias (`warn`) por fuente.
# MAGIC 3. **Falla** si alguna tabla de Gold queda vacía.

# COMMAND ----------

dbutils.widgets.text("catalog", "electrocasa_dev")
CATALOG = dbutils.widgets.get("catalog")

BRONZE = ["ventas", "catalogo", "empleados", "resenas", "devoluciones", "sucursales", "tracking_envios"]
SILVER = ["ventas", "catalogo", "empleados_eventos", "empleados_scd2", "resenas", "devoluciones",
          "sucursales", "tracking_envios", "cuarentena"]
GOLD = ["ventas_netas_sucursal_mes", "top10_productos_unidades", "top10_productos_tasa_devolucion",
        "dotacion_sucursal_mes", "ventas_vendedor_sucursal_mes", "resenas_negativas_categoria_mes",
        "resenas_negativas_origen", "despachos_courier", "empleados_actuales"]

# COMMAND ----------

filas = []
for capa, tablas in (("bronze", BRONZE), ("silver", SILVER), ("gold", GOLD)):
    for t in tablas:
        nombre = f"{CATALOG}.{capa}.{t}"
        try:
            n = spark.table(nombre).count()
        except Exception as e:  # tabla aún no creada
            n = None
            print(f"[WARN] {nombre}: {str(e)[:120]}")
        filas.append((capa, t, n))

conteos = spark.createDataFrame(filas, "capa string, tabla string, filas long")
display(conteos)

# COMMAND ----------

# Bronze por lote (para la tabla de conteos del README)
display(spark.sql(f"""
SELECT 'ventas' AS fuente, _lote, COUNT(*) AS filas FROM {CATALOG}.bronze.ventas GROUP BY ALL
UNION ALL SELECT 'empleados', _lote, COUNT(*) FROM {CATALOG}.bronze.empleados GROUP BY ALL
UNION ALL SELECT 'resenas', _lote, COUNT(*) FROM {CATALOG}.bronze.resenas GROUP BY ALL
UNION ALL SELECT 'devoluciones', _lote, COUNT(*) FROM {CATALOG}.bronze.devoluciones GROUP BY ALL
UNION ALL SELECT 'catalogo', _lote, COUNT(*) FROM {CATALOG}.bronze.catalogo GROUP BY ALL
ORDER BY fuente, _lote
"""))

# COMMAND ----------

# Rechazados por fuente: registros distintos (un registro puede fallar varias reglas)
display(spark.sql(f"""
SELECT fuente, politica,
       COUNT(DISTINCT llave, registro_original) AS registros,
       COUNT(*)                                 AS violaciones
FROM {CATALOG}.silver.cuarentena
GROUP BY ALL ORDER BY fuente, politica
"""))

display(spark.sql(f"""
SELECT fuente, regla, politica, COUNT(*) AS violaciones
FROM {CATALOG}.silver.cuarentena
GROUP BY ALL ORDER BY fuente, politica, violaciones DESC
"""))

# COMMAND ----------

vacias = [t for (capa, t, n) in filas if capa == "gold" and not n]
if vacias:
    raise Exception(f"VALIDACIÓN FALLIDA: tablas Gold vacías o inexistentes: {vacias}")
print("OK: todas las tablas Gold tienen datos")
