"""
Capa SILVER: limpieza, estandarización, deduplicación y calidad.

Patrón usado en cada fuente:
  1. stg_<fuente> (vista temporal del pipeline): normaliza tipos y valores, deduplica
     por la llave y calcula qué reglas falla cada fila (_fallas_drop / _fallas_warn).
  2. silver.<fuente> (materialized view): declara las MISMAS reglas como expectativas
     (expect_all_or_drop / expect_all) y publica solo las columnas de negocio.
  3. silver.cuarentena: junta las filas que fallan alguna regla, con fuente, regla,
     política, lote, archivo y el registro original en JSON (se puede reprocesar).

Las reglas viven en un solo diccionario por fuente: expectativa y cuarentena usan
exactamente la misma condición, y es la misma que declara el contrato (contratos/*.yml).

Se usan materialized views en Silver porque el volumen es chico (decenas de miles de
filas) y así cada refresh recalcula la deduplicación y la historia de Empleados con
todos los lotes: un evento que llega tarde queda ubicado en su lugar sin lógica extra.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("catalog")
MASK_DNI = f"{CATALOG}.default.mask_dni"
MASK_SALARIO = f"{CATALOG}.default.mask_salario"

INICIO_PERIODO = "2026-03-01"
FIN_PERIODO = "2026-10-31"

# --------------------------------------------------------------------------------------
# Reglas de calidad (deben coincidir con contratos/*.yml)
# Todas las condiciones son null-safe: una condición NULL cuenta como violación.
# --------------------------------------------------------------------------------------
REGLAS = {
    "ventas": {
        "drop": {
            "venta_id_no_nulo": "venta_id IS NOT NULL",
            "monto_positivo": "monto_total IS NOT NULL AND monto_total > 0",
            "cantidad_positiva": "cantidad IS NOT NULL AND cantidad > 0",
            "sucursal_informada": "sucursal_id IS NOT NULL",
            "fecha_valida": "fecha_venta IS NOT NULL",
            "canal_valido": "canal IN ('tienda_fisica', 'ecommerce')",
        },
        "warn": {
            "producto_en_catalogo": "en_catalogo",
            "metodo_pago_conocido": "metodo_pago <> 'otro'",
            "vendedor_en_tienda_fisica": "canal <> 'tienda_fisica' OR vendedor_id IS NOT NULL",
        },
    },
    "catalogo": {
        "drop": {"producto_id_no_nulo": "producto_id IS NOT NULL"},
        "warn": {
            "precio_valido": "precio_lista IS NOT NULL AND precio_lista > 0",
            "categoria_conocida": "categoria IN ('linea_blanca','cocina','electronica','climatizacion','entretenimiento')",
            "marca_informada": "marca IS NOT NULL",
        },
    },
    "empleados": {
        "drop": {
            "evento_ubicable": "id_empleado IS NOT NULL AND fecha_evento IS NOT NULL",
            "tipo_evento_valido": "tipo_evento IN ('alta','baja','transferencia','cambio_salario')",
        },
        "warn": {
            "salario_en_rango": "salario_en_rango",
            "dni_informado": "dni IS NOT NULL",
            "dni_unico": "NOT dni_duplicado",
        },
    },
    "resenas": {
        "drop": {
            "resena_id_no_nulo": "resena_id IS NOT NULL",
            "calificacion_en_escala": "calificacion IS NOT NULL AND calificacion BETWEEN 1 AND 5",
            "fecha_valida": "fecha_resena IS NOT NULL",
        },
        "warn": {
            "producto_en_catalogo": "en_catalogo",
            "comentario_informado": "comentario IS NOT NULL",
        },
    },
    "devoluciones": {
        "drop": {
            "venta_origen_existe": "monto_venta IS NOT NULL",
            "reembolso_valido": "monto_reembolso IS NOT NULL AND monto_reembolso >= 0 AND (monto_venta IS NULL OR monto_reembolso <= monto_venta)",
            "fecha_valida": "fecha_devolucion IS NOT NULL",
        },
        "warn": {"motivo_informado": "motivo <> 'sin_motivo'"},
    },
    "tracking_envios": {
        "drop": {
            "evento_id_no_nulo": "evento_id IS NOT NULL",
            "tracking_id_no_nulo": "tracking_id IS NOT NULL",
            "estado_conocido": "estado_entrega IN ('pendiente','en_transito','entregado','devuelto')",
            "fecha_modificacion_no_nula": "fecha_modificacion IS NOT NULL",
        },
        "warn": {"venta_existe": "venta_existe"},
    },
    "sucursales": {
        "drop": {"sucursal_id_no_nulo": "sucursal_id IS NOT NULL"},
        "warn": {},
    },
}


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def _fallas(reglas: dict):
    if not reglas:
        return F.array().cast("array<string>")
    return F.filter(
        F.array(*[F.when(~F.coalesce(F.expr(c), F.lit(False)), F.lit(n)) for n, c in reglas.items()]),
        lambda x: x.isNotNull(),
    )


def _validar(df, fuente: str, llave: str):
    r = REGLAS[fuente]
    return (df.withColumn("_fuente", F.lit(fuente))
              .withColumn("_llave", F.col(llave).cast("string"))
              .withColumn("_fallas_drop", _fallas(r["drop"]))
              .withColumn("_fallas_warn", _fallas(r["warn"])))


def _dedup(df, llave: list, orden):
    """Deja una fila por llave (la primera según `orden`). Las filas con llave nula
    no se agrupan entre sí: siguen de largo y las frena la expectativa."""
    cond_null = " OR ".join(f"{c} IS NULL" for c in llave)
    w = Window.partitionBy(*llave).orderBy(*orden)
    con_llave = (df.filter(f"NOT ({cond_null})")
                   .withColumn("_rn", F.row_number().over(w))
                   .filter("_rn = 1").drop("_rn"))
    return con_llave.unionByName(df.filter(cond_null))


def _registro_raw(df, cols):
    """Guarda el registro tal como llegó (para reprocesarlo desde la cuarentena)."""
    return df.withColumn("_registro", F.to_json(F.struct(*cols)))


def _ids_catalogo():
    return spark.read.table("silver.catalogo").select("producto_id").distinct() \
                .withColumn("en_catalogo", F.lit(True))


def _monto(col):
    """'1.234,50' -> 1234.50 ; '1234.50' -> 1234.50 ; basura -> NULL"""
    return (f"try_cast(CASE WHEN {col} LIKE '%,%' "
            f"THEN replace(replace(trim({col}), '.', ''), ',', '.') "
            f"ELSE trim({col}) END AS DECIMAL(12,2))")


def _fecha(col, formatos):
    # las comillas del patrón (p. ej. 'T') se escapan para el literal SQL
    return "coalesce(" + ", ".join(
        "to_date(try_to_timestamp(trim({c}), '{f}'))".format(c=col, f=f.replace("'", "\\'"))
        for f in formatos) + ")"


# ======================================================================================
# CATÁLOGO  (bronze.catalogo viene de COPY INTO, fuera del pipeline)
# ======================================================================================
@dp.temporary_view(name="stg_catalogo")
def stg_catalogo():
    b = spark.read.table(f"{CATALOG}.bronze.catalogo")
    b = _registro_raw(b, ["producto_id", "nombre_producto", "categoria", "marca", "precio_lista", "activo"])
    df = b.selectExpr(
        "nullif(trim(producto_id), '') AS producto_id",
        "trim(nombre_producto) AS nombre_producto",
        """CASE regexp_replace(translate(lower(trim(categoria)), 'áéíóú', 'aeiou'), '[ _]+', '_')
             WHEN 'linea_blanca' THEN 'linea_blanca'
             WHEN 'cocina' THEN 'cocina'
             WHEN 'electronica' THEN 'electronica'
             WHEN 'climatizacion' THEN 'climatizacion'
             WHEN 'entretenimiento' THEN 'entretenimiento'
             ELSE 'otra' END AS categoria""",
        "nullif(trim(marca), '') AS marca",
        # 'S/ 1,299.90' -> 1299.90 ; '3107.28' -> 3107.28
        r"try_cast(regexp_replace(precio_lista, '[^0-9.\\-]', '') AS DECIMAL(12,2)) AS precio_raw",
        "lower(trim(activo)) = 'true' AS activo",
        "to_date(_lote) AS fecha_snapshot",
        "_lote", "_archivo_origen", "_ingesta_ts", "_registro",
    )
    # Silver = versión del ÚLTIMO snapshot donde aparece cada producto.
    # Duplicados dentro de un snapshot (marzo): gana el precio válido y, entre válidos, el mayor.
    df = _dedup(df, ["producto_id"], [
        F.col("fecha_snapshot").desc(),
        F.when(F.col("precio_raw") > 0, 0).otherwise(1),
        F.col("precio_raw").desc(),
        F.col("_ingesta_ts").desc(),
    ])
    # Precio inválido (texto ilegible, negativo, cero o nulo) -> NULL en Silver + warn.
    # El producto se conserva porque su categoría es necesaria para ventas y reseñas.
    df = df.withColumn("precio_lista", F.when(F.col("precio_raw") > 0, F.col("precio_raw")))
    return _validar(df, "catalogo", "producto_id")


@dp.materialized_view(name="silver.catalogo",
                      comment="Maestro de productos: última versión por producto (conserva descontinuados con activo=false)",
                      table_properties={"quality": "silver"})
@dp.expect_all_or_drop(REGLAS["catalogo"]["drop"])
@dp.expect_all(REGLAS["catalogo"]["warn"])
def silver_catalogo():
    return spark.read.table("stg_catalogo").select(
        "producto_id", "nombre_producto", "categoria", "marca",
        F.col("precio_lista").cast("decimal(12,2)").alias("precio_lista"),
        "activo", "fecha_snapshot", "_lote", "_ingesta_ts")


# ======================================================================================
# SUCURSALES (bronze.sucursales viene de COPY INTO)
# ======================================================================================
@dp.temporary_view(name="stg_sucursales")
def stg_sucursales():
    b = spark.read.table(f"{CATALOG}.bronze.sucursales")
    b = _registro_raw(b, ["sucursal_id", "nombre_sucursal", "distrito", "ciudad", "region", "fecha_apertura"])
    df = b.selectExpr(
        "nullif(trim(sucursal_id), '') AS sucursal_id",
        "trim(nombre_sucursal) AS nombre_sucursal",
        "initcap(lower(trim(distrito))) AS distrito",
        "initcap(lower(trim(ciudad))) AS ciudad",          # 'AREQUIPA' -> 'Arequipa'
        "initcap(lower(trim(region))) AS region",
        f"{_fecha('fecha_apertura', ['yyyy-MM-dd', 'dd/MM/yyyy'])} AS fecha_apertura",
        "_lote", "_archivo_origen", "_ingesta_ts", "_registro",
    )
    df = _dedup(df, ["sucursal_id"], [F.col("_ingesta_ts").desc()])
    return _validar(df, "sucursales", "sucursal_id")


@dp.materialized_view(name="silver.sucursales", comment="Tabla de referencia de sucursales",
                      table_properties={"quality": "silver"})
@dp.expect_all_or_drop(REGLAS["sucursales"]["drop"])
def silver_sucursales():
    return spark.read.table("stg_sucursales").select(
        "sucursal_id", "nombre_sucursal", "distrito", "ciudad", "region", "fecha_apertura")


# ======================================================================================
# VENTAS
# ======================================================================================
@dp.temporary_view(name="stg_ventas")
def stg_ventas():
    b = spark.read.table("bronze.ventas")
    b = _registro_raw(b, ["venta_id", "sucursal_id", "producto_id", "vendedor_id", "cantidad",
                          "monto_total", "metodo_pago", "fecha_venta", "canal"])
    df = b.selectExpr(
        "nullif(trim(venta_id), '') AS venta_id",
        "nullif(trim(sucursal_id), '') AS sucursal_id",
        "nullif(trim(producto_id), '') AS producto_id",
        "nullif(trim(vendedor_id), '') AS vendedor_id",
        "try_cast(trim(cantidad) AS INT) AS cantidad",
        f"{_monto('monto_total')} AS monto_total",
        """CASE lower(trim(metodo_pago))
             WHEN 'plin' THEN 'plin'
             WHEN 'yape' THEN 'yape'
             WHEN 'transferencia' THEN 'transferencia' WHEN 'trf' THEN 'transferencia'
             WHEN 'efectivo' THEN 'efectivo' WHEN 'efv' THEN 'efectivo'
             WHEN 'tarjeta' THEN 'tarjeta' WHEN 'tarjeta de credito' THEN 'tarjeta'
             WHEN 'tarjeta de debito' THEN 'tarjeta'
             WHEN 'tc' THEN 'tarjeta' WHEN 'td' THEN 'tarjeta'
             ELSE 'otro' END AS metodo_pago""",
        f"""{_fecha('fecha_venta', ["yyyy-MM-dd'T'HH:mm:ss", 'yyyy-MM-dd', 'dd/MM/yyyy'])} AS fecha_venta""",
        "lower(trim(canal)) AS canal",
        "_lote", "_archivo_origen", "_ingesta_ts", "_registro",
    )
    # Reingestas: la misma venta llega idéntica en el lote siguiente -> se queda la primera
    df = _dedup(df, ["venta_id"], [F.col("_ingesta_ts").asc(), F.col("_archivo_origen").asc()])
    df = df.join(F.broadcast(_ids_catalogo()), "producto_id", "left") \
           .withColumn("en_catalogo", F.coalesce("en_catalogo", F.lit(False)))
    return _validar(df, "ventas", "venta_id")


@dp.materialized_view(name="silver.ventas",
                      comment="Ventas limpias y deduplicadas (tienda física + ecommerce)",
                      table_properties={"quality": "silver"})
@dp.expect_all_or_drop(REGLAS["ventas"]["drop"])
@dp.expect_all(REGLAS["ventas"]["warn"])
def silver_ventas():
    return spark.read.table("stg_ventas").select(
        "venta_id", "sucursal_id", "producto_id", "vendedor_id", "cantidad", "monto_total",
        "metodo_pago", "fecha_venta", "canal", "en_catalogo", "_lote", "_ingesta_ts")


# ======================================================================================
# DEVOLUCIONES (la regla de reembolso se evalúa sobre la vista ya unida con ventas)
# ======================================================================================
@dp.temporary_view(name="stg_devoluciones")
def stg_devoluciones():
    b = spark.read.table("bronze.devoluciones")
    b = _registro_raw(b, ["devolucion_id", "venta_id", "sucursal_id", "producto_id",
                          "motivo", "monto_reembolso", "fecha_devolucion"])
    df = b.selectExpr(
        "nullif(trim(devolucion_id), '') AS devolucion_id",
        "nullif(trim(venta_id), '') AS venta_id",
        "nullif(trim(sucursal_id), '') AS sucursal_id",
        "nullif(trim(producto_id), '') AS producto_id",
        "coalesce(nullif(lower(trim(motivo)), ''), 'sin_motivo') AS motivo",
        f"{_monto('monto_reembolso')} AS monto_reembolso",
        f"{_fecha('fecha_devolucion', ['yyyy-MM-dd', 'dd/MM/yyyy'])} AS fecha_devolucion",
        "_lote", "_archivo_origen", "_ingesta_ts", "_registro",
    )
    df = _dedup(df, ["devolucion_id"], [F.col("_ingesta_ts").asc(), F.col("_archivo_origen").asc()])
    ventas = spark.read.table("silver.ventas").select(
        "venta_id", F.col("monto_total").alias("monto_venta"))
    df = df.join(ventas, "venta_id", "left")
    return _validar(df, "devoluciones", "devolucion_id")


@dp.materialized_view(name="silver.devoluciones",
                      comment="Devoluciones válidas: venta de origen existente y reembolso entre 0 y el monto de la venta",
                      table_properties={"quality": "silver"})
@dp.expect_all_or_drop(REGLAS["devoluciones"]["drop"])
@dp.expect_all(REGLAS["devoluciones"]["warn"])
def silver_devoluciones():
    return spark.read.table("stg_devoluciones").select(
        "devolucion_id", "venta_id", "sucursal_id", "producto_id", "motivo",
        "monto_reembolso", "monto_venta", "fecha_devolucion", "_lote", "_ingesta_ts")


# ======================================================================================
# RESEÑAS (metadata -> VARIANT)
# ======================================================================================
@dp.temporary_view(name="stg_resenas")
def stg_resenas():
    b = spark.read.table("bronze.resenas")
    b = _registro_raw(b, ["resena_id", "producto_id", "cliente_id", "calificacion", "comentario",
                          "tags", "respuestas", "fecha_resena", "metadata"])
    df = b.selectExpr(
        "nullif(trim(resena_id), '') AS resena_id",
        "nullif(trim(producto_id), '') AS producto_id",
        "nullif(trim(cliente_id), '') AS cliente_id",
        # 5 / '5' / 5.0 -> 5 ; 4.5 -> NULL (no es una calificación entera)
        """CASE WHEN try_cast(calificacion AS DOUBLE) = floor(try_cast(calificacion AS DOUBLE))
                THEN CAST(try_cast(calificacion AS DOUBLE) AS INT) END AS calificacion""",
        "nullif(trim(comentario), '') AS comentario",
        "coalesce(tags, CAST(array() AS ARRAY<STRING>)) AS tags",     # NULL -> []
        """transform(coalesce(respuestas, CAST(array() AS ARRAY<STRUCT<autor:STRING,texto:STRING,fecha:STRING>>)),
                     r -> named_struct('autor', coalesce(nullif(trim(r.autor), ''), 'desconocido'),
                                       'texto', r.texto,
                                       'fecha', to_date(try_to_timestamp(r.fecha, 'yyyy-MM-dd')))) AS respuestas""",
        f"{_fecha('fecha_resena', ['yyyy-MM-dd', 'dd/MM/yyyy'])} AS fecha_resena",
        "try_parse_json(metadata) AS metadata",
        "_lote", "_archivo_origen", "_ingesta_ts", "_registro",
    )
    df = df.withColumn("origen", F.expr("variant_get(metadata, '$.origen', 'string')")) \
           .withColumn("app_version", F.expr("variant_get(metadata, '$.app.version', 'string')"))
    df = _dedup(df, ["resena_id"], [F.col("_ingesta_ts").asc(), F.col("_archivo_origen").asc()])
    df = df.join(F.broadcast(_ids_catalogo()), "producto_id", "left") \
           .withColumn("en_catalogo", F.coalesce("en_catalogo", F.lit(False)))
    return _validar(df, "resenas", "resena_id")


@dp.materialized_view(name="silver.resenas",
                      comment="Reseñas con calificación válida (1-5), deduplicadas; metadata como VARIANT",
                      table_properties={"quality": "silver"})
@dp.expect_all_or_drop(REGLAS["resenas"]["drop"])
@dp.expect_all(REGLAS["resenas"]["warn"])
def silver_resenas():
    return spark.read.table("stg_resenas").select(
        "resena_id", "producto_id", "cliente_id", "calificacion", "comentario", "tags",
        "respuestas", "fecha_resena", "metadata", "origen", "app_version", "en_catalogo",
        "_lote", "_ingesta_ts")


# ======================================================================================
# EMPLEADOS: eventos limpios + historia SCD Type 2 (intervalos armados con ventanas)
# dni y salario se enmascaran con MASK declarado en el schema de la tabla del pipeline.
# ======================================================================================
@dp.temporary_view(name="stg_empleados")
def stg_empleados():
    b = spark.read.table("bronze.empleados")
    b = _registro_raw(b, ["id_empleado", "nombre", "dni", "email", "salario", "sucursal_id",
                          "cargo", "tipo_evento", "fecha_evento", "fecha_registro"])
    df = b.selectExpr(
        "nullif(trim(id_empleado), '') AS id_empleado",
        "initcap(trim(nombre)) AS nombre",
        "nullif(trim(dni), '') AS dni",
        "lower(trim(email)) AS email",
        "try_cast(trim(salario) AS DECIMAL(10,2)) AS salario_origen",
        "nullif(trim(sucursal_id), '') AS sucursal_id",
        "trim(cargo) AS cargo",
        """CASE regexp_replace(lower(trim(tipo_evento)), '[ -]+', '_')
             WHEN 'cese' THEN 'baja'
             ELSE regexp_replace(lower(trim(tipo_evento)), '[ -]+', '_') END AS tipo_evento""",
        f"{_fecha('fecha_evento', ['yyyy-MM-dd', 'dd/MM/yyyy'])} AS fecha_evento",
        "try_to_timestamp(trim(fecha_registro), 'yyyy-MM-dd HH:mm:ss') AS fecha_registro",
        "_lote", "_archivo_origen", "_ingesta_ts", "_registro",
    )
    # Salarios fuera de rango (error de un orden de magnitud: 246616 en vez de 2466.16):
    # no se corrigen a ciegas; se dejan en NULL y la historia arrastra el último salario válido.
    df = df.withColumn("salario_en_rango", F.expr("coalesce(salario_origen BETWEEN 900 AND 20000, false)")) \
           .withColumn("salario", F.when(F.col("salario_en_rango"), F.col("salario_origen")))
    df = _dedup(df, ["id_empleado", "tipo_evento", "fecha_evento", "fecha_registro"],
                [F.col("_ingesta_ts").asc()])
    # Mismo DNI en dos empleados distintos: se conservan ambos y se marcan (warn)
    w_dni = Window.partitionBy("dni")
    df = df.withColumn("dni_duplicado", F.when(
        F.col("dni").isNotNull(), F.size(F.collect_set("id_empleado").over(w_dni)) > 1).otherwise(F.lit(False)))
    return _validar(df, "empleados", "id_empleado")


@dp.materialized_view(
    name="silver.empleados_eventos",
    comment="Eventos de RR.HH. limpios (alta, transferencia, cambio_salario, baja). dni y salario enmascarados.",
    table_properties={"quality": "silver"},
    schema=f"""
        id_empleado STRING NOT NULL,
        nombre STRING,
        dni STRING MASK {MASK_DNI},
        email STRING,
        salario DECIMAL(10,2) MASK {MASK_SALARIO},
        salario_en_rango BOOLEAN,
        dni_duplicado BOOLEAN,
        sucursal_id STRING,
        cargo STRING,
        tipo_evento STRING,
        fecha_evento DATE NOT NULL,
        fecha_registro TIMESTAMP,
        _lote STRING,
        _ingesta_ts TIMESTAMP
    """,
)
@dp.expect_all_or_drop(REGLAS["empleados"]["drop"])
@dp.expect_all(REGLAS["empleados"]["warn"])
def silver_empleados_eventos():
    return spark.read.table("stg_empleados").select(
        "id_empleado", "nombre", "dni", "email", "salario", "salario_en_rango", "dni_duplicado",
        "sucursal_id", "cargo", "tipo_evento", "fecha_evento", "fecha_registro", "_lote", "_ingesta_ts")


@dp.materialized_view(
    name="silver.empleados_scd2",
    comment=("Historia SCD Type 2 de empleados. Una versión por evento, vigente en "
             "[vigente_desde, vigente_hasta). Orden: fecha_evento y, a igualdad, fecha_registro."),
    table_properties={"quality": "silver"},
    schema=f"""
        id_empleado STRING NOT NULL,
        version INT,
        nombre STRING,
        dni STRING MASK {MASK_DNI},
        email STRING,
        salario DECIMAL(10,2) MASK {MASK_SALARIO},
        sucursal_id STRING,
        cargo STRING,
        tipo_evento STRING,
        estado STRING,
        fecha_alta DATE,
        vigente_desde DATE,
        vigente_hasta DATE,
        es_version_actual BOOLEAN,
        fecha_registro TIMESTAMP
    """,
)
def silver_empleados_scd2():
    ev = spark.read.table("silver.empleados_eventos")
    orden = [F.col("fecha_evento").asc(), F.col("fecha_registro").asc()]
    w = Window.partitionBy("id_empleado").orderBy(*orden)
    w_acum = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    w_emp = Window.partitionBy("id_empleado")
    return ev.select(
        "id_empleado",
        F.row_number().over(w).cast("int").alias("version"),
        "nombre", "dni", "email",
        # el salario fuera de rango llegó en NULL: se arrastra el último salario válido
        F.last("salario", ignorenulls=True).over(w_acum).cast("decimal(10,2)").alias("salario"),
        "sucursal_id", "cargo", "tipo_evento",
        F.when(F.col("tipo_evento") == "baja", "inactivo").otherwise("activo").alias("estado"),
        F.min(F.when(F.col("tipo_evento") == "alta", F.col("fecha_evento"))).over(w_emp).alias("fecha_alta"),
        F.col("fecha_evento").alias("vigente_desde"),
        F.lead("fecha_evento").over(w).alias("vigente_hasta"),
        F.lead("fecha_evento").over(w).isNull().alias("es_version_actual"),
        "fecha_registro",
    )


# ======================================================================================
# TRACKING DE ENVÍOS (bronze.tracking_envios = tabla destino del conector Lakeflow Connect)
# ======================================================================================
@dp.temporary_view(name="stg_tracking_envios")
def stg_tracking_envios():
    b = spark.read.table(f"{CATALOG}.bronze.tracking_envios")
    b = _registro_raw(b, ["evento_id", "tracking_id", "venta_id", "courier", "estado_entrega",
                          "sucursal_origen", "fecha_actualizacion", "fecha_modificacion"])
    df = b.selectExpr(
        "CAST(evento_id AS BIGINT) AS evento_id",
        "nullif(trim(tracking_id), '') AS tracking_id",
        "nullif(trim(venta_id), '') AS venta_id",
        "initcap(regexp_replace(lower(trim(courier)), '[_-]+', ' ')) AS courier",
        """CASE
             WHEN lower(estado_entrega) LIKE '%pend%' THEN 'pendiente'
             WHEN lower(estado_entrega) LIKE '%camino%' OR lower(estado_entrega) LIKE '%transit%'
                  OR lower(estado_entrega) LIKE '%ruta%' THEN 'en_transito'
             WHEN lower(estado_entrega) LIKE '%devuel%' OR lower(estado_entrega) LIKE '%retorn%' THEN 'devuelto'
             WHEN lower(estado_entrega) LIKE '%entreg%' THEN 'entregado'
             ELSE lower(trim(estado_entrega)) END AS estado_entrega""",
        "nullif(trim(sucursal_origen), '') AS sucursal_origen",
        "CAST(fecha_actualizacion AS DATE) AS fecha_actualizacion",
        "CAST(fecha_modificacion AS TIMESTAMP) AS fecha_modificacion",
        "'conector_sqlserver' AS _lote",
        "'dbo.TrackingEnvios' AS _archivo_origen",
        "CAST(NULL AS TIMESTAMP) AS _ingesta_ts",
        "_registro",
    )
    # Mensajes reprocesados: mismo envío y mismo estado con distinto evento_id -> se queda el primero
    df = _dedup(df, ["tracking_id", "estado_entrega"],
                [F.col("fecha_modificacion").asc(), F.col("evento_id").asc()])
    ventas = spark.read.table("silver.ventas").select("venta_id").distinct() \
                  .withColumn("venta_existe", F.lit(True))
    df = df.join(ventas, "venta_id", "left") \
           .withColumn("venta_existe", F.coalesce("venta_existe", F.lit(False)))
    return _validar(df, "tracking_envios", "evento_id")


@dp.materialized_view(name="silver.tracking_envios",
                      comment="Eventos de envío estandarizados y sin mensajes reprocesados",
                      table_properties={"quality": "silver"})
@dp.expect_all_or_drop(REGLAS["tracking_envios"]["drop"])
@dp.expect_all(REGLAS["tracking_envios"]["warn"])
def silver_tracking_envios():
    return spark.read.table("stg_tracking_envios").select(
        "evento_id", "tracking_id", "venta_id", "courier", "estado_entrega", "sucursal_origen",
        "fecha_actualizacion", "fecha_modificacion", "venta_existe")


# ======================================================================================
# CUARENTENA: todo lo que falla una regla (drop = rechazado, warn = pasa pero se registra)
# ======================================================================================
@dp.materialized_view(
    name="silver.cuarentena",
    comment="Registros que fallaron una regla de calidad: fuente, regla, política, lote, archivo y registro original",
    table_properties={"quality": "silver"},
)
def silver_cuarentena():
    partes = []
    for fuente in REGLAS:
        stg = spark.read.table(f"stg_{fuente}")
        for col, politica in (("_fallas_drop", "drop"), ("_fallas_warn", "warn")):
            partes.append(
                stg.filter(F.size(col) > 0).select(
                    F.col("_fuente").alias("fuente"),
                    F.col("_llave").alias("llave"),
                    F.explode(col).alias("regla"),
                    F.lit(politica).alias("politica"),
                    F.col("_lote").alias("lote"),
                    F.col("_archivo_origen").alias("archivo_origen"),
                    F.col("_ingesta_ts").alias("fecha_ingesta"),
                    F.col("_registro").alias("registro_original"),
                    F.current_timestamp().alias("fecha_proceso"),
                )
            )
    out = partes[0]
    for p in partes[1:]:
        out = out.unionByName(p)
    return out
