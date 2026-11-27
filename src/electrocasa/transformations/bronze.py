"""
Capa BRONZE (Auto Loader dentro del pipeline).

Fuentes con Auto Loader: ventas, empleados, resenas, devoluciones.
  - Llegan por lotes incrementales (archivos nuevos que se suman a la carpeta).
  - Auto Loader procesa solo los archivos nuevos y el pipeline gestiona checkpoint
    y schema location (no se configuran a mano).
Catálogo y sucursales se cargan con COPY INTO fuera del pipeline
(ver src/electrocasa/ingesta/01_copy_into_bronze.py).

Reglas de Bronze: sin lógica de negocio. Todo se guarda como texto (raw) con un
schema explícito (nunca inferSchema) y columnas técnicas de auditoría.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, StringType, StructField, StructType,
)

LANDING = spark.conf.get("landing_path")  # /Volumes/<catalogo>/default/vol_landing


def _s(*cols):
    """Schema explícito con todas las columnas como STRING (raw)."""
    return StructType([StructField(c, StringType(), True) for c in cols])


SCHEMA_VENTAS = _s("venta_id", "sucursal_id", "producto_id", "vendedor_id", "cantidad",
                   "monto_total", "metodo_pago", "fecha_venta", "canal")
SCHEMA_EMPLEADOS = _s("id_empleado", "nombre", "dni", "email", "salario", "sucursal_id",
                      "cargo", "tipo_evento", "fecha_evento", "fecha_registro")
SCHEMA_DEVOLUCIONES = _s("devolucion_id", "venta_id", "sucursal_id", "producto_id",
                         "motivo", "monto_reembolso", "fecha_devolucion")
SCHEMA_RESENAS = StructType([
    StructField("resena_id", StringType()),
    StructField("producto_id", StringType()),
    StructField("cliente_id", StringType()),
    StructField("calificacion", StringType()),
    StructField("comentario", StringType()),
    StructField("tags", ArrayType(StringType())),
    StructField("respuestas", ArrayType(StructType([
        StructField("autor", StringType()),
        StructField("texto", StringType()),
        StructField("fecha", StringType()),
    ]))),
    StructField("fecha_resena", StringType()),
    # metadata cambia de estructura según el origen (app/web/kiosko):
    # se guarda como texto JSON en Bronze y se convierte a VARIANT en Silver.
    StructField("metadata", StringType()),
])


def _auditoria(df):
    """Columnas técnicas: momento de ingesta, archivo de origen y lote."""
    return df.select(
        "*",
        F.current_timestamp().alias("_ingesta_ts"),
        F.col("_metadata.file_path").alias("_archivo_origen"),
        F.col("_metadata.file_name").alias("_archivo_nombre"),
        F.regexp_extract(F.col("_metadata.file_name"), r"(lote_\d+)", 1).alias("_lote"),
    )


def _autoloader_csv(carpeta, schema):
    return _auditoria(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(schema)
        .load(f"{LANDING}/{carpeta}/")
    )


@dp.table(name="bronze.ventas", comment="Ventas raw (Auto Loader, CSV por lote)",
          table_properties={"quality": "bronze"})
def bronze_ventas():
    return _autoloader_csv("ventas", SCHEMA_VENTAS)


@dp.table(name="bronze.empleados", comment="Eventos de RR.HH. raw (Auto Loader, CSV por lote)",
          table_properties={"quality": "bronze"})
def bronze_empleados():
    return _autoloader_csv("empleados", SCHEMA_EMPLEADOS)


@dp.table(name="bronze.devoluciones", comment="Devoluciones raw (Auto Loader, CSV por lote)",
          table_properties={"quality": "bronze"})
def bronze_devoluciones():
    return _autoloader_csv("devoluciones", SCHEMA_DEVOLUCIONES)


@dp.table(name="bronze.resenas", comment="Reseñas raw (Auto Loader, JSON Lines por lote)",
          table_properties={"quality": "bronze"})
def bronze_resenas():
    return _auditoria(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(SCHEMA_RESENAS)
        .load(f"{LANDING}/resenas/")
    )
