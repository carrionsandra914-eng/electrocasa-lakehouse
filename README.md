# ElectroCasa Lakehouse — Proyecto Integrador Azure Databricks

Plataforma de datos de punta a punta sobre **Azure Databricks** para ElectroCasa, cadena de 40 sucursales de electrodomésticos en Perú: ingesta de seis fuentes heterogéneas, modelo **Medallion** con **Lakeflow Spark Declarative Pipelines**, historización **SCD Type 2**, calidad con **expectativas + cuarentena**, orquestación con **Lakeflow Jobs**, despliegue con **Declarative Automation Bundles** y gobierno con **Unity Catalog** (grupos, `GRANT/REVOKE`, masking).

> Autor: Yomar Vega · Edición 15 · Entrega: repositorio GitHub

---

## 1. Caso de negocio

ElectroCasa no tiene una fuente única de verdad: cada sucursal exporta sus ventas, RR.HH. registra eventos en su sistema, las reseñas se leen a mano y el tracking vive en la base del courier. Este proyecto integra todo en un lakehouse y responde **seis preguntas** (marzo–octubre 2026) desde Gold:

| # | Pregunta | Tabla Gold |
|---|---|---|
| 1 | Ventas, devoluciones, ventas netas, cantidad y ticket promedio por sucursal y mes | `gold.ventas_netas_sucursal_mes` |
| 2 | Top 10 por unidades y top 10 por tasa de devolución | `gold.top10_productos_unidades`, `gold.top10_productos_tasa_devolucion` |
| 3 | Dotación activa por sucursal al cierre de cada mes | `gold.dotacion_sucursal_mes` |
| 4 | Ventas de tienda física por vendedor, **sucursal vigente en la fecha de la venta** y mes | `gold.ventas_vendedor_sucursal_mes` |
| 5 | % de reseñas negativas por categoría y mes | `gold.resenas_negativas_categoria_mes` |
| 6 | Horas promedio pendiente→entregado y % devueltos por courier | `gold.despachos_courier` |
| + | (Reto VARIANT) % negativas por origen y versión de app | `gold.resenas_negativas_origen` |
| + | Empleados vigentes (evidencia de masking en tabla derivada) | `gold.empleados_actuales` |

---

## 2. Arquitectura

![Flujo de datos](img/arquitectura_flujo_datos.png)

![Orden del job](img/arquitectura_orden_job.png)

| Fuente | Formato | Método de ingesta | Bronze | Silver |
|---|---|---|---|---|
| Ventas | CSV por lote | **Auto Loader** (en el pipeline) | `bronze.ventas` (ST) | `silver.ventas` |
| Empleados | CSV por lote | **Auto Loader** | `bronze.empleados` (ST) | `silver.empleados_eventos`, `silver.empleados_scd2` |
| Reseñas | JSON Lines por lote | **Auto Loader** | `bronze.resenas` (ST) | `silver.resenas` |
| Devoluciones | CSV por lote | **Auto Loader** | `bronze.devoluciones` (ST) | `silver.devoluciones` |
| Catálogo | JSON (snapshot completo) | **COPY INTO** (tarea del job) | `bronze.catalogo` (Delta) | `silver.catalogo` |
| Sucursales | CSV único | **COPY INTO** | `bronze.sucursales` (Delta) | `silver.sucursales` |
| Tracking | Azure SQL `dbo.TrackingEnvios` | **Lakeflow Connect query-based** (cursor `fecha_modificacion`, PK `evento_id`) | `bronze.tracking_envios` | `silver.tracking_envios` |
| Rechazos | — | — | — | `silver.cuarentena` |

### Por qué cada método de ingesta
- **Auto Loader** para ventas, empleados, reseñas y devoluciones: llegan como **lotes incrementales** (archivos nuevos que se suman a la carpeta). Auto Loader procesa solo lo nuevo, recuerda qué archivos leyó y el pipeline gestiona checkpoint y schema location (no se configuran a mano, así un full refresh los limpia).
- **COPY INTO** para catálogo y sucursales: el catálogo llega como **snapshot completo** dos veces al año y sucursales es un archivo de referencia de 40 filas. No hay flujo continuo que justifique un stream; una carga batch **idempotente** (COPY INTO no recarga archivos ya cargados) es más simple. Corre fuera del pipeline como tarea del job y el pipeline lee esas tablas como fuente batch (materialized views).
- **Tracking:** obligatorio con el conector query-based; ver §4.

### Estructura del repositorio
```
electrocasa-lakehouse/
├── databricks.yml                       # bundle: variables y targets dev / prod
├── resources/
│   ├── electrocasa_pipeline.yml         # Lakeflow Spark Declarative Pipeline (serverless)
│   └── electrocasa_job.yml              # Job: COPY INTO -> Tracking -> Pipeline -> Validación
├── src/electrocasa/
│   ├── transformations/
│   │   ├── bronze.py                    # Auto Loader (4 streaming tables)
│   │   ├── silver.py                    # limpieza, reglas, SCD2, masking, cuarentena
│   │   └── gold.py                      # 6 preguntas + retos
│   ├── ingesta/copy_into_bronze.py      # tarea COPY INTO (notebook)
│   ├── validacion/validacion.py         # tarea de validación (notebook)
│   └── utils/
├── contratos/                           # 7 contratos de datos (uno por fuente)
├── notebooks/
│   ├── 00_setup.ipynb                   # catálogo, schemas, volume, máscaras, GRANT/REVOKE
│   └── 01_monitoreo_y_evidencias.ipynb  # event log, system tables, tracking, masking, lineage
└── img/
```

---

## 3. Decisiones de diseño (justificadas)

### 3.1 Bronze
- Todo como `STRING` con **schema explícito** (nunca `inferSchema`). Reseñas: `tags ARRAY<STRING>`, `respuestas ARRAY<STRUCT<autor,texto,fecha>>`, `metadata` como texto JSON.
- Columnas de auditoría: `_ingesta_ts`, `_archivo_origen` / `_archivo_nombre` (de `_metadata`, no `input_file_name()`), `_lote`.
- Sin limpieza, filtros ni particionado por fecha de negocio.

### 3.2 Silver: patrón staging → expectativas → cuarentena
Cada fuente tiene una vista temporal `stg_<fuente>` que normaliza, deduplica y calcula qué reglas falla cada fila. La tabla Silver declara **las mismas reglas** como expectativas (`dp.expect_all_or_drop` / `dp.expect_all`) desde un único diccionario `REGLAS` en `silver.py`, que coincide 1 a 1 con `contratos/*.yml`. `silver.cuarentena` junta todas las violaciones con **fuente, llave, regla, política, lote, archivo, fecha de ingesta, fecha de proceso y el registro original en JSON** (reprocesable) → reto *Cuarentena con trazabilidad completa*.

Silver se implementa con **materialized views**: el volumen es chico (decenas de miles de filas) y recalcular en cada refresh garantiza que las reingestas no dupliquen y que los eventos tardíos de Empleados queden en su lugar sin lógica incremental extra.

### 3.3 Políticas de violación

| Fuente | Regla | Política | Por qué |
|---|---|---|---|
| Ventas | `monto_positivo`, `cantidad_positiva`, `sucursal_informada`, `fecha_valida`, `venta_id_no_nulo`, `canal_valido` | **drop** | Distorsionan P1/P2/P4 y no se pueden corregir sin la fuente. `fail` detendría todo el pipeline por unas decenas de filas. |
| Ventas | `producto_en_catalogo`, `metodo_pago_conocido`, `vendedor_en_tienda_fisica` | warn | La venta existió y su dinero cuenta. |
| Catálogo | `precio_valido` | **warn** (precio → NULL) | Ninguna pregunta usa el precio; descartar el producto dejaría huérfanas sus ventas/reseñas y perderíamos su categoría (P5). |
| Empleados | `evento_ubicable` (id y fecha no nulos), `tipo_evento_valido` | **drop** | Sin ellos el evento no se puede ubicar en la historia. |
| Empleados | `salario_en_rango`, `dni_informado`, `dni_unico` | warn | El evento (p. ej. una transferencia) sigue siendo válido para P3/P4. |
| Reseñas | `calificacion_en_escala` | **drop** | P5 exige excluirlas de numerador y denominador. |
| Devoluciones | `reembolso_valido` (0 ≤ reembolso ≤ venta), `venta_origen_existe` | **drop** | Se evalúa sobre la vista unida con ventas; un reembolso inválido o no verificable altera las ventas netas. |
| Tracking | `estado_conocido` | **drop** | Un estado sin mapear no sirve para P6. |

### 3.4 Decisiones abiertas del enunciado
- **Referencias huérfanas:** ventas y reseñas con producto inexistente **se conservan** (warn). En P5 la reseña sin producto queda fuera por el `JOIN` con el catálogo. Devoluciones con venta inexistente o nula **se descartan** (no se puede validar el monto). Tracking con venta inexistente se conserva (warn), porque P6 mide al courier.
- **Mismo `dni` en dos empleados:** se conservan ambos (son personas distintas por `id_empleado`), se marca `dni_duplicado = true` y se registra en cuarentena como warn para que RR.HH. corrija.
- **Salarios fuera de rango** (un orden de magnitud arriba, p. ej. 246616): no se corrigen a ciegas dividiendo entre 100. Quedan en `NULL` y en la historia SCD2 se **arrastra el último salario válido**.
- **Máscaras:** `dni` → `*****` + 3 últimos dígitos; `salario` → `NULL`. Solo `electrocasa_ingenieria` ve el valor real.
- **Conector Tracking:** **SCD Type 1**. Cada evento es inmutable y `evento_id` es único, así que el merge por llave hace la carga idempotente. Con append only una relectura del cursor podría duplicar filas.
- **Catálogo descontinuado:** se conserva con `activo = false` (tiene ventas y reseñas históricas).
- **Duplicados del snapshot de marzo:** gana el precio válido y, entre válidos, el mayor. Silver guarda la versión del **último snapshot** donde aparece cada producto.

### 3.5 Historización de Empleados (SCD Type 2)
Implementada **a mano con funciones de ventana** en `silver.empleados_scd2` (materialized view):
- Una versión por evento, ordenada por `fecha_evento` y, a igualdad, por `fecha_registro` (columnas de la fuente; nunca `current_timestamp()`).
- `vigente_desde = fecha_evento`, `vigente_hasta = LEAD(fecha_evento)`, intervalo semiabierto `[desde, hasta)`. Con dos eventos el mismo día, el primero queda con intervalo vacío y vale el registrado después.
- **Llegada tardía:** como la vista se recalcula con todos los eventos, un evento de julio que llega en el lote de octubre corta la versión anterior y queda en su lugar (63 eventos tardíos en el lote 3).
- `estado = inactivo` desde la `baja`/`cese`; `fecha_alta` = primera alta del empleado.
- P3: activo en la fecha de corte = versión vigente, `estado = activo` y `fecha_alta <= corte`.
- P4: join `vendedor_id = id_empleado AND vigente_desde <= fecha_venta < vigente_hasta AND estado = 'activo'`; si no hay versión vigente → `SIN_ASIGNAR`.
- `NOT NULL` en el schema no frena una llave nula: la frena la expectativa `evento_ubicable` (drop).

### 3.6 Normalizaciones de Silver
- Fechas: `try_to_timestamp` con todos los formatos de origen (`yyyy-MM-dd`, `dd/MM/yyyy`, `yyyy-MM-ddTHH:mm:ss`) → `DATE`/`TIMESTAMP` sin fallar con ANSI.
- Montos `1.234,50` y precios `"S/ 1,299.90"` → `DECIMAL(12,2)`.
- Conjuntos cerrados: métodos de pago (17 → 5), categorías (17 → 5), tipos de evento (13 → 4), estados de entrega (→ 4), couriers (initcap).
- `ciudad` de sucursales con `initcap` (AREQUIPA → Arequipa).
- Deduplicación por llave (ventas, devoluciones, reseñas, catálogo) y de mensajes reprocesados de tracking (mismo `tracking_id` + estado → el primero por `fecha_modificacion`).

---

## 4. Ingesta de Tracking (Lakeflow Connect)

1. **Connection de Unity Catalog** `conn_electrocasa_sqlserver` (tipo SQL Server). El host, el usuario y la **contraseña quedan guardados en la Connection**, nunca en el código ni en el repo.
2. Catálogo foráneo `electrocasa_sqlserver` (solo para explorar la tabla).
3. Ingesta **query-based**: tabla `dbo.TrackingEnvios`, cursor `fecha_modificacion`, PK `evento_id`, modo **SCD Type 1**, destino `electrocasa_dev.bronze.tracking_envios`. Se dejan las 8 columnas declaradas en el contrato.
4. Evidencia de las dos corridas (`DESCRIBE HISTORY`):

| Corrida | Operación | Filas |
|---|---|---|
| 1 | MERGE | 15.139 |
| 2 (sin cambios en la fuente) | sin nueva versión | 0 |

> 📸 *[Captura: DESCRIBE HISTORY bronze.tracking_envios — notebook 01, sección 1]*

---

## 5. Conteos por lote

Procedimiento: subir al volume solo el lote 1 (más el snapshot de marzo y `sucursales.csv`), correr el job y anotar; repetir con el lote 2 (+ snapshot de agosto) y el lote 3. Los valores se validaron ejecutando el mismo código con Spark sobre los archivos; las capturas de Databricks están debajo.

| Tabla | Lote 1 | Lote 2 | Lote 3 |
|---|---:|---:|---:|
| bronze.ventas | 6.783 | 13.622 | 18.240 |
| bronze.empleados | 2.257 | 3.524 | 4.448 |
| bronze.resenas | 5.292 | 9.280 | 9.280 |
| bronze.devoluciones | 1.387 | 3.091 | 4.217 |
| bronze.catalogo | 2.305 | 4.705 | 4.705 |
| bronze.sucursales | 40 | 40 | 40 |
| silver.ventas | 6.604 | 13.118 | **17.488** |
| silver.catalogo | 2.280 | 2.400 | 2.400 |
| silver.empleados_eventos | 2.239 | 3.495 | 4.410 |
| silver.empleados_scd2 | 2.239 | 3.495 | 4.410 |
| silver.resenas | 5.226 | 9.097 | 9.097 |
| silver.devoluciones | 1.301 | 2.877 | 3.908 |
| silver.sucursales | 40 | 40 | 40 |

Rechazados (`politica = drop`, registros distintos) después del lote 3: ventas 512 · devoluciones 249 · reseñas 103 · empleados 38.

Qué demuestran:
- **Reingestas sin duplicar:** Bronze ventas suma 18.240 filas, pero hay 18.000 `venta_id` distintos; Silver solo cuenta ventas únicas (17.488 = 18.000 − 512 rechazadas). Reseñas: 9.280 en Bronze → 9.200 ids únicos → 9.097 válidas.
- **Snapshot de marzo:** 2.305 filas → 2.280 productos (25 duplicados).
- **Llegada tardía:** los 63 eventos de junio–agosto del lote 3 se insertan en su lugar en `empleados_scd2`.
- **Devoluciones cuyo venta llega en un lote posterior:** quedan fuera en el lote 1 y se recuperan solas en el refresh siguiente (materialized view).

> 📸 *[Capturas de la tarea `validacion` después de cada lote]*

---

## 6. Orquestación (Lakeflow Jobs)

`copy_into_bronze` (notebook) → `ingesta_tracking` (pipeline del conector) → `pipeline_medallion` (pipeline) → `validacion` (notebook).

- **Dependencias reales y en secuencia:** el conector y el pipeline principal no corren en paralelo.
- **Trigger:** **por llegada de archivo** sobre `/Volumes/<catalogo>/default/vol_landing/`. Los lotes llegan de forma irregular (mayo, agosto, octubre); un schedule correría en vacío casi siempre. `wait_after_last_change_seconds: 180` espera a que termine de subirse el lote completo. En `dev` el trigger queda pausado (modo development) y se corre a mano.
- **Reintentos:** 2 en COPY INTO y Tracking (fallas transitorias de red/SQL), 1 en el pipeline y 0 en validación (si falla, es un problema de datos que no se arregla reintentando).
- **Alertas:** correo al usuario que despliega (`${workspace.current_user.userName}`) en éxito y falla.
- **Validación:** muestra conteos de Bronze/Silver/Gold, rechazados por fuente y regla, y **lanza una excepción si alguna tabla Gold está vacía**.

> 📸 *[Captura del job corriendo con las 4 tareas en verde]*

---

## 7. CI/CD con Declarative Automation Bundles

Dos targets en el mismo workspace, diferenciados por catálogo: `dev` → `electrocasa_dev` (mode development) y `prod` → `electrocasa_prod` (mode production, trigger activo).

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run      -t dev electrocasa_job
```

> 📸 *[Captura del deploy exitoso: panel Deployments del Git Folder o salida del CLI]*

---

## 8. Monitoreo y costos

- `notebooks/01_monitoreo_y_evidencias.ipynb`: event log del pipeline (`event_log(TABLE(...))`) con métricas de expectativas (pasaron/fallaron), historial del job en `system.lakeflow.job_run_timeline` y (reto) consumo por recurso en `system.billing.usage`. Si una tabla de sistema no está disponible, se documenta aquí el error y se usa el historial de corridas de la UI.
- **Compute:**
  - Pipeline **serverless, triggered**: lotes chicos y esporádicos; arranca en segundos y no cobra en ocio. Nunca continuo.
  - Notebooks del job en **serverless jobs**: tareas cortas (COPY INTO y validación). No se usan clusters all-purpose.
  - Conector de Tracking: compute serverless gestionado por Lakeflow Connect.

> 📸 *[Capturas del event log y de system.lakeflow / system.billing]*

---

## 9. Gobierno y seguridad

- Catálogo propio (`electrocasa_dev`), schemas `bronze`, `silver`, `gold` y volume `default.vol_landing`. Todas las tablas son **managed**.
- Grupos creados en **Settings > Identity and access > Groups**; el dueño del pipeline es miembro de `electrocasa_ingenieria`.

| Grupo | Permisos |
|---|---|
| `electrocasa_ingenieria` | `USE CATALOG`; `USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE MATERIALIZED VIEW` en bronze/silver/gold; `READ/WRITE VOLUME` |
| `electrocasa_analistas` | `USE CATALOG`; `USE SCHEMA, SELECT` en gold |
| `electrocasa_auditoria` | `USE CATALOG`; `USE SCHEMA, SELECT` en gold; `system.access.audit`, `table_lineage`, `column_lineage` |

- **REVOKE:** `00_setup` otorga `SELECT` sobre silver a analistas, lo revoca y muestra `SHOW GRANTS` antes y después.
  > 📸 *[3 capturas de SHOW GRANTS]*
- **Auditoría sobre `system`:** si el `GRANT` falla por falta de `MANAGE` sobre el catálogo `system`, el error queda en la salida de `00_setup` y la evidencia es la pestaña **Lineage** de Catalog Explorer.
  > 📸 *[Error o grant exitoso + captura de Lineage]*
- **Masking:** `mask_dni` y `mask_salario` usan `is_account_group_member('electrocasa_ingenieria')` y se declaran con la cláusula `MASK` **en el schema de las tablas del pipeline** (`silver.empleados_eventos`, `silver.empleados_scd2`, `gold.empleados_actuales`), no con `ALTER TABLE`. Como el dueño del pipeline está en ingeniería, `gold.empleados_actuales` (derivada) **conserva los valores reales** y la máscara decide al consultar: ingeniería ve `22108722 / 3058.00`, analistas ven `*****722 / NULL`.
  > 📸 *[Consulta de gold.empleados_actuales como miembro y como no miembro]*

---

## 10. Retos opcionales implementados
1. **Cuarentena con trazabilidad completa:** `silver.cuarentena` guarda fuente, regla, política, lote, archivo, fecha de ingesta, fecha de proceso y el registro original en JSON (reprocesable).
2. **`VARIANT` en Reseñas:** `metadata` → `VARIANT` (`try_parse_json`) y `gold.resenas_negativas_origen` usa `variant_get` para origen y versión de app.
3. (Extra) **Costos medidos:** consulta a `system.billing.usage` en el notebook 01.

---

## 11. Despliegue paso a paso
1. Crear los 3 grupos en Settings y agregarse a `electrocasa_ingenieria`.
2. Ejecutar `notebooks/00_setup.ipynb` con `catalog = electrocasa_dev`.
3. Crear la Connection SQL Server y la ingesta de Tracking (§4). Copiar el **pipeline ID** en `databricks.yml` → `targets.dev.variables.tracking_pipeline_id`.
4. Subir el lote 1 al volume (`ventas/`, `empleados/`, `resenas/`, `devoluciones/`, `catalogo/` marzo, `sucursales/`).
5. `databricks bundle deploy -t dev` (o Deploy desde el Git Folder).
6. Correr `electrocasa_job` → anotar conteos de `validacion`. Repetir con los lotes 2 y 3.
7. Ejecutar `notebooks/01_monitoreo_y_evidencias.ipynb` y pegar las capturas.

## 12. Respuestas (después del lote 3)
> 📸 *[Capturas de las tablas Gold — notebook 01, sección 6]*
