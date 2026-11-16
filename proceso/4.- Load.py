# Databricks notebook source
# ============================================================
# PROYECTO DATABRICKS - COMPARATIVO DE INGRESOS
# Carga de tablas Silver hacia Golden
# ============================================================

# COMMAND ----------

# DBTITLE 1,Cell 1 - Limpieza de widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Cell 2 - Declaración de widgets
dbutils.widgets.text("catalogo", "catalog_au")
dbutils.widgets.text("esquema_source", "silver")
dbutils.widgets.text("esquema_sink", "golden")

# COMMAND ----------

# DBTITLE 1,Cell 3 - Recuperación de parámetros
catalogo = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")

# COMMAND ----------

# DBTITLE 1,Cell 4 - Declaración de tablas
# Tablas intermedias de origen - Silver
tabla_montos_silver = f"{catalogo}.{esquema_source}.monto_totales"
tabla_final_silver = f"{catalogo}.{esquema_source}.comparativo_ingresos_final"

# Tablas de destino - Golden
tabla_montos_golden = f"{catalogo}.{esquema_sink}.monto_totales"
tabla_final_golden = f"{catalogo}.{esquema_sink}.comparativo_ingresos_final"

# Migración idempotente: agregar LEYENDA si la tabla Golden ya existía sin ella.
if spark.catalog.tableExists(tabla_final_golden):
    columnas_golden = set(spark.table(tabla_final_golden).columns)
    if "LEYENDA" not in columnas_golden:
        spark.sql(
            f"ALTER TABLE {tabla_final_golden} ADD COLUMNS (LEYENDA STRING)"
        )


print(f"Origen Silver - montos nacionales: {tabla_montos_silver}")
print(f"Origen Silver - comparativo final: {tabla_final_silver}")
print(f"Destino Golden - montos nacionales: {tabla_montos_golden}")
print(f"Destino Golden - comparativo final: {tabla_final_golden}")

# COMMAND ----------

# DBTITLE 1,Cell 5 - Lectura de tablas intermedias Silver
df_montos_silver = spark.table(tabla_montos_silver)
df_final_silver = spark.table(tabla_final_silver)

print("Registros Silver de montos nacionales:", df_montos_silver.count())
print("Registros Silver del comparativo final:", df_final_silver.count())

# COMMAND ----------

# DBTITLE 1,Cell 6 - Carga de tabla Golden de montos nacionales
(
    df_montos_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(tabla_montos_golden)
)

print(f"Tabla Golden de montos nacionales cargada: {tabla_montos_golden}")

# COMMAND ----------

# DBTITLE 1,Cell 7 - Carga de tabla Golden final
columnas_finales = [
    "PERIODO",
    "EJECUTORA",
    "EJECUTORA_NOMBRE",
    "UBIGEO",
    "NOMBDEP",
    "NOMBPROV",
    "NOMBDIST",
    "NOM_CAPITAL",
    "TIP_RECURSO",
    "LEYENDA",
    "COD_TIP_RECURSO",
    "COD_LEYENDA",
    "MONTO_PIA",
    "MONTO_PIM",
    "MONTO_RECAUDADO"
]

if "LEYENDA" not in df_final_silver.columns:
    raise ValueError(
        f"La tabla Silver {tabla_final_silver} no contiene LEYENDA. "
        "Ejecuta primero el archivo 3 - Transform actualizado."
    )

(
    df_final_silver.select(*columnas_finales)
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(tabla_final_golden)
)

print(f"Tabla Golden final cargada: {tabla_final_golden}")

# COMMAND ----------

# DBTITLE 1,Cell 8 - Validación de carga Golden
df_golden_montos = spark.table(tabla_montos_golden)
df_golden_final = spark.table(tabla_final_golden)

print("Total de registros Golden - montos nacionales:", df_golden_montos.count())
print("Total de registros Golden - comparativo final:", df_golden_final.count())

df_golden_montos.printSchema()
df_golden_final.printSchema()
if "LEYENDA" not in df_golden_final.columns:
    raise ValueError(
        f"La tabla Golden {tabla_final_golden} no contiene la columna LEYENDA."
    )

display(
    df_golden_final
    .groupBy("LEYENDA")
    .count()
    .orderBy("LEYENDA")
)

display(df_golden_montos.orderBy("PERIODO", "LEYENDA"))
display(df_golden_final.orderBy("PERIODO", "EJECUTORA", "TIP_RECURSO").limit(50))