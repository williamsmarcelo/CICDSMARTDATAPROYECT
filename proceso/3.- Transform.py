# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# ============================================================
# PROYECTO DATABRICKS - COMPARATIVO DE INGRESOS
# Arquitectura Medallion: Bronze -> Silver -> Golden
# Año base: 2025 | Proyecciones: 2026-2035
# ============================================================

# COMMAND ----------

# DBTITLE 1,Cell 1 - Limpieza de widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Cell 2 - Importación de librerías
from pyspark.sql import functions as F
from pyspark.sql import types as T
from functools import reduce

# COMMAND ----------

# DBTITLE 1,Cell 3 - Declaración de widgets
dbutils.widgets.text("catalogo", "catalog_au")
dbutils.widgets.text("esquema_source", "bronze")
dbutils.widgets.text("esquema_sink", "silver")
dbutils.widgets.text("esquema_quality", "quality")

# COMMAND ----------

# DBTITLE 1,Cell 4 - Recuperación de parámetros
catalogo = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")
esquema_quality = dbutils.widgets.get("esquema_quality")

# COMMAND ----------

# DBTITLE 1,Cell 5 - Declaración de tablas
# Tablas de origen - Bronze
tabla_ingresos = f"{catalogo}.{esquema_source}.comparativo_ingresos_2012_2025"
tabla_tasas = f"{catalogo}.{esquema_source}.TASAS_CRECIMIENTO"
tabla_ubigeo = f"{catalogo}.{esquema_source}.TABLA_COD_UBIGEO"

# Tablas de destino - Silver, Golden (carga en archivo 4) y Quality
tabla_comparativo = f"{catalogo}.{esquema_sink}.comparativo_ingresos"
tabla_montos_silver = f"{catalogo}.{esquema_sink}.monto_totales"
tabla_final_silver = f"{catalogo}.{esquema_sink}.comparativo_ingresos_final"

# Asegurar que una tabla Silver creada anteriormente también tenga LEYENDA.
# CREATE TABLE IF NOT EXISTS no agrega columnas a tablas ya existentes.
if spark.catalog.tableExists(tabla_final_silver):
    columnas_silver = set(spark.table(tabla_final_silver).columns)
    if "LEYENDA" not in columnas_silver:
        spark.sql(
            f"ALTER TABLE {tabla_final_silver} ADD COLUMNS (LEYENDA STRING)"
        )

tabla_quality = f"{catalogo}.{esquema_quality}"

def guardar_quality(dataframe, nombre_tabla):
    (dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{tabla_quality}.{nombre_tabla}"))

# Parámetros del modelo
anio_base = 2025
anio_final = 2035

print("Configuración del proyecto:")
print(f"Tabla ingresos: {tabla_ingresos}")
print(f"Tabla tasas: {tabla_tasas}")
print(f"Tabla UBIGEO: {tabla_ubigeo}")
print(f"Tabla Silver: {tabla_comparativo}")
print(f"Tabla montos nacionales Silver: {tabla_montos_silver}")
print(f"Tabla final intermedia Silver: {tabla_final_silver}")
print(f"Esquema de validaciones: {tabla_quality}")
print(f"Año base: {anio_base} | Año final: {anio_final}")

# COMMAND ----------

# DBTITLE 1,Cell 6 - Lectura de tablas Bronze
df_ingresos = spark.table(tabla_ingresos)
df_tasas_crecimiento = spark.table(tabla_tasas)
df_ubigeo_bronze = spark.table(tabla_ubigeo)

print("Estructura de ingresos:")
df_ingresos.printSchema()

print("Estructura de tasas:")
df_tasas_crecimiento.printSchema()

print("Estructura de UBIGEO:")
df_ubigeo_bronze.printSchema()

# COMMAND ----------

# DBTITLE 1,Cell 7 - Validación de columnas de origen
columnas_dimensiones = [
    "NIVEL_GOBIERNO",
    "NIVEL_GOBIERNO_NOMBRE",
    "SECTOR",
    "SECTOR_NOMBRE",
    "PLIEGO",
    "PLIEGO_NOMBRE",
    "DEPARTAMENTO_EJECUTORA",
    "DEPARTAMENTO_EJECUTORA_NOMBRE",
    "PROVINCIA_EJECUTORA",
    "PROVINCIA_EJECUTORA_NOMBRE",
    "DISTRITO_EJECUTORA",
    "DISTRITO_EJECUTORA_NOMBRE",
    "EJECUTORA",
    "EJECUTORA_NOMBRE",
    "UBIGEO",
    "RUBRO",
    "RUBRO_NOMBRE",
    "FUENTE_FINANC",
    "FUENTE_FINANC_NOMBRE",
    "TIPO_RECURSO",
    "TIPO_RECURSO_NOMBRE",
    "GENERICA",
    "GENERICA_NOMBRE",
    "SUBGENERICA",
    "SUBGENERICA_NOMBRE",
    "SUBGENERICA_DET",
    "SUBGENERICA_DET_NOMBRE",
    "ESPECIFICA",
    "ESPECIFICA_NOMBRE",
    "ESPECIFICA_DET",
    "ESPECIFICA_DET_NOMBRE"
]

columnas_montos_anuales = [
    f"{prefijo}_{anio}"
    for anio in range(2012, 2026)
    for prefijo in ["MONTO_PIA", "MONTO_PIM", "MONTO_RECAUDADO"]
]

columnas_requeridas_ingresos = columnas_dimensiones + columnas_montos_anuales
columnas_faltantes_ingresos = sorted(
    set(columnas_requeridas_ingresos) - set(df_ingresos.columns)
)

if columnas_faltantes_ingresos:
    raise ValueError(
        "Faltan columnas en la tabla de ingresos Bronze: "
        + ", ".join(columnas_faltantes_ingresos)
    )

columnas_requeridas_tasas = {"PERIODO", "CRECIMIENTO_REAL"}
columnas_faltantes_tasas = sorted(
    columnas_requeridas_tasas - set(df_tasas_crecimiento.columns)
)

if columnas_faltantes_tasas:
    raise ValueError(
        "Faltan columnas en la tabla de tasas: "
        + ", ".join(columnas_faltantes_tasas)
    )

columnas_requeridas_ubigeo = {
    "IDDIST", "NOMBDEP", "NOMBPROV", "NOMBDIST", "NOM_CAPITAL"
}
columnas_faltantes_ubigeo = sorted(
    columnas_requeridas_ubigeo - set(df_ubigeo_bronze.columns)
)

if columnas_faltantes_ubigeo:
    raise ValueError(
        "Faltan columnas en la tabla UBIGEO: "
        + ", ".join(columnas_faltantes_ubigeo)
    )

print("Validación de columnas completada correctamente.")

# COMMAND ----------

# DBTITLE 1,Cell 8 - Definición de columnas dimensionales
# Las dimensiones se mantienen en la tabla Silver.
# Para la tabla Golden final se utilizarán EJECUTORA, EJECUTORA_NOMBRE,
# UBIGEO y la clasificación TIP_RECURSO.

# COMMAND ----------

# DBTITLE 1,Cell 9 - Consolidación de ingresos 2012-2025
lista_ingresos = []

for anio in range(2012, 2026):
    df_anual = df_ingresos.select(
        *[F.col(c) for c in columnas_dimensiones],
        F.lit(str(anio)).alias("PERIODO"),
        F.col(f"MONTO_PIA_{anio}").cast("double").alias("MONTO_PIA"),
        F.col(f"MONTO_PIM_{anio}").cast("double").alias("MONTO_PIM"),
        F.col(f"MONTO_RECAUDADO_{anio}").cast("double").alias("MONTO_RECAUDADO")
    )

    # Excluir niveles de gobierno E y R, de acuerdo con la consulta original.
    df_anual = df_anual.filter(
        ~F.col("NIVEL_GOBIERNO").isin("E", "R")
    )

    lista_ingresos.append(df_anual)

if not lista_ingresos:
    raise ValueError("No se generaron DataFrames anuales para consolidar.")

df_comparativo = reduce(
    lambda df1, df2: df1.unionByName(df2),
    lista_ingresos
)

print("Consolidación histórica 2012-2025 completada.")
print(f"Registros generados: {df_comparativo.count():,}")
display(df_comparativo.limit(10))

# COMMAND ----------

# DBTITLE 1,Cell 10 - Normalización de códigos UBIGEO en Silver
df_comparativo = df_comparativo.withColumn(
    "UBIGEO",
    F.when(
        F.length(F.trim(F.col("UBIGEO").cast("string"))).between(1, 6),
        F.lpad(F.trim(F.col("UBIGEO").cast("string")), 6, "0")
    ).otherwise(F.lit("OTRO"))
)

display(
    df_comparativo.select("UBIGEO").distinct().limit(20)
)

# COMMAND ----------

# DBTITLE 1,Cell 11 - Validación de consolidación Silver
print("Total de registros consolidados:", df_comparativo.count())

print("Registros por periodo:")
display(
    df_comparativo.groupBy("PERIODO")
    .count()
    .orderBy("PERIODO")
)
guardar_quality(df_comparativo.groupBy("PERIODO").count().orderBy("PERIODO"), "validacion_registros_silver_periodo")

print("Registros por nivel de gobierno:")
display(
    df_comparativo.groupBy("NIVEL_GOBIERNO")
    .count()
)
guardar_quality(df_comparativo.groupBy("NIVEL_GOBIERNO").count(), "validacion_registros_silver_nivel_gobierno")

print("Longitud de códigos UBIGEO:")
display(
    df_comparativo.groupBy(
        F.length("UBIGEO").alias("LONGITUD_UBIGEO")
    )
    .count()
    .orderBy("LONGITUD_UBIGEO")
)
guardar_quality(df_comparativo.groupBy(F.length("UBIGEO").alias("LONGITUD_UBIGEO")).count().orderBy("LONGITUD_UBIGEO"), "validacion_longitud_ubigeo")

# COMMAND ----------

# DBTITLE 1,Cell 12 - Escritura de tabla Silver
(
    df_comparativo.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(tabla_comparativo)
)

print(f"Tabla Silver generada: {tabla_comparativo}")

# COMMAND ----------

# DBTITLE 1,Cell 13 - Lectura de tabla Silver
df_silver = spark.table(tabla_comparativo)

print("Total de registros Silver:", df_silver.count())
display(df_silver.limit(10))

# COMMAND ----------

# DBTITLE 1,Cell 14 - Cálculo de montos históricos nacionales
df_monto_totales_historico = (
    df_silver
    .groupBy("PERIODO")
    .agg(
        F.sum("MONTO_PIA").alias("MONTO_PIA"),
        F.sum("MONTO_PIM").alias("MONTO_PIM"),
        F.sum("MONTO_RECAUDADO").alias("MONTO_RECAUDADO")
    )
    .withColumn("LEYENDA", F.lit("MONTO_REAL"))
    .select(
        F.col("PERIODO").cast("string").alias("PERIODO"),
        "MONTO_PIA",
        "MONTO_PIM",
        "MONTO_RECAUDADO",
        "LEYENDA"
    )
)

display(df_monto_totales_historico.orderBy("PERIODO"))

# COMMAND ----------

# DBTITLE 1,Cell 15 - Preparación de tasas de crecimiento
df_tasas = df_tasas_crecimiento.select(
    F.col("PERIODO").cast("int").alias("PERIODO"),
    F.col("CRECIMIENTO_REAL").cast("double").alias("CRECIMIENTO_REAL")
)

# Validar duplicados de periodo en las tasas.
df_tasas_duplicadas = (
    df_tasas.groupBy("PERIODO")
    .count()
    .filter(F.col("count") > 1)
)

guardar_quality(df_tasas_duplicadas, "validacion_tasas_duplicadas")

if df_tasas_duplicadas.limit(1).count() > 0:
    display(df_tasas_duplicadas)
    raise ValueError("La tabla de tasas contiene periodos duplicados.")

display(df_tasas.orderBy("PERIODO"))

# COMMAND ----------

# DBTITLE 1,Cell 16 - Identificación del año base 2025
anio_base = 2025
anio_final = 2035

df_base_2025 = (
    df_monto_totales_historico
    .filter(
        (F.col("PERIODO").cast("int") == anio_base) &
        (F.col("LEYENDA") == "MONTO_REAL")
    )
    .select(
        F.col("PERIODO").cast("int").alias("PERIODO"),
        F.col("MONTO_PIA").cast("double").alias("MONTO_PIA"),
        F.col("MONTO_PIM").cast("double").alias("MONTO_PIM"),
        F.col("MONTO_RECAUDADO").cast("double").alias("MONTO_RECAUDADO")
    )
)

cantidad_base = df_base_2025.count()
if cantidad_base != 1:
    raise ValueError(
        f"Se esperaba un único registro nacional para 2025; "
        f"se encontraron {cantidad_base}."
    )

display(df_base_2025)

# COMMAND ----------

# DBTITLE 1,Cell 17 - Generación de proyecciones nacionales 2026-2035
# La proyección de cada año utiliza el resultado del año inmediatamente anterior.
df_proyeccion_actual = df_base_2025
lista_proyecciones = []

for anio in range(anio_base + 1, anio_final + 1):
    df_tasa_anual = (
        df_tasas
        .filter(F.col("PERIODO") == anio)
        .select("CRECIMIENTO_REAL")
    )

    cantidad_tasas = df_tasa_anual.count()
    if cantidad_tasas != 1:
        raise ValueError(
            f"Se esperaba exactamente una tasa para {anio}; "
            f"se encontraron {cantidad_tasas}."
        )

    tasa_valor = df_tasa_anual.first()["CRECIMIENTO_REAL"]
    if tasa_valor is None:
        raise ValueError(f"La tasa CRECIMIENTO_REAL de {anio} es NULL.")

    df_proyeccion_anual = (
        df_proyeccion_actual
        .crossJoin(df_tasa_anual)
        .select(
            F.lit(anio).cast("string").alias("PERIODO"),
            (F.col("MONTO_PIA") * (1 + F.col("CRECIMIENTO_REAL"))).alias("MONTO_PIA"),
            (F.col("MONTO_PIM") * (1 + F.col("CRECIMIENTO_REAL"))).alias("MONTO_PIM"),
            (F.col("MONTO_RECAUDADO") * (1 + F.col("CRECIMIENTO_REAL"))).alias("MONTO_RECAUDADO")
        )
        .withColumn("LEYENDA", F.lit("MONTO_PROY"))
    )

    lista_proyecciones.append(df_proyeccion_anual)

    df_proyeccion_actual = df_proyeccion_anual.select(
        F.col("PERIODO").cast("int").alias("PERIODO"),
        "MONTO_PIA",
        "MONTO_PIM",
        "MONTO_RECAUDADO"
    )

if not lista_proyecciones:
    raise ValueError("No se generaron proyecciones. Revisa el año base y el año final.")

df_proyecciones = reduce(
    lambda df1, df2: df1.unionByName(df2),
    lista_proyecciones
)

display(df_proyecciones.orderBy("PERIODO"))

# COMMAND ----------

# DBTITLE 1,Cell 18 - Unión de montos históricos y proyectados nacionales
df_monto_totales_final = (
    df_monto_totales_historico
    .unionByName(df_proyecciones)
    .orderBy("PERIODO", "LEYENDA")
)

display(df_monto_totales_final)

# COMMAND ----------

# DBTITLE 1,Cell 19 - Validación de montos nacionales
display(
    df_monto_totales_final
    .groupBy("LEYENDA")
    .agg(
        F.count("*").alias("CANTIDAD_REGISTROS"),
        F.min("PERIODO").alias("ANIO_INICIAL"),
        F.max("PERIODO").alias("ANIO_FINAL")
    )
)
guardar_quality(df_monto_totales_final.groupBy("LEYENDA").agg(F.count("*").alias("CANTIDAD_REGISTROS"), F.min("PERIODO").alias("ANIO_INICIAL"), F.max("PERIODO").alias("ANIO_FINAL")), "validacion_montos_nacionales")

# COMMAND ----------

# DBTITLE 1,Cell 20 - Escritura de tabla intermedia Silver de montos nacionales
(
    df_monto_totales_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(tabla_montos_silver)
)

print(f"Tabla Silver de montos nacionales generada: {tabla_montos_silver}")

# COMMAND ----------

# DBTITLE 1,Cell 21 - Lectura y validación de montos nacionales Silver
df_golden_montos = spark.table(tabla_montos_silver)

print("Total de registros en montos nacionales:", df_golden_montos.count())
display(df_golden_montos.orderBy("PERIODO", "LEYENDA"))

# COMMAND ----------

# DBTITLE 1,Cell 22 - Consolidación histórica por ejecutora y tipo de recurso
# Clasificar CANON cuando TIPO_RECURSO_NOMBRE contiene la palabra CANON.
# Los demás registros se clasifican como NORMAL.
df_historico = (
    df_silver
    .withColumn(
        "TIP_RECURSO",
        F.when(
            F.upper(
                F.coalesce(F.col("TIPO_RECURSO_NOMBRE").cast("string"), F.lit(""))
            ).contains("CANON"),
            F.lit("CANON")
        ).otherwise(F.lit("NORMAL"))
    )
    .groupBy(
        "PERIODO",
        "EJECUTORA",
        "EJECUTORA_NOMBRE",
        "UBIGEO",
        "TIP_RECURSO"
    )
    .agg(
        F.sum("MONTO_PIA").alias("MONTO_PIA"),
        F.sum("MONTO_PIM").alias("MONTO_PIM"),
        F.sum("MONTO_RECAUDADO").alias("MONTO_RECAUDADO")
    )
)

display(df_historico.limit(20))

# COMMAND ----------

# DBTITLE 1,Cell 23 - Montos base por ejecutora del año 2025
df_base_distribucion = (
    df_historico
    .filter(F.col("PERIODO").cast("int") == anio_base)
    .select(
        "EJECUTORA",
        "EJECUTORA_NOMBRE",
        "UBIGEO",
        "TIP_RECURSO",
        "MONTO_PIA",
        "MONTO_PIM",
        "MONTO_RECAUDADO"
    )
)

print("Registros de distribución del año 2025:", df_base_distribucion.count())
display(df_base_distribucion.limit(20))

# COMMAND ----------

# DBTITLE 1,Cell 24 - Montos totales nacionales del año 2025
df_totales_base = (
    df_golden_montos
    .filter(
        (F.col("PERIODO").cast("int") == anio_base) &
        (F.col("LEYENDA") == "MONTO_REAL")
    )
    .select(
        F.col("MONTO_PIA").cast("double").alias("TOTAL_PIA_2025"),
        F.col("MONTO_PIM").cast("double").alias("TOTAL_PIM_2025"),
        F.col("MONTO_RECAUDADO").cast("double").alias("TOTAL_RECAUDADO_2025")
    )
)

cantidad_totales = df_totales_base.count()
if cantidad_totales != 1:
    raise ValueError(
        f"Se esperaba un único registro nacional para 2025; "
        f"se encontraron {cantidad_totales}."
    )

valores_totales = df_totales_base.first().asDict()
for columna_total, valor in valores_totales.items():
    if valor is None or valor == 0:
        raise ValueError(
            f"El denominador {columna_total} es NULL o cero. "
            "No se puede calcular el factor de distribución."
        )

display(df_totales_base)

# COMMAND ----------

# DBTITLE 1,Cell 25 - Cálculo de factores de distribución 2025
df_ratios = (
    df_base_distribucion
    .crossJoin(df_totales_base)
    .withColumn(
        "FACTOR_PIA",
        F.col("MONTO_PIA") / F.col("TOTAL_PIA_2025")
    )
    .withColumn(
        "FACTOR_PIM",
        F.col("MONTO_PIM") / F.col("TOTAL_PIM_2025")
    )
    .withColumn(
        "FACTOR_RECAUDADO",
        F.col("MONTO_RECAUDADO") / F.col("TOTAL_RECAUDADO_2025")
    )
    .select(
        "EJECUTORA",
        "EJECUTORA_NOMBRE",
        "UBIGEO",
        "TIP_RECURSO",
        "FACTOR_PIA",
        "FACTOR_PIM",
        "FACTOR_RECAUDADO"
    )
)

display(df_ratios.limit(20))

# COMMAND ----------

# DBTITLE 1,Cell 26 - Distribución de proyecciones por ejecutora 2026-2035
df_totales_proyectados = (
    df_golden_montos
    .filter(
        (F.col("LEYENDA") == "MONTO_PROY") &
        F.col("PERIODO").cast("int").between(anio_base + 1, anio_final)
    )
    .select(
        F.col("PERIODO").cast("string").alias("PERIODO"),
        F.col("MONTO_PIA").cast("double").alias("TOTAL_PIA_PROY"),
        F.col("MONTO_PIM").cast("double").alias("TOTAL_PIM_PROY"),
        F.col("MONTO_RECAUDADO").cast("double").alias("TOTAL_RECAUDADO_PROY")
    )
)

# Verificar que exista una proyección para cada año 2026-2035.
anios_proyectados = {
    int(row["PERIODO"])
    for row in df_totales_proyectados.select("PERIODO").distinct().collect()
}
anios_esperados = set(range(anio_base + 1, anio_final + 1))
anios_faltantes = sorted(anios_esperados - anios_proyectados)

if anios_faltantes:
    raise ValueError(
        f"Faltan proyecciones nacionales para los años: {anios_faltantes}"
    )

# Aplicar a cada ejecutora los factores calculados con los montos 2025.
df_proyeccion_distribuida = (
    df_ratios
    .crossJoin(df_totales_proyectados)
    .select(
        "PERIODO",
        "EJECUTORA",
        "EJECUTORA_NOMBRE",
        "UBIGEO",
        "TIP_RECURSO",
        (F.col("FACTOR_PIA") * F.col("TOTAL_PIA_PROY")).alias("MONTO_PIA"),
        (F.col("FACTOR_PIM") * F.col("TOTAL_PIM_PROY")).alias("MONTO_PIM"),
        (F.col("FACTOR_RECAUDADO") * F.col("TOTAL_RECAUDADO_PROY")).alias("MONTO_RECAUDADO")
    )
)

display(df_proyeccion_distribuida.limit(20))

# COMMAND ----------

# DBTITLE 1,Cell 27 - Unión de históricos y proyecciones por ejecutora
df_historico_final = df_historico.select(
    F.col("PERIODO").cast("string").alias("PERIODO"),
    "EJECUTORA",
    "EJECUTORA_NOMBRE",
    "UBIGEO",
    "TIP_RECURSO",
    F.lit("MONTO_REAL").alias("LEYENDA"),
    F.col("MONTO_PIA").cast("double").alias("MONTO_PIA"),
    F.col("MONTO_PIM").cast("double").alias("MONTO_PIM"),
    F.col("MONTO_RECAUDADO").cast("double").alias("MONTO_RECAUDADO")
)

df_proyeccion_distribuida = df_proyeccion_distribuida.select(
    F.col("PERIODO").cast("string").alias("PERIODO"),
    "EJECUTORA",
    "EJECUTORA_NOMBRE",
    "UBIGEO",
    "TIP_RECURSO",
    F.lit("MONTO_PROY").alias("LEYENDA"),
    F.col("MONTO_PIA").cast("double").alias("MONTO_PIA"),
    F.col("MONTO_PIM").cast("double").alias("MONTO_PIM"),
    F.col("MONTO_RECAUDADO").cast("double").alias("MONTO_RECAUDADO")
)

df_final = df_historico_final.unionByName(df_proyeccion_distribuida)

# Validar que la clasificación LEYENDA se conserve en históricos y proyecciones.
leyendas_obtenidas = {
    row["LEYENDA"]
    for row in df_final.select("LEYENDA").distinct().collect()
}
leyendas_esperadas = {"MONTO_REAL", "MONTO_PROY"}
if not leyendas_esperadas.issubset(leyendas_obtenidas):
    raise ValueError(
        "La columna LEYENDA no contiene las dos clasificaciones esperadas. "
        f"Valores encontrados: {sorted(str(x) for x in leyendas_obtenidas)}"
    )
if df_final.filter(F.col("LEYENDA").isNull()).limit(1).count() > 0:
    raise ValueError("Se encontraron registros sin valor en LEYENDA.")


display(
    df_final.orderBy("PERIODO", "EJECUTORA", "TIP_RECURSO").limit(50)
)

# COMMAND ----------

# DBTITLE 1,Cell 28 - Validaciones de factores y proyecciones distribuidas
# Los factores deben sumar aproximadamente 1 para cada indicador.
df_validacion_ratios = df_ratios.agg(
    F.sum("FACTOR_PIA").alias("SUMA_FACTOR_PIA"),
    F.sum("FACTOR_PIM").alias("SUMA_FACTOR_PIM"),
    F.sum("FACTOR_RECAUDADO").alias("SUMA_FACTOR_RECAUDADO")
)

display(df_validacion_ratios)
guardar_quality(df_validacion_ratios, "validacion_factores_distribucion")

# Comparar la suma distribuida con los totales nacionales proyectados.
df_validacion_distribucion = (
    df_proyeccion_distribuida
    .groupBy("PERIODO")
    .agg(
        F.sum("MONTO_PIA").alias("MONTO_PIA_DISTRIBUIDO"),
        F.sum("MONTO_PIM").alias("MONTO_PIM_DISTRIBUIDO"),
        F.sum("MONTO_RECAUDADO").alias("MONTO_RECAUDADO_DISTRIBUIDO")
    )
    .join(
        df_totales_proyectados,
        on="PERIODO",
        how="inner"
    )
    .withColumn(
        "DIFERENCIA_PIA",
        F.col("MONTO_PIA_DISTRIBUIDO") - F.col("TOTAL_PIA_PROY")
    )
    .withColumn(
        "DIFERENCIA_PIM",
        F.col("MONTO_PIM_DISTRIBUIDO") - F.col("TOTAL_PIM_PROY")
    )
    .withColumn(
        "DIFERENCIA_RECAUDADO",
        F.col("MONTO_RECAUDADO_DISTRIBUIDO") - F.col("TOTAL_RECAUDADO_PROY")
    )
    .select(
        "PERIODO",
        "MONTO_PIA_DISTRIBUIDO",
        "TOTAL_PIA_PROY",
        "DIFERENCIA_PIA",
        "MONTO_PIM_DISTRIBUIDO",
        "TOTAL_PIM_PROY",
        "DIFERENCIA_PIM",
        "MONTO_RECAUDADO_DISTRIBUIDO",
        "TOTAL_RECAUDADO_PROY",
        "DIFERENCIA_RECAUDADO"
    )
    .orderBy("PERIODO")
)

display(df_validacion_distribucion)
guardar_quality(df_validacion_distribucion, "validacion_distribucion_proyecciones")

# Resumen de registros y montos históricos/proyectados.
df_validacion_resumen_periodo = (
    df_final
    .groupBy("PERIODO")
    .agg(
        F.count("*").alias("TOTAL_REGISTROS"),
        F.sum("MONTO_PIA").alias("TOTAL_PIA"),
        F.sum("MONTO_PIM").alias("TOTAL_PIM"),
        F.sum("MONTO_RECAUDADO").alias("TOTAL_RECAUDADO")
    )
    .orderBy("PERIODO")
)
display(df_validacion_resumen_periodo)
guardar_quality(df_validacion_resumen_periodo, "validacion_resumen_periodo")

# COMMAND ----------

# DBTITLE 1,Cell 29 - Preparación de dimensión UBIGEO y JOIN final
# Normalizar IDDIST con la misma regla de seis dígitos utilizada en Silver.
df_ubigeo = (
    df_ubigeo_bronze
    .select(
        F.when(
            F.length(F.trim(F.col("IDDIST").cast("string"))).between(1, 6),
            F.lpad(F.trim(F.col("IDDIST").cast("string")), 6, "0")
        ).otherwise(F.trim(F.col("IDDIST").cast("string"))).alias("UBIGEO"),
        F.col("NOMBDEP"),
        F.col("NOMBPROV"),
        F.col("NOMBDIST"),
        F.col("NOM_CAPITAL")
    )
    .filter(F.col("UBIGEO").isNotNull())
)

# La dimensión debe tener una sola fila por código UBIGEO para no duplicar montos.
df_ubigeo_duplicados = (
    df_ubigeo
    .groupBy("UBIGEO")
    .count()
    .filter(F.col("count") > 1)
)

cantidad_ubigeo_duplicados = df_ubigeo_duplicados.count()
guardar_quality(df_ubigeo_duplicados, "validacion_ubigeo_duplicados")
if cantidad_ubigeo_duplicados > 0:
    display(df_ubigeo_duplicados)
    raise ValueError(
        f"La dimensión UBIGEO contiene {cantidad_ubigeo_duplicados} códigos duplicados. "
        "Corrige la dimensión antes de realizar el JOIN."
    )

# LEFT JOIN: conserva todos los registros financieros aunque no exista correspondencia.
df_final_ubigeo = (
    df_final.alias("a")
    .join(
        df_ubigeo.alias("b"),
        F.col("a.UBIGEO") == F.col("b.UBIGEO"),
        "left"
    )
    .select(
        F.col("a.PERIODO").alias("PERIODO"),
        F.col("a.EJECUTORA").alias("EJECUTORA"),
        F.col("a.EJECUTORA_NOMBRE").alias("EJECUTORA_NOMBRE"),
        F.col("a.UBIGEO").alias("UBIGEO"),
        F.col("b.NOMBDEP").alias("NOMBDEP"),
        F.col("b.NOMBPROV").alias("NOMBPROV"),
        F.col("b.NOMBDIST").alias("NOMBDIST"),
        F.col("b.NOM_CAPITAL").alias("NOM_CAPITAL"),
        F.col("a.TIP_RECURSO").alias("TIP_RECURSO"),
        F.col("a.LEYENDA").alias("LEYENDA"),
        F.when(F.col("a.TIP_RECURSO") == "CANON", F.lit(1))
         .when(F.col("a.TIP_RECURSO") == "NORMAL", F.lit(2))
         .cast("int").alias("COD_TIP_RECURSO"),
        F.when(F.col("a.LEYENDA") == "MONTO_REAL", F.lit(1))
         .when(F.col("a.LEYENDA") == "MONTO_PROY", F.lit(2))
         .cast("int").alias("COD_LEYENDA"),
        F.col("a.MONTO_PIA").alias("MONTO_PIA"),
        F.col("a.MONTO_PIM").alias("MONTO_PIM"),
        F.col("a.MONTO_RECAUDADO").alias("MONTO_RECAUDADO")
    )
)

display(df_final_ubigeo.limit(20))

# COMMAND ----------

display( df_ubigeo_bronze.limit(20))

# COMMAND ----------

# DBTITLE 1,Cell 30 - Validación de tabla financiera y geográfica
registros_financieros = df_final.count()
registros_con_ubigeo = df_final_ubigeo.count()

print("Registros financieros antes del JOIN:", registros_financieros)
print("Registros después del JOIN UBIGEO:", registros_con_ubigeo)
df_validacion_registros_join = spark.createDataFrame(
    [(registros_financieros, registros_con_ubigeo)],
    ["REGISTROS_FINANCIEROS", "REGISTROS_CON_UBIGEO"]
)
guardar_quality(df_validacion_registros_join, "validacion_registros_join")

if registros_financieros != registros_con_ubigeo:
    raise ValueError(
        "La cantidad de registros cambió después del JOIN UBIGEO. "
        "Revisa la unicidad de la dimensión geográfica."
    )

# Mostrar registros sin correspondencia geográfica.
df_ubigeo_no_encontrado = df_final_ubigeo.filter(F.col("NOMBDEP").isNull())

print(
    "Registros sin correspondencia geográfica:",
    df_ubigeo_no_encontrado.count()
)

df_ubigeo_no_encontrado_quality = (
    df_ubigeo_no_encontrado
    .select("UBIGEO", "EJECUTORA", "EJECUTORA_NOMBRE")
    .distinct()
)
display(df_ubigeo_no_encontrado_quality.limit(50))
guardar_quality(df_ubigeo_no_encontrado_quality, "validacion_ubigeo_no_encontrado")

# Validar montos por periodo y departamento.
df_validacion_montos_departamento = (
    df_final_ubigeo
    .groupBy("PERIODO", "NOMBDEP")
    .agg(
        F.sum("MONTO_PIA").alias("MONTO_PIA"),
        F.sum("MONTO_PIM").alias("MONTO_PIM"),
        F.sum("MONTO_RECAUDADO").alias("MONTO_RECAUDADO"),
        F.count("*").alias("TOTAL_REGISTROS")
    )
    .orderBy("PERIODO", "NOMBDEP")
)
display(df_validacion_montos_departamento)
guardar_quality(df_validacion_montos_departamento, "validacion_montos_por_departamento")

# COMMAND ----------

# DBTITLE 1,Cell 31 - Escritura de tabla intermedia Silver final
# La carga de las tablas Golden se realiza exclusivamente en el archivo 4 - Load.
(
    df_final_ubigeo.select(
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
    )
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(tabla_final_silver)
)

print("Proceso de transformación finalizado correctamente.")
print(f"Tabla intermedia Silver generada: {tabla_final_silver}")
df_silver_final_validacion = spark.table(tabla_final_silver)
if "LEYENDA" not in df_silver_final_validacion.columns:
    raise ValueError(
        f"La tabla {tabla_final_silver} fue escrita sin la columna LEYENDA."
    )
display(
    df_silver_final_validacion
    .groupBy("LEYENDA")
    .count()
    .orderBy("LEYENDA")
)
print("La carga de las tablas Golden queda a cargo del archivo 4 - Load.")