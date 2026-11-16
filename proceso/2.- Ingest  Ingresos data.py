# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "catalog_au")
dbutils.widgets.text("esquema", "bronze")
dbutils.widgets.text("storageName", "adlssmartdataprojects")

# COMMAND ----------

container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")
storageName = dbutils.widgets.get("storageName")

ruta = f"abfss://{container}@{storageName}.dfs.core.windows.net/comparativo_ingresos_2012_2025.csv"

# COMMAND ----------

df_ingresos = spark.read.option('header', True)\
                        .option('inferSchema', True)\
                        .csv(ruta)

# COMMAND ----------

ingresos_schema = StructType(fields=[
    StructField("NIVEL_GOBIERNO", StringType(), True),
    StructField("NIVEL_GOBIERNO_NOMBRE", StringType(), True),
    StructField("SECTOR", StringType(), True),
    StructField("SECTOR_NOMBRE", StringType(), True),
    StructField("PLIEGO", StringType(), True),
    StructField("PLIEGO_NOMBRE", StringType(), True),
    StructField("DEPARTAMENTO_EJECUTORA", StringType(), True),
    StructField("DEPARTAMENTO_EJECUTORA_NOMBRE", StringType(), True),
    StructField("PROVINCIA_EJECUTORA", StringType(), True),
    StructField("PROVINCIA_EJECUTORA_NOMBRE", StringType(), True),
    StructField("DISTRITO_EJECUTORA", StringType(), True),
    StructField("DISTRITO_EJECUTORA_NOMBRE", StringType(), True),
    StructField("EJECUTORA", StringType(), True),
    StructField("EJECUTORA_NOMBRE", StringType(), True),
    StructField("UBIGEO", StringType(), True),
    StructField("RUBRO", StringType(), True),
    StructField("RUBRO_NOMBRE", StringType(), True),
    StructField("FUENTE_FINANC", StringType(), True),
    StructField("FUENTE_FINANC_NOMBRE", StringType(), True),
    StructField("TIPO_RECURSO", StringType(), True),
    StructField("TIPO_RECURSO_NOMBRE", StringType(), True),
    StructField("GENERICA", StringType(), True),
    StructField("GENERICA_NOMBRE", StringType(), True),
    StructField("SUBGENERICA", StringType(), True),
    StructField("SUBGENERICA_NOMBRE", StringType(), True),
    StructField("SUBGENERICA_DET", StringType(), True),
    StructField("SUBGENERICA_DET_NOMBRE", StringType(), True),
    StructField("ESPECIFICA", StringType(), True),
    StructField("ESPECIFICA_NOMBRE", StringType(), True),
    StructField("ESPECIFICA_DET", StringType(), True),
    StructField("ESPECIFICA_DET_NOMBRE", StringType(), True),
    StructField("MONTO_PIA_2012", DoubleType(), True),
    StructField("MONTO_PIM_2012", DoubleType(), True),
    StructField("MONTO_RECAUDADO_2012", DoubleType(), True),
    StructField("MONTO_PIA_2013", DoubleType(), True),
    StructField("MONTO_PIM_2013", DoubleType(), True),
    StructField("MONTO_RECAUDADO_2013", DoubleType(), True),
    StructField("MONTO_PIA_2014", DoubleType(), True),
    StructField("MONTO_PIM_2014", DoubleType(), True),
    StructField("MONTO_RECAUDADO_2014", DoubleType(), True),
    StructField("MONTO_PIA_2015", DoubleType(), True),
    StructField("MONTO_PIM_2015", DoubleType(), True),
    StructField("MONTO_RECAUDADO_2015", DoubleType(), True),
    StructField("MONTO_PIA_2016", DoubleType(), True),
    StructField("MONTO_PIM_2016", DoubleType(), True),
    StructField("MONTO_RECAUDADO_2016", DoubleType(), True),
    StructField("MONTO_PIA_2017", DoubleType(), True),
    StructField("MONTO_PIM_2017", DoubleType(), True),
    StructField("MONTO_RECAUDADO_2017", DoubleType(), True),
    StructField("MONTO_PIA_2018", DoubleType(), True),
    StructField("MONTO_PIM_2018", DoubleType(), True),
    StructField("MONTO_RECAUDADO_2018", DoubleType(), True),
    StructField("MONTO_PIA_2019", DoubleType(), True),
    StructField("MONTO_PIM_2019", DoubleType(), True),
    StructField("MONTO_RECAUDADO_2019", DoubleType(), True),
    StructField("MONTO_PIA_2020", DoubleType(), True),
    StructField("MONTO_PIM_2020", DoubleType(), True),
    StructField("MONTO_RECAUDADO_2020", DoubleType(), True),
    StructField("MONTO_PIA_2021", DoubleType(), True),
    StructField("MONTO_PIM_2021", DoubleType(), True),
    StructField("MONTO_RECAUDADO_2021", DoubleType(), True),
    StructField("MONTO_PIA_2022", DoubleType(), True),
    StructField("MONTO_PIM_2022", DoubleType(), True),
    StructField("MONTO_RECAUDADO_2022", DoubleType(), True),
    StructField("MONTO_PIA_2023", DoubleType(), True),
    StructField("MONTO_PIM_2023", DoubleType(), True),
    StructField("MONTO_RECAUDADO_2023", DoubleType(), True),
    StructField("MONTO_PIA_2024", DoubleType(), True),
    StructField("MONTO_PIM_2024", DoubleType(), True),
    StructField("MONTO_RECAUDADO_2024", DoubleType(), True),
    StructField("MONTO_PIA_2025", DoubleType(), True),
    StructField("MONTO_PIM_2025", DoubleType(), True),
    StructField("MONTO_RECAUDADO_2025", DoubleType(), True)
])

# COMMAND ----------

# DBTITLE 1,Use user specified schema to load df with correct types
df_ingresos_final = spark.read\
.option('header', True)\
.schema(ingresos_schema)\
.csv(ruta)

# COMMAND ----------

# DBTITLE 1,select only specific cols
ingresos_selected_df = df_ingresos_final.select(
    col("NIVEL_GOBIERNO"),
    col("NIVEL_GOBIERNO_NOMBRE"),
    col("SECTOR"),
    col("SECTOR_NOMBRE"),
    col("PLIEGO"),
    col("PLIEGO_NOMBRE"),
    col("DEPARTAMENTO_EJECUTORA"),
    col("DEPARTAMENTO_EJECUTORA_NOMBRE"),
    col("PROVINCIA_EJECUTORA"),
    col("PROVINCIA_EJECUTORA_NOMBRE"),
    col("DISTRITO_EJECUTORA"),
    col("DISTRITO_EJECUTORA_NOMBRE"),
    col("EJECUTORA"),
    col("EJECUTORA_NOMBRE"),
    col("UBIGEO"),
    col("RUBRO"),
    col("RUBRO_NOMBRE"),
    col("FUENTE_FINANC"),
    col("FUENTE_FINANC_NOMBRE"),
    col("TIPO_RECURSO"),
    col("TIPO_RECURSO_NOMBRE"),
    col("GENERICA"),
    col("GENERICA_NOMBRE"),
    col("SUBGENERICA"),
    col("SUBGENERICA_NOMBRE"),
    col("SUBGENERICA_DET"),
    col("SUBGENERICA_DET_NOMBRE"),
    col("ESPECIFICA"),
    col("ESPECIFICA_NOMBRE"),
    col("ESPECIFICA_DET"),
    col("ESPECIFICA_DET_NOMBRE"),
    col("MONTO_PIA_2012"),
    col("MONTO_PIM_2012"),
    col("MONTO_RECAUDADO_2012"),
    col("MONTO_PIA_2013"),
    col("MONTO_PIM_2013"),
    col("MONTO_RECAUDADO_2013"),
    col("MONTO_PIA_2014"),
    col("MONTO_PIM_2014"),
    col("MONTO_RECAUDADO_2014"),
    col("MONTO_PIA_2015"),
    col("MONTO_PIM_2015"),
    col("MONTO_RECAUDADO_2015"),
    col("MONTO_PIA_2016"),
    col("MONTO_PIM_2016"),
    col("MONTO_RECAUDADO_2016"),
    col("MONTO_PIA_2017"),
    col("MONTO_PIM_2017"),
    col("MONTO_RECAUDADO_2017"),
    col("MONTO_PIA_2018"),
    col("MONTO_PIM_2018"),
    col("MONTO_RECAUDADO_2018"),
    col("MONTO_PIA_2019"),
    col("MONTO_PIM_2019"),
    col("MONTO_RECAUDADO_2019"),
    col("MONTO_PIA_2020"),
    col("MONTO_PIM_2020"),
    col("MONTO_RECAUDADO_2020"),
    col("MONTO_PIA_2021"),
    col("MONTO_PIM_2021"),
    col("MONTO_RECAUDADO_2021"),
    col("MONTO_PIA_2022"),
    col("MONTO_PIM_2022"),
    col("MONTO_RECAUDADO_2022"),
    col("MONTO_PIA_2023"),
    col("MONTO_PIM_2023"),
    col("MONTO_RECAUDADO_2023"),
    col("MONTO_PIA_2024"),
    col("MONTO_PIM_2024"),
    col("MONTO_RECAUDADO_2024"),
    col("MONTO_PIA_2025"),
    col("MONTO_PIM_2025"),
    col("MONTO_RECAUDADO_2025")
)

# COMMAND ----------

# DBTITLE 1,Add col with current timestamp 
ingresos_final_df = ingresos_selected_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

ingresos_final_df.write \
    .mode("overwrite") \
    .insertInto("catalog_au.bronze.comparativo_ingresos_2012_2025")

# COMMAND ----------

