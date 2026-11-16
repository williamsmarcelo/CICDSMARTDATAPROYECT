# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

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

ruta = f"abfss://{container}@{storageName}.dfs.core.windows.net/CODIGO_UBIGEO.csv"

# COMMAND ----------

ubigeo_schema = StructType(fields=[
    StructField("IDDIST", StringType(), True),
    StructField("NOMBDEP", StringType(), True),
    StructField("NOMBPROV", StringType(), True),
    StructField("NOMBDIST", StringType(), True),
    StructField("NOM_CAPITAL", StringType(), True)
])

# COMMAND ----------

ubigeo_df = spark.read \
    .option("header", True) \
    .schema(ubigeo_schema) \
    .csv(ruta)

# COMMAND ----------

ubigeo_with_timestamp_df = ubigeo_df.withColumn(
    "ingestion_date",
    current_timestamp()
)

# COMMAND ----------

ubigeo_with_timestamp_df.write \
    .mode("overwrite") \
    .saveAsTable(f"{catalogo}.{esquema}.TABLA_COD_UBIGEO")

# COMMAND ----------

df_ubigeo = spark.table(f"{catalogo}.{esquema}.TABLA_COD_UBIGEO")

display(df_ubigeo)

# COMMAND ----------

