# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType,  DoubleType
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

ruta = f"abfss://{container}@{storageName}.dfs.core.windows.net/TASAS_CRECIMIENTO.json"

# COMMAND ----------

# DBTITLE 1,Cell 5
from pyspark.sql.types import DoubleType

tasas_crecimiento_schema = StructType(fields=[
    StructField("PERIODO", StringType(), True),
    StructField("CRECIMIENTO", DoubleType(), True),
    StructField("CRECIMIENTO_REAL", DoubleType(), True)
])

# COMMAND ----------

df_tasas_crecimiento = spark.read \
    .schema(tasas_crecimiento_schema) \
    .option("multiLine", True) \
    .json(ruta)

# COMMAND ----------

tasas_crecimiento_final_df = df_tasas_crecimiento.select(
    col("PERIODO"),
    col("CRECIMIENTO"),
    col("CRECIMIENTO_REAL")
).withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

tasas_crecimiento_final_df.write \
    .mode("overwrite") \
    .insertInto(f"{catalogo}.{esquema}.TASAS_CRECIMIENTO")