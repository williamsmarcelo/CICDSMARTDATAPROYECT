-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

create widget text storageName default "adlssmartdataprojects";

-- COMMAND ----------

-- MAGIC %python
-- MAGIC storageName = dbutils.widgets.get("storageName")

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-raw`
URL 'abfss://raw@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `credential`)
COMMENT 'Ubicación externa para las tablas raw del Data Lake';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-bronze`
URL 'abfss://bronze@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `credential`)
COMMENT 'Ubicación externa para las tablas bronze del Data Lake';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-silver`
URL 'abfss://silver@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `credential`)
COMMENT 'Ubicación externa para las tablas silver del Data Lake';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-golden`
URL 'abfss://golden@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `credential`)
COMMENT 'Ubicación externa para las tablas golden del Data Lake';

-- COMMAND ----------

DROP CATALOG IF EXISTS catalog_au CASCADE;

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-unit-catalog`
URL 'abfss://unit-catalog@adlssmartdataprojects.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `credential`)
COMMENT 'Ubicacion administrada para el catalogo Unity Catalog';

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS catalog_au
MANAGED LOCATION 'abfss://unit-catalog@adlssmartdataprojects.dfs.core.windows.net/'
COMMENT 'Catalogo para la arquitectura medallion del ambiente de dev';

-- COMMAND ----------

DROP SCHEMA IF EXISTS catalog_au.raw;
DROP SCHEMA IF EXISTS catalog_au.bronze;
DROP SCHEMA IF EXISTS catalog_au.silver;
DROP SCHEMA IF EXISTS catalog_au.golden;
DROP SCHEMA IF EXISTS catalog_au.quality;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm(f"abfss://bronze@{storageName}.dfs.core.windows.net/",True)
-- MAGIC dbutils.fs.rm(f"abfss://silver@{storageName}.dfs.core.windows.net/",True)
-- MAGIC dbutils.fs.rm(f"abfss://golden@{storageName}.dfs.core.windows.net/",True)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_au.raw;
CREATE SCHEMA IF NOT EXISTS catalog_au.bronze;
CREATE SCHEMA IF NOT EXISTS catalog_au.silver;
CREATE SCHEMA IF NOT EXISTS catalog_au.golden;
CREATE SCHEMA IF NOT EXISTS catalog_au.quality;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Tablas Bronze

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS catalog_au.bronze.comparativo_ingresos_2012_2025 (
    NIVEL_GOBIERNO string,
    NIVEL_GOBIERNO_NOMBRE string,
    SECTOR string,
    SECTOR_NOMBRE string,
    PLIEGO string,
    PLIEGO_NOMBRE string,
    DEPARTAMENTO_EJECUTORA string,
    DEPARTAMENTO_EJECUTORA_NOMBRE string,
    PROVINCIA_EJECUTORA string,
    PROVINCIA_EJECUTORA_NOMBRE string,
    DISTRITO_EJECUTORA string,
    DISTRITO_EJECUTORA_NOMBRE string,
    EJECUTORA string,
    EJECUTORA_NOMBRE string,
    UBIGEO string,
    RUBRO string,
    RUBRO_NOMBRE string,
    FUENTE_FINANC string,
    FUENTE_FINANC_NOMBRE string,
    TIPO_RECURSO string,
    TIPO_RECURSO_NOMBRE string,
    GENERICA string,
    GENERICA_NOMBRE string,
    SUBGENERICA string,
    SUBGENERICA_NOMBRE string,
    SUBGENERICA_DET string,
    SUBGENERICA_DET_NOMBRE string,
    ESPECIFICA string,
    ESPECIFICA_NOMBRE string,
    ESPECIFICA_DET string,
    ESPECIFICA_DET_NOMBRE string,
    MONTO_PIA_2012 double,
    MONTO_PIM_2012 double,
    MONTO_RECAUDADO_2012 double,
    MONTO_PIA_2013 double,
    MONTO_PIM_2013 double,
    MONTO_RECAUDADO_2013 double,
    MONTO_PIA_2014 double,
    MONTO_PIM_2014 double,
    MONTO_RECAUDADO_2014 double,
    MONTO_PIA_2015 double,
    MONTO_PIM_2015 double,
    MONTO_RECAUDADO_2015 double,
    MONTO_PIA_2016 double,
    MONTO_PIM_2016 double,
    MONTO_RECAUDADO_2016 double,
    MONTO_PIA_2017 double,
    MONTO_PIM_2017 double,
    MONTO_RECAUDADO_2017 double,
    MONTO_PIA_2018 double,
    MONTO_PIM_2018 double,
    MONTO_RECAUDADO_2018 double,
    MONTO_PIA_2019 double,
    MONTO_PIM_2019 double,
    MONTO_RECAUDADO_2019 double,
    MONTO_PIA_2020 double,
    MONTO_PIM_2020 double,
    MONTO_RECAUDADO_2020 double,
    MONTO_PIA_2021 double,
    MONTO_PIM_2021 double,
    MONTO_RECAUDADO_2021 double,
    MONTO_PIA_2022 double,
    MONTO_PIM_2022 double,
    MONTO_RECAUDADO_2022 double,
    MONTO_PIA_2023 double,
    MONTO_PIM_2023 double,
    MONTO_RECAUDADO_2023 double,
    MONTO_PIA_2024 double,
    MONTO_PIM_2024 double,
    MONTO_RECAUDADO_2024 double,
    MONTO_PIA_2025 double,
    MONTO_PIM_2025 double,
    MONTO_RECAUDADO_2025 double,
    FECHA_INGESTA TIMESTAMP
)
USING DELTA
LOCATION "abfss://bronze@${storageName}.dfs.core.windows.net/comparativo_ingresos_2012_2025"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS catalog_au.bronze.TASAS_CRECIMIENTO (
    PERIODO STRING,
    CRECIMIENTO DOUBLE,
    CRECIMIENTO_REAL DOUBLE,
    INGESTION_DATE TIMESTAMP
)
USING DELTA
LOCATION "abfss://bronze@${storageName}.dfs.core.windows.net/TASAS_CRECIMIENTO";

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS catalog_au.bronze.TABLA_COD_UBIGEO (
    IDDIST string,
    NOMBDEP string,
    NOMBPROV string,
    NOMBDIST string,
    NOM_CAPITAL string,
    INGESTION_DATE TIMESTAMP
)
USING DELTA
LOCATION "abfss://bronze@${storageName}.dfs.core.windows.net/TABLA_COD_UBIGEO"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Tablas Silver

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS catalog_au.silver.comparativo_ingresos (
    NIVEL_GOBIERNO STRING,
    NIVEL_GOBIERNO_NOMBRE STRING,
    SECTOR STRING,
    SECTOR_NOMBRE STRING,
    PLIEGO STRING,
    PLIEGO_NOMBRE STRING,
    DEPARTAMENTO_EJECUTORA STRING,
    DEPARTAMENTO_EJECUTORA_NOMBRE STRING,
    PROVINCIA_EJECUTORA STRING,
    PROVINCIA_EJECUTORA_NOMBRE STRING,
    DISTRITO_EJECUTORA STRING,
    DISTRITO_EJECUTORA_NOMBRE STRING,
    EJECUTORA STRING,
    EJECUTORA_NOMBRE STRING,
    UBIGEO STRING,
    RUBRO STRING,
    RUBRO_NOMBRE STRING,
    FUENTE_FINANC STRING,
    FUENTE_FINANC_NOMBRE STRING,
    TIPO_RECURSO STRING,
    TIPO_RECURSO_NOMBRE STRING,
    GENERICA STRING,
    GENERICA_NOMBRE STRING,
    SUBGENERICA STRING,
    SUBGENERICA_NOMBRE STRING,
    SUBGENERICA_DET STRING,
    SUBGENERICA_DET_NOMBRE STRING,
    ESPECIFICA STRING,
    ESPECIFICA_NOMBRE STRING,
    ESPECIFICA_DET STRING,
    ESPECIFICA_DET_NOMBRE STRING,
    PERIODO STRING,
    MONTO_PIA DOUBLE,
    MONTO_PIM DOUBLE,
    MONTO_RECAUDADO DOUBLE
)
USING DELTA
LOCATION "abfss://silver@${storageName}.dfs.core.windows.net/comparativo_ingresos";

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS catalog_au.silver.monto_totales (
    PERIODO STRING,
    MONTO_PIA DOUBLE,
    MONTO_PIM DOUBLE,
    MONTO_RECAUDADO DOUBLE,
    LEYENDA STRING
)
USING DELTA
LOCATION "abfss://silver@${storageName}.dfs.core.windows.net/monto_totales";

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS catalog_au.silver.comparativo_ingresos_final (
    PERIODO STRING,
    EJECUTORA STRING,
    EJECUTORA_NOMBRE STRING,
    UBIGEO STRING,
    NOMBDEP STRING,
    NOMBPROV STRING,
    NOMBDIST STRING,
    NOM_CAPITAL STRING,
    TIP_RECURSO STRING,
    LEYENDA STRING,
    COD_TIP_RECURSO INT,
    COD_LEYENDA INT,
    MONTO_PIA DOUBLE,
    MONTO_PIM DOUBLE,
    MONTO_RECAUDADO DOUBLE
)
USING DELTA
LOCATION "abfss://silver@${storageName}.dfs.core.windows.net/comparativo_ingresos_final";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Tablas Golden

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS catalog_au.golden.monto_totales (
    PERIODO STRING,
    MONTO_PIA DOUBLE,
    MONTO_PIM DOUBLE,
    MONTO_RECAUDADO DOUBLE,
    LEYENDA STRING
)
USING DELTA
LOCATION "abfss://golden@${storageName}.dfs.core.windows.net/monto_totales";

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS catalog_au.golden.comparativo_ingresos_final (
    PERIODO STRING,
    EJECUTORA STRING,
    EJECUTORA_NOMBRE STRING,
    UBIGEO STRING,
    NOMBDEP STRING,
    NOMBPROV STRING,
    NOMBDIST STRING,
    NOM_CAPITAL STRING,
    TIP_RECURSO STRING,
    LEYENDA STRING,
    COD_TIP_RECURSO INT,
    COD_LEYENDA INT,
    MONTO_PIA DOUBLE,
    MONTO_PIM DOUBLE,
    MONTO_RECAUDADO DOUBLE
)
USING DELTA
LOCATION "abfss://golden@${storageName}.dfs.core.windows.net/comparativo_ingresos_final";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Tablas Quality - Duplicados y validaciones

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS catalog_au.quality.validacion_tasas_duplicadas (
    PERIODO INT,
    `count` BIGINT
) USING DELTA;

CREATE TABLE IF NOT EXISTS catalog_au.quality.validacion_ubigeo_duplicados (
    UBIGEO STRING,
    `count` BIGINT
) USING DELTA;

CREATE TABLE IF NOT EXISTS catalog_au.quality.validacion_factores_distribucion (
    SUMA_FACTOR_PIA DOUBLE,
    SUMA_FACTOR_PIM DOUBLE,
    SUMA_FACTOR_RECAUDADO DOUBLE
) USING DELTA;

CREATE TABLE IF NOT EXISTS catalog_au.quality.validacion_distribucion_proyecciones (
    PERIODO STRING,
    MONTO_PIA_DISTRIBUIDO DOUBLE,
    TOTAL_PIA_PROY DOUBLE,
    DIFERENCIA_PIA DOUBLE,
    MONTO_PIM_DISTRIBUIDO DOUBLE,
    TOTAL_PIM_PROY DOUBLE,
    DIFERENCIA_PIM DOUBLE,
    MONTO_RECAUDADO_DISTRIBUIDO DOUBLE,
    TOTAL_RECAUDADO_PROY DOUBLE,
    DIFERENCIA_RECAUDADO DOUBLE
) USING DELTA;

CREATE TABLE IF NOT EXISTS catalog_au.quality.validacion_resumen_periodo (
    PERIODO STRING,
    TOTAL_REGISTROS BIGINT,
    TOTAL_PIA DOUBLE,
    TOTAL_PIM DOUBLE,
    TOTAL_RECAUDADO DOUBLE
) USING DELTA;

CREATE TABLE IF NOT EXISTS catalog_au.quality.validacion_ubigeo_no_encontrado (
    UBIGEO STRING,
    EJECUTORA STRING,
    EJECUTORA_NOMBRE STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS catalog_au.quality.validacion_registros_join (
    REGISTROS_FINANCIEROS BIGINT,
    REGISTROS_CON_UBIGEO BIGINT
) USING DELTA;

CREATE TABLE IF NOT EXISTS catalog_au.quality.validacion_registros_silver_periodo (
    PERIODO STRING,
    `count` BIGINT
) USING DELTA;

CREATE TABLE IF NOT EXISTS catalog_au.quality.validacion_registros_silver_nivel_gobierno (
    NIVEL_GOBIERNO STRING,
    `count` BIGINT
) USING DELTA;

CREATE TABLE IF NOT EXISTS catalog_au.quality.validacion_longitud_ubigeo (
    LONGITUD_UBIGEO INT,
    `count` BIGINT
) USING DELTA;

CREATE TABLE IF NOT EXISTS catalog_au.quality.validacion_montos_nacionales (
    LEYENDA STRING,
    CANTIDAD_REGISTROS BIGINT,
    ANIO_INICIAL STRING,
    ANIO_FINAL STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS catalog_au.quality.validacion_montos_por_departamento (
    PERIODO STRING,
    NOMBDEP STRING,
    MONTO_PIA DOUBLE,
    MONTO_PIM DOUBLE,
    MONTO_RECAUDADO DOUBLE,
    TOTAL_REGISTROS BIGINT
) USING DELTA;
