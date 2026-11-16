-- Databricks notebook source
-- ============================================================
-- PROYECTO DATABRICKS - COMPARATIVO DE INGRESOS
-- Seguridad y permisos Unity Catalog
-- Catalogo: catalog_au
-- Grupos existentes: DEs, DAs, DMs
-- IMPORTANTE: No se crean grupos ni usuarios; se utilizan los
-- grupos que ya existen en el workspace/account.
-- ============================================================

-- COMMAND ----------

-- 1. Permitir que los grupos utilicen el catalogo.
GRANT USE CATALOG ON CATALOG catalog_au TO `DEs`;
GRANT USE CATALOG ON CATALOG catalog_au TO `DAs`;
GRANT USE CATALOG ON CATALOG catalog_au TO `DMs`;

-- COMMAND ----------

-- 2. Permisos de esquema para Data Engineers.
GRANT USE SCHEMA ON SCHEMA catalog_au.raw TO `DEs`;
GRANT USE SCHEMA ON SCHEMA catalog_au.bronze TO `DEs`;
GRANT USE SCHEMA ON SCHEMA catalog_au.silver TO `DEs`;
GRANT USE SCHEMA ON SCHEMA catalog_au.golden TO `DEs`;
GRANT USE SCHEMA ON SCHEMA catalog_au.quality TO `DEs`;

GRANT CREATE TABLE ON SCHEMA catalog_au.raw TO `DEs`;
GRANT CREATE TABLE ON SCHEMA catalog_au.bronze TO `DEs`;
GRANT CREATE TABLE ON SCHEMA catalog_au.silver TO `DEs`;
GRANT CREATE TABLE ON SCHEMA catalog_au.golden TO `DEs`;
GRANT CREATE TABLE ON SCHEMA catalog_au.quality TO `DEs`;

-- COMMAND ----------

-- 3. Permisos de lectura para Data Analysts.
GRANT USE SCHEMA ON SCHEMA catalog_au.silver TO `DAs`;
GRANT USE SCHEMA ON SCHEMA catalog_au.golden TO `DAs`;
GRANT USE SCHEMA ON SCHEMA catalog_au.quality TO `DAs`;

GRANT SELECT ON TABLE catalog_au.silver.comparativo_ingresos TO `DAs`;
GRANT SELECT ON TABLE catalog_au.golden.monto_totales TO `DAs`;
GRANT SELECT ON TABLE catalog_au.golden.comparativo_ingresos_final TO `DAs`;

-- COMMAND ----------

-- 4. Permisos de lectura para Data Managers.
GRANT USE SCHEMA ON SCHEMA catalog_au.golden TO `DMs`;
GRANT SELECT ON TABLE catalog_au.golden.comparativo_ingresos_final TO `DMs`;
GRANT SELECT ON TABLE catalog_au.golden.monto_totales TO `DMs`;

-- COMMAND ----------

-- 5. Permisos de modificacion para Data Engineers sobre las tablas
-- que forman parte del proyecto y que ya existen.
GRANT MODIFY ON TABLE catalog_au.bronze.comparativo_ingresos_2012_2025 TO `DEs`;
GRANT MODIFY ON TABLE catalog_au.bronze.TASAS_CRECIMIENTO TO `DEs`;
GRANT MODIFY ON TABLE catalog_au.bronze.TABLA_COD_UBIGEO TO `DEs`;
GRANT MODIFY ON TABLE catalog_au.silver.comparativo_ingresos TO `DEs`;
GRANT MODIFY ON TABLE catalog_au.golden.monto_totales TO `DEs`;
GRANT MODIFY ON TABLE catalog_au.golden.comparativo_ingresos_final TO `DEs`;

-- COMMAND ----------

-- 6. Permisos sobre ubicaciones externas.
-- Los nombres siguientes corresponden a las external locations
-- declaradas en Preparacion_Ambiente.sql.
GRANT READ FILES ON EXTERNAL LOCATION `exlt-raw` TO `DEs`;
GRANT READ FILES ON EXTERNAL LOCATION `exlt-bronze` TO `DEs`;
GRANT WRITE FILES ON EXTERNAL LOCATION `exlt-bronze` TO `DEs`;
GRANT READ FILES ON EXTERNAL LOCATION `exlt-silver` TO `DEs`;
GRANT WRITE FILES ON EXTERNAL LOCATION `exlt-silver` TO `DEs`;
GRANT READ FILES ON EXTERNAL LOCATION `exlt-golden` TO `DEs`;
GRANT WRITE FILES ON EXTERNAL LOCATION `exlt-golden` TO `DEs`;

-- COMMAND ----------

-- 7. Verificacion de permisos asignados.
SHOW GRANTS ON CATALOG catalog_au;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA catalog_au.bronze;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA catalog_au.silver;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA catalog_au.golden;

-- COMMAND ----------

SHOW GRANTS ON TABLE catalog_au.golden.comparativo_ingresos_final;

-- COMMAND ----------

SHOW EXTERNAL LOCATIONS;

-- COMMAND ----------

SHOW GROUPS;