-- Databricks notebook source
-- ============================================================
-- PROYECTO DATABRICKS - COMPARATIVO DE INGRESOS
-- ARCHIVO 5: DELTA SHARING
--
-- Catalogo: catalog_au
-- Esquema: golden
-- Tabla compartida:
--   catalog_au.golden.comparativo_ingresos_final
--
-- Este archivo publica la tabla Golden final mediante
-- Unity Catalog Delta Sharing.
-- ============================================================

-- COMMAND ----------

-- 1. Verificar la tabla Golden que se compartira.

DESCRIBE TABLE catalog_au.golden.comparativo_ingresos_final;

SELECT *
FROM catalog_au.golden.comparativo_ingresos_final
LIMIT 20;

-- COMMAND ----------

-- 2. Crear el Share del proyecto.

CREATE SHARE IF NOT EXISTS SHARE_INGRESOS_GOLDEN
COMMENT 'Comparativo de ingresos tributarios - tabla Golden final';

-- COMMAND ----------

-- 3. Verificar los Shares existentes.

SHOW SHARES;

-- COMMAND ----------

-- 5. Verificar las tablas incluidas en el Share.

SHOW ALL IN SHARE SHARE_INGRESOS_GOLDEN;

-- COMMAND ----------

-- 6. Crear el Recipient externo.
--
-- El nombre es referencial. Reemplazarlo si existe un nombre
-- acordado con la organizacion destinataria.

CREATE RECIPIENT IF NOT EXISTS RECIPIENT_INGRESOS_EXTERNO
COMMENT 'Destinatario externo del comparativo de ingresos';

-- COMMAND ----------

-- 7. Verificar los Recipients existentes.

SHOW RECIPIENTS;

-- COMMAND ----------

-- 8. Otorgar permisos de lectura al Recipient.

GRANT SELECT
ON SHARE SHARE_INGRESOS_GOLDEN
TO RECIPIENT RECIPIENT_INGRESOS_EXTERNO;

-- COMMAND ----------

-- 9. Verificar los permisos del Share.

SHOW GRANTS ON SHARE SHARE_INGRESOS_GOLDEN;

-- COMMAND ----------

-- 10. Consultar la configuracion del Recipient.

DESCRIBE RECIPIENT RECIPIENT_INGRESOS_EXTERNO;

-- COMMAND ----------

-- 11. Verificacion final del Share y sus permisos.

SHOW ALL IN SHARE SHARE_INGRESOS_GOLDEN;

SHOW GRANTS ON SHARE SHARE_INGRESOS_GOLDEN;

-- ============================================================
-- NOTAS DE EJECUCION
--
-- 1. Ejecutar despues de completar:
--      1.- Preparacion_Ambiente.sql
--      2.- Archivos Ingest
--      3.- Transform.py
--      4.- Load.py
--
-- 2. La tabla compartida se carga exclusivamente en:
--      catalog_au.golden.comparativo_ingresos_final
--
-- 3. Este archivo no crea ni modifica tablas Bronze, Silver
--    o Golden, ni reemplaza los permisos internos definidos
--    en 4. Grants-Medallion.sql.
--
-- 4. La creacion del Recipient no completa por si sola la
--    entrega al destinatario. El administrador debe obtener
--    y entregar los detalles de activacion/credenciales que
--    Databricks habilite para ese Recipient, por un canal seguro.
--
-- 5. No compartir credenciales administrativas ni tokens
--    personales.
-- ============================================================