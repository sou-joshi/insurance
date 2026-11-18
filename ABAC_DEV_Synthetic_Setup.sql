-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ABAC validation — create a synthetic fixture
-- MAGIC Import as a SQL notebook, select the existing Pro SQL warehouse and Run all.
-- MAGIC Creates one five-row Delta table in the existing governance_observability schema.
-- MAGIC Uses generated strings and fixed dates/timestamps only. Customer tables are not read.
-- MAGIC Applies existing governed sensitivity tags to eight fixture columns.
-- MAGIC Does not create or modify policies, functions, group memberships or grants.
-- MAGIC Requires USE CATALOG, USE SCHEMA, CREATE TABLE and ASSIGN on ifbi_sensitivity.
-- MAGIC The table creator owns the new fixture. A tag-permission failure is a setup failure;
-- MAGIC do not interpret an incompletely tagged fixture as a successful enforcement test.
-- MAGIC Run this setup under one identity without concurrent runs. On retry, the marked
-- MAGIC fixture is reused and checked; existing rows are not replaced or appended.

-- COMMAND ----------
-- These are the only environment bindings. The target schema must already exist.
DECLARE OR REPLACE VARIABLE abac_test_catalog STRING DEFAULT 'ifbi_edw_dev_raw';
DECLARE OR REPLACE VARIABLE abac_test_schema STRING DEFAULT 'governance_observability';
DECLARE OR REPLACE VARIABLE abac_test_table STRING DEFAULT 'abac_synthetic_fixture_v1';
DECLARE OR REPLACE VARIABLE abac_test_fqn STRING DEFAULT
  concat(abac_test_catalog,'.',abac_test_schema,'.',abac_test_table);

-- COMMAND ----------
-- Preflight: stop rather than alter an unrelated object with the same name.
SELECT assert_true(
  abac_test_catalog RLIKE '^[A-Za-z_][A-Za-z0-9_]*$'
  AND abac_test_schema RLIKE '^[A-Za-z_][A-Za-z0-9_]*$'
  AND abac_test_table RLIKE '^[A-Za-z_][A-Za-z0-9_]*$',
  'Use plain catalog/schema/table identifiers'
);
SELECT assert_true(
  EXISTS (
    SELECT 1 FROM system.information_schema.schemata
    WHERE catalog_name=abac_test_catalog AND schema_name=abac_test_schema
  ),
  'The configured fixture schema must exist and be visible'
);
SELECT assert_true(
  NOT EXISTS (
    SELECT 1 FROM system.information_schema.tables
    WHERE table_catalog=abac_test_catalog
      AND table_schema=abac_test_schema
      AND table_name=abac_test_table
      AND NOT (
        (comment <=> 'IFBI_ABAC_SYNTHETIC_FIXTURE_V1')
        AND data_source_format='DELTA'
        AND table_type='MANAGED'
      )
  ),
  'Fixture name is occupied by another object; no changes made to it'
);

-- COMMAND ----------
-- CTAS is atomic. On rerun IF NOT EXISTS retains the existing fixture data.
CREATE TABLE IF NOT EXISTS IDENTIFIER(abac_test_fqn)
USING DELTA
COMMENT 'IFBI_ABAC_SYNTHETIC_FIXTURE_V1'
TBLPROPERTIES ('ifbi.component'='abac_synthetic_fixture_v1')
AS
SELECT
  test_id,
  test_case,
  text_value AS critical_text,
  text_value AS restricted_text,
  text_value AS sensitive_text,
  text_value AS unclassified_text,
  timestamp_value AS critical_ts,
  date_value AS restricted_date,
  timestamp_value AS restricted_ts,
  concat('CONTROL-',cast(test_id AS STRING)) AS untagged_control,
  concat('INTERNAL-',cast(test_id AS STRING)) AS internal_control
FROM VALUES
  (1,'NORMAL','SYNTHETIC-123456',DATE'2026-11-18',TIMESTAMP'2026-11-18 12:34:56.123456'),
  (2,'NULL',CAST(NULL AS STRING),CAST(NULL AS DATE),CAST(NULL AS TIMESTAMP)),
  (3,'EMPTY','',CAST(NULL AS DATE),CAST(NULL AS TIMESTAMP)),
  (4,'SHORT','AB',DATE'2024-02-29',TIMESTAMP'2024-02-29 00:00:00'),
  (5,'FOUR','ABCD',DATE'2000-01-01',TIMESTAMP'2000-01-01 23:59:59')
AS fixture_source(test_id,test_case,text_value,date_value,timestamp_value);

-- COMMAND ----------
-- Recheck identity and exact column shape before assigning any tags.
SELECT assert_true(
  EXISTS (
    SELECT 1 FROM system.information_schema.tables
    WHERE table_catalog=abac_test_catalog AND table_schema=abac_test_schema
      AND table_name=abac_test_table
      AND comment='IFBI_ABAC_SYNTHETIC_FIXTURE_V1'
      AND data_source_format='DELTA' AND table_type='MANAGED'
  ),
  'Fixture identity check failed; stop before tag assignment'
);
WITH expected AS (
  SELECT * FROM VALUES
    ('test_id','INT'),('test_case','STRING'),
    ('critical_text','STRING'),('restricted_text','STRING'),
    ('sensitive_text','STRING'),('unclassified_text','STRING'),
    ('critical_ts','TIMESTAMP'),('restricted_date','DATE'),
    ('restricted_ts','TIMESTAMP'),('untagged_control','STRING'),
    ('internal_control','STRING')
  AS e(column_name,data_type)
), actual AS (
  SELECT column_name,data_type FROM system.information_schema.columns
  WHERE table_catalog=abac_test_catalog AND table_schema=abac_test_schema
    AND table_name=abac_test_table
)
SELECT assert_true(
  (SELECT count(*) FROM actual)=11
  AND (SELECT count(*) FROM expected e JOIN actual a
    ON e.column_name=a.column_name AND e.data_type=a.data_type)=11,
  'Fixture schema differs from the expected version; stop before tag assignment'
);

-- COMMAND ----------
-- Each ALTER is limited to this marked synthetic fixture.
ALTER TABLE IDENTIFIER(abac_test_fqn) ALTER COLUMN critical_text
  SET TAGS ('ifbi_sensitivity'='Confidential Critical');
ALTER TABLE IDENTIFIER(abac_test_fqn) ALTER COLUMN restricted_text
  SET TAGS ('ifbi_sensitivity'='Confidential Restricted');
ALTER TABLE IDENTIFIER(abac_test_fqn) ALTER COLUMN sensitive_text
  SET TAGS ('ifbi_sensitivity'='Confidential Sensitive');
ALTER TABLE IDENTIFIER(abac_test_fqn) ALTER COLUMN unclassified_text
  SET TAGS ('ifbi_sensitivity'='Unclassified');
ALTER TABLE IDENTIFIER(abac_test_fqn) ALTER COLUMN critical_ts
  SET TAGS ('ifbi_sensitivity'='Confidential Critical');
ALTER TABLE IDENTIFIER(abac_test_fqn) ALTER COLUMN restricted_date
  SET TAGS ('ifbi_sensitivity'='Confidential Restricted');
ALTER TABLE IDENTIFIER(abac_test_fqn) ALTER COLUMN restricted_ts
  SET TAGS ('ifbi_sensitivity'='Confidential Restricted');
ALTER TABLE IDENTIFIER(abac_test_fqn) ALTER COLUMN internal_control
  SET TAGS ('ifbi_sensitivity'='Internal Use Only');

-- COMMAND ----------
-- Verify the eight required sensitivity tags and the intentionally untagged columns.
WITH expected AS (
  SELECT * FROM VALUES
    ('critical_text','Confidential Critical'),
    ('restricted_text','Confidential Restricted'),
    ('sensitive_text','Confidential Sensitive'),
    ('unclassified_text','Unclassified'),
    ('critical_ts','Confidential Critical'),
    ('restricted_date','Confidential Restricted'),
    ('restricted_ts','Confidential Restricted'),
    ('internal_control','Internal Use Only')
  AS e(column_name,tag_value)
), actual AS (
  SELECT column_name,tag_value
  FROM system.information_schema.column_tags
  WHERE catalog_name=abac_test_catalog AND schema_name=abac_test_schema
    AND table_name=abac_test_table AND tag_name='ifbi_sensitivity'
)
SELECT assert_true(
  (SELECT count(*) FROM actual)=8
  AND (SELECT count(*) FROM expected e JOIN actual a
    ON e.column_name=a.column_name AND e.tag_value=a.tag_value)=8,
  'Fixture sensitivity tags do not match the test contract; stop before reader tests'
);

-- COMMAND ----------
-- Read only the untagged test identifier; no protected temporal projection here.
-- A row-filter-induced count change is a setup blocker, not a passing mask test.
SELECT assert_true(
  count(*)=5 AND count(DISTINCT test_id)=5
  AND min(test_id)=1 AND max(test_id)=5,
  'Fixture must expose exactly five expected identifiers to the setup identity'
)
FROM IDENTIFIER(abac_test_fqn);

-- COMMAND ----------
-- Actual effective policies on this fixture, including child-scope policies.
DECLARE OR REPLACE VARIABLE abac_test_policy_sql STRING DEFAULT
  concat('SHOW EFFECTIVE POLICIES ON TABLE ',abac_test_fqn);
EXECUTE IMMEDIATE abac_test_policy_sql;

-- COMMAND ----------
-- Save/share this result. READY confirms setup, not successful policy enforcement.
SELECT
  'SYNTHETIC_FIXTURE_READY' AS setup_result,
  abac_test_fqn AS fixture_table,
  current_user() AS setup_identity,
  5 AS fixture_rows,
  11 AS fixture_columns,
  8 AS sensitivity_tagged_columns,
  is_account_group_member('P DBX EDW Workspace PII Full View DEV') AS setup_full_view_dev,
  is_account_group_member('P DBX EDW Workspace PII Partial View DEV') AS setup_partial_view_dev,
  'Next: separate reader queries under an existing Partial View identity that is not also Full View or excepted' AS next_action;

-- MAGIC %md
-- MAGIC ## Reader handoff
-- MAGIC Send the final setup result and effective-policy result before running reader tests.
-- MAGIC The reader must have SELECT on this fixture plus existing USE CATALOG/USE SCHEMA
-- MAGIC permissions. This setup does not grant access. If reader access is denied, record the
-- MAGIC exact error for the table owner; broad schema/catalog data grants are not required.
-- MAGIC The final two membership flags are DEV-specific labels. When porting, change those
-- MAGIC group literals to the environment's approved groups as well as the catalog/schema bindings.
-- MAGIC Keep each temporal projection in a separate reader cell. Do not use SELECT *, cast the
-- MAGIC temporal outputs to strings, call the UDFs directly, or use TRY_CAST to hide errors.
-- MAGIC Those would not provide the intended native-type table-policy test.
-- MAGIC This fixture remains until test evidence is captured and its owner performs cleanup.
