DECLARE OR REPLACE VARIABLE monitor_manual_run_token STRING;

SET VAR monitor_manual_run_token =
  concat('manual_monitor_', uuid());

CALL raw_classification_collect_sql(
  session.monitor_manual_run_token
);


WITH actor AS (
  SELECT
    current_user() AS test_user,
    is_account_group_member(
      'P DBX EDW Workspace PII Full View DEV'
    ) AS full_view,
    is_account_group_member(
      'P DBX EDW Workspace PII Partial View DEV'
    ) AS partial_view
),
samples AS (
  SELECT * FROM VALUES
    (1, 'NULL',  CAST(NULL AS STRING)),
    (2, 'EMPTY', ''),
    (3, 'SHORT', 'AB'),
    (4, 'FOUR',  'ABCD'),
    (5, 'LONG',  'SYNTHETIC-123456')
  AS t(test_id, test_case, input_value)
)
SELECT
  a.*,
  s.test_case,
  s.input_value,
  ifbi_edw_dev_raw.governance.mask_critical(
    s.input_value
  ) AS critical_result,
  ifbi_edw_dev_raw.governance.mask_restricted(
    s.input_value
  ) AS restricted_result,
  ifbi_edw_dev_raw.governance.mask_sensitive(
    s.input_value
  ) AS sensitive_result
FROM samples s
CROSS JOIN actor a
ORDER BY s.test_id;



SELECT
  t.tag_value AS sensitivity,
  c.data_type,
  c.full_data_type,
  COUNT(*) AS column_count
FROM system.information_schema.columns c
JOIN system.information_schema.column_tags t
  ON c.table_catalog = t.catalog_name
 AND c.table_schema  = t.schema_name
 AND c.table_name    = t.table_name
 AND c.column_name   = t.column_name
WHERE c.table_catalog = 'ifbi_edw_dev_raw'
  AND t.tag_name = 'ifbi_sensitivity'
  AND t.tag_value IN (
    'Confidential Critical',
    'Confidential Restricted',
    'Confidential Sensitive',
    'Unclassified'
  )
GROUP BY t.tag_value, c.data_type, c.full_data_type
ORDER BY t.tag_value, c.data_type, c.full_data_type;



WITH config AS (
  SELECT 'ifbi_edw_dev_raw' AS catalog_name
),
tag_evidence AS (
  SELECT
    t.catalog_name,
    t.schema_name,
    t.table_name,
    t.column_name,
    SORT_ARRAY(COLLECT_SET(
      CASE WHEN t.tag_name = 'ifbi_sensitivity'
           THEN t.tag_value END
    )) AS sensitivity_values,
    SORT_ARRAY(COLLECT_SET(
      CASE WHEN t.tag_name = 'auto_class'
           THEN t.tag_value END
    )) AS detected_classes
  FROM system.information_schema.column_tags t
  JOIN config cfg ON t.catalog_name = cfg.catalog_name
  WHERE t.tag_name IN ('ifbi_sensitivity', 'auto_class')
  GROUP BY
    t.catalog_name, t.schema_name,
    t.table_name, t.column_name
)
SELECT
  c.table_catalog,
  c.table_schema,
  c.table_name,
  c.column_name,
  c.full_data_type,
  c.comment AS column_description,
  t.sensitivity_values,
  t.detected_classes,
  'PENDING_STEWARD_REVIEW' AS label_review_status
FROM system.information_schema.columns c
JOIN config cfg ON c.table_catalog = cfg.catalog_name
LEFT JOIN tag_evidence t
  ON  c.table_catalog = t.catalog_name
  AND c.table_schema = t.schema_name
  AND c.table_name = t.table_name
  AND c.column_name = t.column_name
WHERE c.table_schema NOT IN (
  'information_schema',
  'governance',
  'governance_observability',
  'collate'
)
ORDER BY c.table_schema, c.table_name, c.ordinal_position;
