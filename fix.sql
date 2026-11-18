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
