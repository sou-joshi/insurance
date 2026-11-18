DECLARE OR REPLACE VARIABLE monitor_manual_run_token STRING;

SET VAR monitor_manual_run_token =
  concat('manual_monitor_', uuid());

CALL raw_classification_collect_sql(
  session.monitor_manual_run_token
);
