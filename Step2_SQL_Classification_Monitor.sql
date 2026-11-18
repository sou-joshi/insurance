-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Step 2 — SQL classification monitor and baseline
-- MAGIC Run this SQL notebook on the same approved Pro SQL warehouse and identity used for the inventory views.
-- MAGIC It creates one Delta metadata journal, two invoker-rights procedures and four monitoring views.
-- MAGIC The first run queues ONE activation summary and zero baseline column incidents. Sources and tags are read-only.
-- MAGIC Email recipient: C000926@infarmbureau.com. Import the paired IFBI_DEV_Classification_Changes.dbalert.json to connect this collector to email.
-- MAGIC SQL alone does not register email subscribers or send messages; the paired native alert evaluates this monitor's real pending events.
-- MAGIC Permissions: metadata/reference visibility, CREATE TABLE and CREATE FUNCTION in the existing output schema.
-- MAGIC No catalog, warehouse, Spark cluster, external endpoint or customer-value scan is created.

-- COMMAND ----------
SET TIME ZONE 'UTC';
DECLARE OR REPLACE VARIABLE monitor_cfg DEFAULT from_json('{
  "environment": "DEV",
  "output_catalog": "ifbi_edw_dev_raw",
  "output_schema": "governance_observability",
  "classification_grace_minutes": 30,
  "reminder_hours": 24,
  "maximum_notifications_per_run": 500,
  "maximum_columns": 50000,
  "maximum_inventory_drop_fraction": 0.2,
  "retention_days": 30,
  "poll_minutes": 60,
  "daily_summary_enabled": true,
  "daily_summary_hour_utc": 13
}','STRUCT<environment:STRING,output_catalog:STRING,output_schema:STRING,classification_grace_minutes:INT,reminder_hours:INT,maximum_notifications_per_run:INT,maximum_columns:INT,maximum_inventory_drop_fraction:DOUBLE,retention_days:INT,poll_minutes:INT,daily_summary_enabled:BOOLEAN,daily_summary_hour_utc:INT>');

-- COMMAND ----------
DECLARE OR REPLACE VARIABLE monitor_rendered MAP<STRING,STRING> DEFAULT map();
DECLARE OR REPLACE VARIABLE monitor_journal STRING DEFAULT '';
DECLARE OR REPLACE VARIABLE monitor_name STRING DEFAULT '';
DECLARE OR REPLACE VARIABLE monitor_prefix STRING DEFAULT '';
BEGIN
 DECLARE v_sql STRING;
 DECLARE v_check BOOLEAN;
 DECLARE v_map MAP<STRING,STRING>;
 DECLARE v_comment STRING;
 DECLARE v_source_definition STRING;
 DECLARE v_expected_identity STRING;
 DECLARE v_legacy_expected_identity STRING;
 SET v_check=assert_true(monitor_cfg.environment RLIKE '^[A-Za-z0-9_-]+$'
  AND monitor_cfg.output_catalog RLIKE '^[A-Za-z_][A-Za-z0-9_]*$'
  AND monitor_cfg.output_schema RLIKE '^[A-Za-z_][A-Za-z0-9_]*$','Use a valid environment and plain output catalog/schema identifiers');
 SET v_check=assert_true(monitor_cfg.classification_grace_minutes BETWEEN 0 AND 1440 AND monitor_cfg.reminder_hours BETWEEN 1 AND 720
  AND monitor_cfg.maximum_notifications_per_run BETWEEN 1 AND 5000 AND monitor_cfg.maximum_columns BETWEEN 1 AND 100000
  AND monitor_cfg.maximum_inventory_drop_fraction BETWEEN 0 AND 0.5 AND monitor_cfg.retention_days BETWEEN 7 AND 365
  AND monitor_cfg.poll_minutes IN (15,30,60) AND monitor_cfg.daily_summary_hour_utc BETWEEN 0 AND 23
  AND monitor_cfg.daily_summary_enabled IS NOT NULL,'Review monitor settings');
 SET monitor_prefix=concat('`',monitor_cfg.output_catalog,'`.`',monitor_cfg.output_schema,'`');
 SET monitor_journal=concat(monitor_cfg.output_catalog,'.',monitor_cfg.output_schema,'.classification_monitor_sql_journal');
 SET monitor_name=concat('sql_',lower(monitor_cfg.environment),'_',substring(sha2(monitor_journal,256),1,12));
 SET v_source_definition=(SELECT view_definition FROM system.information_schema.views
  WHERE table_catalog=monitor_cfg.output_catalog AND table_schema=monitor_cfg.output_schema AND table_name='raw_classification_column_inventory');
 SET v_check=assert_true(v_source_definition IS NOT NULL,'Use the inventory deployment identity with readable inventory view definition');
 SET v_check=assert_true(NOT EXISTS (SELECT 1 FROM system.information_schema.tables
  WHERE table_catalog=monitor_cfg.output_catalog AND table_schema=monitor_cfg.output_schema AND table_name='classification_monitor_sql_journal'
  AND NOT (comment <=> 'IFBI_SQL_MONITOR_V1')),'Existing journal name belongs to another component');
 SET v_check=assert_true(NOT EXISTS (SELECT 1 FROM system.information_schema.routines
  WHERE routine_catalog=monitor_cfg.output_catalog AND routine_schema=monitor_cfg.output_schema
  AND routine_name IN ('raw_classification_collect_sql','raw_classification_ack_sql')
  AND NOT (comment <=> 'IFBI_SQL_MONITOR_V1')),'Existing routine name belongs to another component');
 SET v_check=assert_true(NOT EXISTS (SELECT 1 FROM system.information_schema.tables
  WHERE table_catalog=monitor_cfg.output_catalog AND table_schema=monitor_cfg.output_schema
  AND table_name IN ('raw_classification_monitor_health','raw_classification_monitor_incidents','raw_classification_monitor_notifications','raw_classification_monitor_daily_trends')
  AND (table_type<>'VIEW' OR NOT (comment <=> 'IFBI_SQL_MONITOR_V1'))),'Existing monitor view name belongs to another component');
 SET v_sql=concat('CREATE TABLE IF NOT EXISTS ',monitor_prefix,'.classification_monitor_sql_journal (',
  'monitor_id STRING,run_id STRING,record_id STRING,record_type STRING,observed_at TIMESTAMP,payload_json STRING) USING DELTA ',
  'COMMENT ',char(39),'IFBI_SQL_MONITOR_V1',char(39),' TBLPROPERTIES (',char(39),'ifbi.component',char(39),'=',char(39),'classification_monitor_sql_v1',char(39),')');
 EXECUTE IMMEDIATE v_sql;
 SET v_check=assert_true(NOT EXISTS (SELECT 1 FROM IDENTIFIER(monitor_journal) WHERE monitor_id<>monitor_name),
  'Use a separate governance output schema for each environment and monitor deployment');
 SET v_expected_identity=(SELECT sha2(concat(sha2(concat(to_json(monitor_cfg),'|','f9dbbf2f33d34553abd96beec26de8e196b1f1b700d633fab819b227ff060d87'),256),
  '|',current_metastore(),'|',current_user(),'|',sha2(concat(t.comment,'|',t.table_owner,'|',v.view_definition),256)),256)
  FROM system.information_schema.tables t JOIN system.information_schema.views v
   ON t.table_catalog=v.table_catalog AND t.table_schema=v.table_schema AND t.table_name=v.table_name
  WHERE t.table_catalog=monitor_cfg.output_catalog AND t.table_schema=monitor_cfg.output_schema
   AND t.table_name='raw_classification_column_inventory' AND startswith(t.comment,'IFBI_SQL_INVENTORY_V1:'));
 SET v_check=assert_true(v_expected_identity IS NOT NULL,'Inventory view identity or ownership is not readable');
 SET v_legacy_expected_identity=(SELECT sha2(concat(sha2(concat(to_json(monitor_cfg),'|','e8cbf326a2866dfa22b630bc7671b2fe6f32e680b40122aaf47810535755f7c5'),256),
  '|',current_metastore(),'|',current_user(),'|',sha2(concat(t.comment,'|',t.table_owner,'|',v.view_definition),256)),256)
  FROM system.information_schema.tables t JOIN system.information_schema.views v
   ON t.table_catalog=v.table_catalog AND t.table_schema=v.table_schema AND t.table_name=v.table_name
  WHERE t.table_catalog=monitor_cfg.output_catalog AND t.table_schema=monitor_cfg.output_schema
   AND t.table_name='raw_classification_column_inventory' AND startswith(t.comment,'IFBI_SQL_INVENTORY_V1:'));
 IF EXISTS (SELECT 1 FROM IDENTIFIER(monitor_journal) WHERE monitor_id=monitor_name AND record_type='RUN') THEN
  SET v_check=assert_true((SELECT get_json_object(payload_json,'$.control.identity_hash') FROM IDENTIFIER(monitor_journal)
   WHERE monitor_id=monitor_name AND record_type='RUN' ORDER BY observed_at DESC,run_id DESC LIMIT 1) IN (v_expected_identity,v_legacy_expected_identity),
   'Existing baseline uses different source, identity, settings or implementation; review migration before redeploying');
 END IF;
 SET v_map=map('{{PREFIX}}',monitor_prefix,'{{JOURNAL}}',concat(monitor_prefix,'.classification_monitor_sql_journal'),
  '{{MONITOR_ID}}',monitor_name,'{{ENVIRONMENT}}',monitor_cfg.environment,'{{CATALOG}}',monitor_cfg.output_catalog,'{{SCHEMA}}',monitor_cfg.output_schema,
  '{{PROFILE_HASH}}',sha2(concat(to_json(monitor_cfg),'|','f9dbbf2f33d34553abd96beec26de8e196b1f1b700d633fab819b227ff060d87'),256),
  '{{LEGACY_PROFILE_HASH}}',sha2(concat(to_json(monitor_cfg),'|','e8cbf326a2866dfa22b630bc7671b2fe6f32e680b40122aaf47810535755f7c5'),256),'{{INITIAL_ENABLED}}','true',
  '{{GRACE}}',cast(monitor_cfg.classification_grace_minutes AS STRING),
  '{{REMINDER_HOURS}}',cast(monitor_cfg.reminder_hours AS STRING),'{{BATCH_SIZE}}',cast(monitor_cfg.maximum_notifications_per_run AS STRING),
  '{{MAX_COLUMNS}}',cast(monitor_cfg.maximum_columns AS STRING),'{{CAPTURE_LIMIT}}',cast(monitor_cfg.maximum_columns+1 AS STRING),
  '{{MAX_DROP}}',cast(monitor_cfg.maximum_inventory_drop_fraction AS STRING),'{{RETENTION_DAYS}}',cast(monitor_cfg.retention_days AS STRING),
  '{{DAILY_ENABLED}}',cast(monitor_cfg.daily_summary_enabled AS STRING),'{{DAILY_HOUR}}',cast(monitor_cfg.daily_summary_hour_utc AS STRING),'INTERVAL 135 MINUTES',concat('INTERVAL ',cast(2*monitor_cfg.poll_minutes+15 AS STRING),' MINUTES'));
 FOR script_row AS SELECT key AS script_name,value AS script_sql FROM explode(map('collect','CREATE OR REPLACE PROCEDURE {{PREFIX}}.raw_classification_collect_sql(IN p_run_token STRING)
 LANGUAGE SQL SQL SECURITY INVOKER MODIFIES SQL DATA COMMENT \'IFBI_SQL_MONITOR_V1\'
 AS BEGIN
 DECLARE v_run_id STRING;
 DECLARE v_parent STRING;
 DECLARE v_now TIMESTAMP;
 DECLARE v_identity STRING;
 DECLARE v_legacy_identity STRING;
 DECLARE v_source_signature STRING;
 DECLARE v_baseline_at TIMESTAMP;
 DECLARE v_count BIGINT;
 DECLARE v_prior_count BIGINT;
 DECLARE v_check BOOLEAN;
 DECLARE v_activation_enqueued BOOLEAN;
 SET v_check=assert_true(p_run_token IS NOT NULL AND length(trim(p_run_token)) BETWEEN 1 AND 128,\'Provide a nonempty run token of at most 128 characters\');
 SET v_run_id=sha2(concat(\'{{MONITOR_ID}}\',\':\',p_run_token),256);
 IF NOT EXISTS (SELECT 1 FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'RUN\' AND run_id=v_run_id) THEN
  SET v_source_signature=(SELECT sha2(concat(t.comment,\'|\',t.table_owner,\'|\',v.view_definition),256)
   FROM system.information_schema.tables t JOIN system.information_schema.views v
    ON t.table_catalog=v.table_catalog AND t.table_schema=v.table_schema AND t.table_name=v.table_name
   WHERE t.table_catalog=\'{{CATALOG}}\' AND t.table_schema=\'{{SCHEMA}}\' AND t.table_name=\'raw_classification_column_inventory\'
     AND startswith(t.comment,\'IFBI_SQL_INVENTORY_V1:\'));
  SET v_check=assert_true(v_source_signature IS NOT NULL,\'Read access to the inventory view definition is required; use the approved stable monitor identity\');
  SET v_identity=sha2(concat(\'{{PROFILE_HASH}}\',\'|\',current_metastore(),\'|\',current_user(),\'|\',v_source_signature),256);
  SET v_legacy_identity=sha2(concat(\'{{LEGACY_PROFILE_HASH}}\',\'|\',current_metastore(),\'|\',current_user(),\'|\',v_source_signature),256);
  SET v_parent=(SELECT run_id FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'RUN\' ORDER BY observed_at DESC,run_id DESC LIMIT 1);
  SET v_baseline_at=(SELECT cast(get_json_object(payload_json,\'$.control.baseline_at\') AS TIMESTAMP) FROM {{JOURNAL}}
   WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'RUN\' AND run_id=v_parent);
  SET v_activation_enqueued=coalesce((SELECT cast(get_json_object(payload_json,\'$.control.activation_enqueued\') AS BOOLEAN) FROM {{JOURNAL}}
   WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'RUN\' AND run_id=v_parent),false);
  IF v_parent IS NOT NULL THEN
   SET v_check=assert_true((SELECT get_json_object(payload_json,\'$.control.identity_hash\') FROM {{JOURNAL}}
    WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'RUN\' AND run_id=v_parent) IN (v_identity,v_legacy_identity),
    \'Monitor identity, view definition, owner or configuration changed; retain the baseline and review the deployment\');
  END IF;

  -- A single staged capture pins source observations and the previous published run.
  IF NOT EXISTS (SELECT 1 FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'ATTEMPT\' AND run_id=v_run_id) THEN
   SET v_now=current_timestamp();
   MERGE INTO {{JOURNAL}} target USING (
    SELECT \'{{MONITOR_ID}}\' AS monitor_id,v_run_id AS run_id,asset_key AS record_id,\'OBSERVATION\' AS record_type,v_now AS observed_at,payload_json
    FROM (SELECT sha2(to_json(array(catalog_name,schema_name,table_name,column_name)),256) AS asset_key,
     to_json(named_struct(\'asset_key\',sha2(to_json(array(catalog_name,schema_name,table_name,column_name)),256),\'catalog_name\',catalog_name,\'schema_name\',schema_name,\'table_name\',table_name,\'column_name\',column_name,\'column_fqn\',column_fqn,\'data_type\',data_type,\'remediation_owner\',remediation_owner,\'current_sensitivity\',current_sensitivity,\'sensitivity_state\',sensitivity_state,\'actual_priority\',actual_priority,\'expected_priority\',expected_priority,\'expected_sensitivity\',expected_sensitivity,\'invalid_rule_label_count\',invalid_rule_label_count)) AS payload_json FROM {{PREFIX}}.raw_classification_column_inventory LIMIT {{CAPTURE_LIMIT}}) source_rows
    UNION ALL SELECT \'{{MONITOR_ID}}\',v_run_id,v_run_id,\'ATTEMPT\',v_now,
      to_json(named_struct(\'parent_run_id\',v_parent,\'identity_hash\',v_identity))
   ) incoming ON target.monitor_id=incoming.monitor_id AND target.run_id=incoming.run_id
      AND target.record_type=incoming.record_type AND target.record_id=incoming.record_id
   WHEN NOT MATCHED THEN INSERT *;
  END IF;
  SET v_check=assert_true((SELECT get_json_object(payload_json,\'$.parent_run_id\') FROM {{JOURNAL}}
   WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'ATTEMPT\' AND run_id=v_run_id) <=> v_parent,
   \'A newer snapshot has been published; do not replay this older attempt. Use a new run token\');
  SET v_check=assert_true((SELECT get_json_object(payload_json,\'$.identity_hash\') FROM {{JOURNAL}}
   WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'ATTEMPT\' AND run_id=v_run_id)=v_identity,\'Staged capture identity changed\');
  SET v_now=(SELECT observed_at FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'ATTEMPT\' AND run_id=v_run_id);
  SET v_count=(SELECT count(*) FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'OBSERVATION\' AND run_id=v_run_id);
  SET v_check=assert_true(v_count BETWEEN 1 AND {{MAX_COLUMNS}},\'Inventory is empty or over the configured budget; the previous published snapshot is retained\');
  SET v_prior_count=(SELECT count(*) FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'COLUMN\' AND run_id=v_parent
   AND get_json_object(payload_json,\'$.observed\')=\'true\');
  SET v_check=assert_true(v_parent IS NULL OR v_count>=v_prior_count*(1.0-{{MAX_DROP}}),
   \'Visible column count dropped unexpectedly; check permissions and scope before continuing\');
  SET v_check=assert_true(NOT EXISTS (
   SELECT 1 FROM {{JOURNAL}} p WHERE p.monitor_id=\'{{MONITOR_ID}}\' AND p.record_type=\'COLUMN\' AND p.run_id=v_parent
   AND get_json_object(p.payload_json,\'$.observed\')=\'true\'
   AND NOT EXISTS (SELECT 1 FROM {{JOURNAL}} n WHERE n.monitor_id=\'{{MONITOR_ID}}\' AND n.record_type=\'OBSERVATION\' AND n.run_id=v_run_id
    AND get_json_object(p.payload_json,\'$.catalog_name\')=get_json_object(n.payload_json,\'$.catalog_name\')
    AND get_json_object(p.payload_json,\'$.schema_name\')=get_json_object(n.payload_json,\'$.schema_name\'))),
   \'An expected catalog/schema is no longer visible; do not treat missing evidence as recovery\');
  SET v_check=assert_true((SELECT count(DISTINCT record_id) FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\'
   AND ((record_type=\'OBSERVATION\' AND run_id=v_run_id) OR (record_type=\'COLUMN\' AND run_id=v_parent)))<={{MAX_COLUMNS}},
   \'Retained column history exceeds the configured budget; review scope and retirement handling\');

  -- State, notices and the RUN publication marker commit in one Delta MERGE.
  MERGE INTO {{JOURNAL}} target USING (
WITH
old_columns AS (
 SELECT from_json(payload_json,\'STRUCT<asset_key:STRING,catalog_name:STRING,schema_name:STRING,table_name:STRING,column_name:STRING,column_fqn:STRING,data_type:STRING,remediation_owner:STRING,current_sensitivity:STRING,sensitivity_state:STRING,actual_priority:INT,expected_priority:INT,expected_sensitivity:STRING,invalid_rule_label_count:BIGINT,observed:BOOLEAN,first_seen_at:TIMESTAMP,last_seen_at:TIMESTAMP,origin:STRING,baseline_missing:BOOLEAN,ever_tagged:BOOLEAN,protection_floor:INT,baseline_signature:STRING>\') AS c FROM {{JOURNAL}}
 WHERE monitor_id=\'{{MONITOR_ID}}\' AND run_id=v_parent AND record_type=\'COLUMN\'
),
observations AS (
 SELECT from_json(payload_json,\'STRUCT<asset_key:STRING,catalog_name:STRING,schema_name:STRING,table_name:STRING,column_name:STRING,column_fqn:STRING,data_type:STRING,remediation_owner:STRING,current_sensitivity:STRING,sensitivity_state:STRING,actual_priority:INT,expected_priority:INT,expected_sensitivity:STRING,invalid_rule_label_count:BIGINT>\') AS c FROM {{JOURNAL}}
 WHERE monitor_id=\'{{MONITOR_ID}}\' AND run_id=v_run_id AND record_type=\'OBSERVATION\'
),
prior_issues AS (
 SELECT from_json(payload_json,\'STRUCT<issue_key:STRING,asset_key:STRING,generation:INT,finding_type:STRING,priority:STRING,catalog_name:STRING,schema_name:STRING,table_name:STRING,column_name:STRING,column_fqn:STRING,owner:STRING,current_sensitivity:STRING,expected_sensitivity:STRING,required_priority:INT,detail:STRING,status:STRING,opened_at:TIMESTAMP,updated_at:TIMESTAMP,resolved_at:TIMESTAMP,eligible_at:TIMESTAMP,ever_announced:BOOLEAN,last_accepted_at:TIMESTAMP,recovery_accepted_at:TIMESTAMP>\') AS i FROM {{JOURNAL}}
 WHERE monitor_id=\'{{MONITOR_ID}}\' AND run_id=v_parent AND record_type=\'ISSUE\'
),
prior_notices AS (
 SELECT from_json(payload_json,\'STRUCT<notice_id:STRING,issue_key:STRING,asset_key:STRING,generation:INT,finding_type:STRING,priority:STRING,catalog_name:STRING,schema_name:STRING,table_name:STRING,column_name:STRING,column_fqn:STRING,owner:STRING,current_sensitivity:STRING,expected_sensitivity:STRING,required_priority:INT,detail:STRING,kind:STRING,created_at:TIMESTAMP,selected_for_batch:BOOLEAN>\') AS n FROM {{JOURNAL}}
 WHERE monitor_id=\'{{MONITOR_ID}}\' AND run_id=v_parent AND record_type=\'NOTICE\'
),
accepted AS (
 SELECT get_json_object(payload_json,\'$.issue_key\') AS issue_key,
 cast(get_json_object(payload_json,\'$.generation\') AS INT) AS generation,
 max(CASE WHEN get_json_object(payload_json,\'$.kind\')<>\'RECOVERY\' THEN observed_at END) AS announced_at,
 max(CASE WHEN get_json_object(payload_json,\'$.kind\')=\'RECOVERY\' THEN observed_at END) AS recovered_at
 FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'ACK\'
 GROUP BY get_json_object(payload_json,\'$.issue_key\'),cast(get_json_object(payload_json,\'$.generation\') AS INT)
),
column_inputs AS (
 SELECT CASE WHEN n.c.asset_key IS NOT NULL THEN n.c.asset_key ELSE o.c.asset_key END AS asset_key,
 CASE WHEN n.c.asset_key IS NOT NULL THEN n.c.catalog_name ELSE o.c.catalog_name END AS catalog_name,
 CASE WHEN n.c.asset_key IS NOT NULL THEN n.c.schema_name ELSE o.c.schema_name END AS schema_name,
 CASE WHEN n.c.asset_key IS NOT NULL THEN n.c.table_name ELSE o.c.table_name END AS table_name,
 CASE WHEN n.c.asset_key IS NOT NULL THEN n.c.column_name ELSE o.c.column_name END AS column_name,
 CASE WHEN n.c.asset_key IS NOT NULL THEN n.c.column_fqn ELSE o.c.column_fqn END AS column_fqn,
 CASE WHEN n.c.asset_key IS NOT NULL THEN n.c.data_type ELSE o.c.data_type END AS data_type,
 CASE WHEN n.c.asset_key IS NOT NULL THEN n.c.remediation_owner ELSE o.c.remediation_owner END AS remediation_owner,
 CASE WHEN n.c.asset_key IS NOT NULL THEN n.c.current_sensitivity ELSE o.c.current_sensitivity END AS current_sensitivity,
 CASE WHEN n.c.asset_key IS NOT NULL THEN n.c.sensitivity_state ELSE o.c.sensitivity_state END AS sensitivity_state,
 CASE WHEN n.c.asset_key IS NOT NULL THEN n.c.actual_priority ELSE o.c.actual_priority END AS actual_priority,
 CASE WHEN n.c.asset_key IS NOT NULL THEN n.c.expected_priority ELSE o.c.expected_priority END AS expected_priority,
 CASE WHEN n.c.asset_key IS NOT NULL THEN n.c.expected_sensitivity ELSE o.c.expected_sensitivity END AS expected_sensitivity,
 CASE WHEN n.c.asset_key IS NOT NULL THEN n.c.invalid_rule_label_count ELSE o.c.invalid_rule_label_count END AS invalid_rule_label_count,
 n.c.asset_key IS NOT NULL AS observed,
 coalesce(o.c.first_seen_at,v_now) AS first_seen_at,
 CASE WHEN n.c.asset_key IS NOT NULL THEN v_now ELSE o.c.last_seen_at END AS last_seen_at,
 coalesce(o.c.origin,CASE WHEN v_parent IS NULL THEN \'BASELINE\' ELSE \'NEW\' END) AS origin,
 coalesce(o.c.baseline_missing,v_parent IS NULL AND n.c.sensitivity_state=\'Missing sensitivity tag\') AS baseline_missing,
 coalesce(o.c.ever_tagged,false) OR coalesce(n.c.sensitivity_state<>\'Missing sensitivity tag\',false) AS ever_tagged,
 least(o.c.protection_floor,CASE WHEN n.c.sensitivity_state=\'Recognized sensitivity value\' THEN n.c.actual_priority END) AS protection_floor,
 o.c.baseline_signature AS old_baseline_signature
 FROM observations n FULL OUTER JOIN old_columns o ON n.c.asset_key=o.c.asset_key
),
column_fingerprints AS (
 SELECT c.*,sha2(to_json(named_struct(\'state\',sensitivity_state,\'assigned\',current_sensitivity,
 \'required\',expected_priority,\'mapping_errors\',invalid_rule_label_count)),256) AS fingerprint
 FROM column_inputs c
),
columns_state AS (
 SELECT asset_key,catalog_name,schema_name,table_name,column_name,column_fqn,data_type,remediation_owner,current_sensitivity,sensitivity_state,actual_priority,expected_priority,expected_sensitivity,invalid_rule_label_count,observed,first_seen_at,last_seen_at,origin,baseline_missing,ever_tagged,protection_floor,
 CASE WHEN v_parent IS NULL THEN fingerprint
 WHEN NOT observed OR old_baseline_signature=fingerprint THEN old_baseline_signature
 ELSE NULL END AS baseline_signature FROM column_fingerprints
),
raw_candidates AS (
 SELECT asset_key,\'NEW_COLUMN_UNTAGGED\' AS finding_type,
 CASE WHEN expected_priority IS NULL THEN \'P2\' ELSE \'P1\' END AS priority,
 expected_priority AS required_priority,\'Newly observed column needs a reviewed sensitivity classification.\' AS detail,
 true AS grace,false AS evidence_hold
 FROM columns_state WHERE observed AND sensitivity_state=\'Missing sensitivity tag\' AND origin=\'NEW\' AND NOT ever_tagged
 UNION ALL
 SELECT asset_key,\'SENSITIVITY_TAG_REMOVED\',\'P1\',expected_priority,
 \'Previously observed sensitivity assignment is absent.\',false,false
 FROM columns_state WHERE observed AND sensitivity_state=\'Missing sensitivity tag\' AND ever_tagged
 UNION ALL
 SELECT asset_key,\'SENSITIVITY_DOWNGRADED\',\'P1\',protection_floor,
 \'Assigned tier is weaker than the strongest previously observed valid tier; steward review is required.\',false,false
 FROM columns_state WHERE observed AND sensitivity_state=\'Recognized sensitivity value\' AND actual_priority>protection_floor
 UNION ALL
 SELECT asset_key,\'MAPPED_TIER_MISSING\',\'P1\',expected_priority,
 \'Mapped source evidence requires a sensitivity assignment.\',false,false
 FROM columns_state WHERE observed AND sensitivity_state=\'Missing sensitivity tag\' AND expected_priority IS NOT NULL
 AND origin=\'BASELINE\' AND NOT ever_tagged
 UNION ALL
 SELECT asset_key,\'MAPPED_TIER_TOO_WEAK\',\'P1\',expected_priority,
 \'Assigned sensitivity is weaker than the mapped source requirement.\',false,false
 FROM columns_state WHERE observed AND sensitivity_state=\'Recognized sensitivity value\' AND actual_priority>expected_priority
 UNION ALL
 SELECT asset_key,\'CONFLICTING_SENSITIVITY_VALUES\',\'P1\',cast(NULL AS INT),
 \'Resolve multiple sensitivity values against the approved classification.\',false,false
 FROM columns_state WHERE observed AND sensitivity_state=\'Conflicting sensitivity values\'
 UNION ALL
 SELECT asset_key,\'INVALID_SENSITIVITY_VALUE\',\'P1\',cast(NULL AS INT),
 \'Sensitivity value is outside the configured taxonomy.\',false,false
 FROM columns_state WHERE observed AND sensitivity_state=\'Value outside configured taxonomy\'
 UNION ALL
 SELECT asset_key,\'SOURCE_MAPPING_INCONSISTENT\',\'P1\',expected_priority,
 \'Matching source classification rules are inconsistent or conflicting.\',false,false
 FROM columns_state WHERE observed AND invalid_rule_label_count>0
 UNION ALL
 SELECT asset_key,\'COLUMN_NOT_OBSERVED\',\'P2\',cast(NULL AS INT),
 \'Previously observed column is absent; check deletion or reduced visibility. Earlier incidents remain open.\',false,false
 FROM columns_state WHERE NOT observed
 UNION ALL
 SELECT c.asset_key,p.i.finding_type,p.i.priority,p.i.required_priority,
 \'Column evidence is unavailable; prior incident is retained for review.\',false,true
 FROM columns_state c JOIN prior_issues p ON c.asset_key=p.i.asset_key
 WHERE NOT c.observed AND p.i.status<>\'RESOLVED\' AND p.i.finding_type<>\'COLUMN_NOT_OBSERVED\'
 UNION ALL
 SELECT c.asset_key,p.i.finding_type,p.i.priority,p.i.required_priority,
 \'Mapped evidence disappeared; the earlier requirement is retained until classification or evidence is restored.\',false,true
 FROM columns_state c JOIN prior_issues p ON c.asset_key=p.i.asset_key
 WHERE c.observed AND c.expected_priority IS NULL AND p.i.status<>\'RESOLVED\'
 AND p.i.finding_type IN (\'MAPPED_TIER_MISSING\',\'MAPPED_TIER_TOO_WEAK\')
 AND NOT coalesce(c.sensitivity_state=\'Recognized sensitivity value\' AND c.actual_priority<=p.i.required_priority,false)
),
candidates AS (
 SELECT r.*,sha2(to_json(array(r.asset_key,r.finding_type)),256) AS issue_key,
 CASE WHEN r.grace THEN timestampadd(MINUTE,{{GRACE}},c.first_seen_at) ELSE v_now END AS eligible_at
 FROM raw_candidates r JOIN columns_state c ON r.asset_key=c.asset_key
 WHERE c.baseline_signature IS NULL OR NOT c.observed
),
issue_changes AS (
 SELECT coalesce(n.issue_key,p.i.issue_key) AS issue_key,coalesce(n.asset_key,p.i.asset_key) AS asset_key,
 coalesce(n.finding_type,p.i.finding_type) AS finding_type,coalesce(n.priority,p.i.priority) AS priority,
 CASE WHEN n.issue_key IS NOT NULL THEN n.required_priority ELSE p.i.required_priority END AS required_priority,
 CASE WHEN n.issue_key IS NOT NULL THEN n.detail ELSE p.i.detail END AS detail,
 CASE WHEN n.issue_key IS NOT NULL AND (p.i.issue_key IS NULL OR p.i.status=\'RESOLVED\')
 THEN coalesce(p.i.generation,0)+1 ELSE p.i.generation END AS generation,
 CASE WHEN n.issue_key IS NOT NULL AND (p.i.issue_key IS NULL OR p.i.status=\'RESOLVED\') THEN v_now ELSE p.i.opened_at END AS opened_at,
 CASE WHEN n.issue_key IS NULL THEN coalesce(p.i.resolved_at,v_now) END AS resolved_at,
 CASE WHEN n.issue_key IS NULL THEN \'RESOLVED\' WHEN n.evidence_hold THEN \'EVIDENCE_GAP\'
 WHEN n.grace AND v_now<n.eligible_at THEN \'GRACE\' ELSE \'OPEN\' END AS status,
 CASE WHEN n.issue_key IS NULL THEN p.i.eligible_at WHEN n.grace THEN n.eligible_at
 ELSE CASE WHEN p.i.status IN (\'OPEN\',\'EVIDENCE_GAP\') THEN p.i.eligible_at ELSE v_now END END AS eligible_at,
 CASE WHEN n.issue_key IS NOT NULL OR p.i.status<>\'RESOLVED\' THEN v_now ELSE p.i.updated_at END AS updated_at,
 CASE WHEN n.issue_key IS NOT NULL AND p.i.status=\'RESOLVED\' THEN false ELSE coalesce(p.i.ever_announced,false) END AS prior_announced,
 CASE WHEN n.issue_key IS NOT NULL AND p.i.status=\'RESOLVED\' THEN cast(NULL AS TIMESTAMP) ELSE p.i.last_accepted_at END AS prior_accepted,
 CASE WHEN n.issue_key IS NOT NULL AND p.i.status=\'RESOLVED\' THEN cast(NULL AS TIMESTAMP) ELSE p.i.recovery_accepted_at END AS prior_recovery
 FROM candidates n FULL OUTER JOIN prior_issues p ON n.issue_key=p.i.issue_key
),
issues_state AS (
 SELECT i.issue_key,i.asset_key,i.finding_type,i.priority,i.required_priority,i.detail,i.generation,
 c.catalog_name,c.schema_name,c.table_name,c.column_name,c.column_fqn,c.remediation_owner AS owner,
 c.current_sensitivity,c.expected_sensitivity,i.status,i.opened_at,i.updated_at,i.resolved_at,i.eligible_at,
 i.prior_announced OR a.announced_at IS NOT NULL AS ever_announced,
 greatest(i.prior_accepted,a.announced_at) AS last_accepted_at,
 greatest(i.prior_recovery,a.recovered_at) AS recovery_accepted_at
 FROM issue_changes i JOIN columns_state c ON i.asset_key=c.asset_key
 LEFT JOIN accepted a ON i.issue_key=a.issue_key AND i.generation=a.generation
),
carried_notices AS (
 SELECT p.n.notice_id AS notice_id,p.n.issue_key AS issue_key,p.n.asset_key AS asset_key,p.n.generation AS generation,p.n.finding_type AS finding_type,p.n.priority AS priority,p.n.catalog_name AS catalog_name,p.n.schema_name AS schema_name,p.n.table_name AS table_name,p.n.column_name AS column_name,p.n.column_fqn AS column_fqn,p.n.owner AS owner,p.n.current_sensitivity AS current_sensitivity,p.n.expected_sensitivity AS expected_sensitivity,p.n.required_priority AS required_priority,p.n.detail AS detail,p.n.kind AS kind,p.n.created_at AS created_at,p.n.selected_for_batch AS selected_for_batch FROM prior_notices p LEFT JOIN issues_state i ON p.n.issue_key=i.issue_key AND p.n.generation=i.generation
 WHERE NOT EXISTS (SELECT 1 FROM {{JOURNAL}} a WHERE a.monitor_id=\'{{MONITOR_ID}}\' AND a.record_type=\'ACK\' AND a.record_id=p.n.notice_id)
 AND (p.n.kind IN (\'SUMMARY\',\'ACTIVATION\') OR (i.issue_key IS NOT NULL AND (i.status<>\'RESOLVED\' OR p.n.kind=\'RECOVERY\')))
),
new_notice_kinds AS (
 SELECT i.*,CASE WHEN status=\'RESOLVED\' AND ever_announced AND recovery_accepted_at IS NULL THEN \'RECOVERY\'
 WHEN status IN (\'OPEN\',\'EVIDENCE_GAP\') AND NOT ever_announced THEN \'NEW\'
 WHEN status IN (\'OPEN\',\'EVIDENCE_GAP\') AND last_accepted_at<=timestampadd(HOUR,-{{REMINDER_HOURS}},v_now) THEN \'REMINDER\'
 END AS kind
 FROM issues_state i
 WHERE NOT EXISTS (SELECT 1 FROM carried_notices n WHERE n.issue_key=i.issue_key AND n.generation=i.generation)
),
new_notices AS (
 SELECT sha2(to_json(named_struct(\'issue\',issue_key,\'generation\',generation,\'kind\',kind,\'last_accepted_at\',last_accepted_at)),256) AS notice_id,
 issue_key,asset_key,generation,finding_type,priority,catalog_name,schema_name,table_name,column_name,column_fqn,owner,current_sensitivity,expected_sensitivity,required_priority,detail,kind,v_now AS created_at,false AS selected_for_batch
 FROM new_notice_kinds WHERE kind IS NOT NULL
),
activation_notice AS (
 SELECT sha2(concat(\'{{MONITOR_ID}}\',\':ACTIVATION\'),256) AS notice_id,
 \'ACTIVATION\' AS issue_key,\'ACTIVATION\' AS asset_key,1 AS generation,\'MONITOR_ACTIVATED\' AS finding_type,\'P3\' AS priority,
 \'(configured scope)\' AS catalog_name,\'(summary)\' AS schema_name,\'(summary)\' AS table_name,
 concat(cast(count_if(observed) AS STRING),\' visible columns; \',cast(count_if(observed AND sensitivity_state=\'Missing sensitivity tag\') AS STRING),\' missing sensitivity tags\') AS column_name,
 \'Classification monitor activation summary\' AS column_fqn,\'Governance team\' AS owner,
 cast(NULL AS STRING) AS current_sensitivity,cast(NULL AS STRING) AS expected_sensitivity,cast(NULL AS INT) AS required_priority,
 \'The monitor has been initialized. Existing classification backlog is summarized, not emitted as per-column incidents.\' AS detail,
 \'ACTIVATION\' AS kind,v_now AS created_at,false AS selected_for_batch
 FROM columns_state HAVING {{INITIAL_ENABLED}} AND NOT coalesce(v_activation_enqueued,false)
),
summary_notice AS (
 SELECT sha2(concat(\'{{MONITOR_ID}}\',\':SUMMARY:\',cast(cast(v_now AS DATE) AS STRING)),256) AS notice_id,
 \'SUMMARY\' AS issue_key,\'SUMMARY\' AS asset_key,1 AS generation,\'COVERAGE_SUMMARY\' AS finding_type,\'P3\' AS priority,
 \'(configured scope)\' AS catalog_name,\'(summary)\' AS schema_name,\'(summary)\' AS table_name,
 concat(cast(count_if(observed AND sensitivity_state=\'Missing sensitivity tag\') AS STRING),\' of \',cast(count_if(observed) AS STRING),\' visible columns missing sensitivity tags\') AS column_name,
 \'Environment coverage summary\' AS column_fqn,\'Governance team\' AS owner,
 cast(NULL AS STRING) AS current_sensitivity,cast(NULL AS STRING) AS expected_sensitivity,cast(NULL AS INT) AS required_priority,
 \'Coverage backlog is summarized; this is not one incident per baseline column.\' AS detail,\'SUMMARY\' AS kind,v_now AS created_at,false AS selected_for_batch
 FROM columns_state
 HAVING {{DAILY_ENABLED}} AND v_parent IS NOT NULL AND hour(v_now)>={{DAILY_HOUR}}
 AND NOT EXISTS (SELECT 1 FROM carried_notices WHERE kind IN (\'SUMMARY\',\'ACTIVATION\'))
 AND NOT EXISTS (SELECT 1 FROM activation_notice)
 AND NOT EXISTS (SELECT 1 FROM {{JOURNAL}} n WHERE n.monitor_id=\'{{MONITOR_ID}}\' AND n.record_type=\'NOTICE\'
 AND n.record_id=sha2(concat(\'{{MONITOR_ID}}\',\':SUMMARY:\',cast(cast(v_now AS DATE) AS STRING)),256))
),
pending_unranked AS (
 SELECT notice_id,issue_key,asset_key,generation,finding_type,priority,catalog_name,schema_name,table_name,column_name,column_fqn,owner,current_sensitivity,expected_sensitivity,required_priority,detail,kind,created_at FROM carried_notices
 UNION ALL SELECT notice_id,issue_key,asset_key,generation,finding_type,priority,catalog_name,schema_name,table_name,column_name,column_fqn,owner,current_sensitivity,expected_sensitivity,required_priority,detail,kind,created_at FROM new_notices
 UNION ALL SELECT notice_id,issue_key,asset_key,generation,finding_type,priority,catalog_name,schema_name,table_name,column_name,column_fqn,owner,current_sensitivity,expected_sensitivity,required_priority,detail,kind,created_at FROM summary_notice
 UNION ALL SELECT notice_id,issue_key,asset_key,generation,finding_type,priority,catalog_name,schema_name,table_name,column_name,column_fqn,owner,current_sensitivity,expected_sensitivity,required_priority,detail,kind,created_at FROM activation_notice
),
pending_ranked AS (
 SELECT p.*,row_number() OVER (ORDER BY priority,created_at,notice_id) AS notification_rank FROM pending_unranked p
),
notices_state AS (
 SELECT notice_id,issue_key,asset_key,generation,finding_type,priority,catalog_name,schema_name,table_name,column_name,column_fqn,owner,current_sensitivity,expected_sensitivity,required_priority,detail,kind,created_at,notification_rank<={{BATCH_SIZE}} AS selected_for_batch FROM pending_ranked
),
run_summary AS (
 SELECT count_if(observed) AS observed_columns,count(*) AS known_columns,
 count_if(observed AND sensitivity_state=\'Missing sensitivity tag\') AS missing_tag_columns,
 count_if(observed AND baseline_missing AND NOT ever_tagged AND sensitivity_state=\'Missing sensitivity tag\') AS baseline_backlog,
 (SELECT coalesce(count_if(status IN (\'OPEN\',\'EVIDENCE_GAP\')),0) FROM issues_state) AS open_incidents,
 (SELECT coalesce(count_if(status=\'GRACE\'),0) FROM issues_state) AS grace_incidents,
 (SELECT coalesce(count_if(status=\'OPEN\' AND finding_type=\'NEW_COLUMN_UNTAGGED\'),0) FROM issues_state) AS new_untagged_open,
 (SELECT count(*) FROM notices_state) AS pending_notifications,
 (SELECT coalesce(count_if(selected_for_batch),0) FROM notices_state) AS selected_notifications,
 v_parent IS NULL AS baseline_run,v_now AS observed_at FROM columns_state
),
records AS (
 SELECT asset_key AS record_id,\'COLUMN\' AS record_type,to_json(named_struct(\'asset_key\',asset_key,\'catalog_name\',catalog_name,\'schema_name\',schema_name,\'table_name\',table_name,\'column_name\',column_name,\'column_fqn\',column_fqn,\'data_type\',data_type,\'remediation_owner\',remediation_owner,\'current_sensitivity\',current_sensitivity,\'sensitivity_state\',sensitivity_state,\'actual_priority\',actual_priority,\'expected_priority\',expected_priority,\'expected_sensitivity\',expected_sensitivity,\'invalid_rule_label_count\',invalid_rule_label_count,\'observed\',observed,\'first_seen_at\',first_seen_at,\'last_seen_at\',last_seen_at,\'origin\',origin,\'baseline_missing\',baseline_missing,\'ever_tagged\',ever_tagged,\'protection_floor\',protection_floor,\'baseline_signature\',baseline_signature)) AS payload_json FROM columns_state
 UNION ALL SELECT issue_key,\'ISSUE\',to_json(named_struct(\'issue_key\',issue_key,\'asset_key\',asset_key,\'generation\',generation,\'finding_type\',finding_type,\'priority\',priority,\'catalog_name\',catalog_name,\'schema_name\',schema_name,\'table_name\',table_name,\'column_name\',column_name,\'column_fqn\',column_fqn,\'owner\',owner,\'current_sensitivity\',current_sensitivity,\'expected_sensitivity\',expected_sensitivity,\'required_priority\',required_priority,\'detail\',detail,\'status\',status,\'opened_at\',opened_at,\'updated_at\',updated_at,\'resolved_at\',resolved_at,\'eligible_at\',eligible_at,\'ever_announced\',ever_announced,\'last_accepted_at\',last_accepted_at,\'recovery_accepted_at\',recovery_accepted_at)) FROM issues_state
 UNION ALL SELECT notice_id,\'NOTICE\',to_json(named_struct(\'notice_id\',notice_id,\'issue_key\',issue_key,\'asset_key\',asset_key,\'generation\',generation,\'finding_type\',finding_type,\'priority\',priority,\'catalog_name\',catalog_name,\'schema_name\',schema_name,\'table_name\',table_name,\'column_name\',column_name,\'column_fqn\',column_fqn,\'owner\',owner,\'current_sensitivity\',current_sensitivity,\'expected_sensitivity\',expected_sensitivity,\'required_priority\',required_priority,\'detail\',detail,\'kind\',kind,\'created_at\',created_at,\'selected_for_batch\',selected_for_batch)) FROM notices_state
 UNION ALL SELECT v_run_id,\'RUN\',to_json(named_struct(\'control\',named_struct(
 \'version\',\'SQL_V1\',\'environment\',\'{{ENVIRONMENT}}\',\'identity_hash\',v_identity,\'run_token\',p_run_token,
 \'baseline_at\',coalesce(v_baseline_at,v_now),\'activation_enqueued\',
 coalesce(v_activation_enqueued,false) OR EXISTS (SELECT 1 FROM activation_notice)),
 \'summary\',named_struct(\'observed_columns\',observed_columns,\'known_columns\',known_columns,\'missing_tag_columns\',missing_tag_columns,\'baseline_backlog\',baseline_backlog,\'open_incidents\',open_incidents,\'grace_incidents\',grace_incidents,\'new_untagged_open\',new_untagged_open,\'pending_notifications\',pending_notifications,\'selected_notifications\',selected_notifications,\'baseline_run\',baseline_run,\'observed_at\',observed_at))) FROM run_summary
)
SELECT \'{{MONITOR_ID}}\' AS monitor_id,v_run_id AS run_id,record_id,record_type,v_now AS observed_at,payload_json FROM records

  ) incoming ON target.monitor_id=incoming.monitor_id AND target.run_id=incoming.run_id
   AND target.record_type=incoming.record_type AND target.record_id=incoming.record_id
  WHEN NOT MATCHED THEN INSERT *;
  DELETE FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND run_id<>v_run_id
   AND observed_at<timestampadd(DAY,-{{RETENTION_DAYS}},v_now);
 END IF;
 SELECT run_id,observed_at,get_json_object(payload_json,\'$.summary\') AS summary
 FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'RUN\' AND run_id=v_run_id;
END',
'ack','CREATE OR REPLACE PROCEDURE {{PREFIX}}.raw_classification_ack_sql(IN p_run_token STRING)
 LANGUAGE SQL SQL SECURITY INVOKER MODIFIES SQL DATA COMMENT \'IFBI_SQL_MONITOR_V1\'
 AS BEGIN
 DECLARE v_run_id STRING;
 DECLARE v_check BOOLEAN;
 SET v_run_id=sha2(concat(\'{{MONITOR_ID}}\',\':\',p_run_token),256);
 SET v_check=assert_true(EXISTS (SELECT 1 FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'RUN\' AND run_id=v_run_id),
  \'Cannot acknowledge a run that has not been published\');
 SET v_check=assert_true(v_run_id=(SELECT run_id FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'RUN\'
  ORDER BY observed_at DESC,run_id DESC LIMIT 1),\'Do not acknowledge an older run after a newer snapshot was published\');
 -- Run only as the success-dependent task AFTER native SQL alert evaluation.
 -- A receipt attests successful evaluation, not delivery or recipient reading.
 MERGE INTO {{JOURNAL}} target USING (
  SELECT \'{{MONITOR_ID}}\' AS monitor_id,v_run_id AS run_id,record_id,\'ACK\' AS record_type,current_timestamp() AS observed_at,
   to_json(named_struct(\'notice_id\',record_id,\'issue_key\',get_json_object(payload_json,\'$.issue_key\'),
    \'generation\',cast(get_json_object(payload_json,\'$.generation\') AS INT),\'kind\',get_json_object(payload_json,\'$.kind\'),
    \'source_run_id\',v_run_id,\'meaning\',\'SQL alert evaluation accepted; delivery is not attested\')) AS payload_json
  FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND run_id=v_run_id AND record_type=\'NOTICE\'
   AND get_json_object(payload_json,\'$.selected_for_batch\')=\'true\'
 ) incoming ON target.monitor_id=incoming.monitor_id AND target.record_type=\'ACK\' AND target.record_id=incoming.record_id
 WHEN NOT MATCHED THEN INSERT *;
END',
'monitor_health','SELECT CASE WHEN b.attention_required=1 OR EXISTS (SELECT 1 FROM {{JOURNAL}} a WHERE a.monitor_id=\'{{MONITOR_ID}}\' AND a.record_type=\'ATTEMPT\'
      AND a.observed_at>b.last_success_at AND a.observed_at<current_timestamp()-INTERVAL 20 MINUTES
      AND NOT EXISTS (SELECT 1 FROM {{JOURNAL}} r WHERE r.monitor_id=a.monitor_id AND r.run_id=a.run_id AND r.record_type=\'RUN\')) THEN 1 ELSE 0 END AS attention_required,
CASE WHEN EXISTS (SELECT 1 FROM {{JOURNAL}} a WHERE a.monitor_id=\'{{MONITOR_ID}}\' AND a.record_type=\'ATTEMPT\'
      AND a.observed_at>b.last_success_at AND a.observed_at<current_timestamp()-INTERVAL 20 MINUTES
      AND NOT EXISTS (SELECT 1 FROM {{JOURNAL}} r WHERE r.monitor_id=a.monitor_id AND r.run_id=a.run_id AND r.record_type=\'RUN\')) THEN \'Monitor capture failed or is unfinished\' ELSE b.monitor_status END AS monitor_status,
b.* EXCEPT(attention_required,monitor_status) FROM (WITH published AS (
     SELECT run_id,observed_at,payload_json,
     row_number() OVER (ORDER BY observed_at DESC,run_id DESC) AS rn
     FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'RUN\'
    ), latest AS (SELECT * FROM published WHERE rn=1),
    acks AS (
     SELECT get_json_object(payload_json,\'$.notice_id\') AS notice_id,max(observed_at) AS evaluation_accepted_at
     FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'ACK\'
     GROUP BY get_json_object(payload_json,\'$.notice_id\')
    )
    SELECT
     CASE WHEN l.run_id IS NULL THEN 1
     WHEN l.observed_at<current_timestamp()-INTERVAL 135 MINUTES THEN 1
     WHEN (SELECT max(observed_at) FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'FAILURE\')>l.observed_at THEN 1
     ELSE 0 END AS attention_required,
     CASE WHEN l.run_id IS NULL THEN \'Initialize the monitor baseline\'
     WHEN l.observed_at<current_timestamp()-INTERVAL 135 MINUTES THEN \'Scheduled classification check overdue\'
     WHEN (SELECT max(observed_at) FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'FAILURE\')>l.observed_at THEN \'Latest monitor attempt failed\'
     ELSE \'Monitor current\' END AS monitor_status,
     l.observed_at AS last_success_at,
     (SELECT max(observed_at) FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'FAILURE\') AS last_failure_at,
     cast(get_json_object(l.payload_json,\'$.summary.observed_columns\') AS BIGINT) AS last_visible_columns,
     cast(get_json_object(l.payload_json,\'$.summary.missing_tag_columns\') AS BIGINT) AS missing_tag_columns,
     cast(get_json_object(l.payload_json,\'$.summary.baseline_backlog\') AS BIGINT) AS baseline_backlog,
     cast(get_json_object(l.payload_json,\'$.summary.grace_incidents\') AS BIGINT) AS grace_incidents,
     cast(get_json_object(l.payload_json,\'$.summary.open_incidents\') AS BIGINT) AS open_incidents,
     cast(get_json_object(l.payload_json,\'$.summary.pending_notifications\') AS BIGINT) AS pending_notifications,
     current_timestamp() AS refreshed_at
    FROM (SELECT 1 AS anchor) x LEFT JOIN latest l ON true) b',
'monitor_incidents','WITH published AS (
     SELECT run_id,observed_at,payload_json,
     row_number() OVER (ORDER BY observed_at DESC,run_id DESC) AS rn
     FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'RUN\'
    ), latest AS (SELECT * FROM published WHERE rn=1),
    acks AS (
     SELECT get_json_object(payload_json,\'$.notice_id\') AS notice_id,max(observed_at) AS evaluation_accepted_at
     FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'ACK\'
     GROUP BY get_json_object(payload_json,\'$.notice_id\')
    )
    SELECT
     get_json_object(j.payload_json,\'$.issue_key\') AS issue_key,
     get_json_object(j.payload_json,\'$.finding_type\') AS finding_type,
     get_json_object(j.payload_json,\'$.status\') AS incident_status,
     get_json_object(j.payload_json,\'$.priority\') AS priority,
     get_json_object(j.payload_json,\'$.schema_name\') AS schema_name,
     get_json_object(j.payload_json,\'$.table_name\') AS table_name,
     get_json_object(j.payload_json,\'$.column_name\') AS column_name,
     get_json_object(j.payload_json,\'$.owner\') AS responsible_owner,
     get_json_object(j.payload_json,\'$.current_sensitivity\') AS current_sensitivity,
     get_json_object(j.payload_json,\'$.expected_sensitivity\') AS expected_sensitivity,
     cast(get_json_object(j.payload_json,\'$.generation\') AS INT) AS incident_generation,
     cast(get_json_object(j.payload_json,\'$.opened_at\') AS TIMESTAMP) AS opened_at,
     cast(get_json_object(j.payload_json,\'$.eligible_at\') AS TIMESTAMP) AS alert_eligible_at,
     cast(get_json_object(j.payload_json,\'$.last_accepted_at\') AS TIMESTAMP) AS last_evaluation_accepted_at,
     cast(get_json_object(j.payload_json,\'$.resolved_at\') AS TIMESTAMP) AS resolved_at,
     get_json_object(j.payload_json,\'$.detail\') AS action_context
    FROM {{JOURNAL}} j JOIN latest l ON j.run_id=l.run_id
    WHERE j.monitor_id=\'{{MONITOR_ID}}\' AND j.record_type=\'ISSUE\'
    ORDER BY priority,incident_status,opened_at',
'monitor_notifications','WITH published AS (
     SELECT run_id,observed_at,payload_json,
     row_number() OVER (ORDER BY observed_at DESC,run_id DESC) AS rn
     FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'RUN\'
    ), latest AS (SELECT * FROM published WHERE rn=1),
    acks AS (
     SELECT get_json_object(payload_json,\'$.notice_id\') AS notice_id,max(observed_at) AS evaluation_accepted_at
     FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'ACK\'
     GROUP BY get_json_object(payload_json,\'$.notice_id\')
    )
    , last_notice AS (
     SELECT *,row_number() OVER (PARTITION BY record_id ORDER BY observed_at DESC,run_id DESC) AS notice_rank
     FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'NOTICE\'
    )
    SELECT n.record_id AS notice_id,
     get_json_object(n.payload_json,\'$.issue_key\') AS issue_key,
     get_json_object(n.payload_json,\'$.kind\') AS event_kind,
     get_json_object(n.payload_json,\'$.finding_type\') AS finding_type,
     get_json_object(n.payload_json,\'$.priority\') AS priority,
     get_json_object(n.payload_json,\'$.column_fqn\') AS column_fqn,
     get_json_object(n.payload_json,\'$.owner\') AS responsible_owner,
     cast(get_json_object(n.payload_json,\'$.created_at\') AS TIMESTAMP) AS created_at,
     CASE WHEN a.notice_id IS NOT NULL THEN \'Alert evaluation accepted\'
     WHEN p.record_id IS NOT NULL THEN \'Pending alert evaluation\'
     ELSE \'Withdrawn before evaluation or older incident\' END AS notification_status,
     a.evaluation_accepted_at
    FROM last_notice n LEFT JOIN acks a ON n.record_id=a.notice_id
    LEFT JOIN (SELECT j.record_id FROM {{JOURNAL}} j JOIN latest l ON j.run_id=l.run_id
     WHERE j.monitor_id=\'{{MONITOR_ID}}\' AND j.record_type=\'NOTICE\') p ON n.record_id=p.record_id
    WHERE n.notice_rank=1 ORDER BY created_at DESC,notice_id',
'monitor_daily_trends','WITH published AS (
     SELECT run_id,observed_at,payload_json,
     row_number() OVER (ORDER BY observed_at DESC,run_id DESC) AS rn
     FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'RUN\'
    ), latest AS (SELECT * FROM published WHERE rn=1),
    acks AS (
     SELECT get_json_object(payload_json,\'$.notice_id\') AS notice_id,max(observed_at) AS evaluation_accepted_at
     FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'ACK\'
     GROUP BY get_json_object(payload_json,\'$.notice_id\')
    )
    SELECT observed_date,visible_columns,missing_tags,baseline_backlog,new_untagged_open,open_incidents FROM (
     SELECT cast(observed_at AS DATE) AS observed_date,
     cast(get_json_object(payload_json,\'$.summary.observed_columns\') AS BIGINT) AS visible_columns,
     cast(get_json_object(payload_json,\'$.summary.missing_tag_columns\') AS BIGINT) AS missing_tags,
     cast(get_json_object(payload_json,\'$.summary.baseline_backlog\') AS BIGINT) AS baseline_backlog,
     cast(get_json_object(payload_json,\'$.summary.new_untagged_open\') AS BIGINT) AS new_untagged_open,
     cast(get_json_object(payload_json,\'$.summary.open_incidents\') AS BIGINT) AS open_incidents,
     row_number() OVER (PARTITION BY cast(observed_at AS DATE) ORDER BY observed_at DESC,run_id DESC) AS daily_rank
     FROM published
    ) WHERE daily_rank=1 ORDER BY observed_date',
'alert_query','WITH binding AS (
 SELECT CASE WHEN EXISTS (SELECT 1 FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'RUN\' AND run_id=CASE WHEN :monitor_run_token=\'__LATEST__\' THEN (SELECT run_id FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'RUN\' ORDER BY observed_at DESC,run_id DESC LIMIT 1) ELSE sha2(concat(\'{{MONITOR_ID}}\',\':\',:monitor_run_token),256) END AND run_id=(SELECT run_id FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'RUN\' ORDER BY observed_at DESC,run_id DESC LIMIT 1))
 THEN 0 ELSE raise_error(\'Monitor run parameter is not bound to a published batch\') END AS binding_error
), selected AS (
 SELECT get_json_object(payload_json,\'$.notice_id\') AS notice_id,
 get_json_object(payload_json,\'$.kind\') AS event_kind,
 get_json_object(payload_json,\'$.finding_type\') AS finding_type,
 get_json_object(payload_json,\'$.priority\') AS priority,
 get_json_object(payload_json,\'$.catalog_name\') AS catalog_name,
 get_json_object(payload_json,\'$.schema_name\') AS schema_name,
 get_json_object(payload_json,\'$.table_name\') AS table_name,
 get_json_object(payload_json,\'$.column_name\') AS column_name,
 get_json_object(payload_json,\'$.owner\') AS responsible_owner
 FROM {{JOURNAL}} j
 WHERE monitor_id=\'{{MONITOR_ID}}\' AND run_id=CASE WHEN :monitor_run_token=\'__LATEST__\' THEN (SELECT run_id FROM {{JOURNAL}} WHERE monitor_id=\'{{MONITOR_ID}}\' AND record_type=\'RUN\' ORDER BY observed_at DESC,run_id DESC LIMIT 1) ELSE sha2(concat(\'{{MONITOR_ID}}\',\':\',:monitor_run_token),256) END
 AND record_type=\'NOTICE\' AND get_json_object(payload_json,\'$.selected_for_batch\')=\'true\'
 AND NOT EXISTS (SELECT 1 FROM {{JOURNAL}} a WHERE a.monitor_id=j.monitor_id AND a.record_type=\'ACK\'
 AND get_json_object(a.payload_json,\'$.notice_id\')=j.record_id)
), totals AS (SELECT count(*) AS notice_count FROM selected), grouped AS (
 SELECT event_kind,finding_type,priority,catalog_name,schema_name,table_name,responsible_owner,
 count(*) AS affected_events,
 concat_ws(\'; \',slice(sort_array(collect_set(column_name)),1,5)) AS example_columns
 FROM selected GROUP BY event_kind,finding_type,priority,catalog_name,schema_name,table_name,responsible_owner
 ORDER BY priority,affected_events DESC LIMIT 99
)
SELECT t.notice_count+b.binding_error AS notice_count,g.* FROM totals t CROSS JOIN binding b LEFT JOIN grouped g ON true
ORDER BY g.priority,g.affected_events DESC')) DO
  SET v_sql=script_row.script_sql;
  FOR token AS SELECT key AS token_name,value AS replacement FROM explode(v_map) DO
   SET v_sql=replace(v_sql,token.token_name,token.replacement);
  END FOR;
  SET v_check=assert_true(instr(v_sql,'{{')=0,'Unbound monitor template token');
  SET monitor_rendered=map_concat(monitor_rendered,map(script_row.script_name,v_sql));
 END FOR;
 SET v_sql=element_at(monitor_rendered,'collect');
 EXECUTE IMMEDIATE v_sql;
 SET v_sql=element_at(monitor_rendered,'ack');
 EXECUTE IMMEDIATE v_sql;
END;

-- COMMAND ----------
-- CALL cannot run inside EXECUTE IMMEDIATE. Select the configured namespace
-- in separate statements, then invoke the procedure as a direct SQL command.
DECLARE OR REPLACE VARIABLE monitor_previous_catalog STRING DEFAULT current_catalog();
DECLARE OR REPLACE VARIABLE monitor_previous_schema STRING DEFAULT current_schema();
DECLARE OR REPLACE VARIABLE monitor_target_catalog STRING DEFAULT monitor_cfg.output_catalog;
DECLARE OR REPLACE VARIABLE monitor_target_schema STRING DEFAULT monitor_cfg.output_schema;
USE CATALOG IDENTIFIER(monitor_target_catalog);
USE SCHEMA IDENTIFIER(monitor_target_schema);

-- COMMAND ----------
-- Every manual run collects a fresh snapshot; the first establishes a baseline.
CALL raw_classification_collect_sql(concat('manual_monitor_',uuid()));

-- COMMAND ----------
USE CATALOG IDENTIFIER(monitor_previous_catalog);
USE SCHEMA IDENTIFIER(monitor_previous_schema);

-- COMMAND ----------
BEGIN
 DECLARE v_sql STRING;
 DECLARE v_exists BIGINT;
 FOR view_row AS SELECT key AS view_suffix,value AS query_sql FROM explode(monitor_rendered) WHERE startswith(key,'monitor_') DO
  SET v_exists=(SELECT count(*) FROM system.information_schema.tables WHERE table_catalog=monitor_cfg.output_catalog
   AND table_schema=monitor_cfg.output_schema AND table_name=concat('raw_classification_',view_row.view_suffix));
  IF v_exists=0 THEN
   SET v_sql=concat('CREATE VIEW ',monitor_prefix,'.raw_classification_',view_row.view_suffix,' COMMENT ',char(39),'IFBI_SQL_MONITOR_V1',char(39),' AS ',view_row.query_sql);
  ELSE
   SET v_sql=concat('ALTER VIEW ',monitor_prefix,'.raw_classification_',view_row.view_suffix,' AS ',view_row.query_sql);
  END IF;
  EXECUTE IMMEDIATE v_sql;
 END FOR;
END;

-- COMMAND ----------
-- This is the result to review. No alert or schedule has been activated.
SELECT CASE WHEN get_json_object(payload_json,'$.summary.baseline_run')='true' THEN 'BASELINE_READY' ELSE 'MONITOR_READY' END AS result,
 'C000926@infarmbureau.com' AS requested_email,
 monitor_cfg.environment AS environment,get_json_object(payload_json,'$.control.run_token') AS run_token,
 cast(get_json_object(payload_json,'$.summary.observed_columns') AS BIGINT) AS visible_columns,
 cast(get_json_object(payload_json,'$.summary.missing_tag_columns') AS BIGINT) AS missing_sensitivity_columns,
 cast(get_json_object(payload_json,'$.summary.baseline_backlog') AS BIGINT) AS baseline_backlog,
 cast(get_json_object(payload_json,'$.summary.open_incidents') AS BIGINT) AS open_incidents,
 cast(get_json_object(payload_json,'$.summary.pending_notifications') AS BIGINT) AS pending_notifications,
 observed_at AS last_success_at
FROM IDENTIFIER(monitor_journal) WHERE monitor_id=monitor_name AND record_type='RUN'
ORDER BY observed_at DESC,run_id DESC LIMIT 1;

-- COMMAND ----------
-- This is the ACTUAL monitor alert query; the paired native alert uses it.
SELECT element_at(monitor_rendered,'alert_query') AS change_digest_alert_query;
