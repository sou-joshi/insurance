-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Step 1 — SQL warehouse inventory and reconciliation views
-- MAGIC Attach this **SQL** notebook to the existing approved **Pro SQL warehouse**.
-- MAGIC This step creates eight metadata views. It reads catalog metadata, the existing
-- MAGIC classification mapping and the Collate glossary assignment feed.
-- MAGIC It does not create a Spark cluster, scan customer values, change classifications,
-- MAGIC create incidents, install alerts or change dashboard sharing.
-- MAGIC DEV bindings are supplied in the next cell. For other environments, edit only that JSON.
-- MAGIC Run all. A failed preflight stops before view deployment. A final `VIEWS_READY` summary
-- MAGIC means this step succeeded; it does not mean monitoring or enforcement has been tested.

-- COMMAND ----------

-- Configuration: existing IFBI DEV bindings. No warehouse ID, cluster ID, token or email needed.
DECLARE OR REPLACE VARIABLE gov_cfg DEFAULT from_json(
'{
  "environment": "DEV",
  "output_catalog": "ifbi_edw_dev_raw",
  "output_schema": "governance_observability",
  "scopes": [
    {
      "catalog": "ifbi_edw_dev_raw",
      "excluded_schemas": [
        "information_schema",
        "governance",
        "governance_observability"
      ],
      "mapping_table": "ifbi_edw_dev_raw.governance.classification_mapping",
      "collate_assignment_table": "ifbi_edw_dev_raw.collate.collate_glossary_asset_assignment",
      "collate_service": "co_ws_ifbi_edw_dev"
    }
  ],
  "sensitivity_tag": "ifbi_sensitivity",
  "legacy_pii_tag": "pii_class",
  "source_classification_tag_keys": [
    "auto_class"
  ],
  "native_tag_prefix": "class.",
  "taxonomy": [
    {
      "classification": "Confidential Critical",
      "pii_class": "critical",
      "priority": 1
    },
    {
      "classification": "Confidential Restricted",
      "pii_class": "restricted",
      "priority": 2
    },
    {
      "classification": "Confidential Sensitive",
      "pii_class": "sensitive",
      "priority": 3
    },
    {
      "classification": "Internal Use Only",
      "pii_class": "internal",
      "priority": 4
    }
  ],
  "collate_freshness_days": 7
}',
'STRUCT<environment:STRING,output_catalog:STRING,output_schema:STRING,scopes:ARRAY<STRUCT<catalog:STRING,excluded_schemas:ARRAY<STRING>,mapping_table:STRING,collate_assignment_table:STRING,collate_service:STRING>>,sensitivity_tag:STRING,legacy_pii_tag:STRING,source_classification_tag_keys:ARRAY<STRING>,native_tag_prefix:STRING,taxonomy:ARRAY<STRUCT<classification:STRING,pii_class:STRING,priority:INT>>,collate_freshness_days:INT>');

-- COMMAND ----------

-- Build and validate queries without changing any source or target object.
DECLARE OR REPLACE VARIABLE gov_queries MAP<STRING,STRING> DEFAULT map();
DECLARE OR REPLACE VARIABLE gov_view_marker STRING DEFAULT '';
DECLARE OR REPLACE VARIABLE gov_preflight_ok BOOLEAN DEFAULT false;

BEGIN
 DECLARE q STRING;
 DECLARE scoped_queries ARRAY<STRING>;
 DECLARE replacements MAP<STRING,STRING>;
 DECLARE exclusions ARRAY<STRING>;
 DECLARE scoped_count BIGINT;
 DECLARE readable_mapping_rows BIGINT;
 DECLARE readable_feed_rows BIGINT;
 DECLARE invalid_existing BIGINT;
 DECLARE taxonomy_values STRING;
 DECLARE source_keys STRING;
 DECLARE quoted_source STRING;
 DECLARE full_query STRING;
 DECLARE sql_text STRING;
 DECLARE output_prefix STRING;
 DECLARE marker STRING;

 SELECT assert_true(gov_cfg IS NOT NULL AND gov_cfg.environment RLIKE '^[A-Za-z0-9_-]+$',
                    'Invalid environment configuration');
 SELECT assert_true(gov_cfg.output_catalog RLIKE '^[A-Za-z_][A-Za-z0-9_]*$'
                    AND gov_cfg.output_schema RLIKE '^[A-Za-z_][A-Za-z0-9_]*$',
                    'Use plain catalog and schema identifiers in configuration');
 SELECT assert_true(size(gov_cfg.scopes) BETWEEN 1 AND 50,
                    'Configure between one and fifty source catalogs');
 SELECT assert_true(size(array_distinct(transform(gov_cfg.scopes,x -> x.catalog)))=size(gov_cfg.scopes),
                    'Source catalogs must be unique');
 SELECT assert_true(gov_cfg.collate_freshness_days BETWEEN 1 AND 365,
                    'Collate freshness must be between one and 365 days');
 SELECT assert_true(gov_cfg.sensitivity_tag IS NOT NULL AND trim(gov_cfg.sensitivity_tag)<>''
                    AND gov_cfg.legacy_pii_tag IS NOT NULL AND gov_cfg.native_tag_prefix IS NOT NULL
                    AND gov_cfg.source_classification_tag_keys IS NOT NULL,
                    'Supply sensitivity, legacy, native-prefix and source-key settings');
 SELECT assert_true(forall(concat(array(gov_cfg.sensitivity_tag,gov_cfg.legacy_pii_tag,gov_cfg.native_tag_prefix),
                    gov_cfg.source_classification_tag_keys,
                    transform(gov_cfg.taxonomy,x -> x.classification),transform(gov_cfg.taxonomy,x -> x.pii_class),
                    transform(gov_cfg.scopes,x -> coalesce(x.collate_service,''))),x -> instr(x,char(92))=0),
                    'Backslashes in configuration labels or service identifiers require a reviewed quoting extension');
 SELECT assert_true(size(gov_cfg.taxonomy)>=2
                    AND size(array_distinct(transform(gov_cfg.taxonomy,x -> x.classification)))=size(gov_cfg.taxonomy)
                    AND size(array_distinct(transform(gov_cfg.taxonomy,x -> x.priority)))=size(gov_cfg.taxonomy)
                    AND forall(gov_cfg.taxonomy,x -> x.classification IS NOT NULL AND trim(x.classification)<>''
                       AND x.pii_class IS NOT NULL AND trim(x.pii_class)<>'' AND x.priority IS NOT NULL AND x.priority>0),
                    'Taxonomy needs unique labels and positive unique priorities');
 SET taxonomy_values = array_join(transform(gov_cfg.taxonomy, x -> concat('(',
     char(39),replace(x.classification,char(39),concat(char(92),char(39))),char(39),',',
     char(39),replace(x.pii_class,char(39),concat(char(92),char(39))),char(39),',',cast(x.priority AS STRING),')')),',');
 SET source_keys = CASE WHEN size(gov_cfg.source_classification_tag_keys)=0 THEN concat(char(92),char(39))
     ELSE array_join(transform(gov_cfg.source_classification_tag_keys,x -> concat(char(39),
          replace(x,char(39),concat(char(92),char(39))),char(39))),',') END;
 SET output_prefix = concat('`',gov_cfg.output_catalog,'`.`',gov_cfg.output_schema,'`');
 SET marker = concat('IFBI_SQL_INVENTORY_V1:',sha2(concat(to_json(gov_cfg),'|',current_metastore()),256));

 FOR ds AS SELECT key AS dataset_name,value AS tail FROM explode(map(
  'column_inventory', 'SELECT * FROM column_results ORDER BY action_priority,coalesce(expected_priority,5),schema_name,table_name,ordinal_position',
  'tag_dictionary', 'SELECT \'{{RAW}}\' AS catalog_name,t.tag_name,t.tag_value,count(*) AS column_assignments,
 count(DISTINCT concat(t.catalog_name,\'.\',t.schema_name,\'.\',t.table_name)) AS table_count,
 count(DISTINCT t.schema_name) AS schema_count,
 CASE WHEN t.tag_name=\'{{SENSITIVITY_TAG}}\' THEN \'Assigned sensitivity tier\'
 WHEN t.tag_name=\'{{LEGACY_PII_TAG}}\' THEN \'Legacy treatment tag\'
 WHEN t.tag_name=\'auto_class\' THEN \'auto_class source label\'
 WHEN t.tag_name LIKE \'{{NATIVE_PREFIX}}%\' THEN \'Native classification tag\'
 WHEN t.tag_name IN (SELECT source_tag_name FROM mapping_raw) THEN \'Mapped source tag key\'
 ELSE \'Other tag\' END AS evidence_kind
FROM tags t GROUP BY t.tag_name,t.tag_value
ORDER BY column_assignments DESC,t.tag_name,t.tag_value',
  'mapping_rule_health', 'SELECT \'{{RAW}}\' AS catalog_name,m.source_tag_name,m.source_tag_value,m.collate_fqn,m.ifbi_classification,
 m.pii_class,m.tier_priority,m.rule_state,m.target_variants,m.key_rows,
 count(DISTINCT c.column_fqn) AS matching_visible_columns
FROM mapping_checked m LEFT JOIN tags t
 ON t.tag_name=m.source_tag_name AND (t.tag_value=m.source_tag_value OR (m.source_tag_value IS NULL AND m.source_tag_name LIKE \'{{NATIVE_PREFIX}}%\'))
LEFT JOIN inventory c ON t.catalog_name=c.catalog_name AND t.schema_name=c.schema_name
 AND t.table_name=c.table_name AND t.column_name=c.column_name
GROUP BY m.source_tag_name,m.source_tag_value,m.collate_fqn,m.ifbi_classification,
 m.pii_class,m.tier_priority,m.rule_state,m.target_variants,m.key_rows
ORDER BY m.tier_priority,m.source_tag_name,m.source_tag_value',
  'unmapped_source_labels', 'SELECT \'{{RAW}}\' AS catalog_name,tag_name,tag_value,
 count(DISTINCT concat(catalog_name,\'.\',schema_name,\'.\',table_name,\'.\',column_name)) AS affected_columns,
 concat_ws(\'; \',sort_array(collect_set(schema_name))) AS affected_schemas,
 \'Review source provenance and add an approved exact-key/value mapping; do not infer tag aliases\' AS recommended_action
FROM source_matches WHERE rule_state IS NULL GROUP BY tag_name,tag_value
ORDER BY affected_columns DESC,tag_name,tag_value',
  'collate_feed_health', 'SELECT \'{{RAW}}\' AS catalog_name,f.*,current_timestamp() AS refreshed_at,
 \'{{COLLATE_SOURCE_DISPLAY}}\' AS source_table,
 \'{{SERVICE}}\' AS expected_service,
 {{FRESH_DAYS}} AS freshness_target_days,
 (SELECT count(*) FROM glossary_ranked) AS source_rows_before_dedup,
 (SELECT count(*) FROM glossary_latest WHERE assignment_id IS NULL OR trim(assignment_id)=\'\') AS missing_assignment_ids,
 (SELECT count(DISTINCT field_fqn) FROM glossary_raw) AS raw_candidate_columns,
 (SELECT count(*) FROM collate_rows WHERE column_name IS NOT NULL) AS identity_matched_rows,
 (SELECT count(*) FROM collate_rows WHERE assignment_state=\'Refresh Collate snapshot\') AS older_raw_assignments,
 (SELECT count(*) FROM collate_rows WHERE reconciliation_state=\'Glossary term has no sensitivity rule\') AS unmapped_glossary_rows,
 CASE WHEN f.raw_candidate_rows=0 THEN \'Connect the Raw classification assignment feed; this glossary feed currently has no Raw candidates\'
 WHEN f.usable_raw_assignments=0 THEN \'Use the assignment exceptions table to resolve identity, confirmation, freshness or mapping issues\'
 ELSE \'Review per-column comparisons and resolve any differences\' END AS recommended_action
FROM collate_feed f',
  'collate_assignment_trace', 'SELECT assignment_id,source_scope,source_service_fqn,asset_fqn,field_fqn,uc_column_fqn,
 coalesce(catalog_name,\'{{RAW}}\') AS catalog_name,schema_name,table_name,column_name,term_fqn,assignment_level,label_state,
 assignment_state,reconciliation_state,term_priority,last_seen_at,
 current_timestamp() AS refreshed_at
FROM collate_rows ORDER BY reconciliation_state,field_fqn,term_fqn',
  'scope_and_refresh', 'SELECT c.schema_name,c.scope_category,c.object_type,c.remediation_owner,
 count(DISTINCT c.table_fqn) AS table_or_view_count,count(*) AS visible_columns,
 current_user() AS metadata_reader,current_timestamp() AS refreshed_at,
 \'{{RAW}}\' AS catalog_name,
 \'{{EXCLUDED_DISPLAY}}\' AS excluded_schemas
FROM column_results c
GROUP BY c.schema_name,c.scope_category,c.object_type,c.remediation_owner
ORDER BY visible_columns DESC',
  'actionable_inventory', 'SELECT * FROM column_results WHERE action_priority IN (\'P1 - Correct configuration\',\'P2 - Steward review\') ORDER BY action_priority,coalesce(expected_priority,999),schema_name,table_name,ordinal_position'
)) DO
  SET scoped_queries = array();
  FOR binding AS SELECT explode(gov_cfg.scopes) AS scope DO
   SELECT assert_true(binding.scope.catalog RLIKE '^[A-Za-z_][A-Za-z0-9_]*$'
                      AND binding.scope.mapping_table RLIKE '^[A-Za-z_][A-Za-z0-9_]*[.][A-Za-z_][A-Za-z0-9_]*[.][A-Za-z_][A-Za-z0-9_]*$',
                      'Invalid source catalog or mapping-table identifier');
   SELECT assert_true(binding.scope.excluded_schemas IS NOT NULL
                      AND forall(binding.scope.excluded_schemas,x -> x RLIKE '^[A-Za-z_][A-Za-z0-9_]*$'),
                      'Provide a list of plain schema identifiers for exclusions');
   SELECT assert_true(binding.scope.collate_assignment_table IS NULL
                      OR (binding.scope.collate_assignment_table RLIKE '^[A-Za-z_][A-Za-z0-9_]*[.][A-Za-z_][A-Za-z0-9_]*[.][A-Za-z_][A-Za-z0-9_]*$'
                          AND binding.scope.collate_service IS NOT NULL AND trim(binding.scope.collate_service)<>''),
                      'Supply a valid Collate assignment table and service, or set both to null');
   SET exclusions = array_sort(array_distinct(concat(binding.scope.excluded_schemas,array('information_schema'),
           CASE WHEN binding.scope.catalog=gov_cfg.output_catalog THEN array(gov_cfg.output_schema) ELSE array() END)));
   SET quoted_source = CASE WHEN binding.scope.collate_assignment_table IS NOT NULL
       THEN concat('`',replace(binding.scope.collate_assignment_table,'.','`.`'),'`')
       ELSE '(SELECT CAST(NULL AS STRING) AS assignment_id,CAST(NULL AS STRING) AS source_scope,CAST(NULL AS STRING) AS source_service_fqn,CAST(NULL AS STRING) AS asset_fqn,CAST(NULL AS STRING) AS field_fqn,CAST(NULL AS STRING) AS term_fqn,CAST(NULL AS STRING) AS assignment_level,CAST(NULL AS STRING) AS label_state,CAST(NULL AS BOOLEAN) AS term_is_deleted,CAST(NULL AS BOOLEAN) AS asset_is_deleted,CAST(NULL AS TIMESTAMP) AS last_seen_at,CAST(NULL AS TIMESTAMP) AS asset_updated_at_ts,CAST(NULL AS STRING) AS last_run_id,CAST(NULL AS STRING) AS source_payload_sha256 WHERE false)' END;
   SET replacements = map(
       '{{RAW}}',binding.scope.catalog,
       '{{SERVICE}}',replace(coalesce(binding.scope.collate_service,''),char(39),concat(char(92),char(39))),
       '{{FRESH_DAYS}}',cast(gov_cfg.collate_freshness_days AS STRING),
       '{{EXCLUSIONS}}',array_join(transform(exclusions,x -> concat(char(39),x,char(39))),','),
       '{{EXCLUDED_DISPLAY}}',array_join(exclusions,', '),
       '{{MAPPING_TABLE}}',concat('`',replace(binding.scope.mapping_table,'.','`.`'),'`'),
       '{{COLLATE_SOURCE}}',quoted_source,
       '{{COLLATE_SOURCE_DISPLAY}}',replace(coalesce(binding.scope.collate_assignment_table,'(source not configured)'),char(39),concat(char(92),char(39))),
       '{{COLLATE_CONFIGURED}}',CASE WHEN binding.scope.collate_assignment_table IS NULL THEN 'false' ELSE 'true' END,
       '{{TAXONOMY_VALUES}}',taxonomy_values,
       '{{SENSITIVITY_TAG}}',replace(gov_cfg.sensitivity_tag,char(39),concat(char(92),char(39))),
       '{{LEGACY_PII_TAG}}',replace(gov_cfg.legacy_pii_tag,char(39),concat(char(92),char(39))),
       '{{SOURCE_KEYS}}',source_keys,
       '{{NATIVE_PREFIX}}',replace(gov_cfg.native_tag_prefix,char(39),concat(char(92),char(39))),
       '{{LOWEST_PRIORITY}}',cast(array_max(transform(gov_cfg.taxonomy,x -> x.priority)) AS STRING));
   SET q = concat('-- Metadata only. Scope, service identity and freshness target are set in config.json.
WITH
settings AS (
 SELECT \'{{RAW}}\' AS raw_catalog, \'{{SERVICE}}\' AS collate_service,
        {{FRESH_DAYS}} AS freshness_days
),
taxonomy AS (
 SELECT * FROM VALUES {{TAXONOMY_VALUES}}
 AS expected_taxonomy(classification,pii_class,priority)
),
inventory AS (
 SELECT c.table_catalog AS catalog_name,c.table_schema AS schema_name,
 c.table_name,c.column_name,c.data_type,c.ordinal_position,
 concat(c.table_catalog,\'.\',c.table_schema,\'.\',c.table_name) AS table_fqn,
 concat(c.table_catalog,\'.\',c.table_schema,\'.\',c.table_name,\'.\',c.column_name) AS column_fqn,
 coalesce(t.table_type,\'Metadata object\') AS object_type,
 coalesce(t.table_owner,\'Assign steward\') AS remediation_owner,
 CASE WHEN c.table_schema=\'collate\' THEN \'Collate integration metadata\'
 ELSE \'Raw catalog objects\' END AS scope_category,
 CASE WHEN c.comment IS NULL OR trim(c.comment)=\'\' THEN \'Description missing\'
 ELSE \'Description present\' END AS description_state
 FROM system.information_schema.columns c
 LEFT JOIN system.information_schema.tables t
 ON c.table_catalog=t.table_catalog AND c.table_schema=t.table_schema AND c.table_name=t.table_name
 WHERE c.table_catalog=\'{{RAW}}\'
 AND c.table_schema NOT IN ({{EXCLUSIONS}})
),
tags AS (
 SELECT DISTINCT t.catalog_name,t.schema_name,t.table_name,t.column_name,t.tag_name,t.tag_value
 FROM system.information_schema.column_tags t
 JOIN inventory c ON t.catalog_name=c.catalog_name AND t.schema_name=c.schema_name
 AND t.table_name=c.table_name AND t.column_name=c.column_name
 WHERE t.catalog_name=\'{{RAW}}\'
),
mapping_raw AS (
 SELECT source_tag_name,source_tag_value,collate_fqn,ifbi_classification,pii_class,tier_priority
 FROM {{MAPPING_TABLE}}
),
source_key_health AS (
 SELECT source_tag_name,source_tag_value,
 count(DISTINCT concat(coalesce(ifbi_classification,\'<NULL>\'),\'|\',coalesce(pii_class,\'<NULL>\'),\'|\',coalesce(cast(tier_priority AS STRING),\'<NULL>\'))) AS target_variants,
 count(*) AS key_rows
 FROM mapping_raw GROUP BY source_tag_name,source_tag_value
),
mapping_checked AS (
 SELECT m.*,k.target_variants,k.key_rows,
 CASE WHEN m.source_tag_name IS NULL OR trim(m.source_tag_name)=\'\' THEN \'Source tag name missing\'
 WHEN x.classification IS NULL THEN \'Classification outside configured taxonomy\'
 WHEN m.pii_class IS NULL OR m.tier_priority IS NULL OR m.pii_class<>x.pii_class OR m.tier_priority<>x.priority THEN \'Tier, pii_class or priority disagree\'
 WHEN m.source_tag_value IS NULL AND m.source_tag_name NOT LIKE \'{{NATIVE_PREFIX}}%\' THEN \'NULL match value requires explicit matching rule\'
 WHEN k.target_variants>1 THEN \'Same source tag maps to conflicting targets\'
 ELSE \'Rule usable\' END AS rule_state
 FROM mapping_raw m
 JOIN source_key_health k ON m.source_tag_name <=> k.source_tag_name AND m.source_tag_value <=> k.source_tag_value
 LEFT JOIN taxonomy x ON m.ifbi_classification=x.classification
),
source_tags AS (
 SELECT t.* FROM tags t
 WHERE t.tag_name<>\'{{SENSITIVITY_TAG}}\'
 AND (t.tag_name IN ({{SOURCE_KEYS}}) OR t.tag_name LIKE \'{{NATIVE_PREFIX}}%\'
 OR t.tag_name IN (SELECT source_tag_name FROM mapping_raw))
),
source_matches AS (
 SELECT t.*,m.ifbi_classification,m.pii_class,m.tier_priority,m.rule_state
 FROM source_tags t LEFT JOIN mapping_checked m
 ON t.tag_name=m.source_tag_name
 AND (t.tag_value=m.source_tag_value OR (m.source_tag_value IS NULL AND m.source_tag_name LIKE \'{{NATIVE_PREFIX}}%\'))
),
source_rollup AS (
 SELECT catalog_name,schema_name,table_name,column_name,
 count(DISTINCT concat(tag_name,\'=\',coalesce(tag_value,\'<NULL>\'))) AS source_label_count,
 count(DISTINCT CASE WHEN rule_state=\'Rule usable\' THEN concat(tag_name,\'=\',coalesce(tag_value,\'<NULL>\')) END) AS mapped_source_label_count,
 count(DISTINCT CASE WHEN rule_state IS NULL THEN concat(tag_name,\'=\',coalesce(tag_value,\'<NULL>\')) END) AS unmapped_source_label_count,
 count(DISTINCT CASE WHEN rule_state IS NOT NULL AND rule_state<>\'Rule usable\' THEN concat(tag_name,\'=\',coalesce(tag_value,\'<NULL>\')) END) AS invalid_rule_label_count,
 min(CASE WHEN rule_state=\'Rule usable\' THEN tier_priority END) AS uc_expected_priority,
 concat_ws(\'; \',sort_array(collect_set(concat(tag_name,\'=\',coalesce(tag_value,\'<NULL>\'))))) AS source_labels,
 concat_ws(\'; \',sort_array(collect_set(CASE WHEN rule_state IS NULL THEN concat(tag_name,\'=\',coalesce(tag_value,\'<NULL>\')) END))) AS unmapped_source_labels
 FROM source_matches GROUP BY catalog_name,schema_name,table_name,column_name
),
tag_rollup AS (
 SELECT t.catalog_name,t.schema_name,t.table_name,t.column_name,
 count(*) AS tag_assignment_count,
 count(DISTINCT CASE WHEN t.tag_name=\'{{SENSITIVITY_TAG}}\' THEN t.tag_value END) AS sensitivity_value_count,
 count_if(t.tag_name=\'{{SENSITIVITY_TAG}}\') AS sensitivity_assignment_count,
 min(CASE WHEN t.tag_name=\'{{SENSITIVITY_TAG}}\' THEN t.tag_value END) AS single_tier_value,
 min(CASE WHEN t.tag_name=\'{{SENSITIVITY_TAG}}\' THEN x.priority END) AS actual_priority,
 concat_ws(\'; \',sort_array(collect_set(CASE WHEN t.tag_name=\'{{SENSITIVITY_TAG}}\' THEN t.tag_value END))) AS sensitivity_values,
 concat_ws(\'; \',sort_array(collect_set(CASE WHEN t.tag_name=\'auto_class\' THEN t.tag_value END))) AS auto_class_values,
 concat_ws(\'; \',sort_array(collect_set(CASE WHEN t.tag_name=\'{{LEGACY_PII_TAG}}\' THEN t.tag_value END))) AS legacy_pii_values,
 count(DISTINCT CASE WHEN t.tag_name=\'{{LEGACY_PII_TAG}}\' THEN t.tag_value END) AS legacy_pii_value_count,
 min(CASE WHEN t.tag_name=\'{{LEGACY_PII_TAG}}\' THEN t.tag_value END) AS single_legacy_pii,
 concat_ws(\'; \',sort_array(collect_set(CASE WHEN t.tag_name LIKE \'{{NATIVE_PREFIX}}%\' THEN t.tag_name END))) AS native_class_tags,
 concat_ws(\'; \',sort_array(collect_set(concat(t.tag_name,\'=\',coalesce(t.tag_value,\'<NULL>\'))))) AS all_tag_evidence
 FROM tags t LEFT JOIN taxonomy x ON t.tag_name=\'{{SENSITIVITY_TAG}}\' AND t.tag_value=x.classification
 GROUP BY t.catalog_name,t.schema_name,t.table_name,t.column_name
),
glossary_ranked AS (
 SELECT a.*,row_number() OVER (
 PARTITION BY source_scope,source_service_fqn,assignment_id
 ORDER BY last_seen_at DESC NULLS LAST,asset_updated_at_ts DESC NULLS LAST,
 last_run_id DESC NULLS LAST,source_payload_sha256 DESC NULLS LAST) AS record_rank
 FROM {{COLLATE_SOURCE}} a
),
glossary_latest AS (
 SELECT * FROM glossary_ranked
 WHERE record_rank=1 OR assignment_id IS NULL OR trim(assignment_id)=\'\'
),
glossary_raw AS (
 SELECT a.*,
 CASE WHEN startswith(a.field_fqn,concat(s.collate_service,\'.{{RAW}}.\'))
 THEN substring(a.field_fqn,length(s.collate_service)+2)
 WHEN a.source_service_fqn=s.collate_service AND startswith(a.field_fqn,\'{{RAW}}.\') THEN a.field_fqn
 ELSE NULL END AS uc_column_fqn,
 CASE WHEN a.assignment_id IS NULL OR trim(a.assignment_id)=\'\' THEN \'Assignment identity missing\'
 WHEN a.source_service_fqn IS NOT NULL AND a.source_service_fqn<>s.collate_service THEN \'Different Collate service\'
 WHEN a.term_is_deleted=true OR a.asset_is_deleted=true THEN \'Deleted term or asset\'
 WHEN a.term_is_deleted IS NULL OR a.asset_is_deleted IS NULL THEN \'Deletion state missing\'
 WHEN a.assignment_level IS NULL OR a.assignment_level<>\'column\' THEN \'Assignment is not column-level\'
 WHEN a.label_state IS NULL OR a.label_state<>\'Confirmed\' THEN \'Steward confirmation required\'
 WHEN a.last_seen_at IS NULL THEN \'Observation timestamp missing\'
 WHEN a.last_seen_at>current_timestamp()+INTERVAL 5 MINUTES THEN \'Observation timestamp is in the future\'
 WHEN a.last_seen_at<current_timestamp()-INTERVAL {{FRESH_DAYS}} DAYS THEN \'Refresh Collate snapshot\'
 ELSE \'Current confirmed assignment\' END AS assignment_state
 FROM glossary_latest a CROSS JOIN settings s
 WHERE instr(lower(coalesce(a.field_fqn,\'\')),\'.{{RAW}}.\')>0
 OR startswith(lower(coalesce(a.field_fqn,\'\')),\'{{RAW}}.\')
 OR instr(lower(coalesce(a.asset_fqn,\'\')),\'.{{RAW}}.\')>0
 OR startswith(lower(coalesce(a.asset_fqn,\'\')),\'{{RAW}}.\')
),
term_targets AS (
 SELECT collate_fqn,
 count(DISTINCT CASE WHEN rule_state=\'Rule usable\' THEN ifbi_classification END) AS valid_target_count,
 min(CASE WHEN rule_state=\'Rule usable\' THEN tier_priority END) AS term_priority,
 count_if(rule_state<>\'Rule usable\') AS unusable_rules
 FROM mapping_checked WHERE collate_fqn IS NOT NULL GROUP BY collate_fqn
),
collate_rows AS (
 SELECT a.assignment_id,a.source_scope,a.source_service_fqn,a.asset_fqn,a.field_fqn,
 a.term_fqn,a.assignment_level,a.label_state,a.last_seen_at,a.assignment_state,
 a.uc_column_fqn,c.catalog_name,c.schema_name,c.table_name,c.column_name,
 m.term_priority,
 CASE WHEN a.assignment_state<>\'Current confirmed assignment\' THEN a.assignment_state
 WHEN a.uc_column_fqn IS NULL THEN \'Column identity format needs mapping\'
 WHEN c.column_name IS NULL THEN \'Column absent from visible Raw inventory\'
 WHEN m.collate_fqn IS NULL THEN \'Glossary term has no sensitivity rule\'
 WHEN m.valid_target_count<>1 OR m.unusable_rules>0 THEN \'Glossary term mapping needs correction\'
 ELSE \'Current mapped column assignment\' END AS reconciliation_state
 FROM glossary_raw a
 LEFT JOIN inventory c ON a.uc_column_fqn=c.column_fqn
 LEFT JOIN term_targets m ON a.term_fqn=m.collate_fqn
),
collate_rollup AS (
 SELECT catalog_name,schema_name,table_name,column_name,
 count(*) AS collate_assignment_rows,
 count_if(reconciliation_state=\'Current mapped column assignment\') AS usable_collate_assignments,
 min(CASE WHEN reconciliation_state=\'Current mapped column assignment\' THEN term_priority END) AS collate_expected_priority,
 concat_ws(\'; \',sort_array(collect_set(term_fqn))) AS collate_terms,
 concat_ws(\'; \',sort_array(collect_set(reconciliation_state))) AS collate_details,
 max(last_seen_at) AS collate_last_seen_at
 FROM collate_rows WHERE column_name IS NOT NULL
 GROUP BY catalog_name,schema_name,table_name,column_name
),
collate_feed AS (
 SELECT (SELECT count(*) FROM glossary_latest) AS glossary_feed_rows,
 (SELECT count(*) FROM glossary_raw) AS raw_candidate_rows,
 (SELECT coalesce(count_if(reconciliation_state=\'Current mapped column assignment\'),0) FROM collate_rows) AS usable_raw_assignments,
 (SELECT max(last_seen_at) FROM glossary_latest) AS latest_feed_observation,
 (SELECT max(last_seen_at) FROM glossary_raw) AS latest_raw_observation,
 CASE WHEN {{COLLATE_CONFIGURED}}=false THEN \'Collate assignment source not configured\'
 WHEN (SELECT count(*) FROM glossary_latest)=0 THEN \'Glossary feed contains no records\'
 WHEN (SELECT count(*) FROM glossary_raw)=0 THEN \'No Raw assignments found in glossary feed\'
 WHEN (SELECT coalesce(count_if(reconciliation_state=\'Current mapped column assignment\'),0) FROM collate_rows)=0 THEN \'Raw glossary assignments need attention\'
 ELSE \'Mapped Raw glossary evidence available\' END AS feed_outcome
),
assembled AS (
 SELECT c.*,coalesce(t.tag_assignment_count,0) AS tag_assignment_count,
 coalesce(t.sensitivity_assignment_count,0) AS sensitivity_assignment_count,
 coalesce(t.sensitivity_value_count,0) AS sensitivity_value_count,
 CASE WHEN coalesce(t.sensitivity_assignment_count,0)=0 THEN \'Missing sensitivity tag\'
 WHEN t.sensitivity_value_count>1 THEN \'Conflicting sensitivity values\'
 WHEN t.actual_priority IS NULL THEN \'Value outside configured taxonomy\'
 ELSE \'Recognized sensitivity value\' END AS sensitivity_state,
 CASE WHEN coalesce(t.sensitivity_assignment_count,0)=0 THEN \'(missing)\'
 ELSE coalesce(t.sensitivity_values,\'(empty value)\') END AS current_sensitivity,
 t.actual_priority,coalesce(t.auto_class_values,\'\') AS auto_class_values,
 coalesce(t.native_class_tags,\'\') AS native_class_tags,
 coalesce(t.legacy_pii_values,\'\') AS legacy_pii_values,
 coalesce(t.all_tag_evidence,\'\') AS all_tag_evidence,
 CASE WHEN coalesce(t.legacy_pii_value_count,0)=0 THEN \'No legacy pii_class tag\'
 WHEN t.legacy_pii_value_count>1 THEN \'Conflicting legacy pii_class values\'
 WHEN x.pii_class IS NULL THEN \'Resolve sensitivity tier before legacy comparison\'
 WHEN t.single_legacy_pii=x.pii_class THEN \'Legacy value agrees\'
 ELSE \'Legacy pii_class disagrees\' END AS legacy_alignment,
 coalesce(u.source_label_count,0) AS source_label_count,
 coalesce(u.mapped_source_label_count,0) AS mapped_source_label_count,
 coalesce(u.unmapped_source_label_count,0) AS unmapped_source_label_count,
 coalesce(u.invalid_rule_label_count,0) AS invalid_rule_label_count,
 coalesce(u.source_labels,\'\') AS source_labels,
 coalesce(u.unmapped_source_labels,\'\') AS unmapped_source_labels,
 u.uc_expected_priority,g.collate_expected_priority,
 coalesce(g.collate_assignment_rows,0) AS collate_assignment_rows,
 coalesce(g.usable_collate_assignments,0) AS usable_collate_assignments,
 coalesce(g.collate_terms,\'\') AS collate_terms,
 coalesce(g.collate_details,f.feed_outcome) AS collate_details,
 g.collate_last_seen_at,
 CASE WHEN u.uc_expected_priority IS NOT NULL AND g.collate_expected_priority IS NOT NULL THEN \'UC source tags + Collate glossary\'
 WHEN u.uc_expected_priority IS NOT NULL THEN \'UC source tags\'
 WHEN g.collate_expected_priority IS NOT NULL THEN \'Collate mapped glossary\'
 ELSE \'No usable source mapping\' END AS reconciliation_basis,
 CASE WHEN u.uc_expected_priority IS NULL THEN g.collate_expected_priority
 WHEN g.collate_expected_priority IS NULL THEN u.uc_expected_priority
 ELSE least(u.uc_expected_priority,g.collate_expected_priority) END AS expected_priority,
 CASE WHEN regexp_like(lower(c.column_name),\'(^|_)(ssn|soc_sec|social_security_number|socialsecuritynumber)($|_)\') THEN \'SSN name hint\'
 WHEN regexp_like(lower(c.column_name),\'(^|_)(email|e_mail|email_address)($|_)\') THEN \'Email name hint\'
 WHEN regexp_like(lower(c.column_name),\'(^|_)(dob|birth_date|date_of_birth)($|_)\') THEN \'Birth date name hint\'
 WHEN regexp_like(lower(c.column_name),\'(^|_)(phone|phone_number|telephone|mobile_number)($|_)\') THEN \'Phone name hint\'
 WHEN regexp_like(lower(c.column_name),\'(^|_)(bank_account|bank_account_number|credit_card|credit_card_number|iban|routing_number)($|_)\') THEN \'Payment identifier name hint\'
 WHEN regexp_like(lower(c.column_name),\'(^|_)(claim_notes|medical_notes|diagnosis|health_data)($|_)\') THEN \'Sensitive free-text or health name hint\'
 ELSE \'No configured name hint\' END AS name_hint
 FROM inventory c
 LEFT JOIN tag_rollup t USING(catalog_name,schema_name,table_name,column_name)
 LEFT JOIN source_rollup u USING(catalog_name,schema_name,table_name,column_name)
 LEFT JOIN collate_rollup g USING(catalog_name,schema_name,table_name,column_name)
 LEFT JOIN taxonomy x ON t.sensitivity_value_count=1 AND t.single_tier_value=x.classification
 CROSS JOIN collate_feed f
),
classified AS (
 SELECT a.*,coalesce(x.classification,\'(no mapped requirement)\') AS expected_sensitivity,
 CASE WHEN sensitivity_state<>\'Recognized sensitivity value\' THEN sensitivity_state
 WHEN invalid_rule_label_count>0 THEN \'Source mapping rule needs correction\'
 WHEN expected_priority IS NULL AND unmapped_source_label_count>0 THEN \'Source labels need mapping rules\'
 WHEN expected_priority IS NULL THEN \'Sensitivity assigned; source mapping absent\'
 WHEN actual_priority>expected_priority THEN \'Assigned tier below mapped requirement\'
 WHEN actual_priority<expected_priority THEN \'Assigned tier above mapped requirement\'
 WHEN unmapped_source_label_count>0 THEN \'Mapped tier agrees; additional labels need rules\'
 ELSE \'Assigned tier agrees with mapped evidence\' END AS reconciliation_outcome,
 CASE WHEN uc_expected_priority IS NOT NULL AND collate_expected_priority IS NOT NULL
 AND uc_expected_priority<>collate_expected_priority THEN \'UC and Collate mapped priorities differ\'
 WHEN uc_expected_priority IS NOT NULL AND collate_expected_priority IS NOT NULL THEN \'UC and Collate mapped priorities agree\'
 WHEN collate_assignment_rows>0 AND usable_collate_assignments=0 THEN \'Collate assignment needs attention\'
 WHEN usable_collate_assignments>0 THEN \'Collate mapped requirement available\'
 ELSE collate_details END AS collate_alignment
 FROM assembled a LEFT JOIN taxonomy x ON a.expected_priority=x.priority
),
scored AS (
 SELECT a.*,
 CASE WHEN sensitivity_state IN (\'Conflicting sensitivity values\',\'Value outside configured taxonomy\') THEN \'P1 - Correct configuration\'
 WHEN invalid_rule_label_count>0 THEN \'P1 - Correct configuration\'
 WHEN expected_priority IS NOT NULL AND (sensitivity_state=\'Missing sensitivity tag\' OR actual_priority>expected_priority) THEN \'P1 - Correct configuration\'
 WHEN legacy_alignment IN (\'Legacy pii_class disagrees\',\'Conflicting legacy pii_class values\') THEN \'P2 - Steward review\'
 WHEN collate_alignment=\'UC and Collate mapped priorities differ\' THEN \'P2 - Steward review\'
 WHEN unmapped_source_label_count>0 THEN \'P2 - Steward review\'
 WHEN name_hint<>\'No configured name hint\' AND (sensitivity_state=\'Missing sensitivity tag\' OR actual_priority={{LOWEST_PRIORITY}}) THEN \'P2 - Steward review\'
 WHEN collate_assignment_rows>0 AND usable_collate_assignments=0 THEN \'P2 - Steward review\'
 WHEN sensitivity_state=\'Missing sensitivity tag\' THEN \'P3 - Complete classification\'
 WHEN expected_priority IS NOT NULL AND actual_priority<expected_priority THEN \'P2 - Steward review\'
 WHEN expected_priority IS NULL THEN \'P3 - Add source evidence\'
 ELSE \'P4 - Maintain\' END AS action_priority
 FROM classified a
),
column_results AS (
 SELECT a.*,
 CASE WHEN sensitivity_state=\'Conflicting sensitivity values\' THEN \'Resolve multiple sensitivity values against the approved classification\'
 WHEN sensitivity_state=\'Value outside configured taxonomy\' THEN \'Correct the sensitivity value or update the approved taxonomy\'
 WHEN invalid_rule_label_count>0 THEN \'Correct conflicting or inconsistent source mapping rules before changing tags\'
 WHEN expected_priority IS NOT NULL AND sensitivity_state=\'Missing sensitivity tag\' THEN concat(\'Review source evidence and assign \',expected_sensitivity)
 WHEN expected_priority IS NOT NULL AND actual_priority>expected_priority THEN concat(\'Review possible under-classification; mapped requirement is \',expected_sensitivity)
 WHEN legacy_alignment IN (\'Legacy pii_class disagrees\',\'Conflicting legacy pii_class values\') THEN \'Resolve legacy pii_class and sensitivity-tier disagreement\'
 WHEN collate_alignment=\'UC and Collate mapped priorities differ\' THEN \'Steward to reconcile UC and Collate source classification priorities\'
 WHEN unmapped_source_label_count>0 THEN \'Review unmapped source labels; add approved exact-key/value rules\'
 WHEN sensitivity_state=\'Missing sensitivity tag\' AND name_hint<>\'No configured name hint\' THEN \'Inspect column context for the name hint and assign a reviewed classification\'
 WHEN actual_priority={{LOWEST_PRIORITY}} AND name_hint<>\'No configured name hint\' THEN \'Review the sensitive-name hint against the lowest configured tier; confirm intended classification\'
 WHEN collate_assignment_rows>0 AND usable_collate_assignments=0 THEN \'Refresh or correct the Collate assignment evidence\'
 WHEN sensitivity_state=\'Missing sensitivity tag\' THEN \'Assign a steward-reviewed sensitivity classification\'
 WHEN expected_priority IS NOT NULL AND actual_priority<expected_priority THEN \'Review stronger assigned tier; preserve it until any reduction is approved\'
 WHEN expected_priority IS NULL THEN \'Capture classification rationale or connect the approved source assignment\'
 ELSE \'Retain classification and review when schema or source evidence changes\' END AS recommended_action,
 CASE WHEN expected_priority IS NOT NULL AND sensitivity_state=\'Recognized sensitivity value\'
 THEN true ELSE false END AS comparable_column,
 CASE WHEN name_hint<>\'No configured name hint\' AND (sensitivity_state=\'Missing sensitivity tag\' OR actual_priority={{LOWEST_PRIORITY}})
 THEN true ELSE false END AS name_hint_review,
 current_timestamp() AS refreshed_at,
 current_user() AS metadata_reader
 FROM scored a
)
',chr(10),ds.tail);
   FOR token AS SELECT key AS token_name,value AS replacement FROM explode(replacements) DO
    SET q = replace(q,token.token_name,token.replacement);
   END FOR;
   SELECT assert_true(instr(q,'{{')=0,'Source query still contains an unbound template token');
   SET scoped_queries = concat(scoped_queries,array(concat('SELECT * FROM (',chr(10),q,chr(10),') scope_query')));

   -- Validate each configured source once, before any output DDL.
   IF ds.dataset_name='column_inventory' THEN
    SET sql_text = concat('SELECT count(*) FROM (',q,') visible_inventory');
    EXECUTE IMMEDIATE sql_text INTO scoped_count;
    SELECT assert_true(scoped_count>0,
       concat('No source columns visible in ',binding.scope.catalog,'. Check scope and metadata access. No views deployed.'));
    SET sql_text = concat('SELECT count(*) FROM `',replace(binding.scope.mapping_table,'.','`.`'),'`');
    EXECUTE IMMEDIATE sql_text INTO readable_mapping_rows;
    IF binding.scope.collate_assignment_table IS NOT NULL THEN
     SET sql_text = concat('SELECT count(*) FROM `',replace(binding.scope.collate_assignment_table,'.','`.`'),'`');
     EXECUTE IMMEDIATE sql_text INTO readable_feed_rows;
    ELSE
     SET readable_feed_rows = NULL;
    END IF;
   END IF;
  END FOR;
  SET full_query = concat('SELECT ',char(39),replace(gov_cfg.environment,char(39),concat(char(92),char(39))),
       char(39),' AS environment,q.* FROM (',array_join(scoped_queries,concat(chr(10),'UNION ALL',chr(10))),') q');
  SET gov_queries = map_concat(gov_queries,map(ds.dataset_name,full_query));
 END FOR;

 -- Reject collisions before modifying any existing view; ALTER VIEW preserves grants.
 SET invalid_existing = (SELECT count(*) FROM system.information_schema.tables
   WHERE table_catalog=gov_cfg.output_catalog AND table_schema=gov_cfg.output_schema
     AND array_contains(transform(map_keys(gov_queries),x -> concat('raw_classification_',x)),table_name)
     AND (table_type<>'VIEW' OR NOT (comment <=> marker)));
 SELECT assert_true(invalid_existing=0,
    'Output object collision or configuration change: use an empty approved output schema, or review the existing deployment before replacing it.');
 SET gov_view_marker = marker;
 SET gov_preflight_ok = true;
 SELECT 'PREFLIGHT_PASSED' AS result,gov_cfg.environment AS environment,
        current_metastore() AS metastore,current_user() AS deployment_user,
        size(gov_cfg.scopes) AS source_catalogs,size(gov_queries) AS prepared_views,
        concat(gov_cfg.output_catalog,'.',gov_cfg.output_schema) AS output_schema;
END;

-- COMMAND ----------

BEGIN
 DECLARE sql_text STRING;
 DECLARE view_name STRING;
 DECLARE existing_count BIGINT;
 SELECT assert_true(gov_preflight_ok,'Run the preceding preflight cell successfully first');
 SET sql_text = concat('CREATE SCHEMA IF NOT EXISTS `',gov_cfg.output_catalog,'`.`',gov_cfg.output_schema,'`');
 EXECUTE IMMEDIATE sql_text;
 FOR rendered AS SELECT key AS dataset_name,value AS query_text FROM explode(gov_queries) DO
  SET view_name = concat('`',gov_cfg.output_catalog,'`.`',gov_cfg.output_schema,'`.`raw_classification_',rendered.dataset_name,'`');
  SET existing_count = (SELECT count(*) FROM system.information_schema.tables
       WHERE table_catalog=gov_cfg.output_catalog AND table_schema=gov_cfg.output_schema
         AND table_name=concat('raw_classification_',rendered.dataset_name));
  IF existing_count=0 THEN
   SET sql_text = concat('CREATE VIEW ',view_name,' COMMENT ',char(39),gov_view_marker,char(39),' AS ',rendered.query_text);
  ELSE
   SELECT assert_true((SELECT count(*) FROM system.information_schema.tables
       WHERE table_catalog=gov_cfg.output_catalog AND table_schema=gov_cfg.output_schema
         AND table_name=concat('raw_classification_',rendered.dataset_name)
         AND table_type='VIEW' AND comment=gov_view_marker)=1,'Output object changed after preflight');
   SET sql_text = concat('ALTER VIEW ',view_name,' AS ',rendered.query_text);
  END IF;
  EXECUTE IMMEDIATE sql_text;
 END FOR;
END;

-- COMMAND ----------

-- Final real-data result. Counts are calculated now; no example counts are substituted.
DECLARE OR REPLACE VARIABLE gov_inventory_name STRING DEFAULT concat(gov_cfg.output_catalog,'.',gov_cfg.output_schema,'.raw_classification_column_inventory');
SELECT 'VIEWS_READY' AS result,environment,catalog_name,
       count(*) AS visible_columns,
       count_if(sensitivity_state='Recognized sensitivity value') AS recognized_sensitivity_columns,
       count_if(sensitivity_state='Missing sensitivity tag') AS missing_sensitivity_columns,
       count_if(sensitivity_state IN ('Conflicting sensitivity values','Value outside configured taxonomy')) AS invalid_or_conflicting_columns,
       round(100.0*count_if(sensitivity_state='Recognized sensitivity value')/nullif(count(*),0),2) AS recognized_coverage_pct
FROM IDENTIFIER(gov_inventory_name)
GROUP BY environment,catalog_name;

-- COMMAND ----------

-- Collate reconciliation outcome. Zero Raw assignments remains a specific feed outcome.
DECLARE OR REPLACE VARIABLE gov_feed_name STRING DEFAULT concat(gov_cfg.output_catalog,'.',gov_cfg.output_schema,'.raw_classification_collate_feed_health');
SELECT environment,catalog_name,feed_outcome,raw_candidate_rows,usable_raw_assignments,
       latest_feed_observation,latest_raw_observation,recommended_action
FROM IDENTIFIER(gov_feed_name);
