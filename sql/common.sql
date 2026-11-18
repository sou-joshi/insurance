-- Metadata only. Scope, service identity and freshness target are set in config.json.
WITH
settings AS (
 SELECT '{{RAW}}' AS raw_catalog, '{{SERVICE}}' AS collate_service,
        {{FRESH_DAYS}} AS freshness_days
),
taxonomy AS (
 SELECT * FROM VALUES {{TAXONOMY_VALUES}}
 AS expected_taxonomy(classification,pii_class,priority)
),
inventory AS (
 SELECT c.table_catalog AS catalog_name,c.table_schema AS schema_name,
 c.table_name,c.column_name,c.data_type,c.ordinal_position,
 concat(c.table_catalog,'.',c.table_schema,'.',c.table_name) AS table_fqn,
 concat(c.table_catalog,'.',c.table_schema,'.',c.table_name,'.',c.column_name) AS column_fqn,
 coalesce(t.table_type,'Metadata object') AS object_type,
 coalesce(t.table_owner,'Assign steward') AS remediation_owner,
 CASE WHEN c.table_schema='collate' THEN 'Collate integration metadata'
 ELSE 'Raw catalog objects' END AS scope_category,
 CASE WHEN c.comment IS NULL OR trim(c.comment)='' THEN 'Description missing'
 ELSE 'Description present' END AS description_state
 FROM system.information_schema.columns c
 LEFT JOIN system.information_schema.tables t
 ON c.table_catalog=t.table_catalog AND c.table_schema=t.table_schema AND c.table_name=t.table_name
 WHERE c.table_catalog='{{RAW}}'
 AND c.table_schema NOT IN ({{EXCLUSIONS}})
),
tags AS (
 SELECT DISTINCT t.catalog_name,t.schema_name,t.table_name,t.column_name,t.tag_name,t.tag_value
 FROM system.information_schema.column_tags t
 JOIN inventory c ON t.catalog_name=c.catalog_name AND t.schema_name=c.schema_name
 AND t.table_name=c.table_name AND t.column_name=c.column_name
 WHERE t.catalog_name='{{RAW}}'
),
mapping_raw AS (
 SELECT source_tag_name,source_tag_value,collate_fqn,ifbi_classification,pii_class,tier_priority
 FROM {{MAPPING_TABLE}}
),
source_key_health AS (
 SELECT source_tag_name,source_tag_value,
 count(DISTINCT concat(coalesce(ifbi_classification,'<NULL>'),'|',coalesce(pii_class,'<NULL>'),'|',coalesce(cast(tier_priority AS STRING),'<NULL>'))) AS target_variants,
 count(*) AS key_rows
 FROM mapping_raw GROUP BY source_tag_name,source_tag_value
),
mapping_checked AS (
 SELECT m.*,k.target_variants,k.key_rows,
 CASE WHEN m.source_tag_name IS NULL OR trim(m.source_tag_name)='' THEN 'Source tag name missing'
 WHEN x.classification IS NULL THEN 'Classification outside configured taxonomy'
 WHEN m.pii_class IS NULL OR m.tier_priority IS NULL OR m.pii_class<>x.pii_class OR m.tier_priority<>x.priority THEN 'Tier, pii_class or priority disagree'
 WHEN m.source_tag_value IS NULL AND m.source_tag_name NOT LIKE '{{NATIVE_PREFIX}}%' THEN 'NULL match value requires explicit matching rule'
 WHEN k.target_variants>1 THEN 'Same source tag maps to conflicting targets'
 ELSE 'Rule usable' END AS rule_state
 FROM mapping_raw m
 JOIN source_key_health k ON m.source_tag_name <=> k.source_tag_name AND m.source_tag_value <=> k.source_tag_value
 LEFT JOIN taxonomy x ON m.ifbi_classification=x.classification
),
source_tags AS (
 SELECT t.* FROM tags t
 WHERE t.tag_name<>'{{SENSITIVITY_TAG}}'
 AND (t.tag_name IN ({{SOURCE_KEYS}}) OR t.tag_name LIKE '{{NATIVE_PREFIX}}%'
 OR t.tag_name IN (SELECT source_tag_name FROM mapping_raw))
),
source_matches AS (
 SELECT t.*,m.ifbi_classification,m.pii_class,m.tier_priority,m.rule_state
 FROM source_tags t LEFT JOIN mapping_checked m
 ON t.tag_name=m.source_tag_name
 AND (t.tag_value=m.source_tag_value OR (m.source_tag_value IS NULL AND m.source_tag_name LIKE '{{NATIVE_PREFIX}}%'))
),
source_rollup AS (
 SELECT catalog_name,schema_name,table_name,column_name,
 count(DISTINCT concat(tag_name,'=',coalesce(tag_value,'<NULL>'))) AS source_label_count,
 count(DISTINCT CASE WHEN rule_state='Rule usable' THEN concat(tag_name,'=',coalesce(tag_value,'<NULL>')) END) AS mapped_source_label_count,
 count(DISTINCT CASE WHEN rule_state IS NULL THEN concat(tag_name,'=',coalesce(tag_value,'<NULL>')) END) AS unmapped_source_label_count,
 count(DISTINCT CASE WHEN rule_state IS NOT NULL AND rule_state<>'Rule usable' THEN concat(tag_name,'=',coalesce(tag_value,'<NULL>')) END) AS invalid_rule_label_count,
 min(CASE WHEN rule_state='Rule usable' THEN tier_priority END) AS uc_expected_priority,
 concat_ws('; ',sort_array(collect_set(concat(tag_name,'=',coalesce(tag_value,'<NULL>'))))) AS source_labels,
 concat_ws('; ',sort_array(collect_set(CASE WHEN rule_state IS NULL THEN concat(tag_name,'=',coalesce(tag_value,'<NULL>')) END))) AS unmapped_source_labels
 FROM source_matches GROUP BY catalog_name,schema_name,table_name,column_name
),
tag_rollup AS (
 SELECT t.catalog_name,t.schema_name,t.table_name,t.column_name,
 count(*) AS tag_assignment_count,
 count(DISTINCT CASE WHEN t.tag_name='{{SENSITIVITY_TAG}}' THEN t.tag_value END) AS sensitivity_value_count,
 count_if(t.tag_name='{{SENSITIVITY_TAG}}') AS sensitivity_assignment_count,
 min(CASE WHEN t.tag_name='{{SENSITIVITY_TAG}}' THEN t.tag_value END) AS single_tier_value,
 min(CASE WHEN t.tag_name='{{SENSITIVITY_TAG}}' THEN x.priority END) AS actual_priority,
 concat_ws('; ',sort_array(collect_set(CASE WHEN t.tag_name='{{SENSITIVITY_TAG}}' THEN t.tag_value END))) AS sensitivity_values,
 concat_ws('; ',sort_array(collect_set(CASE WHEN t.tag_name='auto_class' THEN t.tag_value END))) AS auto_class_values,
 concat_ws('; ',sort_array(collect_set(CASE WHEN t.tag_name='{{LEGACY_PII_TAG}}' THEN t.tag_value END))) AS legacy_pii_values,
 count(DISTINCT CASE WHEN t.tag_name='{{LEGACY_PII_TAG}}' THEN t.tag_value END) AS legacy_pii_value_count,
 min(CASE WHEN t.tag_name='{{LEGACY_PII_TAG}}' THEN t.tag_value END) AS single_legacy_pii,
 concat_ws('; ',sort_array(collect_set(CASE WHEN t.tag_name LIKE '{{NATIVE_PREFIX}}%' THEN t.tag_name END))) AS native_class_tags,
 concat_ws('; ',sort_array(collect_set(concat(t.tag_name,'=',coalesce(t.tag_value,'<NULL>'))))) AS all_tag_evidence
 FROM tags t LEFT JOIN taxonomy x ON t.tag_name='{{SENSITIVITY_TAG}}' AND t.tag_value=x.classification
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
 WHERE record_rank=1 OR assignment_id IS NULL OR trim(assignment_id)=''
),
glossary_raw AS (
 SELECT a.*,
 CASE WHEN startswith(a.field_fqn,concat(s.collate_service,'.{{RAW}}.'))
 THEN substring(a.field_fqn,length(s.collate_service)+2)
 WHEN a.source_service_fqn=s.collate_service AND startswith(a.field_fqn,'{{RAW}}.') THEN a.field_fqn
 ELSE NULL END AS uc_column_fqn,
 CASE WHEN a.assignment_id IS NULL OR trim(a.assignment_id)='' THEN 'Assignment identity missing'
 WHEN a.source_service_fqn IS NOT NULL AND a.source_service_fqn<>s.collate_service THEN 'Different Collate service'
 WHEN a.term_is_deleted=true OR a.asset_is_deleted=true THEN 'Deleted term or asset'
 WHEN a.term_is_deleted IS NULL OR a.asset_is_deleted IS NULL THEN 'Deletion state missing'
 WHEN a.assignment_level IS NULL OR a.assignment_level<>'column' THEN 'Assignment is not column-level'
 WHEN a.label_state IS NULL OR a.label_state<>'Confirmed' THEN 'Steward confirmation required'
 WHEN a.last_seen_at IS NULL THEN 'Observation timestamp missing'
 WHEN a.last_seen_at>current_timestamp()+INTERVAL 5 MINUTES THEN 'Observation timestamp is in the future'
 WHEN a.last_seen_at<current_timestamp()-INTERVAL {{FRESH_DAYS}} DAYS THEN 'Refresh Collate snapshot'
 ELSE 'Current confirmed assignment' END AS assignment_state
 FROM glossary_latest a CROSS JOIN settings s
 WHERE instr(lower(coalesce(a.field_fqn,'')),'.{{RAW}}.')>0
 OR startswith(lower(coalesce(a.field_fqn,'')),'{{RAW}}.')
 OR instr(lower(coalesce(a.asset_fqn,'')),'.{{RAW}}.')>0
 OR startswith(lower(coalesce(a.asset_fqn,'')),'{{RAW}}.')
),
term_targets AS (
 SELECT collate_fqn,
 count(DISTINCT CASE WHEN rule_state='Rule usable' THEN ifbi_classification END) AS valid_target_count,
 min(CASE WHEN rule_state='Rule usable' THEN tier_priority END) AS term_priority,
 count_if(rule_state<>'Rule usable') AS unusable_rules
 FROM mapping_checked WHERE collate_fqn IS NOT NULL GROUP BY collate_fqn
),
collate_rows AS (
 SELECT a.assignment_id,a.source_scope,a.source_service_fqn,a.asset_fqn,a.field_fqn,
 a.term_fqn,a.assignment_level,a.label_state,a.last_seen_at,a.assignment_state,
 a.uc_column_fqn,c.catalog_name,c.schema_name,c.table_name,c.column_name,
 m.term_priority,
 CASE WHEN a.assignment_state<>'Current confirmed assignment' THEN a.assignment_state
 WHEN a.uc_column_fqn IS NULL THEN 'Column identity format needs mapping'
 WHEN c.column_name IS NULL THEN 'Column absent from visible Raw inventory'
 WHEN m.collate_fqn IS NULL THEN 'Glossary term has no sensitivity rule'
 WHEN m.valid_target_count<>1 OR m.unusable_rules>0 THEN 'Glossary term mapping needs correction'
 ELSE 'Current mapped column assignment' END AS reconciliation_state
 FROM glossary_raw a
 LEFT JOIN inventory c ON a.uc_column_fqn=c.column_fqn
 LEFT JOIN term_targets m ON a.term_fqn=m.collate_fqn
),
collate_rollup AS (
 SELECT catalog_name,schema_name,table_name,column_name,
 count(*) AS collate_assignment_rows,
 count_if(reconciliation_state='Current mapped column assignment') AS usable_collate_assignments,
 min(CASE WHEN reconciliation_state='Current mapped column assignment' THEN term_priority END) AS collate_expected_priority,
 concat_ws('; ',sort_array(collect_set(term_fqn))) AS collate_terms,
 concat_ws('; ',sort_array(collect_set(reconciliation_state))) AS collate_details,
 max(last_seen_at) AS collate_last_seen_at
 FROM collate_rows WHERE column_name IS NOT NULL
 GROUP BY catalog_name,schema_name,table_name,column_name
),
collate_feed AS (
 SELECT (SELECT count(*) FROM glossary_latest) AS glossary_feed_rows,
 (SELECT count(*) FROM glossary_raw) AS raw_candidate_rows,
 (SELECT coalesce(count_if(reconciliation_state='Current mapped column assignment'),0) FROM collate_rows) AS usable_raw_assignments,
 (SELECT max(last_seen_at) FROM glossary_latest) AS latest_feed_observation,
 (SELECT max(last_seen_at) FROM glossary_raw) AS latest_raw_observation,
 CASE WHEN {{COLLATE_CONFIGURED}}=false THEN 'Collate assignment source not configured'
 WHEN (SELECT count(*) FROM glossary_latest)=0 THEN 'Glossary feed contains no records'
 WHEN (SELECT count(*) FROM glossary_raw)=0 THEN 'No Raw assignments found in glossary feed'
 WHEN (SELECT coalesce(count_if(reconciliation_state='Current mapped column assignment'),0) FROM collate_rows)=0 THEN 'Raw glossary assignments need attention'
 ELSE 'Mapped Raw glossary evidence available' END AS feed_outcome
),
assembled AS (
 SELECT c.*,coalesce(t.tag_assignment_count,0) AS tag_assignment_count,
 coalesce(t.sensitivity_assignment_count,0) AS sensitivity_assignment_count,
 coalesce(t.sensitivity_value_count,0) AS sensitivity_value_count,
 CASE WHEN coalesce(t.sensitivity_assignment_count,0)=0 THEN 'Missing sensitivity tag'
 WHEN t.sensitivity_value_count>1 THEN 'Conflicting sensitivity values'
 WHEN t.actual_priority IS NULL THEN 'Value outside configured taxonomy'
 ELSE 'Recognized sensitivity value' END AS sensitivity_state,
 CASE WHEN coalesce(t.sensitivity_assignment_count,0)=0 THEN '(missing)'
 ELSE coalesce(t.sensitivity_values,'(empty value)') END AS current_sensitivity,
 t.actual_priority,coalesce(t.auto_class_values,'') AS auto_class_values,
 coalesce(t.native_class_tags,'') AS native_class_tags,
 coalesce(t.legacy_pii_values,'') AS legacy_pii_values,
 coalesce(t.all_tag_evidence,'') AS all_tag_evidence,
 CASE WHEN coalesce(t.legacy_pii_value_count,0)=0 THEN 'No legacy pii_class tag'
 WHEN t.legacy_pii_value_count>1 THEN 'Conflicting legacy pii_class values'
 WHEN x.pii_class IS NULL THEN 'Resolve sensitivity tier before legacy comparison'
 WHEN t.single_legacy_pii=x.pii_class THEN 'Legacy value agrees'
 ELSE 'Legacy pii_class disagrees' END AS legacy_alignment,
 coalesce(u.source_label_count,0) AS source_label_count,
 coalesce(u.mapped_source_label_count,0) AS mapped_source_label_count,
 coalesce(u.unmapped_source_label_count,0) AS unmapped_source_label_count,
 coalesce(u.invalid_rule_label_count,0) AS invalid_rule_label_count,
 coalesce(u.source_labels,'') AS source_labels,
 coalesce(u.unmapped_source_labels,'') AS unmapped_source_labels,
 u.uc_expected_priority,g.collate_expected_priority,
 coalesce(g.collate_assignment_rows,0) AS collate_assignment_rows,
 coalesce(g.usable_collate_assignments,0) AS usable_collate_assignments,
 coalesce(g.collate_terms,'') AS collate_terms,
 coalesce(g.collate_details,f.feed_outcome) AS collate_details,
 g.collate_last_seen_at,
 CASE WHEN u.uc_expected_priority IS NOT NULL AND g.collate_expected_priority IS NOT NULL THEN 'UC source tags + Collate glossary'
 WHEN u.uc_expected_priority IS NOT NULL THEN 'UC source tags'
 WHEN g.collate_expected_priority IS NOT NULL THEN 'Collate mapped glossary'
 ELSE 'No usable source mapping' END AS reconciliation_basis,
 CASE WHEN u.uc_expected_priority IS NULL THEN g.collate_expected_priority
 WHEN g.collate_expected_priority IS NULL THEN u.uc_expected_priority
 ELSE least(u.uc_expected_priority,g.collate_expected_priority) END AS expected_priority,
 CASE WHEN regexp_like(lower(c.column_name),'(^|_)(ssn|soc_sec|social_security_number|socialsecuritynumber)($|_)') THEN 'SSN name hint'
 WHEN regexp_like(lower(c.column_name),'(^|_)(email|e_mail|email_address)($|_)') THEN 'Email name hint'
 WHEN regexp_like(lower(c.column_name),'(^|_)(dob|birth_date|date_of_birth)($|_)') THEN 'Birth date name hint'
 WHEN regexp_like(lower(c.column_name),'(^|_)(phone|phone_number|telephone|mobile_number)($|_)') THEN 'Phone name hint'
 WHEN regexp_like(lower(c.column_name),'(^|_)(bank_account|bank_account_number|credit_card|credit_card_number|iban|routing_number)($|_)') THEN 'Payment identifier name hint'
 WHEN regexp_like(lower(c.column_name),'(^|_)(claim_notes|medical_notes|diagnosis|health_data)($|_)') THEN 'Sensitive free-text or health name hint'
 ELSE 'No configured name hint' END AS name_hint
 FROM inventory c
 LEFT JOIN tag_rollup t USING(catalog_name,schema_name,table_name,column_name)
 LEFT JOIN source_rollup u USING(catalog_name,schema_name,table_name,column_name)
 LEFT JOIN collate_rollup g USING(catalog_name,schema_name,table_name,column_name)
 LEFT JOIN taxonomy x ON t.sensitivity_value_count=1 AND t.single_tier_value=x.classification
 CROSS JOIN collate_feed f
),
classified AS (
 SELECT a.*,coalesce(x.classification,'(no mapped requirement)') AS expected_sensitivity,
 CASE WHEN sensitivity_state<>'Recognized sensitivity value' THEN sensitivity_state
 WHEN invalid_rule_label_count>0 THEN 'Source mapping rule needs correction'
 WHEN expected_priority IS NULL AND unmapped_source_label_count>0 THEN 'Source labels need mapping rules'
 WHEN expected_priority IS NULL THEN 'Sensitivity assigned; source mapping absent'
 WHEN actual_priority>expected_priority THEN 'Assigned tier below mapped requirement'
 WHEN actual_priority<expected_priority THEN 'Assigned tier above mapped requirement'
 WHEN unmapped_source_label_count>0 THEN 'Mapped tier agrees; additional labels need rules'
 ELSE 'Assigned tier agrees with mapped evidence' END AS reconciliation_outcome,
 CASE WHEN uc_expected_priority IS NOT NULL AND collate_expected_priority IS NOT NULL
 AND uc_expected_priority<>collate_expected_priority THEN 'UC and Collate mapped priorities differ'
 WHEN uc_expected_priority IS NOT NULL AND collate_expected_priority IS NOT NULL THEN 'UC and Collate mapped priorities agree'
 WHEN collate_assignment_rows>0 AND usable_collate_assignments=0 THEN 'Collate assignment needs attention'
 WHEN usable_collate_assignments>0 THEN 'Collate mapped requirement available'
 ELSE collate_details END AS collate_alignment
 FROM assembled a LEFT JOIN taxonomy x ON a.expected_priority=x.priority
),
scored AS (
 SELECT a.*,
 CASE WHEN sensitivity_state IN ('Conflicting sensitivity values','Value outside configured taxonomy') THEN 'P1 - Correct configuration'
 WHEN invalid_rule_label_count>0 THEN 'P1 - Correct configuration'
 WHEN expected_priority IS NOT NULL AND (sensitivity_state='Missing sensitivity tag' OR actual_priority>expected_priority) THEN 'P1 - Correct configuration'
 WHEN legacy_alignment IN ('Legacy pii_class disagrees','Conflicting legacy pii_class values') THEN 'P2 - Steward review'
 WHEN collate_alignment='UC and Collate mapped priorities differ' THEN 'P2 - Steward review'
 WHEN unmapped_source_label_count>0 THEN 'P2 - Steward review'
 WHEN name_hint<>'No configured name hint' AND (sensitivity_state='Missing sensitivity tag' OR actual_priority={{LOWEST_PRIORITY}}) THEN 'P2 - Steward review'
 WHEN collate_assignment_rows>0 AND usable_collate_assignments=0 THEN 'P2 - Steward review'
 WHEN sensitivity_state='Missing sensitivity tag' THEN 'P3 - Complete classification'
 WHEN expected_priority IS NOT NULL AND actual_priority<expected_priority THEN 'P2 - Steward review'
 WHEN expected_priority IS NULL THEN 'P3 - Add source evidence'
 ELSE 'P4 - Maintain' END AS action_priority
 FROM classified a
),
column_results AS (
 SELECT a.*,
 CASE WHEN sensitivity_state='Conflicting sensitivity values' THEN 'Resolve multiple sensitivity values against the approved classification'
 WHEN sensitivity_state='Value outside configured taxonomy' THEN 'Correct the sensitivity value or update the approved taxonomy'
 WHEN invalid_rule_label_count>0 THEN 'Correct conflicting or inconsistent source mapping rules before changing tags'
 WHEN expected_priority IS NOT NULL AND sensitivity_state='Missing sensitivity tag' THEN concat('Review source evidence and assign ',expected_sensitivity)
 WHEN expected_priority IS NOT NULL AND actual_priority>expected_priority THEN concat('Review possible under-classification; mapped requirement is ',expected_sensitivity)
 WHEN legacy_alignment IN ('Legacy pii_class disagrees','Conflicting legacy pii_class values') THEN 'Resolve legacy pii_class and sensitivity-tier disagreement'
 WHEN collate_alignment='UC and Collate mapped priorities differ' THEN 'Steward to reconcile UC and Collate source classification priorities'
 WHEN unmapped_source_label_count>0 THEN 'Review unmapped source labels; add approved exact-key/value rules'
 WHEN sensitivity_state='Missing sensitivity tag' AND name_hint<>'No configured name hint' THEN 'Inspect column context for the name hint and assign a reviewed classification'
 WHEN actual_priority={{LOWEST_PRIORITY}} AND name_hint<>'No configured name hint' THEN 'Review the sensitive-name hint against the lowest configured tier; confirm intended classification'
 WHEN collate_assignment_rows>0 AND usable_collate_assignments=0 THEN 'Refresh or correct the Collate assignment evidence'
 WHEN sensitivity_state='Missing sensitivity tag' THEN 'Assign a steward-reviewed sensitivity classification'
 WHEN expected_priority IS NOT NULL AND actual_priority<expected_priority THEN 'Review stronger assigned tier; preserve it until any reduction is approved'
 WHEN expected_priority IS NULL THEN 'Capture classification rationale or connect the approved source assignment'
 ELSE 'Retain classification and review when schema or source evidence changes' END AS recommended_action,
 CASE WHEN expected_priority IS NOT NULL AND sensitivity_state='Recognized sensitivity value'
 THEN true ELSE false END AS comparable_column,
 CASE WHEN name_hint<>'No configured name hint' AND (sensitivity_state='Missing sensitivity tag' OR actual_priority={{LOWEST_PRIORITY}})
 THEN true ELSE false END AS name_hint_review,
 current_timestamp() AS refreshed_at,
 current_user() AS metadata_reader
 FROM scored a
)
