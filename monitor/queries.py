"""Environment-bound queries generated from the deployment configuration."""
def build(table,monitor_id,maximum_age_minutes):
    schema='.'.join(table.split('.')[:-1])
    TABLE=table
    MONITOR_ID=monitor_id
    DDL=f'''CREATE SCHEMA IF NOT EXISTS {schema};
    CREATE TABLE IF NOT EXISTS {TABLE} (
     monitor_id STRING,run_id STRING,record_id STRING,record_type STRING,
     observed_at TIMESTAMP,payload_json STRING
    ) USING DELTA TBLPROPERTIES ('ifbi.component'='classification_monitor_v2');'''
    
    BASE=f'''WITH published AS (
     SELECT run_id,observed_at,payload_json,
     row_number() OVER (ORDER BY observed_at DESC,run_id DESC) AS rn
     FROM {TABLE} WHERE monitor_id='{MONITOR_ID}' AND record_type='RUN'
    ), latest AS (SELECT * FROM published WHERE rn=1),
    acks AS (
     SELECT get_json_object(payload_json,'$.notice_id') AS notice_id,max(observed_at) AS evaluation_accepted_at
     FROM {TABLE} WHERE monitor_id='{MONITOR_ID}' AND record_type='ACK'
     GROUP BY get_json_object(payload_json,'$.notice_id')
    )
    '''
    
    def alert_query():
        return f"""WITH binding AS (
 SELECT CASE WHEN EXISTS (SELECT 1 FROM {TABLE} WHERE monitor_id='{MONITOR_ID}' AND record_type='RUN' AND run_id=:monitor_run_id)
 THEN 0 ELSE raise_error('Monitor run parameter is not bound to a published batch') END AS binding_error
), selected AS (
 SELECT get_json_object(payload_json,'$.notice_id') AS notice_id,
 get_json_object(payload_json,'$.kind') AS event_kind,
 get_json_object(payload_json,'$.finding_type') AS finding_type,
 get_json_object(payload_json,'$.priority') AS priority,
 get_json_object(payload_json,'$.catalog_name') AS catalog_name,
 get_json_object(payload_json,'$.schema_name') AS schema_name,
 get_json_object(payload_json,'$.table_name') AS table_name,
 get_json_object(payload_json,'$.column_name') AS column_name,
 get_json_object(payload_json,'$.owner') AS responsible_owner
 FROM {TABLE} j
 WHERE monitor_id='{MONITOR_ID}' AND run_id=:monitor_run_id
 AND record_type='NOTICE' AND get_json_object(payload_json,'$.selected_for_batch')='true'
 AND NOT EXISTS (SELECT 1 FROM {TABLE} a WHERE a.monitor_id=j.monitor_id AND a.record_type='ACK'
 AND get_json_object(a.payload_json,'$.notice_id')=j.record_id)
), totals AS (SELECT count(*) AS notice_count FROM selected), grouped AS (
 SELECT event_kind,finding_type,priority,catalog_name,schema_name,table_name,responsible_owner,
 count(*) AS affected_events,
 concat_ws('; ',slice(sort_array(collect_set(column_name)),1,5)) AS example_columns
 FROM selected GROUP BY event_kind,finding_type,priority,catalog_name,schema_name,table_name,responsible_owner
 ORDER BY priority,affected_events DESC LIMIT 99
)
SELECT t.notice_count+b.binding_error AS notice_count,g.* FROM totals t CROSS JOIN binding b LEFT JOIN grouped g ON true
ORDER BY g.priority,g.affected_events DESC"""

    HEALTH=BASE+f'''SELECT
     CASE WHEN l.run_id IS NULL THEN 1
     WHEN l.observed_at<current_timestamp()-INTERVAL {{MAX_AGE}} MINUTES THEN 1
     WHEN (SELECT max(observed_at) FROM {TABLE} WHERE monitor_id='{MONITOR_ID}' AND record_type='FAILURE')>l.observed_at THEN 1
     ELSE 0 END AS attention_required,
     CASE WHEN l.run_id IS NULL THEN 'Initialize the monitor baseline'
     WHEN l.observed_at<current_timestamp()-INTERVAL {{MAX_AGE}} MINUTES THEN 'Scheduled classification check overdue'
     WHEN (SELECT max(observed_at) FROM {TABLE} WHERE monitor_id='{MONITOR_ID}' AND record_type='FAILURE')>l.observed_at THEN 'Latest monitor attempt failed'
     ELSE 'Monitor current' END AS monitor_status,
     l.observed_at AS last_success_at,
     (SELECT max(observed_at) FROM {TABLE} WHERE monitor_id='{MONITOR_ID}' AND record_type='FAILURE') AS last_failure_at,
     cast(get_json_object(l.payload_json,'$.summary.observed_columns') AS BIGINT) AS last_visible_columns,
     cast(get_json_object(l.payload_json,'$.summary.missing_tag_columns') AS BIGINT) AS missing_tag_columns,
     cast(get_json_object(l.payload_json,'$.summary.baseline_backlog') AS BIGINT) AS baseline_backlog,
     cast(get_json_object(l.payload_json,'$.summary.grace_incidents') AS BIGINT) AS grace_incidents,
     cast(get_json_object(l.payload_json,'$.summary.open_incidents') AS BIGINT) AS open_incidents,
     cast(get_json_object(l.payload_json,'$.summary.pending_notifications') AS BIGINT) AS pending_notifications,
     current_timestamp() AS refreshed_at
    FROM (SELECT 1 AS anchor) x LEFT JOIN latest l ON true'''
    
    INCIDENTS=BASE+f'''SELECT
     get_json_object(j.payload_json,'$.issue_key') AS issue_key,
     get_json_object(j.payload_json,'$.finding_type') AS finding_type,
     get_json_object(j.payload_json,'$.status') AS incident_status,
     get_json_object(j.payload_json,'$.priority') AS priority,
     get_json_object(j.payload_json,'$.schema_name') AS schema_name,
     get_json_object(j.payload_json,'$.table_name') AS table_name,
     get_json_object(j.payload_json,'$.column_name') AS column_name,
     get_json_object(j.payload_json,'$.owner') AS responsible_owner,
     get_json_object(j.payload_json,'$.current_sensitivity') AS current_sensitivity,
     get_json_object(j.payload_json,'$.expected_sensitivity') AS expected_sensitivity,
     cast(get_json_object(j.payload_json,'$.generation') AS INT) AS incident_generation,
     cast(get_json_object(j.payload_json,'$.opened_at') AS TIMESTAMP) AS opened_at,
     cast(get_json_object(j.payload_json,'$.eligible_at') AS TIMESTAMP) AS alert_eligible_at,
     cast(get_json_object(j.payload_json,'$.last_accepted_at') AS TIMESTAMP) AS last_evaluation_accepted_at,
     cast(get_json_object(j.payload_json,'$.resolved_at') AS TIMESTAMP) AS resolved_at,
     get_json_object(j.payload_json,'$.detail') AS action_context
    FROM {TABLE} j JOIN latest l ON j.run_id=l.run_id
    WHERE j.monitor_id='{MONITOR_ID}' AND j.record_type='ISSUE'
    ORDER BY priority,incident_status,opened_at'''
    
    NOTICES=BASE+f''', last_notice AS (
     SELECT *,row_number() OVER (PARTITION BY record_id ORDER BY observed_at DESC,run_id DESC) AS notice_rank
     FROM {TABLE} WHERE monitor_id='{MONITOR_ID}' AND record_type='NOTICE'
    )
    SELECT n.record_id AS notice_id,
     get_json_object(n.payload_json,'$.issue_key') AS issue_key,
     get_json_object(n.payload_json,'$.kind') AS event_kind,
     get_json_object(n.payload_json,'$.finding_type') AS finding_type,
     get_json_object(n.payload_json,'$.priority') AS priority,
     get_json_object(n.payload_json,'$.column_fqn') AS column_fqn,
     get_json_object(n.payload_json,'$.owner') AS responsible_owner,
     cast(get_json_object(n.payload_json,'$.created_at') AS TIMESTAMP) AS created_at,
     CASE WHEN a.notice_id IS NOT NULL THEN 'Alert evaluation accepted'
     WHEN p.record_id IS NOT NULL THEN 'Pending alert evaluation'
     ELSE 'Withdrawn before evaluation or older incident' END AS notification_status,
     a.evaluation_accepted_at
    FROM last_notice n LEFT JOIN acks a ON n.record_id=a.notice_id
    LEFT JOIN (SELECT j.record_id FROM {TABLE} j JOIN latest l ON j.run_id=l.run_id
     WHERE j.monitor_id='{MONITOR_ID}' AND j.record_type='NOTICE') p ON n.record_id=p.record_id
    WHERE n.notice_rank=1 ORDER BY created_at DESC,notice_id'''
    
    TRENDS=BASE+'''SELECT observed_date,visible_columns,missing_tags,baseline_backlog,new_untagged_open,open_incidents FROM (
     SELECT cast(observed_at AS DATE) AS observed_date,
     cast(get_json_object(payload_json,'$.summary.observed_columns') AS BIGINT) AS visible_columns,
     cast(get_json_object(payload_json,'$.summary.missing_tag_columns') AS BIGINT) AS missing_tags,
     cast(get_json_object(payload_json,'$.summary.baseline_backlog') AS BIGINT) AS baseline_backlog,
     cast(get_json_object(payload_json,'$.summary.new_untagged_open') AS BIGINT) AS new_untagged_open,
     cast(get_json_object(payload_json,'$.summary.open_incidents') AS BIGINT) AS open_incidents,
     row_number() OVER (PARTITION BY cast(observed_at AS DATE) ORDER BY observed_at DESC,run_id DESC) AS daily_rank
     FROM published
    ) WHERE daily_rank=1 ORDER BY observed_date'''
    return {'ddl':DDL,'alert':alert_query(),'health':HEALTH.replace('{MAX_AGE}',str(maximum_age_minutes)),'incidents':INCIDENTS,'notices':NOTICES,'trends':TRENDS}
