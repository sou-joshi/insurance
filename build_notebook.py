"""Produce a single importable notebook with all generic source modules embedded."""
from pathlib import Path
import json
ROOT=Path(__file__).resolve().parent
modules={n:(ROOT/p).read_text() for n,p in {'configuration':'configuration.py','compiler':'compiler.py','monitor_engine':'monitor/engine.py','monitor_provision':'monitor/provision.py','monitor_queries':'monitor/queries.py'}.items()}
constants={'MODULES':modules,'COMMON_SOURCE':(ROOT/'sql/common.sql').read_text(),'DATASET_TAILS':json.loads((ROOT/'sql/dataset_tails.json').read_text()),'DASHBOARD_TEMPLATE':json.loads((ROOT/'Raw_Classification_Assurance.lvdash.json').read_text()),'RUNTIME_SOURCE':(ROOT/'monitor/runtime.py').read_text()}
header='''# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy classification dashboard and monitoring
# MAGIC One implementation, separate environment configurations and state.
# MAGIC Import this notebook; run `demo` for a synthetic no-write test, or paste an environment configuration and choose `install`.
# MAGIC Installation creates one owned metadata journal, logical views, a dashboard draft, a scheduled job and native SQL alerts.
# MAGIC Initial missing classifications are summarized as backlog; new changes produce grouped digests.
# MAGIC Use approved existing Spark compute and a non-serverless Pro SQL warehouse.

# COMMAND ----------
import json,sys,types,uuid,re
from datetime import datetime,timezone,timedelta
from pathlib import Path
dbutils.widgets.dropdown('mode','demo',['demo','install','baseline','collect','ack'])
dbutils.widgets.text('environment_config_json','{}')
dbutils.widgets.text('run_token','')
dbutils.widgets.text('installed_job_id','')
dbutils.widgets.text('target_run_id','')
dbutils.widgets.text('scope_signature','')
mode=dbutils.widgets.get('mode')

# COMMAND ----------
'''
s=header+''.join(name+' = '+repr(value)+'\n' for name,value in constants.items())
s+='''
for name,source in MODULES.items():
    module=types.ModuleType(name);module.__file__=name+'.py';sys.modules[name]=module
    exec(compile(source,module.__file__,'exec'),module.__dict__)
from configuration import validate,quoted,literal
from compiler import compile_sources
from monitor_engine import evaluate,ScopeError,digest,stamp
from monitor_provision import validate_inputs,alert_definition,job_definition
from monitor_queries import build as build_monitor_queries

# COMMAND ----------
def demonstrate():
    # Fictional metadata only. No table, API, source reads or notifications.
    settings={'maximum_columns':50000,'maximum_inventory_drop_fraction':.2,'classification_grace_minutes':30,'reminder_hours':24,'maximum_notifications_per_run':1000}
    identity={'workspace_id':'synthetic','metastore_id':'synthetic','metadata_reader':'synthetic'}
    now=datetime(2030,1,1,tzinfo=timezone.utc)
    def col(name,tier=None):
        return dict(catalog_name='synthetic_raw',schema_name='source',table_name='party',column_name=name,data_type='STRING',
            sensitivity_state='Recognized sensitivity value' if tier else 'Missing sensitivity tag',
            current_sensitivity='Configured tier' if tier else '(missing)',actual_priority=tier,expected_priority=None,
            expected_sensitivity='(no mapped requirement)',remediation_owner='synthetic_owner',invalid_rule_label_count=0)
    original=[col('original_untagged'),col('protected',1)]
    base=evaluate(original,None,now,identity,'synthetic_scope',settings)
    assert not base['issues'] and not base['pending']
    new=original+[col('new_email')]
    grace=evaluate(new,base,now+timedelta(minutes=1),identity,'synthetic_scope',settings)
    assert not grace['pending']
    alert=evaluate(new,grace,now+timedelta(minutes=32),identity,'synthetic_scope',settings)
    assert len(alert['pending'])==1
    receipt=[{'notice_id':n,'accepted_at':stamp(now+timedelta(minutes=33))} for n in alert['pending']]
    repaired=original+[col('new_email',3)]
    recovery=evaluate(repaired,alert,now+timedelta(minutes=34),identity,'synthetic_scope',settings,receipt)
    assert {n['kind'] for n in recovery['pending'].values()}=={'RECOVERY'}
    print(json.dumps({'result':'PASS','baseline_backlog':base['summary']['baseline_backlog'],
       'baseline_incidents':len(base['issues']),'notifications_during_grace':len(grace['pending']),
       'new_events_after_grace':len(alert['pending']),'recovery_events':len(recovery['pending']),
       'notification_mode':'GROUPED_DIGEST','customer_data_access':False,'notifications_sent':False},indent=2))

if mode=='demo':
    demonstrate()
else:
    from databricks.sdk import WorkspaceClient
    cfg=validate(json.loads(dbutils.widgets.get('environment_config_json')),require_runtime=mode=='install')
    client=WorkspaceClient()
    assert str(client.get_workspace_id())==str(cfg['expected_workspace_id']),'Workspace does not match this environment configuration.'
    settings=dict(cfg['monitor'],environment=cfg['environment'],monitor_id=cfg['monitor_id'],expected_workspace_id=str(cfg['expected_workspace_id']),
       environment_config_json=json.dumps(cfg),scope_signature=cfg['scope_signature'])
    if dbutils.widgets.get('scope_signature'):
        assert dbutils.widgets.get('scope_signature')==cfg['scope_signature'],'Installed job configuration signature changed.'
    source_queries=compile_sources(cfg,COMMON_SOURCE,DATASET_TAILS)
    COLUMN_QUERY=source_queries['column_inventory']
    monitor_sql=build_monitor_queries(cfg['state_table'],cfg['monitor_id'],max(60,settings['poll_minutes']*2+15))
    MONITOR_DDL=monitor_sql['ddl'];ALERT_QUERY=monitor_sql['alert'];HEALTH_QUERY=monitor_sql['health']
    exec(compile(RUNTIME_SOURCE,'monitor_runtime.py','exec'),globals())
    spark.sql("SET TIME ZONE 'UTC'")
    journal=Journal(spark,cfg['state_table'],cfg['monitor_id'])
    if mode in ('install','baseline'):journal.initialize()
    resources=journal.installation()
    if mode in ('collect','ack'):
        assert dbutils.widgets.get('installed_job_id')==str(resources.get('job_id')),'Monitor writes must run through the installed single-writer job.'
        assert re.fullmatch(r'\\d+',dbutils.widgets.get('run_token')),'A Databricks job run token is required.'
    if mode=='ack':
        count=record_evaluation(journal,dbutils.widgets.get('target_run_id'))
        print(json.dumps({'accepted_evaluation_events':count,'delivery_attestation':False}))
    else:
        run_id=digest([cfg['monitor_id'],dbutils.widgets.get('run_token')])[:32] if mode=='collect' else uuid.uuid4().hex
        try:
            if mode=='install':
                validate_inputs(cfg['warehouse_id'],cfg['existing_cluster_id'],cfg['notifications']['emails'],cfg['notifications']['destination_ids'],'',client.config.host,settings['poll_minutes'])
                if resources.get('job_id'):
                    active=client.api_client.do('GET','/api/2.2/jobs/runs/list',query={'job_id':resources['job_id'],'active_only':True})
                    assert not active.get('runs'),'Wait for the current managed monitor run to finish before deployment.'
                    client.api_client.do('POST','/api/2.2/jobs/update',body={'job_id':resources['job_id'],'new_settings':{'schedule':{'quartz_cron_schedule':'0 2 * * * ?','timezone_id':'UTC','pause_status':'PAUSED'}}})
            result=collect_snapshot(spark,journal,client,settings,cfg['scope_signature'],run_id,baseline_only=mode in ('install','baseline'))
            if mode=='collect':
                dbutils.jobs.taskValues.set(key='monitor_run_id',value=run_id)
                dbutils.jobs.taskValues.set(key='notification_count',value=result['summary']['selected_notifications'])
                retain_journal(journal,settings['retention_days'])
            elif mode=='install':
                dashboard_url=deploy_views_and_dashboard(spark,client,journal,cfg,source_queries,DASHBOARD_TEMPLATE)
                path=dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
                installed=configure_resources(client,journal,settings,cfg['scope_signature'],path,cfg['warehouse_id'],cfg['existing_cluster_id'],
                    cfg['notifications']['emails'],cfg['notifications']['destination_ids'],dashboard_url)
                print(json.dumps({'dashboard':dashboard_url,'job_id':installed['job_id'],'changes_alert_id':installed['changes_alert_id'],
                    'health_alert_id':installed['health_alert_id'],'schedule_minutes':settings['poll_minutes'],
                    'initial_backlog_is_summary_only':True,'baseline_incidents':len(result['issues'])},indent=2))
            print(json.dumps(result['summary'],indent=2))
        except Exception as exc:
            journal.failure(run_id,exc)
            raise
'''
compile(s,'Deploy_Governance_Monitor.py','exec')
(ROOT/'Deploy_Governance_Monitor.py').write_text(s)
print('Built self-contained deployment notebook.')
