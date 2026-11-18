"""Native Databricks resource definitions. No API calls on import."""
import re
from urllib.parse import urlparse

def csv_values(value):return sorted(set(x.strip() for x in value.split(',') if x.strip()))
def validate_inputs(warehouse,cluster,emails,destinations,dashboard_url,workspace_host,poll):
    assert re.fullmatch(r'[0-9a-fA-F]{16,}',warehouse or ''),'Enter the existing SQL warehouse ID.'
    assert re.fullmatch(r'[A-Za-z0-9-]+',cluster or ''),'Enter an existing approved Spark cluster ID.'
    assert len(emails)+len(destinations)<=100,'At most 100 alert subscribers/destinations.'
    assert emails or destinations,'Enter workspace notification emails or existing notification destination IDs.'
    assert all(re.fullmatch(r'[^\s<>@]+@[^\s<>@]+\.[^\s<>@]+',e) for e in emails),'Use valid workspace notification emails.'
    assert all(re.fullmatch(r'[A-Za-z0-9-]+',d) for d in destinations),'Use destination IDs, not webhook URLs.'
    assert poll in (15,30,60),'Supported schedules: 15, 30 or 60 minutes.'
    if dashboard_url:
        p=urlparse(dashboard_url);h=urlparse(workspace_host)
        assert p.scheme=='https' and p.hostname==h.hostname and not p.username and not p.password,'Dashboard URL must belong to this workspace.'

def cron(minutes,offset=2):return f'0 {offset} * * * ?' if minutes==60 else f'0 {offset}/{minutes} * * * ?'
def subscriptions(emails,destinations):
    return [{'user_email':e} for e in emails]+[{'destination_id':d} for d in destinations]

def alert_definition(name,query,warehouse,emails,destinations,body,*,heartbeat=False,poll=60):
    result={'display_name':name,'query_text':query,'warehouse_id':warehouse,
      'evaluation':{'source':{'name':'attention_required' if heartbeat else 'notice_count','aggregation':'MAX'},
       'comparison_operator':'GREATER_THAN','threshold':{'value':{'double_value':0}},
       'empty_result_state':'ERROR','notification':{'subscriptions':subscriptions(emails,destinations),
        'retrigger_seconds':86400 if heartbeat else 1,'notify_on_ok':heartbeat}},
      'schedule':{'quartz_cron_schedule':cron(poll,7 if heartbeat else 2),'timezone_id':'UTC','pause_status':'PAUSED'},
      'custom_template_format':'MARKDOWN','custom_summary':name,
      'custom_description':body+'\n\n@QUERY_RESULT_TABLE\n\nAlert details: @ALERT_URL'}
    if not heartbeat:result['parameters']=[{'name':'monitor_run_id','type':'STRING','value':'__NO_RUN__'}]
    return result

def job_definition(monitor_id,notebook_path,cluster,warehouse,alert_id,emails,destinations,settings):
    base={'scope_signature':settings['scope_signature'],'environment_config_json':settings['environment_config_json']}
    collect={'task_key':'collect','existing_cluster_id':cluster,'max_retries':1,'min_retry_interval_millis':60000,'timeout_seconds':1800,
      'notebook_task':{'notebook_path':notebook_path,'source':'WORKSPACE','base_parameters':dict(base,mode='collect',run_token='{{job.run_id}}',installed_job_id='{{job.id}}')}}
    condition={'task_key':'has_notifications','depends_on':[{'task_key':'collect'}],
      'condition_task':{'left':'{{tasks.collect.values.notification_count}}','op':'GREATER_THAN','right':'0'}}
    evaluate={'task_key':'evaluate_alert','depends_on':[{'task_key':'has_notifications','outcome':'true'}],
      'max_retries':0,'timeout_seconds':900,
      'alert_task':{'alert_id':alert_id,'warehouse_id':warehouse,
       'parameters':{'monitor_run_id':'{{tasks.collect.values.monitor_run_id}}'}}}
    ack={'task_key':'record_evaluation','depends_on':[{'task_key':'evaluate_alert'}],
      'existing_cluster_id':cluster,'max_retries':1,'min_retry_interval_millis':60000,'timeout_seconds':600,
      'notebook_task':{'notebook_path':notebook_path,'source':'WORKSPACE','base_parameters':dict(base,mode='ack',
       target_run_id='{{tasks.collect.values.monitor_run_id}}',run_token='{{job.run_id}}',installed_job_id='{{job.id}}')}}
    return {'name':'Raw Classification Monitor - '+settings['environment']+' '+monitor_id[-8:],'description':'Stateful metadata-only classification monitor; baseline, grace, deduplicated notices, reminders and recovery. Managed by monitor '+monitor_id,
      'tags':{'governance_component':'classification_monitor','monitor_id':monitor_id,'environment':settings['environment']},
      'max_concurrent_runs':1,'queue':{'enabled':True},'timeout_seconds':3600,
      'email_notifications':{'on_failure':emails},
      'webhook_notifications':{'on_failure':[{'id':d} for d in destinations]},
      'schedule':{'quartz_cron_schedule':cron(settings['poll_minutes']),'timezone_id':'UTC','pause_status':'PAUSED'},
      'tasks':[collect,condition,evaluate,ack]}
