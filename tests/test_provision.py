import copy,json,runpy,sys,unittest
from pathlib import Path
from types import SimpleNamespace
from databricks.sdk.service.sql import AlertV2
from databricks.sdk.service.jobs import JobSettings
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from configuration import validate,quoted,literal
from compiler import compile_sources
from build_dashboard import build
from monitor.engine import evaluate,ScopeError,digest,stamp
from monitor.queries import build as build_queries
from monitor.provision import validate_inputs,alert_definition,job_definition
RUNTIME=runpy.run_path(str(ROOT/'monitor/runtime.py'),init_globals=dict(evaluate=evaluate,ScopeError=ScopeError,digest=digest,stamp=stamp,quoted=quoted,literal=literal,
 validate_inputs=validate_inputs,alert_definition=alert_definition,job_definition=job_definition,build_monitor_queries=build_queries,ALERT_QUERY='SELECT 0 AS notice_count',HEALTH_QUERY='SELECT 0 AS attention_required'))

class API:
 def __init__(self,serverless=False):self.calls=[];self.alerts={};self.jobs={};self.dashboard=None;self.serverless=serverless
 def do(self,method,path,body=None,query=None):
  self.calls.append((method,path,copy.deepcopy(body),copy.deepcopy(query)))
  if path.startswith('/api/2.0/sql/warehouses/'):return dict(warehouse_type='PRO',enable_serverless_compute=self.serverless)
  if path=='/api/2.0/clusters/get':return {'cluster_id':query['cluster_id']}
  if path=='/api/2.0/alerts':
   if method=='GET':return {'alerts':[dict(v,id=k) for k,v in self.alerts.items()]}
   self.assert_alert(body);n=str(len(self.alerts)+1);self.alerts[n]=copy.deepcopy(body);return {'id':n}
  if path.startswith('/api/2.0/alerts/'):
   n=path.rsplit('/',1)[1]
   if method=='GET':return dict(self.alerts[n],id=n)
   assert query and query.get('update_mask');assert 'alert' not in body
   self.alerts[n].update(copy.deepcopy(body));return dict(self.alerts[n],id=n)
  if path=='/api/2.2/jobs/list':return {'jobs':[{'job_id':k,'settings':v} for k,v in self.jobs.items()]}
  if path=='/api/2.2/jobs/create':
   parsed=JobSettings.from_dict(body);assert parsed.max_concurrent_runs==1
   n=len(self.jobs)+1;self.jobs[n]=copy.deepcopy(body);return {'job_id':n}
  if path=='/api/2.2/jobs/get':return {'job_id':query['job_id'],'settings':self.jobs[query['job_id']]}
  if path=='/api/2.2/jobs/reset':self.jobs[body['job_id']]=copy.deepcopy(body['new_settings']);return {}
  if path=='/api/2.2/jobs/update':self.jobs[body['job_id']].update(copy.deepcopy(body['new_settings']));return {}
  if path=='/api/2.0/lakeview/dashboards':
   assert query.get('dataset_catalog') and query.get('dataset_schema');self.dashboard=copy.deepcopy(body);return {'dashboard_id':'a'*32}
  if path.startswith('/api/2.0/lakeview/dashboards/'):
   if method=='GET':return dict(self.dashboard,etag='version1')
   assert body['etag']=='version1';self.dashboard=copy.deepcopy(body);return {}
  raise AssertionError((method,path))
 def assert_alert(self,body):
  assert 'alert' not in body
  model=AlertV2.from_dict(body);assert model.evaluation.empty_result_state.value=='ERROR'
class Journal:
 def __init__(self):self.resources={};self.published=[]
 def installation(self):return copy.deepcopy(self.resources)
 def receipt(self,payload):self.resources=copy.deepcopy(payload)
 def append(self,*args):self.published.append(args)
class Spark:
 def __init__(self):self.queries=[]
 def sql(self,q):self.queries.append(q)

class ProvisionTests(unittest.TestCase):
 def setUp(self):
  self.cfg=validate(json.loads((ROOT/'configs/dev.example.json').read_text()));self.cfg.update(warehouse_id='a'*16,existing_cluster_id='0101-123456-approved')
  self.settings=dict(self.cfg['monitor'],environment=self.cfg['environment'],monitor_id=self.cfg['monitor_id'],expected_workspace_id=self.cfg['expected_workspace_id'],scope_signature=self.cfg['scope_signature'],environment_config_json=json.dumps(self.cfg))
  self.api=API();self.client=SimpleNamespace(api_client=self.api,config=SimpleNamespace(host='https://workspace.example.test'))
 def install(self,journal=None):
  j=journal or Journal();r=RUNTIME['configure_resources'](self.client,j,self.settings,self.cfg['scope_signature'],'/Shared/Deploy',self.cfg['warehouse_id'],self.cfg['existing_cluster_id'],['governance@example.test'],[],'https://workspace.example.test/sql/dashboards/'+'a'*32)
  return r,j
 def test_resources_have_digest_batch_binding_and_success_only_receipt(self):
  r,j=self.install();job=self.api.jobs[r['job_id']];self.assertEqual(job['schedule']['pause_status'],'UNPAUSED');self.assertEqual(job['max_concurrent_runs'],1)
  tasks={t['task_key']:t for t in job['tasks']};self.assertEqual(tasks['evaluate_alert']['depends_on'][0]['outcome'],'true')
  self.assertEqual(tasks['evaluate_alert']['max_retries'],0)
  self.assertEqual(tasks['evaluate_alert']['alert_task']['parameters']['monitor_run_id'],'{{tasks.collect.values.monitor_run_id}}')
  self.assertEqual(tasks['record_evaluation']['depends_on'],[{'task_key':'evaluate_alert'}])
  self.assertEqual(self.api.alerts[r['changes_alert_id']]['schedule']['pause_status'],'PAUSED')
  self.assertEqual(self.api.alerts[r['health_alert_id']]['schedule']['pause_status'],'UNPAUSED')
 def test_redeploy_reuses_resource_ids(self):
  first,j=self.install();second,j=self.install(j);self.assertEqual(first['job_id'],second['job_id']);self.assertEqual(len(self.api.alerts),2);self.assertEqual(len(self.api.jobs),1)
 def test_serverless_rejected_before_any_resource_creation(self):
  self.api.serverless=True
  with self.assertRaises(AssertionError):self.install()
  self.assertFalse(any(m=='POST' for m,p,b,q in self.api.calls))
 def test_environment_is_part_of_resource_names_and_job_payload(self):
  self.settings['environment']='STG';r,j=self.install();self.assertIn('STG',self.api.jobs[r['job_id']]['name']);self.assertIn('STG',self.api.alerts[r['changes_alert_id']]['display_name'])
 def test_dashboard_context_is_bound_by_deployment_not_a_viewer_parameter(self):
  j=Journal();spark=Spark();url=RUNTIME['deploy_views_and_dashboard'](spark,self.client,j,self.cfg,compile_sources(self.cfg),build())
  self.assertEqual(len(spark.queries),12);self.assertIn('a'*32,url)
  for q in spark.queries:self.assertTrue(q.startswith('CREATE OR REPLACE VIEW'))
  self.assertFalse(any(':raw_catalog' in ''.join(d['queryLines']) for d in build()['datasets']))
 def test_snapshot_published_in_one_append_not_partial_table_updates(self):
  from test_monitor import run,row
  j=Journal();RUNTIME['Journal'].publish(j,'b'*32,run([row()]))
  self.assertEqual(len(j.published),1);records=j.published[0][1];self.assertEqual(records[0][0],'RUN');self.assertTrue(any(k=='COLUMN' for k,_,_ in records))
 def test_missing_recipients_and_cross_workspace_dashboard_links_rejected(self):
  for recipients,link in [([],''),(['governance@example.test'],'https://other.example.test/sql/dashboards/a')]:
   with self.assertRaises(AssertionError):validate_inputs('a'*16,'valid-cluster',recipients,[],link,'https://workspace.example.test',60)
 def test_configuration_prevents_per_column_mode_and_initial_backlog_flood(self):
  for key,value in [('notification_mode','per_column'),('initial_backlog_notifications',True)]:
   c=copy.deepcopy(self.cfg);c['monitor'][key]=value
   with self.assertRaises(AssertionError):validate(c)

if __name__=='__main__':unittest.main()
