import copy,datetime,json,sys,unittest
from pathlib import Path
import duckdb,sqlglot
from sqlglot import exp
from databricks.labs.lsql.lakeview import Dashboard
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from compiler import compile_sources
from configuration import validate,literal
from build_dashboard import build
from monitor.queries import build as monitor_queries
from test_monitor import run,row,NOW

class SQLTests(unittest.TestCase):
 def setUp(self):
  self.cfg=validate(json.loads((ROOT/'configs/dev.example.json').read_text()));self.c=duckdb.connect()
  self.c.execute('CREATE TABLE fixture_columns(table_catalog VARCHAR,table_schema VARCHAR,table_name VARCHAR,column_name VARCHAR,data_type VARCHAR,ordinal_position INTEGER,comment VARCHAR)')
  self.c.execute('CREATE TABLE fixture_tables(table_catalog VARCHAR,table_schema VARCHAR,table_name VARCHAR,table_type VARCHAR,table_owner VARCHAR)')
  self.c.execute('CREATE TABLE fixture_tags(catalog_name VARCHAR,schema_name VARCHAR,table_name VARCHAR,column_name VARCHAR,tag_name VARCHAR,tag_value VARCHAR)')
  self.c.execute('CREATE TABLE fixture_mapping(source_tag_name VARCHAR,source_tag_value VARCHAR,collate_fqn VARCHAR,ifbi_classification VARCHAR,pii_class VARCHAR,tier_priority INTEGER)')
  self.c.execute('CREATE TABLE fixture_assignments(assignment_id VARCHAR,source_scope VARCHAR,source_service_fqn VARCHAR,asset_fqn VARCHAR,field_fqn VARCHAR,term_fqn VARCHAR,assignment_level VARCHAR,label_state VARCHAR,term_is_deleted BOOLEAN,asset_is_deleted BOOLEAN,last_seen_at TIMESTAMP,asset_updated_at_ts TIMESTAMP,last_run_id VARCHAR,source_payload_sha256 VARCHAR)')
  self.c.execute('CREATE TABLE fixture_journal(monitor_id VARCHAR,run_id VARCHAR,record_id VARCHAR,record_type VARCHAR,observed_at TIMESTAMP,payload_json VARCHAR)')
  self.replace={'system.information_schema.columns':'fixture_columns','system.information_schema.tables':'fixture_tables','system.information_schema.column_tags':'fixture_tags',
   self.cfg['scopes'][0]['mapping_table']:'fixture_mapping',self.cfg['scopes'][0]['collate_assignment_table']:'fixture_assignments',self.cfg['state_table']:'fixture_journal'}
 def tearDown(self):self.c.close()
 def sql(self,q):
  ast=sqlglot.parse_one(q.replace(':monitor_run_id',"'run2'"),read='databricks')
  for t in ast.find_all(exp.Table):
   if t.catalog:
    name='.'.join((t.catalog,t.db,t.name));self.assertIn(name,self.replace)
    t.set('this',exp.to_identifier(self.replace[name]));t.set('db',None);t.set('catalog',None)
  for function in ast.find_all(exp.Anonymous):
   if function.name.lower()=='raise_error':function.set('this','error')
  return ast.sql(dialect='duckdb')
 def fetch(self,q):
  cur=self.c.execute(self.sql(q));names=[d[0] for d in cur.description];return [dict(zip(names,r)) for r in cur.fetchall()]
 def source(self,catalog=None):
  cat=catalog or self.cfg['scopes'][0]['catalog']
  self.c.execute('INSERT INTO fixture_columns VALUES (?,?,?,?,?,?,?)',[cat,'source','party','ssn','STRING',1,None])
  self.c.execute('INSERT INTO fixture_tables VALUES (?,?,?,?,?)',[cat,'source','party','MANAGED','governance_owner'])
 def tagged(self,key,value,catalog=None):self.c.execute('INSERT INTO fixture_tags VALUES (?,?,?,?,?,?)',[catalog or self.cfg['scopes'][0]['catalog'],'source','party','ssn',key,value])
 def mapping(self,tier='Confidential Critical',pii='critical',priority=1):self.c.execute('INSERT INTO fixture_mapping VALUES (?,?,?,?,?,?)',['General','SSN','General.SSN',tier,pii,priority])
 def journal(self,state,run_id='run2',at=None):
  at=at or datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
  entries=[('RUN',run_id,{'control':state['control'],'summary':state['summary']})]
  for kind,key in [('COLUMN','columns'),('ISSUE','issues'),('NOTICE','pending')]:entries.extend((kind,k,v) for k,v in state[key].items())
  self.c.executemany('INSERT INTO fixture_journal VALUES (?,?,?,?,?,?)',[(self.cfg['monitor_id'],run_id,k,kind,at,json.dumps(v)) for kind,k,v in entries])
 def test_all_sources_and_widgets_execute(self):
  self.source();self.mapping();self.tagged('General','SSN');self.tagged('ifbi_sensitivity','Internal Use Only')
  sources=compile_sources(self.cfg)
  mon=monitor_queries(self.cfg['state_table'],self.cfg['monitor_id'],135)
  state=run([row(tier=1)]);self.journal(state)
  queries=dict(sources,monitor_health=mon['health'],monitor_incidents=mon['incidents'],monitor_notifications=mon['notices'],monitor_daily_trends=mon['trends'])
  for n,q in queries.items():
   self.c.execute('CREATE VIEW raw_classification_'+n+' AS SELECT '+literal(self.cfg['environment'])+' AS environment,q.* FROM ('+self.sql(q)+') q')
  for d in build()['datasets']:
   self.c.execute('CREATE VIEW '+d['name']+' AS '+''.join(d['queryLines']))
   self.c.execute('SELECT * FROM '+d['name']).fetchall()
  for page in build()['pages']:
   for item in page['layout']:
    for query in item['widget'].get('queries',[]):
     q=query['query'];fields=q['fields'];asts=[sqlglot.parse_one(f['expression'],read='databricks') for f in fields]
     parts=[a.sql(dialect='duckdb')+' AS "'+f['name']+'"' for a,f in zip(asts,fields)]
     sql='SELECT '+','.join(parts)+' FROM '+q['datasetName']
     group=[str(i+1) for i,a in enumerate(asts) if not a.find(exp.AggFunc)]
     if any(a.find(exp.AggFunc) for a in asts) and group:sql+=' GROUP BY '+','.join(group)
     self.c.execute(sql).fetchall()
 def test_different_environment_catalog_and_taxonomy(self):
  c=copy.deepcopy(self.cfg);c.update(environment='STG',expected_workspace_id='9999',output_catalog='governance_stage')
  c['scopes']=[dict(catalog='raw_stage',excluded_schemas=['information_schema'],mapping_table='refs_stage.security.rules',collate_assignment_table=None,collate_service=None)]
  c['taxonomy']=[dict(classification='RestrictedData',pii_class='restricted_data',priority=1),dict(classification='OrdinaryData',pii_class='ordinary',priority=9)]
  self.replace['refs_stage.security.rules']='fixture_mapping';self.source('raw_stage');self.mapping('RestrictedData','restricted_data',1);self.tagged('General','SSN','raw_stage');self.tagged('ifbi_sensitivity','OrdinaryData','raw_stage')
  q=compile_sources(c);r=self.fetch(q['column_inventory'])[0]
  self.assertEqual(r['catalog_name'],'raw_stage');self.assertEqual(r['expected_sensitivity'],'RestrictedData');self.assertEqual(r['action_priority'],'P1 - Correct configuration')
  f=self.fetch(q['collate_feed_health'])[0];self.assertEqual(f['feed_outcome'],'Collate assignment source not configured')
 def test_multi_catalog_inventory_has_no_environment_cross_join(self):
  c=copy.deepcopy(self.cfg);scope=copy.deepcopy(c['scopes'][0]);scope.update(catalog='second_raw',collate_assignment_table=None,collate_service=None);c['scopes'].append(scope)
  self.source();self.source('second_raw');r=self.fetch(compile_sources(c)['column_inventory']);self.assertEqual(len(r),2);self.assertEqual({x['catalog_name'] for x in r},{self.cfg['scopes'][0]['catalog'],'second_raw'})
 def test_multiple_sensitivity_values_are_a_conflict_not_a_chosen_max(self):
  self.source();self.tagged('ifbi_sensitivity','Internal Use Only');self.tagged('ifbi_sensitivity','Confidential Critical')
  r=self.fetch(compile_sources(self.cfg)['column_inventory']);self.assertEqual(len(r),1);self.assertEqual(r[0]['sensitivity_state'],'Conflicting sensitivity values')
 def test_no_auto_class_alias_and_duplicate_mappings_do_not_inflate(self):
  self.source();self.mapping();self.mapping();self.tagged('auto_class','SSN')
  r=self.fetch(compile_sources(self.cfg)['column_inventory']);self.assertEqual(len(r),1);self.assertIsNone(r[0]['expected_priority']);self.assertEqual(r[0]['unmapped_source_label_count'],1)
 def test_current_collate_assignment_uses_configured_identity_and_mapping(self):
  self.source();self.mapping();self.tagged('ifbi_sensitivity','Internal Use Only')
  service=self.cfg['scopes'][0]['collate_service'];fqn=service+'.'+self.cfg['scopes'][0]['catalog']+'.source.party'
  now=datetime.datetime.now()
  self.c.execute('INSERT INTO fixture_assignments VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',['a1','source',service,fqn,fqn+'.ssn','General.SSN','column','Confirmed',False,False,now,now,'run','hash'])
  q=compile_sources(self.cfg);r=self.fetch(q['column_inventory'])[0]
  self.assertEqual(r['reconciliation_basis'],'Collate mapped glossary');self.assertEqual(r['expected_priority'],1)
  self.c.execute('UPDATE fixture_assignments SET last_seen_at=?',[now-datetime.timedelta(days=30)])
  r=self.fetch(q['column_inventory'])[0];self.assertIsNone(r['expected_priority']);self.assertEqual(r['usable_collate_assignments'],0)
 def test_portable_layout_has_unique_names_and_no_overlaps(self):
  names=[]
  for page in build()['pages']:
   for i,a in enumerate(page['layout']):
    names.append(a['widget']['name']);p=a['position'];self.assertLessEqual(p['x']+p['width'],12)
    for b in page['layout'][i+1:]:
     q=b['position'];overlap=p['x']<q['x']+q['width'] and q['x']<p['x']+p['width'] and p['y']<q['y']+q['height'] and q['y']<p['y']+p['height']
     self.assertFalse(overlap,(a['widget']['name'],b['widget']['name']))
  self.assertEqual(len(names),len(set(names)))
 def test_digest_collapses_many_columns_to_one_table_group(self):
  baseline=run([row('anchor')]);grace=run([row('anchor')]+[row('new'+str(i)) for i in range(100)],baseline,1)
  alerts=run([row('anchor')]+[row('new'+str(i)) for i in range(100)],grace,32);self.journal(alerts)
  mon=monitor_queries(self.cfg['state_table'],self.cfg['monitor_id'],135)
  records=self.fetch(mon['alert']);self.assertEqual(len(records),1);self.assertEqual(records[0]['notice_count'],100);self.assertEqual(records[0]['affected_events'],100)
 def test_acknowledged_batch_cannot_be_emailed_again_by_repair(self):
  b=run([row(tier=1)]);a=run([row()],b,1);self.journal(a)
  for n in a['pending']:
   self.c.execute('INSERT INTO fixture_journal VALUES (?,?,?,?,?,?)',[self.cfg['monitor_id'],'run2',n,'ACK',datetime.datetime.now(),json.dumps({'notice_id':n})])
  m=monitor_queries(self.cfg['state_table'],self.cfg['monitor_id'],135);r=self.fetch(m['alert']);self.assertEqual(r[0]['notice_count'],0)
 def test_watchdog_flags_old_success_and_recent_failure(self):
  self.journal(run([row()]),at=datetime.datetime.now()-datetime.timedelta(hours=5))
  m=monitor_queries(self.cfg['state_table'],self.cfg['monitor_id'],135);self.assertEqual(self.fetch(m['health'])[0]['attention_required'],1)
 def test_missing_native_alert_parameter_fails_instead_of_acknowledging_silently(self):
  self.journal(run([row()]))
  q=monitor_queries(self.cfg['state_table'],self.cfg['monitor_id'],135)['alert']
  q=q.replace(':monitor_run_id',"'__NO_RUN__'")
  with self.assertRaisesRegex(Exception,'not bound'):self.fetch(q)
 def test_initial_backlog_is_not_in_detailed_action_queue(self):
  self.source();q=compile_sources(self.cfg);self.assertEqual(len(self.fetch(q['column_inventory'])),1)
  # SSN name hint is a targeted review item, ordinary names are aggregate backlog.
  self.c.execute("UPDATE fixture_columns SET column_name='ordinary_field'")
  self.assertFalse(self.fetch(q['actionable_inventory']))
 def test_portable_dashboard_has_no_catalog_workspace_or_count_bindings(self):
  text=json.dumps(build())
  for forbidden in ['ifbi_edw_dev','7474655731975614','7340','7,340','7847','7,847']:
   self.assertNotIn(forbidden,text)
  model=Dashboard.from_dict(build());self.assertEqual(len(model.datasets),12);self.assertEqual(len(model.pages),10)
 def test_read_only_source_queries_and_no_raw_payloads(self):
  for q in compile_sources(self.cfg).values():
   a=sqlglot.parse_one(q,read='databricks');self.assertIsInstance(a,exp.Select);self.assertFalse(a.find(exp.Insert));self.assertFalse(a.find(exp.Create));self.assertNotIn('source_payload_json',q)

if __name__=='__main__':unittest.main()
