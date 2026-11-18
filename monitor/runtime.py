"""Databricks notebook runtime; embedded by the builder, no live actions here."""
import json,hashlib,uuid,re
from datetime import datetime,timezone,timedelta

class Journal:
    def __init__(self,spark,table,monitor_id):
        self.spark=spark;self.table=table;self.monitor_id=monitor_id
        assert len(table.split('.'))==3;quoted(table)
        self.schema='.'.join(table.split('.')[:-1])
    def initialize(self):
        for statement in MONITOR_DDL.split(';'):
            if statement.strip():self.spark.sql(statement)
        other=self.spark.sql("SELECT count(*) AS n FROM "+self.table+" WHERE monitor_id <> '"+self.monitor_id+"'").first()['n']
        assert other==0,'Use a dedicated output schema for each environment/deployment; logical view names must not overlap.'
        detail=self.spark.sql('DESCRIBE DETAIL '+self.table).first().asDict()
        assert detail.get('format')=='delta' and detail.get('properties',{}).get('ifbi.component')=='classification_monitor_v2','Monitor table identity/type mismatch.'
    def append(self,run_id,records,transaction):
        from pyspark.sql.types import StructType,StructField,StringType,TimestampType
        observed=datetime.now(timezone.utc).replace(tzinfo=None)
        fields=[StructField(n,StringType(),False) for n in ('monitor_id','run_id','record_id','record_type')]+[StructField('observed_at',TimestampType(),False),StructField('payload_json',StringType(),False)]
        data=[(self.monitor_id,run_id,record_id,kind,observed,json.dumps(payload,sort_keys=True,default=str)) for kind,record_id,payload in records]
        self.spark.createDataFrame(data,StructType(fields)).write.format('delta').mode('append').option('txnAppId',self.monitor_id+'.'+transaction).option('txnVersion',0).saveAsTable(self.table)
    def rows(self,where,maximum=300000):
        assert maximum<=500000
        result=self.spark.sql('SELECT * FROM '+self.table+" WHERE monitor_id='"+self.monitor_id+"' AND "+where).limit(maximum+1).collect()
        assert len(result)<=maximum,'Monitor state budget exceeded; review retention and scope.'
        return [r.asDict() for r in result]
    def latest(self,kind):
        rows=self.rows("record_type='"+kind+"' ORDER BY observed_at DESC,run_id DESC LIMIT 1",1)
        return rows[0] if rows else None
    def load(self,maximum_columns):
        latest=self.latest('RUN')
        if not latest:return None,[],None
        head=json.loads(latest['payload_json']);state={'control':head['control'],'summary':head['summary'],'columns':{},'issues':{},'pending':{}}
        run_id=latest['run_id'];assert re.fullmatch(r'[0-9a-f]{32}',run_id)
        for r in self.rows("run_id='"+run_id+"' AND record_type IN ('COLUMN','ISSUE','NOTICE')"):
            payload=json.loads(r['payload_json']);target={'COLUMN':'columns','ISSUE':'issues','NOTICE':'pending'}[r['record_type']]
            state[target][r['record_id']]=payload
        assert len(state['columns'])<=maximum_columns
        cutoff=latest['observed_at'].strftime('%Y-%m-%d %H:%M:%S.%f')
        acks=[dict(notice_id=json.loads(r['payload_json'])['notice_id'],accepted_at=r['observed_at'].replace(tzinfo=timezone.utc).isoformat())
              for r in self.rows("record_type='ACK' AND observed_at>=TIMESTAMP '"+cutoff+"'")]
        return state,acks,latest
    def publish(self,run_id,state):
        entries=[('RUN',run_id,{'control':state['control'],'summary':state['summary']})]
        for kind,key in [('COLUMN','columns'),('ISSUE','issues'),('NOTICE','pending')]:
            entries.extend((kind,record_id,payload) for record_id,payload in state[key].items())
        self.append(run_id,entries,'publish.'+run_id)
    def failure(self,run_id,exc):
        # Do not log tokens, SQL result samples or arbitrary exception messages.
        message=str(exc) if isinstance(exc,ScopeError) else type(exc).__name__
        self.append(run_id,[('FAILURE',run_id,{'error_type':type(exc).__name__,'diagnostic':message[:1500]})],'failure.'+run_id)
    def installation(self):
        row=self.latest('INSTALLATION');return json.loads(row['payload_json']) if row else {}
    def receipt(self,data):
        run_id=uuid.uuid4().hex;self.append(run_id,[('INSTALLATION',run_id,data)],'install.'+run_id)

def read_identity(spark,client):
    return {'workspace_id':str(client.get_workspace_id()),'metastore_id':str(spark.sql('SELECT current_metastore() AS id').first()['id']),
            'metadata_reader':str(spark.sql('SELECT current_user() AS actor').first()['actor'])}

def collect_snapshot(spark,journal,client,settings,scope_hash,run_id,baseline_only=False):
    prior,acks,last=journal.load(settings['maximum_columns'])
    # A repaired/retried task reuses the exact committed run and notice batch.
    if last and last['run_id']==run_id:return prior
    if not baseline_only and not prior:raise ScopeError('BASELINE_REQUIRED')
    identity=read_identity(spark,client)
    if identity['workspace_id']!=settings['expected_workspace_id']:raise ScopeError('WRONG_WORKSPACE')
    if prior and (prior['control']['identity']!=identity or prior['control']['scope_hash']!=scope_hash):raise ScopeError('MONITOR_IDENTITY_OR_SCOPE_CHANGED')
    if baseline_only and prior:return prior
    source=spark.sql(COLUMN_QUERY).limit(settings['maximum_columns']+1).collect()
    rows=[r.asDict(recursive=True) for r in source]
    result=evaluate(rows,prior,datetime.now(timezone.utc),identity,scope_hash,settings,acks)
    journal.publish(run_id,result)
    return result

def record_evaluation(journal,run_id):
    assert re.fullmatch(r'[0-9a-f]{32}',run_id),'Invalid source run ID.'
    notices=journal.rows("run_id='"+run_id+"' AND record_type='NOTICE'")
    records=[]
    for row in notices:
        p=json.loads(row['payload_json'])
        if p.get('selected_for_batch'):
            records.append(('ACK',p['notice_id'],{'notice_id':p['notice_id'],'issue_key':p['issue_key'],
                          'generation':p['generation'],'kind':p['kind'],'source_run_id':run_id,
                          'meaning':'SQL alert evaluation succeeded; recipient delivery is not attested'}))
    if records:journal.append(run_id,records,'ack.'+run_id)
    return len(records)

def configure_resources(client,journal,settings,scope_hash,notebook_path,warehouse,cluster,emails,destinations,dashboard_url):
    validate_inputs(warehouse,cluster,emails,destinations,dashboard_url,client.config.host,settings['poll_minutes'])
    # Pro SQL warehouse only: no serverless compute is selected by this installer.
    wh=client.api_client.do('GET','/api/2.0/sql/warehouses/'+warehouse)
    assert wh.get('warehouse_type')=='PRO' and not wh.get('enable_serverless_compute',False),'Select the existing approved non-serverless Pro warehouse.'
    info=client.api_client.do('GET','/api/2.0/clusters/get',query={'cluster_id':cluster})
    assert info.get('cluster_id')==cluster,'Existing cluster identity mismatch.'
    client.api_client.do('GET','/api/2.0/alerts',query={'page_size':1})
    # Find deterministic resource names before creating, including after a lost response.
    resources=journal.installation();short=digest([settings['monitor_id'],scope_hash])[:8]
    main_name='Classification Changes - '+settings['environment']+' '+short
    health_name='Classification Monitor Health - '+settings['environment']+' '+short
    def find_alert(name):
        found=[];token=None
        while True:
            q={'page_size':100}
            if token:q['page_token']=token
            page=client.api_client.do('GET','/api/2.0/alerts',query=q)
            found.extend(a['id'] for a in page.get('alerts',[]) if a.get('display_name')==name)
            token=page.get('next_page_token')
            if not token:break
        assert len(found)<=1,'Multiple managed alerts with the same name; inspect before continuing.'
        return found[0] if found else None
    link=dashboard_url or client.config.host.rstrip('/')+'/sql/dashboards'
    bodies={
      'changes':alert_definition(main_name,ALERT_QUERY,warehouse,emails,destinations,
        'Classification changes requiring attention. NEW is the first notice, REMINDER is unresolved work, and RECOVERY is a resolved incident. Owner is the observed UC table owner; subscribers are the configured governance team.\n\nDashboard: '+link+'\n\nNotice IDs support deduplication; this digest contains at most '+str(settings['maximum_notifications_per_run'])+' events grouped by owner/table/finding (up to 99 groups shown).',poll=settings['poll_minutes']),
      'health':alert_definition(health_name,HEALTH_QUERY,warehouse,emails,destinations,
        'The classification monitor needs operational attention. Check the managed job run, its metadata visibility and pending alert evaluations.\n\nDashboard: '+link,heartbeat=True,poll=settings['poll_minutes'])}
    for key,name in [('changes',main_name),('health',health_name)]:
        stored=resources.get(key+'_alert_id') or find_alert(name)
        if stored:
            old=client.api_client.do('GET','/api/2.0/alerts/'+stored)
            assert old.get('display_name')==name,'Managed alert identity mismatch.'
            client.api_client.do('PATCH','/api/2.0/alerts/'+stored,body=bodies[key],query={'update_mask':','.join(bodies[key])})
        else:
            response=client.api_client.do('POST','/api/2.0/alerts',body=bodies[key])
            stored=response['id']
        persisted=client.api_client.do('GET','/api/2.0/alerts/'+stored)
        if key=='changes':
            assert any(p.get('name')=='monitor_run_id' for p in persisted.get('parameters',[])),'This workspace did not preserve the required alert parameter. Do not activate the monitor.'
        resources[key+'_alert_id']=stored;journal.receipt(resources)
    job_settings=dict(settings,scope_signature=scope_hash)
    job=job_definition(settings['monitor_id'],notebook_path,cluster,warehouse,resources['changes_alert_id'],emails,destinations,job_settings)
    stored=resources.get('job_id')
    if not stored:
        page=client.api_client.do('GET','/api/2.2/jobs/list',query={'name':job['name'],'expand_tasks':False})
        found=[x['job_id'] for x in page.get('jobs',[]) if x.get('settings',{}).get('tags',{}).get('monitor_id')==settings['monitor_id']]
        assert len(found)<=1,'Multiple managed monitor jobs found.'
        stored=found[0] if found else None
    if stored:
        old=client.api_client.do('GET','/api/2.2/jobs/get',query={'job_id':stored})
        assert old.get('settings',{}).get('tags',{}).get('monitor_id')==settings['monitor_id'],'Managed job identity mismatch.'
        client.api_client.do('POST','/api/2.2/jobs/reset',body={'job_id':stored,'new_settings':job})
    else:stored=client.api_client.do('POST','/api/2.2/jobs/create',body=job)['job_id']
    persisted_job=client.api_client.do('GET','/api/2.2/jobs/get',query={'job_id':stored})
    task=next(t for t in persisted_job.get('settings',{}).get('tasks',[]) if t['task_key']=='evaluate_alert')
    assert task.get('alert_task',{}).get('parameters',{}).get('monitor_run_id')=='{{tasks.collect.values.monitor_run_id}}','Workspace did not preserve the alert task batch binding.'
    resources.update(job_id=stored,poll_minutes=settings['poll_minutes'],warehouse_id=warehouse,cluster_id=cluster,
                     notebook_path=notebook_path,scope_hash=scope_hash,subscribers=emails,destination_ids=destinations,environment=settings['environment'])
    journal.receipt(resources)
    # Activate only after baseline, job and both alert definitions exist.
    active_schedule=dict(job['schedule'],pause_status='UNPAUSED')
    client.api_client.do('POST','/api/2.2/jobs/update',body={'job_id':stored,'new_settings':{'schedule':active_schedule}})
    health=dict(bodies['health']);health['schedule']=dict(health['schedule'],pause_status='UNPAUSED')
    client.api_client.do('PATCH','/api/2.0/alerts/'+resources['health_alert_id'],body=health,query={'update_mask':'schedule'})
    resources['activated_at']=stamp(datetime.now(timezone.utc));journal.receipt(resources)
    return resources

def deploy_views_and_dashboard(spark,client,journal,config,source_queries,dashboard):
    prefix=config['output_catalog']+'.'+config['output_schema']
    monitor_sql=build_monitor_queries(config['state_table'],config['monitor_id'],max(60,config['monitor']['poll_minutes']*2+15))
    all_queries=dict(source_queries,monitor_health=monitor_sql['health'],monitor_incidents=monitor_sql['incidents'],monitor_notifications=monitor_sql['notices'],monitor_daily_trends=monitor_sql['trends'])
    for name,query in all_queries.items():
        name='raw_classification_'+name
        spark.sql('CREATE OR REPLACE VIEW '+quoted(prefix+'.'+name)+' AS SELECT '+literal(config['environment'])+' AS environment, q.* FROM (\n'+query+'\n) q')
    resources=journal.installation()
    dashboard_id=config.get('dashboard_id') or resources.get('dashboard_id')
    title='Raw Classification Assurance - '+config['environment']
    query={'dataset_catalog':config['output_catalog'],'dataset_schema':config['output_schema']}
    body={'display_name':title,'warehouse_id':config['warehouse_id'],'serialized_dashboard':json.dumps(dashboard)}
    if dashboard_id:
        assert re.fullmatch(r'[A-Za-z0-9-]+',dashboard_id)
        old=client.api_client.do('GET','/api/2.0/lakeview/dashboards/'+dashboard_id)
        assert old.get('etag'),'Dashboard update requires a current version guard.'
        body['etag']=old['etag']
        client.api_client.do('PATCH','/api/2.0/lakeview/dashboards/'+dashboard_id,query=query,body=body)
    else:
        if config.get('dashboard_parent_path'):body['parent_path']=config['dashboard_parent_path']
        # Creation is not retried by this code after an uncertain outcome.
        response=client.api_client.do('POST','/api/2.0/lakeview/dashboards',query=query,body=body)
        dashboard_id=response['dashboard_id']
    resources['dashboard_id']=dashboard_id;journal.receipt(resources)
    return client.config.host.rstrip('/')+'/sql/dashboards/'+dashboard_id

def retain_journal(journal,days):
    assert 7<=days<=365
    latest=journal.latest('RUN')
    if not latest:return
    cutoff=(datetime.now(timezone.utc)-timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
    journal.spark.sql("DELETE FROM "+journal.table+" WHERE monitor_id='"+journal.monitor_id+"' AND record_type<>'INSTALLATION' AND observed_at<TIMESTAMP '"+cutoff+"' AND run_id<>'"+latest['run_id']+"'")
