"""Deployment-time bindings. No DEV/PRD catalog, workspace or recipient defaults."""
import copy,hashlib,json,re

def quoted(name):
    assert isinstance(name,str) and all(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',s) for s in name.split('.')),'Use plain UC identifiers in configuration: '+str(name)
    return '.'.join('`'+s+'`' for s in name.split('.'))
def literal(value):return "'"+str(value).replace("'","''")+"'"
def signature(cfg):
    keys=('environment','deployment_id','expected_workspace_id','output_catalog','output_schema','scopes','sensitivity_tag','legacy_pii_tag','source_classification_tag_keys','native_tag_prefix','taxonomy')
    return hashlib.sha256(json.dumps({k:cfg[k] for k in keys},sort_keys=True,separators=(',',':')).encode()).hexdigest()
def monitor_id(cfg):
    identity=[cfg[k] for k in ('deployment_id','environment','expected_workspace_id','output_catalog','output_schema')]
    return cfg['deployment_id']+'_'+cfg['environment'].lower()+'_'+hashlib.sha256(json.dumps(identity).encode()).hexdigest()[:12]
def state_table(cfg):return cfg['output_catalog']+'.'+cfg['output_schema']+'.classification_monitor_journal'
def validate(cfg,require_runtime=False):
    c=copy.deepcopy(cfg)
    c['environment']=c['environment'].upper()
    c['expected_workspace_id']=str(c.get('expected_workspace_id',''))
    assert isinstance(c['notifications']['emails'],list) and isinstance(c['notifications']['destination_ids'],list)
    c['notifications']['emails']=sorted(set(c['notifications']['emails']))
    c['notifications']['destination_ids']=sorted(set(c['notifications']['destination_ids']))
    assert re.fullmatch(r'[A-Za-z0-9_-]+',c['environment'])
    assert re.fullmatch(r'[A-Za-z0-9_-]+',c['deployment_id'])
    quoted(c['output_catalog']);quoted(c['output_schema'])
    assert c['scopes'] and len(c['scopes'])<=50
    seen=set()
    for scope in c['scopes']:
        quoted(scope['catalog']);assert scope['catalog'] not in seen,'Duplicate source catalog';seen.add(scope['catalog'])
        assert len(scope['mapping_table'].split('.'))==3;quoted(scope['mapping_table'])
        if scope.get('collate_assignment_table'):
            assert len(scope['collate_assignment_table'].split('.'))==3;quoted(scope['collate_assignment_table'])
            assert scope.get('collate_service'),'Specify the expected Collate service for the source.'
        if scope['catalog']==c['output_catalog']:
            scope['excluded_schemas']=sorted(set(scope.get('excluded_schemas',[]))|{c['output_schema'],'information_schema'})
        else:scope['excluded_schemas']=sorted(set(scope.get('excluded_schemas',[]))|{'information_schema'})
    taxonomy=c['taxonomy'];assert len(taxonomy)>=2
    assert len({t['classification'] for t in taxonomy})==len(taxonomy)
    assert len({t['priority'] for t in taxonomy})==len(taxonomy)
    assert all(isinstance(t['priority'],int) and t['priority']>0 and t['pii_class'] for t in taxonomy)
    m=c['monitor'];assert m['poll_minutes'] in (15,30,60)
    assert 0<=m['classification_grace_minutes']<=1440
    assert 1<=m['reminder_hours']<=720
    assert 1<=m['maximum_notifications_per_run']<=5000
    assert 1<=m['maximum_columns']<=100000
    assert 0<=m['maximum_inventory_drop_fraction']<=0.5
    assert 7<=m['retention_days']<=365
    assert 0<=m.get('daily_summary_hour_utc',13)<=23
    assert m['notification_mode']=='digest','This framework sends digests, never one email per column.'
    assert 0<c['collate_freshness_days']<=365
    assert m['initial_backlog_notifications'] is False,'Initial backlog is summarized, never emitted as per-column alerts.'
    if require_runtime:
        assert re.fullmatch(r'\d+',str(c.get('expected_workspace_id',''))),'Set this environment\'s workspace ID.'
        assert c.get('warehouse_id') and c.get('existing_cluster_id'),'Set approved warehouse and existing Spark cluster IDs.'
        assert c['notifications']['emails'] or c['notifications']['destination_ids'],'Configure approved recipients or destination IDs.'
    c['scope_signature']=signature(c);c['monitor_id']=monitor_id(c);c['state_table']=state_table(c)
    return c
