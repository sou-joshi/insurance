"""Stateful metadata monitor. Pure Python: no Databricks calls and no sending."""
from __future__ import annotations
import copy,hashlib,json
from datetime import datetime,timedelta,timezone

VERSION='classification_monitor_v2'
OPEN_STATES={'OPEN','BACKLOG','GRACE','EVIDENCE_GAP'}

def stamp(t):
    if t.tzinfo is None:t=t.replace(tzinfo=timezone.utc)
    return t.astimezone(timezone.utc).isoformat()

def moment(t):
    if isinstance(t,datetime):return t.replace(tzinfo=timezone.utc) if t.tzinfo is None else t.astimezone(timezone.utc)
    return datetime.fromisoformat(t.replace('Z','+00:00')).astimezone(timezone.utc)

def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),default=str).encode()).hexdigest()
def asset_key(row):return digest([row[k] for k in ('catalog_name','schema_name','table_name','column_name')])
def issue_key(asset,finding):return digest([asset,finding])

class ScopeError(RuntimeError):pass

def compact(row):
    keys=('catalog_name','schema_name','table_name','column_name','data_type','current_sensitivity',
          'sensitivity_state','actual_priority','expected_priority','expected_sensitivity',
          'remediation_owner','invalid_rule_label_count','name_hint','reconciliation_outcome')
    result={k:row.get(k) for k in keys}
    result['column_fqn']='.'.join(str(result[k]) for k in ('catalog_name','schema_name','table_name','column_name'))
    return result

def evaluate(rows,previous,now,identity,scope_hash,settings,acknowledgements=()):
    """Return a complete new snapshot; caller publishes it in one Delta append.

    Existing baseline findings are muted. Acknowledgements mean SQL alert
    evaluation accepted, not delivery/read confirmation. Unknown visibility
    and lost mapped evidence never silently resolve an incident.
    """
    now=moment(now);now_text=stamp(now)
    if not rows:raise ScopeError('ZERO_VISIBLE_COLUMNS: check scope and metadata permissions')
    if len(rows)>settings['maximum_columns']:raise ScopeError('COLUMN_BUDGET_EXCEEDED')
    observed={asset_key(r):compact(r) for r in rows}
    if len(observed)!=len(rows):raise ScopeError('DUPLICATE_COLUMN_IDENTITIES')
    schemas={r['catalog_name']+'.'+r['schema_name'] for r in rows}
    first=previous is None
    if first:
        state={'control':{'version':VERSION,'identity':identity,'scope_hash':scope_hash,
                          'baseline_at':now_text,'required_schemas':sorted(schemas)},
               'columns':{},'issues':{},'pending':{}}
    else:
        state=copy.deepcopy(previous)
        if state['control']['identity']!=identity:raise ScopeError('MONITOR_IDENTITY_CHANGED')
        if state['control']['scope_hash']!=scope_hash:raise ScopeError('MONITOR_SCOPE_CHANGED: review before creating a new monitor baseline')
        missing=set(state['control']['required_schemas'])-schemas
        if missing:raise ScopeError('EXPECTED_SCHEMA_NOT_VISIBLE: '+','.join(sorted(missing)))
        prior_count=sum(bool(c.get('observed')) for c in state['columns'].values())
        if prior_count and len(rows)<prior_count*(1-settings['maximum_inventory_drop_fraction']):
            raise ScopeError('VISIBLE_COLUMN_COUNT_DROPPED: retain previous snapshot and check scope/permissions')
    issues=state['issues'];pending=state['pending'];columns=state['columns']
    # Apply accepted evaluation receipts before deciding reminders/recovery.
    for ack in acknowledgements:
        notice=pending.pop(ack['notice_id'],None)
        if not notice:continue
        issue=issues.get(notice['issue_key'])
        if issue and issue['generation']==notice['generation']:
            issue['last_accepted_at']=ack['accepted_at']
            if notice['kind']!='RECOVERY':issue['ever_announced']=True
    # Track first observation and strongest observed valid tier across scans.
    for key,old in columns.items():old['observed']=False
    for key,row in observed.items():
        old=columns.get(key)
        recognized=row['sensitivity_state']=='Recognized sensitivity value'
        valid_priority=row['actual_priority'] if recognized else None
        columns[key]=dict(row,first_seen_at=old['first_seen_at'] if old else now_text,
            origin=old['origin'] if old else ('BASELINE' if first else 'NEW'),
            baseline_findings=dict((old or {}).get('baseline_findings',{})),
            baseline_missing=(old or {}).get('baseline_missing',first and row['sensitivity_state']=='Missing sensitivity tag'),
            ever_tagged=bool((old or {}).get('ever_tagged')) or row['sensitivity_state']!='Missing sensitivity tag',
            protection_floor=min(x for x in [valid_priority,(old or {}).get('protection_floor')] if x is not None)
                if any(x is not None for x in [valid_priority,(old or {}).get('protection_floor')]) else None,
            observed=True,last_seen_at=now_text)
    candidates={}
    def add(asset,kind,priority,detail,grace=False,required=None,hold=False):
        row=columns[asset];key=issue_key(asset,kind)
        candidates[key]={'issue_key':key,'asset_key':asset,'finding_type':kind,'priority':priority,
             'column_fqn':row['column_fqn'],'catalog_name':row['catalog_name'],'schema_name':row['schema_name'],
             'table_name':row['table_name'],'column_name':row['column_name'],
             'owner':row.get('remediation_owner') or 'Assign steward','current_sensitivity':row.get('current_sensitivity'),
             'expected_sensitivity':row.get('expected_sensitivity'),'required_priority':required,
             'detail':detail,'grace':grace,'evidence_hold':hold}
    for key,row in columns.items():
        if not row['observed']:
            add(key,'COLUMN_NOT_OBSERVED','P2','Column was previously visible; check deletion or reduced metadata visibility. Existing findings remain open.')
            for k,old in issues.items():
                if old['asset_key']==key and old['status'] in OPEN_STATES and old['finding_type']!='COLUMN_NOT_OBSERVED':
                    add(key,old['finding_type'],old['priority'],'Column evidence is unavailable; prior incident retained for review.',required=old.get('required_priority'),hold=True)
            continue
        status=row['sensitivity_state'];actual=row['actual_priority'];required=row['expected_priority']
        if status=='Missing sensitivity tag':
            if row['ever_tagged']:
                add(key,'SENSITIVITY_TAG_REMOVED','P1','Previously observed sensitivity tag is absent.',required=required)
            elif row['origin']=='NEW':
                add(key,'NEW_COLUMN_UNTAGGED','P1' if required is not None else 'P2',
                    'Newly observed column has no sensitivity tag after the classification grace period.',True,required)
            elif required is not None:
                add(key,'MAPPED_TIER_MISSING','P1','Mapped source evidence requires a sensitivity assignment.',required=required)
            # Ordinary baseline missing tags remain aggregate backlog, not incidents.
        elif status=='Conflicting sensitivity values':
            add(key,'CONFLICTING_SENSITIVITY_VALUES','P1','Multiple sensitivity values were observed on this column.')
        elif status=='Value outside configured taxonomy':
            add(key,'INVALID_SENSITIVITY_VALUE','P1','Sensitivity value is outside the configured taxonomy.')
        elif status=='Recognized sensitivity value':
            if row['protection_floor'] is not None and actual>row['protection_floor']:
                add(key,'SENSITIVITY_DOWNGRADED','P1','Assigned tier is weaker than the strongest previously observed valid tier; review the change.',required=row['protection_floor'])
            if required is not None and actual>required:
                add(key,'MAPPED_TIER_TOO_WEAK','P1','Assigned tier is weaker than the current mapped requirement.',required=required)
        if (row.get('invalid_rule_label_count') or 0)>0:
            add(key,'SOURCE_MAPPING_INCONSISTENT','P1','Matching source classification rules are inconsistent or conflicting.')
        # Loss of a source requirement cannot close an earlier mapped-tier incident.
        if required is None:
            for kind in ('MAPPED_TIER_TOO_WEAK','MAPPED_TIER_MISSING'):
                old=issues.get(issue_key(key,kind))
                if old and old['status'] in OPEN_STATES:
                    restored=status=='Recognized sensitivity value' and actual<=old['required_priority']
                    if not restored:
                        add(key,kind,old['priority'],'Mapped source evidence disappeared; prior requirement retained until evidence or classification is restored.',required=old['required_priority'],hold=True)
    # Suppress unchanged initial findings as backlog without creating one incident
    # per baseline column. Once a suppressed finding clears, recurrence is new.
    candidates_by_asset={}
    for candidate in candidates.values():candidates_by_asset.setdefault(candidate['asset_key'],{})[candidate['finding_type']]=candidate
    for asset,row in columns.items():
        suppressed=dict(row.get('baseline_findings',{}))
        present=candidates_by_asset.get(asset,{})
        for kind in list(suppressed):
            if kind not in present:suppressed.pop(kind)
        for kind,candidate in present.items():
            signature=digest([kind,candidate['current_sensitivity'],candidate['required_priority']])
            if first:suppressed[kind]=signature
            if suppressed.get(kind)==signature:candidates.pop(candidate['issue_key'])
            elif kind in suppressed:suppressed.pop(kind)
        row['baseline_findings']=suppressed
    # Retain incident generations so recurrence differs from initial backlog.
    for key,candidate in candidates.items():
        old=issues.get(key);new_incident=old is None or old['status']=='RESOLVED'
        if new_incident:
            issues[key]=dict(candidate,generation=(old['generation']+1 if old else 1),
                opened_at=now_text,updated_at=now_text,resolved_at=None,
                baseline_muted=first,ever_announced=False,last_accepted_at=None,notice_sequence=0)
        else:
            issues[key].update(candidate,updated_at=now_text)
        issue=issues[key]
        # A previously muted baseline missing-tag finding changing to a materially
        # different finding gets its own key and is therefore eligible to alert.
        eligible_at=moment(columns[issue['asset_key']]['first_seen_at'])+timedelta(minutes=settings['classification_grace_minutes'])
        if issue['evidence_hold']:issue['status']='EVIDENCE_GAP'
        elif issue['grace'] and now<eligible_at:issue['status']='GRACE'
        elif issue['baseline_muted']:issue['status']='BACKLOG'
        else:issue['status']='OPEN'
        issue['eligible_at']=stamp(eligible_at) if issue['grace'] else issue['opened_at']
    for key,issue in issues.items():
        if key not in candidates and issue['status']!='RESOLVED':
            issue.update(status='RESOLVED',resolved_at=now_text,updated_at=now_text)
    # Withdraw undelivered alerts that are already resolved; no false recovery
    # is sent for an issue that was never announced.
    for notice_id,notice in list(pending.items()):
        if notice['kind']=='SUMMARY':continue
        issue=issues.get(notice['issue_key'])
        if not issue or notice['generation']!=issue['generation']:
            pending.pop(notice_id);continue
        if issue['status']=='RESOLVED' and notice['kind']!='RECOVERY':pending.pop(notice_id)
    def enqueue(issue,kind):
        issue['notice_sequence']+=1
        notice_id=digest([issue['issue_key'],issue['generation'],kind,issue['notice_sequence']])
        pending[notice_id]={k:issue.get(k) for k in ('issue_key','asset_key','generation','finding_type','priority','column_fqn','catalog_name','schema_name','table_name','column_name','owner','current_sensitivity','expected_sensitivity','required_priority','detail')}
        pending[notice_id].update(notice_id=notice_id,kind=kind,created_at=now_text)
        issue['last_enqueued_at']=now_text
    for issue in issues.values():
        same_pending=any(n['issue_key']==issue['issue_key'] and n['generation']==issue['generation'] for n in pending.values())
        if same_pending:continue
        if issue['status']=='RESOLVED':
            if issue['ever_announced'] and not issue.get('recovery_enqueued'):
                enqueue(issue,'RECOVERY');issue['recovery_enqueued']=True
        elif issue['status'] in ('OPEN','EVIDENCE_GAP') and not issue['baseline_muted']:
            if not issue['ever_announced']:enqueue(issue,'NEW')
            elif now-moment(issue['last_accepted_at'])>=timedelta(hours=settings['reminder_hours']):enqueue(issue,'REMINDER')
    # A daily environment summary is one digest item, not thousands of incidents.
    if not first and settings.get('daily_summary_enabled',False) and now.hour>=settings.get('daily_summary_hour_utc',13):
        day=now.date().isoformat()
        if state['control'].get('last_summary_date')!=day and not any(n['kind']=='SUMMARY' for n in pending.values()):
            missing=sum(r['sensitivity_state']=='Missing sensitivity tag' for r in rows)
            notice_id=digest(['summary',state['control']['scope_hash'],day])
            pending[notice_id]={'notice_id':notice_id,'issue_key':'SUMMARY','asset_key':'SUMMARY','generation':1,
                'kind':'SUMMARY','finding_type':'COVERAGE_SUMMARY','priority':'P3','column_fqn':'Environment summary',
                'catalog_name':'(configured scope)','schema_name':'(summary)','table_name':'(summary)',
                'column_name':f'{missing} of {len(rows)} visible columns missing sensitivity tags',
                'owner':'Governance team','created_at':now_text,'detail':'Coverage backlog is a summary; no baseline per-column incidents were created.'}
            state['control']['last_summary_date']=day
    ordered=sorted(pending.values(),key=lambda n:(n['priority'],n['created_at'],n['notice_id']))
    selected=[n['notice_id'] for n in ordered[:settings['maximum_notifications_per_run']]]
    for notice in pending.values():notice['selected_for_batch']=notice['notice_id'] in selected
    state['summary']={'observed_columns':len(rows),'known_columns':len(columns),
        'missing_tag_columns':sum(r['sensitivity_state']=='Missing sensitivity tag' for r in rows),
        'open_incidents':sum(i['status'] in ('OPEN','EVIDENCE_GAP') for i in issues.values()),
        'baseline_backlog':sum(c['observed'] and c.get('baseline_missing',False) and not c['ever_tagged'] and c['sensitivity_state']=='Missing sensitivity tag' for c in columns.values()),
        'grace_incidents':sum(i['status']=='GRACE' for i in issues.values()),
        'new_untagged_open':sum(i['finding_type']=='NEW_COLUMN_UNTAGGED' and i['status']=='OPEN' for i in issues.values()),
        'pending_notifications':len(pending),'selected_notifications':len(selected),
        'baseline_run':first,'observed_at':now_text}
    return state
