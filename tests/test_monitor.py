import copy,datetime,importlib.util,json,sys,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from monitor.engine import evaluate,asset_key,issue_key,ScopeError,stamp
from configuration import validate
NOW=datetime.datetime(2030,1,1,tzinfo=datetime.timezone.utc)
SETTINGS={'maximum_columns':50000,'maximum_inventory_drop_fraction':.2,'classification_grace_minutes':30,'reminder_hours':24,'maximum_notifications_per_run':1000}
IDENTITY={'workspace_id':'w1','metastore_id':'m1','metadata_reader':'governance_identity'}
def row(name='original',tier=None,expected=None,catalog='synthetic_raw'):
 return dict(catalog_name=catalog,schema_name='source',table_name='party',column_name=name,data_type='STRING',
  sensitivity_state='Recognized sensitivity value' if tier else 'Missing sensitivity tag',current_sensitivity=str(tier) if tier else '(missing)',
  actual_priority=tier,expected_priority=expected,expected_sensitivity=str(expected) if expected else '(no mapped requirement)',
  remediation_owner='synthetic_owner',invalid_rule_label_count=0)
def run(rows,state=None,minutes=0,settings=None,acks=(),identity=None,scope='scope'):
 return evaluate(rows,state,NOW+datetime.timedelta(minutes=minutes),identity or IDENTITY,scope,settings or SETTINGS,acks)
def acks(state,minutes):
 return [dict(notice_id=n,accepted_at=stamp(NOW+datetime.timedelta(minutes=minutes))) for n in state['pending'] if state['pending'][n]['selected_for_batch']]

class MonitorTests(unittest.TestCase):
 def test_large_initial_backlog_is_summary_not_incidents(self):
  s=run([row('c'+str(i)) for i in range(10000)])
  self.assertEqual(s['summary']['baseline_backlog'],10000);self.assertFalse(s['issues']);self.assertFalse(s['pending'])
 def test_initial_mapped_and_invalid_findings_do_not_flood_next_scan(self):
  rows=[row('mapped',None,1),row('weak',4,1)];bad=row('bad',4);bad.update(sensitivity_state='Value outside configured taxonomy',actual_priority=None);rows.append(bad)
  b=run(rows);s=run(rows,b,60);self.assertFalse(s['issues']);self.assertFalse(s['pending'])
 def test_new_column_obeys_grace_without_resetting_first_observation(self):
  rows=[row()];b=run(rows);g=run(rows+[row('new')],b,1)
  self.assertEqual(g['summary']['grace_incidents'],1);self.assertFalse(g['pending'])
  g2=run(rows+[row('new')],g,20);a=run(rows+[row('new')],g2,31)
  self.assertEqual(len(a['pending']),1);self.assertEqual(next(iter(a['pending'].values()))['finding_type'],'NEW_COLUMN_UNTAGGED')
 def test_tag_added_during_grace_never_notifies(self):
  b=run([row()]);g=run([row(),row('new')],b,1);s=run([row(),row('new',3)],g,10)
  self.assertFalse(s['pending']);self.assertEqual(s['summary']['open_incidents'],0)
 def test_tag_removal_notifies_and_stays_open(self):
  b=run([row(tier=1)]);a=run([row()],b,5);s=run([row()],a,60)
  self.assertEqual(next(iter(s['issues'].values()))['finding_type'],'SENSITIVITY_TAG_REMOVED');self.assertEqual(set(a['pending']),set(s['pending']))
 def test_downgrade_persists_until_original_strength_restored(self):
  b=run([row(tier=1)]);a=run([row(tier=4)],b,5);s=run([row(tier=4)],a,60)
  self.assertEqual(next(iter(s['issues'].values()))['status'],'OPEN')
  accepted=acks(s,61);fixed=run([row(tier=1)],s,62,acks=accepted)
  self.assertEqual({n['kind'] for n in fixed['pending'].values()},{'RECOVERY'})
 def test_failed_evaluation_leaves_same_notice_pending(self):
  b=run([row(tier=1)]);a=run([row()],b,1);again=run([row()],a,60)
  self.assertEqual(set(a['pending']),set(again['pending']))
 def test_accepted_alert_suppresses_repeat_until_reminder_due(self):
  b=run([row(tier=1)]);a=run([row()],b,1);s=run([row()],a,2,acks=acks(a,2));self.assertFalse(s['pending'])
  s2=run([row()],s,100);self.assertFalse(s2['pending'])
  s3=run([row()],s2,1443);self.assertEqual({n['kind'] for n in s3['pending'].values()},{'REMINDER'})
 def test_no_recovery_for_never_announced_issue(self):
  b=run([row(tier=1)]);a=run([row()],b,1);s=run([row(tier=1)],a,2);self.assertFalse(s['pending'])
 def test_recovery_and_recurrence_are_distinct_incidents(self):
  b=run([row(tier=1)]);a=run([row()],b,1);f=run([row(tier=1)],a,2,acks=acks(a,2))
  f2=run([row(tier=1)],f,3,acks=acks(f,3));again=run([row()],f2,4)
  issue=next(iter(again['issues'].values()));self.assertEqual(issue['generation'],2);self.assertEqual({n['kind'] for n in again['pending'].values()},{'NEW'})
 def test_baseline_finding_clears_then_recurrence_alerts(self):
  b=run([row(tier=4,expected=1)]);f=run([row(tier=1,expected=1)],b,1);a=run([row(tier=4,expected=1)],f,2)
  self.assertTrue(a['pending']);self.assertTrue(any(n['finding_type']=='MAPPED_TIER_TOO_WEAK' for n in a['pending'].values()))
 def test_new_mapped_requirement_on_old_backlog_is_new_change(self):
  b=run([row()]);a=run([row(expected=1)],b,60)
  self.assertEqual({n['finding_type'] for n in a['pending'].values()},{'MAPPED_TIER_MISSING'})
 def test_lost_source_evidence_does_not_close_existing_incident(self):
  b=run([row(tier=4)]);a=run([row(tier=4,expected=1)],b,1);s=run([row(tier=4)],a,2,acks=acks(a,2))
  issue=next(i for i in s['issues'].values() if i['finding_type']=='MAPPED_TIER_TOO_WEAK');self.assertEqual(issue['status'],'EVIDENCE_GAP');self.assertFalse(s['pending'])
 def test_partial_disappearance_is_not_automatic_recovery(self):
  rows=[row(str(i),tier=1) for i in range(10)];b=run(rows);changed=copy.deepcopy(rows);changed[0]=row('0');a=run(changed,b,1)
  s=run(changed[1:],a,2,acks=acks(a,2));lost=[i for i in s['issues'].values() if i['finding_type']=='SENSITIVITY_TAG_REMOVED'][0]
  self.assertEqual(lost['status'],'EVIDENCE_GAP');self.assertNotIn('RECOVERY',{n['kind'] for n in s['pending'].values()})
 def test_zero_scope_scope_change_identity_change_and_large_drop_stop(self):
  b=run([row(str(i)) for i in range(10)])
  for rows,kw in [([],{}),([row()],{}),([row(str(i)) for i in range(10)],{'scope':'other'}),([row(str(i)) for i in range(10)],{'identity':dict(IDENTITY,metadata_reader='other')})]:
   with self.assertRaises(ScopeError):run(rows,b,1,**kw)
 def test_catalog_disappearance_detected_even_if_schema_name_is_shared(self):
  b=run([row(catalog='raw_a'),row(catalog='raw_b')])
  with self.assertRaises(ScopeError):run([row(catalog='raw_a')],b,1)
 def test_notification_batch_cap_and_carry_forward(self):
  settings=dict(SETTINGS,maximum_notifications_per_run=2)
  b=run([row('anchor')],settings=settings);g=run([row('anchor')]+[row('new'+str(i)) for i in range(5)],b,1,settings)
  a=run([row('anchor')]+[row('new'+str(i)) for i in range(5)],g,32,settings)
  self.assertEqual(len(a['pending']),5);self.assertEqual(a['summary']['selected_notifications'],2)
  s=run([row('anchor')]+[row('new'+str(i)) for i in range(5)],a,33,settings,acks(a,33))
  self.assertEqual(len(s['pending']),3);self.assertEqual(s['summary']['selected_notifications'],2)
 def test_daily_summary_is_one_digest_item_not_column_exceptions(self):
  settings=dict(SETTINGS,daily_summary_enabled=True,daily_summary_hour_utc=13)
  rows=[row(str(i)) for i in range(100)];b=run(rows,settings=settings);self.assertFalse(b['pending'])
  s=run(rows,b,13*60,settings);self.assertFalse(s['issues']);self.assertEqual(len(s['pending']),1)
  s2=run(rows,s,14*60,settings);self.assertEqual(set(s['pending']),set(s2['pending']))
  s3=run(rows,s2,15*60,settings,acks(s2,15*60));self.assertFalse(s3['pending'])
  s4=run(rows,s3,37*60,settings);self.assertEqual(len(s4['pending']),1)
 def test_baseline_backlog_does_not_include_later_tag_removals(self):
  b=run([row('tagged',1),row('backlog')]);a=run([row('tagged'),row('backlog')],b,1)
  self.assertEqual(a['summary']['baseline_backlog'],1);self.assertEqual(a['summary']['missing_tag_columns'],2)
 def test_scope_signature_changes_without_silently_changing_monitor_identity(self):
  c=json.loads((ROOT/'configs/dev.example.json').read_text());a=validate(c);c['scopes'][0]['excluded_schemas'].append('other');b=validate(c)
  self.assertEqual(a['monitor_id'],b['monitor_id']);self.assertNotEqual(a['scope_signature'],b['scope_signature'])
  c['environment']='PRD';c['expected_workspace_id']='1234';other=validate(c);self.assertNotEqual(a['monitor_id'],other['monitor_id'])

if __name__=='__main__':unittest.main()
