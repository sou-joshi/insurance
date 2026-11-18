"""Build one portable dashboard definition; deployment supplies catalog/schema context."""
from pathlib import Path
import json
ROOT=Path(__file__).resolve().parent
DATASETS=['column_inventory','tag_dictionary','mapping_rule_health','unmapped_source_labels','collate_feed_health','collate_assignment_trace','scope_and_refresh','actionable_inventory','monitor_health','monitor_incidents','monitor_notifications','monitor_daily_trends']
def build(layout=None):
    layout=layout or json.loads((ROOT/'dashboard_layout.json').read_text())
    return {'datasets':[{'name':n,'displayName':n.replace('_',' ').title(),'queryLines':['SELECT * FROM raw_classification_'+n+'\n']} for n in DATASETS],**layout}
if __name__=='__main__':
    result=build();dest=ROOT/'Raw_Classification_Assurance.lvdash.json';dest.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({'dashboard':str(dest),'datasets':len(result['datasets']),'content_pages':len(result['pages'])-1,'widgets':sum(len(p['layout']) for p in result['pages'])}))
