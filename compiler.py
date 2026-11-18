"""Compile a single source contract for one or more environment-bound catalogs."""
import json
from pathlib import Path
from configuration import validate,quoted,literal
HERE=Path(__file__).resolve().parent

def compile_sources(config,common_source=None,tails=None):
    c=validate(config)
    common_source=common_source or (HERE/'sql/common.sql').read_text()
    tails=tails or json.loads((HERE/'sql/dataset_tails.json').read_text())
    result={name:[] for name in tails}
    empty_fields={'assignment_id':'STRING','source_scope':'STRING','source_service_fqn':'STRING','asset_fqn':'STRING','field_fqn':'STRING','term_fqn':'STRING','assignment_level':'STRING','label_state':'STRING','term_is_deleted':'BOOLEAN','asset_is_deleted':'BOOLEAN','last_seen_at':'TIMESTAMP','asset_updated_at_ts':'TIMESTAMP','last_run_id':'STRING','source_payload_sha256':'STRING'}
    empty='(SELECT '+','.join('CAST(NULL AS '+v+') AS '+k for k,v in empty_fields.items())+' WHERE false)'
    for scope in c['scopes']:
        replacements={
            '{{RAW}}':scope['catalog'],'{{SERVICE}}':(scope.get('collate_service') or '').replace("'","''"),
            '{{FRESH_DAYS}}':str(c['collate_freshness_days']),
            '{{EXCLUSIONS}}':','.join(literal(s) for s in scope['excluded_schemas']),
            '{{EXCLUDED_DISPLAY}}':', '.join(scope['excluded_schemas']).replace("'","''"),
            '{{MAPPING_TABLE}}':quoted(scope['mapping_table']),
            '{{COLLATE_SOURCE}}':quoted(scope['collate_assignment_table']) if scope.get('collate_assignment_table') else empty,
            '{{COLLATE_SOURCE_DISPLAY}}':scope.get('collate_assignment_table') or '(source not configured)',
            '{{COLLATE_CONFIGURED}}':'true' if scope.get('collate_assignment_table') else 'false',
            '{{TAXONOMY_VALUES}}':','.join('('+literal(t['classification'])+','+literal(t['pii_class'])+','+str(t['priority'])+')' for t in c['taxonomy']),
            '{{SENSITIVITY_TAG}}':c['sensitivity_tag'].replace("'","''"),
            '{{LEGACY_PII_TAG}}':c['legacy_pii_tag'].replace("'","''"),
            '{{SOURCE_KEYS}}':','.join(literal(s) for s in c['source_classification_tag_keys']) or "''",
            '{{NATIVE_PREFIX}}':c['native_tag_prefix'].replace("'","''"),
            '{{LOWEST_PRIORITY}}':str(max(t['priority'] for t in c['taxonomy']))}
        for name,tail in tails.items():
            query=common_source+'\n'+tail+'\n'
            for a,b in replacements.items():query=query.replace(a,b)
            assert '{{' not in query,'Unbound source template marker'
            # Keep each scoped CTE graph independent before UNION ALL.
            result[name].append(query)
    return {name:queries[0] if len(queries)==1 else '\nUNION ALL\n'.join('SELECT * FROM (\n'+q+'\n)' for q in queries) for name,queries in result.items()}
