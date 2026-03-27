import pathlib

addition = """

@mcp.resource('ledger://applications/{application_id}')
async def get_application_status(application_id: str) -> dict:
    pool = await _get_pool()
    row = await ApplicationSummaryProjection(pool).get(application_id)
    return row or {'error_type': 'NotFound', 'application_id': application_id}


@mcp.resource('ledger://applications/{application_id}/compliance')
async def get_application_compliance(application_id: str) -> dict:
    pool = await _get_pool()
    rows = await ComplianceAuditProjection(pool).get_for_application(application_id)
    return {'application_id': application_id, 'records': rows}


@mcp.resource('ledger://applications/{application_id}/audit-trail')
async def get_audit_trail(application_id: str) -> dict:
    store = await _get_store()
    events = await store.load_stream(f'loan-{application_id}')
    return {
        'application_id': application_id,
        'event_count': len(events),
        'events': [
            {'id': e.id, 'version': e.version, 'event_type': e.event_type,
             'event_data': e.event_data, 'created_at': e.created_at.isoformat()}
            for e in events
        ],
    }


@mcp.resource('ledger://agents/{agent_id}/sessions/{session_id}')
async def get_agent_session(agent_id: str, session_id: str) -> dict:
    store = await _get_store()
    events = await store.load_stream(f'agent-{agent_id}-{session_id}')
    return {
        'agent_id': agent_id,
        'session_id': session_id,
        'event_count': len(events),
        'events': [
            {'id': e.id, 'version': e.version, 'event_type': e.event_type,
             'event_data': e.event_data, 'created_at': e.created_at.isoformat()}
            for e in events
        ],
    }


@mcp.resource('ledger://ledger/health')
async def get_ledger_health() -> dict:
    pool = await _get_pool()
    return {
        'application_summary_lag_ms': await ApplicationSummaryProjection(pool).get_lag(),
        'agent_performance_lag_ms': await AgentPerformanceProjection(pool).get_lag(),
        'compliance_audit_lag_ms': await ComplianceAuditProjection(pool).get_lag(),
        'status': 'healthy',
    }
"""

path = pathlib.Path('src/ledger/mcp_server.py')
content = path.read_text(encoding='utf-8')
marker = 'if __name__ == "__main__":'
content = content.replace(marker, addition + marker)
path.write_text(content, encoding='utf-8')

c = path.read_text()
for fn in ['get_audit_trail', 'get_application_compliance', 'get_ledger_health', 'get_agent_session']:
    print(fn, fn in c)