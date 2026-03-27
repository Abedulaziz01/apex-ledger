import pathlib

files = [
    'src/ledger/agents/base_agent.py',
    'src/ledger/agents/stub_agents.py',
    'src/ledger/agents/credit_analysis_agent.py',
    'src/ledger/schema/events.py',
]

replacements = [
    ('from ledger.agents.base_agent import', 'from src.ledger.agents.base_agent import'),
    ('from ledger.schema.events import', 'from src.ledger.schema.events import'),
    ('from ledger.event_store import', 'from src.ledger.core.event_store import'),
    ('from ledger.domain.aggregates.loan_application import', 'from src.ledger.domain.loan_application import'),
    ('from ledger.', 'from src.ledger.'),
    ('import ledger.', 'import src.ledger.'),
]

es_path = pathlib.Path('src/ledger/core/event_store.py')
es_content = es_path.read_text(encoding='utf-8')
if 'AbstractEventStore' not in es_content:
    es_content += '\n\nAbstractEventStore = EventStore\n'
    es_path.write_text(es_content, encoding='utf-8')
    print('Fixed: event_store.py — added AbstractEventStore')
else:
    print('OK: AbstractEventStore already exists')

for f in files:
    path = pathlib.Path(f)
    if not path.exists():
        print(f'MISSING: {f}')
        continue
    content = path.read_text(encoding='utf-8')
    for old, new in replacements:
        content = content.replace(old, new)
    path.write_text(content, encoding='utf-8')
    print(f'Fixed: {f}')