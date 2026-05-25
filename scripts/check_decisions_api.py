import sys, os
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from back.routers.auth import create_token
import requests

token = create_token(1)
headers = {'Authorization': f'Bearer {token}'}
base = 'http://127.0.0.1:8000'
for params, name in [({'status':'open','per_page':100,'sort_by':'p_go'},'scored'), ({'status':'open','per_page':100,'sort_by':'publication_datetime'},'open'), ({'status':'closed','per_page':100,'sort_by':'publication_datetime'},'closed')]:
    r = requests.get(base + '/tenders', headers=headers, params=params)
    print('---', name, 'status', r.status_code)
    try:
        data = r.json()
    except Exception as e:
        print('json error', e, r.text[:200])
        continue
    print('total', data.get('total'), 'items', len(data.get('items', [])))
    if name == 'scored':
        print('pending count', len([t for t in data.get('items', []) if not t.get('partner_decision') and t.get('p_go') is not None and t.get('p_go') >= 0.7]))
        print('pending ids', [t.get('id') for t in data.get('items', []) if not t.get('partner_decision') and t.get('p_go') is not None and t.get('p_go') >= 0.7][:20])
    if name in ('open', 'closed'):
        decided = [t for t in data.get('items', []) if t.get('partner_decision')]
        print('decided count', len(decided), 'decided ids', [t.get('id') for t in decided][:20])
