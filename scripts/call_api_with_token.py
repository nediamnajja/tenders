import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from back.routers.auth import create_token
import requests

TOKEN = create_token(1)
print('Using token for user 1 (first 16 chars):', TOKEN[:16])
headers = {'Authorization': f'Bearer {TOKEN}'}

base = 'http://127.0.0.1:8000'

def get(endpoint, params=None):
    r = requests.get(base + endpoint, headers=headers, params=params)
    print('\nGET', endpoint, 'status', r.status_code)
    try:
        d = r.json()
        print('keys:', list(d.keys()) if isinstance(d, dict) else type(d))
        return d
    except Exception as e:
        print('non-json response:', r.text[:400])
        return None

# fetch open and closed and scored
scored = get('/tenders', {'status':'open','per_page':100,'sort_by':'p_go'})
open_ = get('/tenders', {'status':'open','per_page':100,'sort_by':'publication_datetime'})
closed = get('/tenders', {'status':'closed','per_page':100,'sort_by':'publication_datetime'})

# check whether known decided tender ids appear
for tid in [20056,17560]:
    d = get(f'/tenders/{tid}')
    if d:
        print('tender', tid, 'p_go', d.get('p_go'), 'partner_decision', d.get('partner_decision'))

print('\nFinished')
