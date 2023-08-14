import httpx
import logging
import orjson
from pathlib import Path
from pydantic import BaseModel
from typing import Any, List, Dict, Optional
from datetime import datetime, date
from asyncio import gather

#==============================================================================#

DEBUG = False
NL = '\n'
ENC = 'utf-8'
HEADERS = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36', 
           'accept': 'application/json,text/*;q=0.99'}
PROXIES = None
BASE_URL = 'https://api.reputation.ru/api'

#==============================================================================#

logging.basicConfig(filename=str(Path(__file__).parent / 'log.log'), filemode='w', style='{',
                    format='[{asctime}] [{levelname}] {message}', datefmt='%d.%m.%Y %H:%M:%S',
                    encoding=ENC, level=logging.DEBUG if DEBUG else logging.INFO)

#==============================================================================#

def serialize(obj) -> str:
    return orjson.dumps(obj, option=orjson.OPT_INDENT_2).decode(ENC)

def deserialize(json: str):
    return orjson.loads(json)

async def exec_method(base_url, method_type, method, client=None, headers=HEADERS, params=None, data=None,
                      files=None, proxies=PROXIES, astext=False):
    async def exec_(client_: httpx.AsyncClient):
        req = client_.build_request(method=method_type, url=method, params=params, json=data, files=files)
        logging.debug(f">>> {serialize({'url': str(req.url), 'headers': str(req.headers), 'data': data})}")
        res_obj = None
        res_text = ''
        try:
            res = await client_.send(req)
            if not res is None and res.is_success:
                res_text = res.text
                res_obj = res.json()            
                logging.debug(f"<<< {res.status_code}: {serialize(res_obj)}")
            else:
                logging.debug(repr(res))
                logging.debug(res_text)
        except Exception as err:
            logging.exception(str(err) + '\n\n' + res_text, exc_info=DEBUG)
        return res_text if astext else res_obj

    if client is None:
        async with httpx.AsyncClient(headers=headers, base_url=base_url, proxies=proxies, verify=False) as client_:
            res = await exec_(client_)
    else:
        res = await exec_(client)
    return res

#==============================================================================#

class Entity(BaseModel):
    id: str
    inn: Optional[str]
    ogrn: Optional[str]
    status: Optional[str]
    name: Optional[str]    
    full_name: Optional[str]
    address: Optional[str]
    manager: Optional[str]
    activity: Optional[str]
    date_registered: Optional[date]
    website: Optional[str]
    phones: Optional[List[str]]
    emails: Optional[List[str]]

def get_val(l: List[Any], index: int, default=''):
    try:
        return l[index]
    except IndexError:
        return default

def make_entity(data: Dict[Any, Any], max_elements: int = None) -> Entity:
    ddata = dict(id=data.get('Id', ''), inn=data.get('Inn', ''), ogrn=data.get('Ogrn', ''),
                 name=data.get('Name', ''), full_name=get_val(data.get('OtherNames', []), 0), status=data.get('Status', ''),
                 address=data.get('Address', ''), manager=data.get('ManagerName', ''), activity=data.get('MainActivityType', {}).get('Name', ''),
                 date_registered=datetime.fromisoformat(data.get('RegistrationDate', None)).date() if data.get('RegistrationDate', None) else None, 
                 website=get_val(data.get('Sites', []), 0), phones=data.get('Phones', [])[:max_elements], emails=data.get('Emails', [])[:max_elements])
    logging.debug(f'<<<< {ddata["inn"]}:{NL}{serialize(ddata)}')
    return Entity(**ddata)

#==============================================================================#

class Reputation:

    def __init__(self, token: str, max_elements: int = None):
        self._token = token
        self._max_elements = max_elements if (isinstance(max_elements, int) and max_elements > 0) else None
        self._headers = HEADERS | {'Authorization': self._token}
        self._client = httpx.AsyncClient(headers=self._headers, base_url=BASE_URL, proxies=PROXIES, verify=False)
        logging.debug('HTTPX CLIENT INITIALIZED')

    async def exec_get(self, method, params=None, astext=False):
        return await exec_method(BASE_URL, 'GET', method, self._client, params=params, astext=astext)
    
    async def exec_post(self, method, params=None, data=None, files=None, astext=False):
        return await exec_method(BASE_URL, 'POST', method, self._client, params=params, data=data, files=files, astext=astext)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *excinfo):
        await self._client.aclose()
        logging.debug('HTTPX CLIENT FREED')

    async def search_entity_by_inn(self, inn: str) -> Entity:
        logging.debug(f'>>>> ПОИСК ПО ИНН: {inn}')
        data = {'QueryText': inn, 'Type': 'Identifiers'}
        res = await self.exec_post('/v1/Entities/Search', data=data)
        if res is None or not res.get('TotalItems', 0): 
            logging.debug(f'<<<< {inn}: ПОИСК НЕ ДАЛ РЕЗУЛЬТАТОВ')
            return None
        items = res.get('Items', [])
        if not items: 
            logging.debug(f'<<<< {inn}: ПОИСК НЕ ДАЛ РЕЗУЛЬТАТОВ')
            return None
        return make_entity(items[0], self._max_elements)
    
    async def batch__search_entity_by_inn(self, entities: List[str]) -> List[Entity]:
        return await gather(*[self.search_entity_by_inn(e) for e in entities])