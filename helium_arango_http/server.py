from arango_queries import *
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pyArango.connection import Connection
import os
from dotenv import load_dotenv
from typing import Optional
import uvicorn
import h3
from datetime import datetime
from metadata import title, description, version, license_info, contact, tags_metadata


load_dotenv()

try:
    conn = Connection(
        arangoURL=os.getenv('ARANGO_URL'),
        username=os.getenv('ARANGO_USERNAME'),
        password=os.getenv('ARANGO_PASSWORD')
    )
except requests.exceptions.ConnectionError:
    raise Exception('Unable to connect to the ArangoDB instance. Please check that it is running and that you have supplied the correct URL/credentials in the .env file.')
db = conn['helium']

# fields from metadata.py
app = FastAPI(
    title=title,
    description=description,
    version=version,
    license_info=license_info,
    contact=contact,
    tags_metadata=tags_metadata
)


@app.get('/payments/{address}/from', response_class=JSONResponse, tags=['payments'])
async def flows_from_account(address: str, limit: Optional[int] = 100, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    return get_top_payees_from_payer(db, address, limit, min_time, max_time)


@app.get('/payments/{address}/to', response_class=JSONResponse, tags=['payments'])
async def flows_to_account(address: str, limit: Optional[int] = 100, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    return get_top_payers_to_payee(db, address, limit, min_time, max_time)


@app.get('/payments/totals', response_class=JSONResponse, tags=['payments'])
async def top_payment_totals(limit: Optional[int] = 100, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    return get_top_payment_totals(db, limit, min_time, max_time)


@app.get('/payments/counts', response_class=JSONResponse, tags=['payments'])
async def top_payment_counts(limit: Optional[int] = 100, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    return get_top_payment_counts(db, limit, min_time, max_time)


@app.get('/payments/payers', response_class=JSONResponse, tags=['payments'])
async def top_payers(limit: Optional[int] = 100, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    return get_top_payers(db, limit, min_time, max_time)


@app.get('/payments/payees', response_class=JSONResponse, tags=['payments'])
async def top_payees(limit: Optional[int] = 100, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    return get_top_payees(db, limit, min_time, max_time)


@app.get('/payments/payers/graph', response_class=JSONResponse, tags=['payments'])
async def top_payers_graph(limit: Optional[int] = 10, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    nodes, edges = get_graph_from_top_payers(db, limit, min_time, max_time)
    return {'nodes': nodes, 'edges': edges}


@app.get('/payments/payees/graph', response_class=JSONResponse, tags=['payments'])
async def top_payees_graph(limit: Optional[int] = 10, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    nodes, edges = get_graph_to_top_payees(db, limit, min_time, max_time)
    return {'nodes': nodes, 'edges': edges}


@app.get('/hotspots/coords/graph', response_class=JSONResponse, tags=['hotspots'])
async def witnesses_near_coords_graph(lat: float, lon: float, limit: Optional[int] = 10):
    nodes, edges = get_witness_graph_near_coordinates(db, lat, lon, limit)
    return {'nodes': nodes, 'edges': edges}


@app.get('/hotspots/hex/graph', response_class=JSONResponse, tags=['hotspots'])
async def witnesses_in_hex_graph(hex: str):
    if h3.h3_is_valid(hex):
        nodes, edges = get_witness_graph_in_hex(db, hex)
        return {'nodes': nodes, 'edges': edges}
    else:
        raise HTTPException(status_code=400, detail='Invalid hex')


@app.get('/hotspots/{address}/outbound', response_class=JSONResponse, tags=['hotspots'])
async def outbound_witnesses_for_hotspot(address: str):
    return {'witnesses': get_outbound_witnesses_for_hotspot(db, address)}


@app.get('/hotspots/{address}/inbound', response_class=JSONResponse, tags=['hotspots'])
async def inbound_witnesses_for_hotspot(address: str):
    return {'witnesses': get_inbound_witnesses_for_hotspot(db, address)}


@app.get('/hotspots/receipts', response_class=JSONResponse, tags=['hotspots'])
async def witness_receipts(address: Optional[str] = None, limit: Optional[int] = 1000):
    return {'receipts': get_sample_of_recent_witness_receipts(db, address, limit)}


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
