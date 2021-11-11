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


load_dotenv()

conn = Connection(
    arangoURL=os.getenv('ARANGO_URL'),
    username=os.getenv('ARANGO_USERNAME'),
    password=os.getenv('ARANGO_PASSWORD')
)
db = conn['helium']

app = FastAPI()


@app.get('/payments/{address}/from', response_class=JSONResponse)
async def flows_from_account(address: str, limit: Optional[int] = 100, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    """Get top outbound payments from an account, grouped by payee"""
    return get_top_payees_from_payer(db, address, limit, min_time, max_time)


@app.get('/payments/{address}/to', response_class=JSONResponse)
async def flows_to_account(address: str, limit: Optional[int] = 100, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    return get_top_payers_to_payee(db, address, limit, min_time, max_time)


@app.get('/payments/totals', response_class=JSONResponse)
async def top_payment_totals(limit: Optional[int] = 100, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    return get_top_payment_totals(db, limit, min_time, max_time)


@app.get('/payments/counts', response_class=JSONResponse)
async def top_payment_counts(limit: Optional[int] = 100, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    return get_top_payment_counts(db, limit, min_time, max_time)


@app.get('/payments/payers', response_class=JSONResponse)
async def top_payers(limit: Optional[int] = 100, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    return get_top_payers(db, limit, min_time, max_time)


@app.get('/payments/payees', response_class=JSONResponse)
async def top_payees(limit: Optional[int] = 100, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    return get_top_payees(db, limit, min_time, max_time)


@app.get('/payments/payers/graph', response_class=JSONResponse)
async def top_payers_graph(limit: Optional[int] = 10, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    nodes, edges = get_graph_from_top_payers(db, limit, min_time, max_time)
    return {'nodes': nodes, 'edges': edges}


@app.get('/payments/payees/graph', response_class=JSONResponse)
async def top_payees_graph(limit: Optional[int] = 10, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    nodes, edges = get_graph_from_top_payers(db, limit, min_time, max_time)
    return {'nodes': nodes, 'edges': edges}


@app.get('/hotspots/coords/graph', response_class=JSONResponse)
async def witnesses_near_coords_graph(lat: float, lon: float, limit: Optional[int] = 10):
    nodes, edges = get_witness_graph_near_coordinates(db, lat, lon, limit)
    return {'nodes': nodes, 'edges': edges}


@app.get('/hotspots/hex/graph', response_class=JSONResponse)
async def witnesses_in_hex_graph(hex: str):
    if h3.h3_is_valid(hex):
        nodes, edges = get_witness_graph_in_hex(db, hex)
        return {'nodes': nodes, 'edges': edges}
    else:
        raise HTTPException(status_code=400, detail='Invalid hex')


@app.get('/hotspots/{address}/outbound', response_class=JSONResponse)
async def outbound_witnesses_for_hotspot(address: str):
    return {'witnesses': get_outbound_witnesses_for_hotspot(db, address)}


@app.get('/hotspots/{address}/inbound', response_class=JSONResponse)
async def inbound_witnesses_for_hotspot(address: str):
    return {'witnesses': get_inbound_witnesses_for_hotspot(db, address)}


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
