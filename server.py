from arango_queries import *
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pyArango.connection import Connection
import os
from dotenv import load_dotenv
from networkx_adapter import *
import matplotlib.pyplot as plt
from typing import Optional
import json
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

