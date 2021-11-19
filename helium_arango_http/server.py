import pyArango.theExceptions
import redis
from arango_queries import *
from utils import get_cluster_centers
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
import pickle


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

# start the redis cache
REDIS_EXPIRATION_SECONDS = 360

if os.getenv('REDIS_ACTIVE'):
    r = redis.Redis('redis')

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
    try:
        return get_top_payees_from_payer(db, address, limit, min_time, max_time)
    except pyArango.theExceptions.AQLFetchError:
        return JSONResponse({'Message': 'No results returned for query'})


@app.get('/payments/{address}/to', response_class=JSONResponse, tags=['payments'])
async def flows_to_account(address: str, limit: Optional[int] = 100, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    try:
        return get_top_payers_to_payee(db, address, limit, min_time, max_time)
    except pyArango.theExceptions.AQLFetchError:
        return JSONResponse({'Message': 'No results returned for query'})


@app.get('/payments/totals', response_class=JSONResponse, tags=['payments'])
async def top_payment_totals(limit: Optional[int] = 100, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    try:
        return get_top_payment_totals(db, limit, min_time, max_time)
    except pyArango.theExceptions.AQLFetchError:
        return JSONResponse({'Message': 'No results returned for query'})

@app.get('/payments/counts', response_class=JSONResponse, tags=['payments'])
async def top_payment_counts(limit: Optional[int] = 100, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    try:
        return get_top_payment_counts(db, limit, min_time, max_time)
    except pyArango.theExceptions.AQLFetchError:
        return JSONResponse({'Message': 'No results returned for query'})

@app.get('/payments/payers', response_class=JSONResponse, tags=['payments'])
async def top_payers(limit: Optional[int] = 100, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    try:
        return get_top_payers(db, limit, min_time, max_time)
    except pyArango.theExceptions.AQLFetchError:
        return JSONResponse({'Message': 'No results returned for query'})

@app.get('/payments/payees', response_class=JSONResponse, tags=['payments'])
async def top_payees(limit: Optional[int] = 100, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    try:
        return get_top_payees(db, limit, min_time, max_time)
    except pyArango.theExceptions.AQLFetchError:
        return JSONResponse({'Message': 'No results returned for query'})

@app.get('/payments/payers/graph', response_class=JSONResponse, tags=['payments'])
async def top_payers_graph(limit: Optional[int] = 10, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    try:
        nodes, edges = get_graph_from_top_payers(db, limit, min_time, max_time)
        return {'nodes': nodes, 'edges': edges}
    except pyArango.theExceptions.AQLFetchError:
        return JSONResponse({'Message': 'No results returned for query'})

@app.get('/payments/payees/graph', response_class=JSONResponse, tags=['payments'])
async def top_payees_graph(limit: Optional[int] = 10, min_time: Optional[int] = 0, max_time: Optional[int] = int(datetime.utcnow().timestamp())):
    try:
        nodes, edges = get_graph_to_top_payees(db, limit, min_time, max_time)
        return {'nodes': nodes, 'edges': edges}
    except pyArango.theExceptions.AQLFetchError:
        return JSONResponse({'Message': 'No results returned for query'})


@app.get('/hotspots/coords/graph', response_class=JSONResponse, tags=['hotspots'])
async def witnesses_near_coords_graph(lat: float, lon: float, limit: Optional[int] = 10):
    try:
        nodes, edges = get_witness_graph_near_coordinates(db, lat, lon, limit)
        return {'nodes': nodes, 'edges': edges}
    except pyArango.theExceptions.AQLFetchError:
        return JSONResponse({'Message': 'No results returned for query'})


@app.get('/hotspots/hex/graph', response_class=JSONResponse, tags=['hotspots'])
async def witnesses_in_hex_graph(hex: str):
    if h3.h3_is_valid(hex) is False:
        return JSONResponse({'Message': 'Invalid hex'})
    else:
        try:
            nodes, edges = get_witness_graph_in_hex(db, hex)
            return {'nodes': nodes, 'edges': edges}
        except pyArango.theExceptions.AQLFetchError:
            return JSONResponse({'Message': 'No results returned for query'})


@app.get('/hotspots/{address}/outbound', response_class=JSONResponse, tags=['hotspots'])
async def outbound_witnesses_for_hotspot(address: str):
    try:
        return {'witnesses': get_outbound_witnesses_for_hotspot(db, address)}
    except pyArango.theExceptions.AQLFetchError:
        return JSONResponse({'Message': 'No results returned for query'})


@app.get('/hotspots/{address}/inbound', response_class=JSONResponse, tags=['hotspots'])
async def inbound_witnesses_for_hotspot(address: str):
    try:
        return {'witnesses': get_inbound_witnesses_for_hotspot(db, address)}
    except pyArango.theExceptions.AQLFetchError:
        return JSONResponse({'Message': 'No results returned for query'})


@app.get('/hotspots/receipts', response_class=JSONResponse, tags=['hotspots'])
async def witness_receipts(address: Optional[str] = None, limit: Optional[int] = 1000):
    try:
        return {'receipts': get_sample_of_recent_witness_receipts(db, address, limit)}
    except pyArango.theExceptions.AQLFetchError:
        return JSONResponse({'Message': 'No results returned for query'})


@app.get('/hotspots/clusters', response_class=JSONResponse, tags=['hotspots'])
async def cluster_centers(n_clusters: Optional[str] = 500):
    if os.getenv('REDIS_ACTIVE'):
        centroid_key = f'centroids_{n_clusters}'
        if r.exists(centroid_key):
            (centroids, error) = pickle.loads(r.get(centroid_key))
        else:
            if r.exists('hotspot_coordinates'):
                coords = pickle.loads(r.get('hotspot_coordinates'))
            else:
                coords = get_hotspot_coordinates(db)
                r.set('hotspot_coordinates', pickle.dumps(coords), ex=REDIS_EXPIRATION_SECONDS)
            (centroids, error) = get_cluster_centers(coords, int(n_clusters))
            r.set(centroid_key, pickle.dumps((centroids, error)), ex=REDIS_EXPIRATION_SECONDS)
    else:
        coords = get_hotspot_coordinates(db)
        (centroids, error) = get_cluster_centers(coords, int(n_clusters))
    return {'centroids': centroids, 'error': error}


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
