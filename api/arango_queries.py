from pyArango.connection import *
import h3


def get_top_payment_totals(database: Database, n: int = 100, min_time: int = 0, max_time: int = int(datetime.utcnow().timestamp())):
    aql = f"""for payment in payments
    filter payment.time > {min_time} and payment.time < {max_time}
    collect from = payment._from, to = payment._to into payment_groups = payment.amount
    let payment_total = SUM(payment_groups)
    sort payment_total desc
    limit {n}
    return {{from: last(split(from,'/')), to: last(split(to,'/')), payment_total: payment_total}}"""
    totals = database.fetch_list(aql)
    return totals


def get_top_payment_counts(database: Database, n: int = 100, min_time: int = 0, max_time: int = int(datetime.utcnow().timestamp())):
    aql = f"""for payment in payments
    filter payment.time > {min_time} and payment.time < {max_time}
    collect from = payment._from, to = payment._to into payment_groups = payment.amount
    let payment_count = LENGTH(payment_groups)
    sort payment_count desc
    limit {n}
    return {{from: last(split(from,'/')), to: last(split(to,'/')), payment_count: payment_count}}"""
    counts = database.fetch_list(aql)
    return counts


def get_top_payers(database: Database, n: int = 100, min_time: int = 0, max_time: int = int(datetime.utcnow().timestamp())):
    aql = f"""for payment in payments
    filter payment.time > {min_time} and payment.time < {max_time}
    collect from = payment._from into payment_groups = payment.amount
    let payment_total = SUM(payment_groups)
    let payment_count = LENGTH(payment_groups)
    sort payment_total desc
    limit {n}
    return {{from: last(split(from,'/')), total_amount: payment_total, num_payments: payment_count}}"""
    totals = database.fetch_list(aql)
    return totals


def get_top_payees(database: Database, n: int = 100, min_time: int = 0, max_time: int = int(datetime.utcnow().timestamp())):
    aql = f"""for payment in payments
    filter payment.time > {min_time} and payment.time < {max_time}
    collect to = payment._to into payment_groups = payment.amount
    let payment_total = SUM(payment_groups)
    let payment_count = SUM(payment_groups)
    sort payment_total desc
    limit {n}
    return {{to: last(split(to,'/')), total_amount: payment_total, num_payments: payment_count}}"""
    totals = database.fetch_list(aql)
    return totals


def get_top_payers_to_payee(database: Database, address: str, n: int = 100, min_time: int = 0, max_time: int = int(datetime.utcnow().timestamp())):
    aql = f"""for payment in payments
    filter payment.time > {min_time} and payment.time < {max_time} and payment._to == 'accounts/{address}'
    collect from = payment._from into payment_groups = payment.amount
    let payment_total = SUM(payment_groups)
    let payment_count = LENGTH(payment_groups)
    sort payment_total desc
    limit {n}
    return {{from: last(split(from,'/')), total_amount: payment_total, num_payments: payment_count}}"""
    totals = database.fetch_list(aql)
    return totals


def get_top_payees_from_payer(database: Database, address: str, n: int = 100, min_time: int = 0, max_time: int = int(datetime.utcnow().timestamp())):
    aql = f"""for payment in payments
    filter payment.time > {min_time} and payment.time < {max_time} and payment._from == 'accounts/{address}'
    collect to = payment._to into payment_groups = payment.amount
    let payment_total = SUM(payment_groups)
    let payment_count = LENGTH(payment_groups)
    sort payment_total desc
    limit {n}
    return {{to: last(split(to,'/')), total_amount: payment_total, num_payments: payment_count}}"""
    totals = database.fetch_list(aql)
    return totals


def get_graph_to_top_payees(database: Database, n: int = 100, min_time: int = 0, max_time: int = int(datetime.utcnow().timestamp())):
    top_accounts = get_top_payees(database, n, min_time, max_time)
    node_addresses = []
    nodes = []
    for node in top_accounts:
        address = node['to']
        node_addresses.append(address)
        nodes.append(database['accounts'][address].getStore())
    aql = f"""for account in accounts
    filter account._key in {[node['_key'] for node in nodes]}
    for v, e, p in 1..1 inbound account payments
        collect from = e._from, to = e._to into edge_groups = e.amount
        let payment_total = sum(edge_groups)
        let payment_count = length(edge_groups)
        sort payment_total desc
        return {{to: last(split(to,'/')), from: last(split(from,'/')), total_amount: payment_total, num_payments: payment_count}}"""
    edges = database.fetch_list(aql)
    for edge in edges:
        from_address = edge['from']
        if from_address not in node_addresses:
            nodes.append(database['accounts'][from_address].getStore())
            node_addresses.append(from_address)
    return nodes, edges


def get_graph_from_top_payers(database: Database, n: int = 100, min_time: int = 0, max_time: int = int(datetime.utcnow().timestamp())):
    top_accounts = get_top_payers(database, n, min_time, max_time)
    node_addresses = []
    nodes = []
    for node in top_accounts:
        address = node['from']
        node_addresses.append(address)
        nodes.append(database['accounts'][address].getStore())
    aql = f"""for account in accounts
    filter account._key in {[node['_key'] for node in nodes]}
    for v, e, p in 1..1 outbound account payments
        collect from = e._from, to = e._to into edge_groups = e.amount
        let payment_total = sum(edge_groups)
        let payment_count = length(edge_groups)
        sort payment_total desc
        return {{to: last(split(to,'/')), from: last(split(from,'/')), total_amount: payment_total, num_payments: payment_count}}"""
    edges = database.fetch_list(aql)
    for edge in edges:
        to_address = edge['to']
        if to_address not in node_addresses:
            nodes.append(database['accounts'][to_address].getStore())
            node_addresses.append(to_address)
    return nodes, edges


def get_outbound_witnesses_for_hotspot(database: Database, address: str):
    aql = f"""for hotspot in hotspots
    filter hotspot.address == '{address}'
    for v, e, p in 1..1 outbound hotspot witnesses
        return{{witness: p.vertices[1]}}"""
    return [witness['witness'] for witness in database.fetch_list(aql)]


def get_inbound_witnesses_for_hotspot(database: Database, address: str):
    aql = f"""for hotspot in hotspots
    filter hotspot.address == '{address}'
    for v, e, p in 1..1 inbound hotspot witnesses
        return{{witness: p.vertices[0]}}"""
    return [witness['witness'] for witness in database.fetch_list(aql)]


def get_witness_graph_near_coordinates(database: Database, lat: float, lon: float, limit: int = 10):
    aql = f"""LET queryCoords = GEO_POINT({lon}, {lat})
    FOR hotspot IN hotspots
        SORT GEO_DISTANCE(queryCoords, hotspot.geo_location)
        LIMIT {limit}
        for v, e, p in 1..1 outbound hotspot witnesses
            sort e._from
            let distance_m = GEO_DISTANCE(p.vertices[0].geo_location, p.vertices[1].geo_location)
            RETURN {{from: last(split(e._from, '/')), to: last(split(e._to, '/')), snr: e.snr, rssi: e.signal, distance_m: distance_m}}"""
    edges = database.fetch_list(aql)
    address_list, nodes = [], []
    vertex_list = [list(edge.values())[:2] for edge in edges]
    for v_pair in vertex_list:
        for v in v_pair:
            if v not in address_list:
                nodes.append(database['hotspots'][v].getStore())
                address_list += v
    return nodes, edges


def get_witness_graph_in_hex(database: Database, hex: str):
    poly_list = [list(p) for p in h3.h3_to_geo_boundary(hex, geo_json=True)]
    nodes_aql = f"""let hex_poly = GEO_POLYGON({poly_list})
    for hotspot in hotspots
    filter GEO_CONTAINS(hex_poly, hotspot.geo_location)
    return {{hotspot}}
    """
    nodes = [hotspot['hotspot'] for hotspot in database.fetch_list(nodes_aql)]
    node_addresses = [node['address'] for node in nodes]
    edges_aql = f"""let hex_poly = GEO_POLYGON({poly_list})
        for hotspot in hotspots
        filter hotspot.address in {node_addresses}
        for v, e, p in 1..1 outbound hotspot witnesses
            filter GEO_CONTAINS(hex_poly, p.vertices[1].geo_location)
            let distance_m = GEO_DISTANCE(p.vertices[0].geo_location, p.vertices[1].geo_location)
            RETURN {{from: last(split(e._from, '/')), to: last(split(e._to, '/')), snr: e.snr, rssi: e.signal, distance_m: distance_m}}"""
    edges = database.fetch_list(edges_aql)
    return nodes, edges
