from pyArango.theExceptions import *
from pyArango.connection import *
from pyArango.graph import *
from pyArango.collection import *
# from arango_schema import *
from typing import *


def process_query(database: Database, aql: str, raw_results: bool = True, batch_size: int = 1) -> List[dict]:
    result_set = database.fetch_list(aql)
    return result_set


def get_top_payment_totals(database: Database, n: int = 100, min_time: int = 0, max_time: int = int(datetime.utcnow().timestamp())):
    aql = f"""for payment in payments
    filter payment.time > {min_time} and payment.time < {max_time}
    collect from = payment._from, to = payment._to into payment_groups = payment.amount
    let payment_total = SUM(payment_groups)
    sort payment_total desc
    limit {n}
    return {{from: from, to: to, payment_total: payment_total}}"""
    totals = database.fetch_list(aql)
    return totals


def get_top_payment_counts(database: Database, n: int = 100, min_time: int = 0, max_time: int = int(datetime.utcnow().timestamp())):
    aql = f"""for payment in payments
    filter payment.time > {min_time} and payment.time < {max_time}
    collect from = payment._from, to = payment._to into payment_groups = payment.amount
    let payment_count = LENGTH(payment_groups)
    sort payment_count desc
    limit {n}
    return {{from: from, to: to, payment_count: payment_count}}"""
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

