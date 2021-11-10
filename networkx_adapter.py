import networkx as nx
from typing import *


def create_networkx_graph(nodes: List[dict], edges: List[dict]) -> nx.Graph:
    g = nx.DiGraph()
    for node in nodes:
        g.add_node(node['address'], attr_dict=node)
    for edge in edges:
        g.add_edge(edge['from'], edge['to'], attr_dict=edge)
    return g