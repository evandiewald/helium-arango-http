from sklearn.cluster import KMeans
from typing import List


def get_cluster_centers(data, n_clusters: int) -> tuple[List[list], float]:
    """
    Identify cluster centers from dataset. Useful for finding city centers from list of hotspot coordinates.
    :param data:
    :param n_clusters:
    :return:
    """
    model = KMeans(n_clusters).fit(data)
    return model.cluster_centers_.tolist(), model.inertia_