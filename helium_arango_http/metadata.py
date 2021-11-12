title = 'helium-arango-http'

description = """
A RESTful API providing routes for blockchain data stored in a native graph database, which is populated by [`helium-arango-etl`](https://github.com/evandiewald/helium-arango-etl).
"""

version = '0.0.1'

license_info = {
    'name': 'GNU General Public License v3.0',
    'url': 'https://www.gnu.org/licenses/gpl-3.0.en.html'
}

contact = {
    'name': 'helium-arango-http',
    'url': 'https://github.com/evandiewald/helium-arango-http/issues'
}

tags_metadata = [
    {
        'name': 'payments',
        'description': 'Get information about token flow over a given time period.'
    },
    {
        'name': 'hotspots',
        'description': 'Get information about hotspot adjacency, expressed as witness paths.'
    }
]
