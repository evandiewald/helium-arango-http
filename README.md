# helium-arango-http
A RESTful API providing routes for blockchain data stored in a native graph database.

## Getting Started
Make a copy of the `.env.template` file called `.env` and provide the necessary environment variables.

Start the FastAPI server with:

`$ uvicorn server:app --host 0.0.0.0 --port 80`

## Documentation
Swagger documentation for each route is provided at the `/docs` endpoint.

## Related Projects
[`helium-arango-etl`](https://github.com/evandiewald/helium-arango-etl): an ETL service that transforms relational blockchain data into a native graph format.

[`helium-arango-tools`](https://github.com/evandiewald/helium-arango-tools): (coming soon)