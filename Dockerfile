FROM python:3.9.8-slim-buster

RUN apt-get -y update && apt-get -y install python3-dev openssl build-essential redis-server

COPY . .

RUN pip3 install -r requirements.txt

EXPOSE 8000

RUN redis-server --daemonize yes

ENTRYPOINT python3 helium_arango_http/server.py