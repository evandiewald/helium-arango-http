FROM python:3.9.8-slim-buster

RUN apt-get -y update && apt-get -y install python3-dev openssl build-essential

COPY . .

RUN pip3 install -r requirements.txt

EXPOSE 8000

ENTRYPOINT python3 helium_arango_http/server.py