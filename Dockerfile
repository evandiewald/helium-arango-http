FROM python:3.9.8-slim-buster

RUN apt-get -y update && apt-get -y install python3-dev openssl build-essential

COPY . .

RUN pip3 install -r requirements.txt

RUN mkdir logs

EXPOSE 8000

ENTRYPOINT python3 api/server.py