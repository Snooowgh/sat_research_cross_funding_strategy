FROM --platform=linux/amd64 python:3.12.11
#FROM arm64v8/python:3.12.11
#FROM amd64/python:3.14-rc-trixie


MAINTAINER darwin_light official@darwinfi.capital
WORKDIR /app

#RUN apk add build-base
#RUN apk add --no-cache gcc musl-dev linux-headers libffi-dev openssl-dev
#RUN apk add --no-cache mariadb-dev bash curl git
RUN apt-get install bash
RUN python -m pip install --upgrade pip

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt
