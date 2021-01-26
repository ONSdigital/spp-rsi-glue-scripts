FROM python:3.7-slim

COPY dev-requirements.txt /
RUN apt-get update -y &&\
    apt-get install git -y
RUN pip install -r /dev-requirements.txt
