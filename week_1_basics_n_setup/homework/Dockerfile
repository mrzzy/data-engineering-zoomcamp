#
# data-engineering-zoomcamp
# Week 1 - Homework
# Dockerfile
# 

FROM python:3.9.13

RUN mkdir /app

COPY requirements.txt /tmp/

# install psycopg2 build dependencyj
RUN apt-get update && apt-get install -y \
  libpq-dev \
  && rm -rf /var/lib/apt/lists/*
RUN pip install wheel -r /tmp/requirements.txt

COPY ingest.py /app

WORKDIR /app
ENTRYPOINT ["python", "ingest.py"]
