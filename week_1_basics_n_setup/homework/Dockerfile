#
# data-engineering-zoomcamp
# Week 1 - Homework
# Dockerfile
# 

FROM python:3.9.13

RUN mkdir /app

COPY requirements.txt /tmp/
RUN pip install wheel -r /tmp/requirements.txt

COPY ingest.py /app

WORKDIR /app
ENTRYPOINT ["python", "ingest.py"]
CMD ["/data/yellow_tripdata_2021-01.csv", "/data/taxi+_zone_lookup.csv", "postgres:5432"]
