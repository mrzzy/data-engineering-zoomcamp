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
CMD ["python", "ingest.py", "-h"]
