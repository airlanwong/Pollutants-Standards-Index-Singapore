FROM --platform=linux/amd64 python:3.10-slim-buster

USER root


WORKDIR /usr/src/dbt

RUN pip install --upgrade pip
RUN pip install dbt-snowflake

CMD dbt deps && dbt build && sleep infinity
