FROM python:2.7

ENV PYTHONPATH /project/catalog-api
ENV LOG_FILE_DIR /project/logs
ENV MEDIA_ROOT /project/media

RUN mkdir /project
RUN mkdir /project/catalog-api
RUN mkdir /project/requirements
RUN mkdir /project/logs
RUN mkdir /project/media
WORKDIR /project/catalog-api

RUN apt-get update -qq && apt-get install -y libpq-dev python-dev mysql-client netcat

COPY requirements/requirements-base.txt /project/requirements/
COPY requirements/requirements-dev.txt /project/requirements/
COPY requirements/requirements-tests.txt /project/requirements/

RUN pip install -r /project/requirements/requirements-base.txt
RUN pip install -r /project/requirements/requirements-dev.txt
RUN pip install -r /project/requirements/requirements-tests.txt

RUN git clone https://github.com/vishnubob/wait-for-it.git ../wait-for-it

ENV PATH $PATH:/project/wait-for-it
ENV DEFAULT_DB_HOST default-db-dev
ENV DEFAULT_DB_PORT 3306
ENV TEST_DEFAULT_DB_HOST default-db-test
ENV TEST_DEFAULT_DB_PORT 3306
ENV TEST_SIERRA_DB_HOST sierra-db-test
ENV TEST_DEFAULT_SIERRA_DB_PORT 5432
ENV SOLR_HOST solr-dev
ENV SOLR_PORT 8983
ENV TEST_SOLR_HOST solr-test
ENV TEST_SOLR_PORT 8983
