FROM python:3.10-buster

RUN apt-get update -qq && \
    apt-get install -y libpq-dev python-dev mariadb-client netcat

ARG userid=999
ARG groupid=999
ARG project_root=/project
ARG log_path=$project_root/logs
ARG media_path=$project_root/media
ARG workdir_path=$project_root/catalog-api
ARG wait_for_it_path=$project_root/wait-for-it

RUN groupadd -o --gid $groupid hostgroup && \
    useradd --no-log-init --uid $userid --gid $groupid appuser
RUN mkdir -p $workdir_path \
             $log_path \
             $media_path \
             /tmp/requirements && \
    chown -R appuser:hostgroup /project

WORKDIR $workdir_path

RUN git clone https://github.com/vishnubob/wait-for-it.git $wait_for_it_path

COPY requirements/* /tmp/requirements/

RUN pip install setuptools-scm==6.3.2

RUN pip install -r /tmp/requirements/requirements-base.txt \
                -r /tmp/requirements/requirements-dev.txt \
                -r /tmp/requirements/requirements-tests.txt; \
    rm /tmp/requirements/*; \
    rmdir /tmp/requirements

ENV PYTHONPATH=$workdir_path \
    LOG_FILE_DIR=$log_path \
    MEDIA_ROOT=$media_path \
    PATH=$PATH:$wait_for_it_path \
    DEFAULT_DB_HOST=default-db-dev \
    DEFAULT_DB_PORT=3306 \
    TEST_DEFAULT_DB_HOST=default-db-test \
    TEST_DEFAULT_DB_PORT=3306 \
    TEST_SIERRA_DB_HOST=sierra-db-test \
    TEST_SIERRA_DB_PORT=5432 \
    SOLR_HOST=solr-dev \
    SOLR_PORT=8983 \
    TEST_SOLR_HOST=solr-test \
    TEST_SOLR_PORT=8983 \
    REDIS_CELERY_HOST=redis-celery-dev \
    REDIS_CELERY_PORT=6379 \
    REDIS_APPDATA_HOST=redis-appdata-dev \
    REDIS_APPDATA_PORT=6379 \
    TEST_REDIS_CELERY_HOST=redis-celery-test \
    TEST_REDIS_CELERY_PORT=6379 \
    TEST_REDIS_APPDATA_HOST=redis-appdata-test \
    TEST_REDIS_APPDATA_PORT=6379

USER appuser
