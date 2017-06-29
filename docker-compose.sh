#!/bin/bash
#
# Wraps docker-compose to provide environment variables set via
# django/sierra/sierra/settings/.env to docker-compose. Simply run this
# wherever and however you would run docker-compose, e.g.:
#
# ./docker-compose.sh up -d

set -o allexport
source ./django/sierra/sierra/settings/.env
USERID=$(id -u)
GROUPID=$(id -g)
set +o allexport
docker-compose "$@"
