#!/bin/bash
#
# Runs all migrations for default-db-dev, default-db-test, and sierra-db-test.

set -o allexport
source ./django/sierra/sierra/settings/.env
set +o allexport

echo "Migrations will run for default-db-test, sierra-db-test, and default-db-dev."
echo
echo "**************************************"
echo "Running migrations for default-db-test"
echo "**************************************"
echo
docker-compose run --rm manage-test migrate --database=default
echo
echo "*************************************"
echo "Running migrations for sierra-db-test"
echo "*************************************"
echo
docker-compose run --rm manage-test migrate --database=sierra
echo
echo "*************************************"
echo "Running migrations for default-db-dev"
echo "*************************************"
echo
docker-compose run --rm manage-dev migrate
echo
echo "All migrations complete."
