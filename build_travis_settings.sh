#!/usr/bin/env sh

path_to_settings=django/sierra/sierra/settings/.env
test_settings="SECRET_KEY=\"Some secret key\"
SETTINGS_MODULE=sierra.settings.test
ALLOWED_HOSTS=localhost
ADMINS=\"Joe Test, joe.test@example.com\"
SIERRA_DB_USER=none
SIERRA_DB_PASSWORD=none
SIERRA_DB_HOST=none
DEFAULT_DB_USER=none
DEFAULT_DB_PASSWORD=none
DEFAULT_DB_PORT=3307
TEST_SIERRA_DB_USER=postgres
TEST_SIERRA_DB_PASSWORD=whatever
TEST_SIERRA_DB_PORT=5332
TEST_DEFAULT_DB_USER=mariadb
TEST_DEFAULT_DB_PASSWORD=whatever
TEST_DEFAULT_DB_PORT=3206
TIME_ZONE=America/Chicago
SOLRMARC_CONFIG_FILE=test_config.properties
EXPORTER_EMAIL_ON_ERROR=false
EXPORTER_EMAIL_ON_WARNING=false
EXPORTER_AUTOMATED_USERNAME=django_admin
"

if [ $TRAVIS ]
  then
    echo "$test_settings" > $path_to_settings
    echo "Settings file created:"
    cat $path_to_settings
  else
    echo "This script is not intended for use outside a Travis CI environment."
fi
