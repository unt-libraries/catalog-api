#!/usr/bin/env sh

path_to_settings=django/sierra/sierra/settings/settings.json
log_file_dir=${TRAVIS_BUILD_DIR}/logs
media_file_dir=${TRAVIS_BUILD_DIR}/django/sierra/media
test_settings="{
  \"SECRET_KEY\": \"Some secret key\",
  \"SETTINGS_MODULE\": \"sierra.settings.dev\",
  \"ALLOWED_HOSTS\": [\"localhost\"],
  \"LOG_FILE_DIR\": \"$log_file_dir\",
  \"MEDIA_ROOT\": \"$media_file_dir\",
  \"ADMINS\": [[\"Joe Test\", \"joe.test@example.com\"]],
  \"SIERRA_DB_USER\": \"none\",
  \"SIERRA_DB_PASSWORD\": \"none\",
  \"SIERRA_DB_HOST\": \"none\",
  \"TEST_SIERRA_DB_USER\": \"postgres\",
  \"TEST_SIERRA_DB_PASSWORD\": \"\",
  \"TEST_SIERRA_DB_HOST\": \"127.0.0.1\",
  \"TIME_ZONE\": \"America/Chicago\",
  \"SOLRMARC_CONFIG_FILE\": \"jt_dev_config.properties\",
  \"EXPORTER_EMAIL_ON_ERROR\": false,
  \"EXPORTER_EMAIL_ON_WARNING\": false,
  \"EXPORTER_AUTOMATED_USERNAME\": \"django_admin\"
}"

if [ $TRAVIS ]
  then
    mkdir $log_file_dir
    echo "$log_file_dir created."
    mkdir $media_file_dir
    echo "$media_file_dir created."
    echo $test_settings > $path_to_settings
    echo "Settings file created:"
    cat $path_to_settings
  else
    echo "This script is not intended for use outside a Travis CI environment."
fi
