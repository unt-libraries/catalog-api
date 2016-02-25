#!/bin/bash

home_path="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
django_path=$home_path/django/sierra
celery_start="celery -A sierra worker -l info -c 4"
#celery_start="celery -A sierra beat -S djcelery.schedulers.DatabaseScheduler"

# start celery...
cd $django_path
$celery_start
wait ${!}
cd $home_path
