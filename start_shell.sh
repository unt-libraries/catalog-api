#!/bin/bash

home_path="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
django_path=$home_path/django/sierra
shell_start="python manage.py shell"

# start shell...
cd $django_path
$shell_start
wait ${!}
cd $home_path
