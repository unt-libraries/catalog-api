#!/bin/bash

# Check environment variables for ports, otherwise use default ports
if [ -z ${DJANGO_PORT} ]
    then
        django_port='8000'
    else
        django_port=${DJANGO_PORT}
fi

if [ -z ${SOLR_PORT} ]
    then
        solr_port='8983'
    else
        solr_port=${SOLR_PORT}
fi

if [ -z ${REDIS_PORT} ]
    then
        redis_port='6379'
    else
        redis_port=${REDIS_PORT}
fi

redis_conf=${REDIS_CONF_PATH}
pid_file="pids.txt"
verbose=$1
home_path="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
django_path=$home_path/django/sierra
django_start="python manage.py runserver 0.0.0.0:$django_port"
solr_path=$home_path/solr/instances
solr_start="java -jar start.jar -Djetty.port=$solr_port"
redis_path=$home_path
redis_start="redis-server $redis_conf --port $redis_port"

# check to see if servers are already running; if yes, stop them.
if [ -f $pid_file ]
    then
        echo "Servers are already running."
        bash stop_servers.sh
        wait ${!}
fi

# start Django...
cd $django_path
if [[ $verbose == "django" || $verbose == "all" ]]
    then
        $django_start &
    else
        $django_start &> /dev/null &
fi

echo "Django server started on port $django_port"

# record PID in pid_file
cd $home_path
echo "$!" > $pid_file

# start Solr...
cd $solr_path
if [[ $verbose == "solr" || $verbose == "all" ]]
    then
        $solr_start &
    else
        $solr_start &> /dev/null &
fi

echo "Solr server started on port $solr_port"

# record PID in pid_file
cd $home_path
echo "$!" >> $pid_file

# start Redis...
cd $redis_path
if [[ $verbose == "redis" || $verbose == "all" ]]
    then
        $redis_start &
    else
        $redis_start &> /dev/null &
fi

echo "Redis server started on port $redis_port"

# record PID in pid_file
cd $home_path
echo "$!" >> $pid_file

echo "Servers started in background."
