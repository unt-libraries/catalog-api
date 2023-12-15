#!/usr/bin/env bash

# Set up needed ENV variables.
set -o allexport
source ./django/sierra/sierra/settings/.env
source ./docker-compose.env
USERID=$(id -u)
GROUPID=$(id -g)
set +o allexport

# I've moved the following variables into `docker-compose.env`:
#   DDPATH
#   SIERRA_FIXTURE_PATH
#   SOLRCONF_PATH
#   SOLRCONF_DOCKER_MOUNT_PATH
#   DEV_SERVICES
#   TEST_SERVICES
#   ALL_SERVICES

SCRIPTNAME="$(basename "$0")"


### FUNCTIONS ###

# service_name_to_env_prefix -- Utility function to convert a Docker-compose
# service name, like `solr-test`, to an ENV variable prefix, like `SOLR_TEST`.
function service_name_to_env_prefix {
  echo $1 | awk '{ gsub("-", "_", $0); print toupper($0) }'
}

# show_help -- Display usage/help text
function show_help {
  echo ""
  echo "Usage: $SCRIPTNAME [-f] [-v | -m] group"
  echo "       $SCRIPTNAME [-f] [-v | -m] service ..."
  echo ""
  echo "-f        Force overwriting existing volumes on the host machine."
  echo "-h        Display this help message."
  echo "-m        Only run migrations (skip volume set up). Cannot be used with -v."
  echo "-s        Display a list of valid service names and descriptions."
  echo "-v        Only run volume setup on host machine (skip migrations). Cannot be"
  echo "          used with -m."
  echo "group     Provide one argument to set up multiple services. Must be \"all\","
  echo "          \"dev\", or \"tests\". \"all\" sets up all services."
  echo "service   One or more service names to initialize. Note that services are set up"
  echo "          in the order specified. Use -s to see more info about services."
  echo ""
  echo "Please note that you are not prevented from specifying a valid group along with"
  echo "one or more services. The group will be expanded into individual services, and"
  echo "they will all be initialized in the order specified. However, including a"
  echo "service name more than once will cause the setup for that service to be run more"
  echo "once. So, be careful, especially if using the -f flag."
  echo ""
  echo "Use -s to get information about services."
  echo ""
  exit 1
}

# show_services -- Display information about services
function show_services {
  echo ""
  echo "Services"
  echo "These are the services that you can set up with this script. Note that there"
  echo "are a few catalog-api Docker services not listed here because they require no"
  echo "local volumes to be set up."
  echo ""
  echo "(service -- group)"
  for dev_service in ${DEV_SERVICES[@]}; do
    local service_def_var=$(service_name_to_env_prefix $dev_service)_DEF
    echo ""
    echo "$dev_service -- dev"
    echo "${!service_def_var}"
  done
  for test_service in ${TEST_SERVICES[@]}; do
    local service_def_var=$(service_name_to_env_prefix $test_service)_DEF
    echo ""
    echo "$test_service -- test"
    echo "${!service_def_var}"
  done
  echo ""
  exit 1
}

# show_summary -- Display summary of what the script will do when it runs,
# based on the user-provided options.
function show_summary {
  local actions="$1"
  local user_services="$2"
  local force="$3"

  do_what="migrate data"
  if [[ $actions == *"v"* ]]; then
    do_what="make volumes"
    if [[ $actions == *"m"* ]]; then
      do_what+=" and migrate data"
    fi
    if [[ $force ]]; then
      use_force="-f (force) is set. Any existing data for these services will be deleted and empty volumes will be recreated."
    else
      use_force="-f (force) is not set. Existing data will be preserved."
    fi
  else
    if [[ $force ]]; then
      use_force="Warning: -f (force) flag is set but will have no effect, as it only affects volume creation (NOT migrations)."
    fi
  fi

  echo ""
  echo "--------------------------------------"
  echo "INITIALIZE DOCKER DATA FOR CATALOG-API"  
  echo "This will attempt to $do_what for these catalog-api docker-compose services: $user_services."
  if [[ $use_force ]]; then echo $use_force; fi
  echo ""
}

# warm_up_sierra_db_test -- Forces postgres to run initdb to create the DB for
# sierra-db-test as the default postgres user and issues a chown on the DB
# to change the owner to the appropriate user. This is a workaround for the
# fact that PostgreSQL won't initialize a database for a user that doesn't
# exist in /etc/passwd in the container. After the database is initialized
# and the owner changed, you can run the postgres server as the correct user
# even though it still doesn't exist in /etc/passwd.
function warm_up_sierra_db_test {
  echo "Initializing PostgreSQL database for \`sierra-db-test\` service"
  local container=$(docker compose run -u root -d sierra-db-test)
  #container="${container##*$'\n'}"
  container=$(echo "$container" | tail -1)
  printf "(waiting for database) ..."
  local limit=60
  local waited=0
  while ! docker logs $container 2>&1 | grep -q "PostgreSQL init process complete"; do
    printf "."; sleep 3; let waited+=3;
    if [[ $waited -ge $limit ]]; then
      echo "Error: Timed out while waiting for sierra-db-test database to be created. Database NOT properly initialized."
      docker stop $container && docker rm $container &> /dev/null
      return 1
    fi
  done
  echo "database created."
  echo "Stopping intermediate container."
  sleep 2; docker stop $container && docker rm $container &> /dev/null; sleep 2;
  echo "Changing ownership of pgdata directory to current user."
  docker compose run --rm -u root --entrypoint="sh -c \"chown -R $USERID:$GROUPID /var/lib/postgresql/data\"" sierra-db-test
  echo "Done. Database initialized."
  return 0
}

# init_solr_volume -- This takes the path to the service volume (e.g.
# /docker_data/solr-test) and creates the filesystem structure for the Solr
# home directory, for all the cores in the $SOLRCONF_PATH. It creates symlinks
# to configuration files in $SOLRCONF_PATH using the
# $SOLRCONF_DOCKER_MOUNT_PATH as the base path to the linked file; note that
# this should be the volume mount point (relative to the docker container)
# defined in `docker-compose.yml` for the service. Relative to the host
# filesystem, unless you've mounted $SOLRCONF_PATH at the
# $SOLRCONF_DOCKER_MOUNT_PATH, the target path doesn't exist, so if you examine
# the links that are created in the Docker data directory, they will appear
# broken. But they will work as intended in the Docker container, because the
# $SOLRCONF_PATH directory will be mounted there.
function init_solr_volume {
  local path=$1
  echo "Initializing Solr volume $path ..."
  mkdir "$path/logs"
  mkdir "$path/data"
  ln -s "$SOLRCONF_DOCKER_MOUNT_PATH/log4j2.xml" "$path/log4j2.xml"
  ln -s "$SOLRCONF_DOCKER_MOUNT_PATH/solr.xml" "$path/data/solr.xml"
  for dir in $SOLRCONF_PATH/*/; do
    if [[ $dir =~ ([^/]+)/$ ]]; then
      local corename="${BASH_REMATCH[1]}"
      local corepath="$path/data/$corename"
      mkdir "$corepath"
      mkdir "$corepath/data"
      touch "$corepath/core.properties"
      ln -s "$SOLRCONF_DOCKER_MOUNT_PATH/$corename/conf" "$corepath/conf"
    fi
  done
  echo "Done. Solr volume initialized."
  return 0
}

# make_volume -- Takes args $path, $force, $service. Sets up the data volume
# for $service at $path if the $path does not exist. If the $path and $force
# exist, then it rm -rf's $path first. Returns 0 if it created a fresh volume
# successfully and 1 if it did not.
function make_volume {
  local path="$1"
  local service="$2"
  local force="$3"
  if [[ -d $path ]]; then
    if [[ $force ]]; then
      echo "Deleting existing data volume on host at $path."
      rm -rf $path
    else
      echo "Warning: data volume for service $service already exists on host at $path. Use -f to force overwriting existing data with a fresh data volume."
      return 1
    fi
  fi
  echo "Creating new data volume on host at $path."
  mkdir -p $path
}

# prepvolume_[service] functions. Define new functions with this naming pattern
# to run any setup that needs to happen between the make_volume and migration
# steps. Each prepvolume function takes an argument that tells you whether or
# not a new volume was created with make_volume.

# prepvolume_sierra_db_test -- Wrapper for warm_up_sierra_db_test.
function prepvolume_sierra_db_test {
  local volume_was_created=$1
  if [[ $volume_was_created ]]; then
    warm_up_sierra_db_test
    return $?
  else
    return 0
  fi
}

# prepvolume_solr_test_for_update
function prepvolume_solr_test_for_update {
  local volume_was_created=$1
  local path="$DDPATH/solr_test_for_update"
  if [[ $volume_was_created ]]; then
    init_solr_volume "$path"
    return $?
  else
    return 0
  fi
}

# prepvolume_solr_test_for_search
function prepvolume_solr_test_for_search {
  local volume_was_created=$1
  local path="$DDPATH/solr_test_for_search"
  if [[ $volume_was_created ]]; then
    init_solr_volume "$path"
    return $?
  else
    return 0
  fi
}

# prepvolume_solr_dev
function prepvolume_solr_dev {
  local volume_was_created=$1
  local path="$DDPATH/solr_dev"
  if [[ $volume_was_created ]]; then
    init_solr_volume "$path"
    return $?
  else
    return 0
  fi
}

# migrate_[service] functions. Define new functions with this naming pattern to
# migrate data (e.g. create database structures, load data fixtures, etc.) for
# a particular service. Each migrate function takes an argument that tells you
# whether the volume is ready for migrations or not.

function migrate_default_db_dev {
  docker compose run --rm manage-dev migrate --database=default
}

function migrate_default_db_test {
  docker compose run --rm manage-test migrate --run-syncdb --database=default
}

function migrate_sierra_db_test {
  local volume_is_ready=$1
  if [[ $volume_is_ready ]]; then
    docker compose run --rm manage-test migrate --run-syncdb --database=sierra
    echo "Installing sierra-db-test fixtures..."
    docker compose run --rm manage-test loaddata --app=base --database=sierra $(find $SIERRA_FIXTURE_PATH/*.json -exec basename {} .json \; | tr '\n' ' ')
  else
    echo "Warning: Database could not be initialized; skipping migrations for \`sierra-db-test\`"
  fi
}

### PARSE OPTIONS ###

user_services=()
want_make_volumes="true"
want_do_migrations="true"

while getopts :fmvhs FLAG; do
  case $FLAG in
    f)
      force="true"
      ;;
    m)
      want_make_volumes=""
      ;;
    v)
      want_do_migrations=""
      ;;
    h)
      show_help
      ;;
    s)
      show_services
      ;;
    \?)
      echo "Unrecognized option $OPTARG."
      echo "Use $SCRIPTNAME -h to see help."
      exit 2
      ;;
  esac
done

shift $((OPTIND-1))

if [[ ! $want_make_volumes && ! $want_do_migrations ]]; then
  echo "You cannot use the -m and -v flags together."
  echo "Use $SCRIPTNAME -h to see help."
  exit 2
fi

if [[ $# -eq 0 ]]; then
  echo "Error: you must specify at least one service or group of services."
  echo "Use $SCRIPTNAME -h to see help or -s to see a list of services."
  exit 2
fi

for arg in $@; do
  case $arg in
    all)
      user_services+=("${ALL_SERVICES[@]}")
      ;;
    dev)
      user_services+=("${DEV_SERVICES[@]}")
      ;;
    tests)
      user_services+=("${TEST_SERVICES[@]}")
      ;;
    *)
      user_services+=("$arg")
      ;;
  esac
done

### MAIN ###

actions=$([[ $want_make_volumes ]] && echo "v")$([[ $want_do_migrations ]] && echo "m")
show_summary $actions "${user_services[*]}" $force

echo "Stopping any running catalog-api Docker services ..."
docker compose down &> /dev/null

# First, loop over user-provided $user_services. Validate each service and set
# volumes up as appropriate
services=()
volumes_were_created=()
for service in ${user_services[@]}; do
  service_paths_var=$(service_name_to_env_prefix $service)_PATHS[@]
  if [[ -n ${!service_paths_var} ]]; then
    services+=($service)
    if [[ $want_make_volumes ]]; then
      volume_was_created="true"
      echo ""
      echo "Making data volume(s) for \`$service\`."
      for path in ${!service_paths_var}; do
        if ! make_volume "$path" $service $force; then
          volume_was_created="false"
        fi
      done
    else
      echo ""
      echo "Warning: Skipping \`$service\`. Either it is not a valid service, or it does not use data volumes."
      volume_was_created="false"
    fi
    volumes_were_created+=($volume_was_created)
  fi
done

# Now loop over all valid services, and this time run any existing
# prepvolume_[service] commands. If the prepvolume command returns anything
# other than 0, then assume something went wrong and the volume is NOT ready.
i=0
volumes_are_ready=()
for service in ${services[@]}; do
  volume_is_ready="true"
  if [[ $want_make_volumes ]]; then
    prep_command="prepvolume_${service//-/_}"
    if [[ "$(type -t $prep_command)" == "function" ]]; then
      echo ""
      echo "Running prepvolume for \`$service\`."
      created_arg=$([[ ${volumes_were_created[$i]} == "true" ]] && echo "true" || echo "")
      if ! $prep_command $created_arg; then
        volume_is_ready="false"
      fi
    fi
  fi
  volumes_are_ready+=($volume_is_ready)
  let i+=1
done

# Finally, if the user wants migrations run, then loop over all valid services
# again and run any existing migrate_[service] commands, passing in a value
# indicating whether or not the volume is ready.
if [[ $want_do_migrations ]]; then
  i=0
  for service in ${services[@]}; do
    migrate_command="migrate_${service//-/_}"
    if [ "$(type -t $migrate_command)" == "function" ]; then
      echo ""
      echo "Running migrations for \`$service\`."
      ready_arg=$([[ ${volumes_are_ready[$i]} == "true" ]] && echo "true" || echo "")
      $migrate_command $ready_arg
    else
      echo ""
      echo "No migrations found for \`$service\`."
    fi
    let i+=1
  done
fi

echo ""
echo "Done. Stopping all running services."
docker compose down &> /dev/null
echo ""
echo "$SCRIPTNAME finished."
echo ""
