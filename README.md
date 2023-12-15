# Catalog API

* [About](#about)
* [Setting up Sierra Users](#sierra-users)
* [Installation and Getting Started, Docker](#installation-docker) (Recommended
for development)
* [Installation and Getting Started, Non-Docker](#installation-nondocker)
(Recommended for production)
* [Configuring Local Settings](#local-settings)
* [Testing](#testing)
* [License](#license)
* [Contributors](#contributors)


<a name="about"></a>About
-------------------------

The Catalog API (or catalog-api) is a Python Django project that provides a
customizable REST API layer for Innovative Interfaces' Sierra ILS. This
differs from the built-in Sierra API in a number of ways, not least of which is
that the API design is fully under your control. In addition to a basic API
implementation, a complete toolkit is provided that allows you to turn any of
the data exposed via Sierra database views (and even data from other sources)
into your own API resources.

### Key Features

- All 300+ Sierra database views are modeled using the Django ORM.

- [Django Rest Framework](http://www.django-rest-framework.org/) provides the
API implementation. Serializers and class-based views are easy to extend.

- The API layer has a built-in browseable API view, and content negotiation is
supported. Visit API URLs in a web browser and get nicely formatted, browseable
HTML; request resources in JSON format and get JSON.

- [HAL](http://stateless.co/hal_specification.html), or Hypertext Application
Language (hal+json), is the media type that is used to serve the built-in
resources. "HAL is a simple format that gives a consistent and easy way to
hyperlink between resources in your API." _But_ you are not restricted to using
HAL—you are free to implement the media types and formats that best fit
your use cases.

- The API supports a wide range of query filters, and more are planned: equals,
greater than, less than, in, range, regular expressions, keyword searches,
and more.

- Your API data is completely decoupled from your Sierra data. An extensible
_exporter_ Django app allows you to define custom 
[ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) processes. Solr
instances that store and index data for the API are included. Or, you can set
up your own data storage and tie your exporters and REST Framework views and
serializers into it.

- Although Sierra data is read-only, the API framework does allow you to
implement POST, PUT, PATCH, and DELETE methods along with GET. So, you can
create your own editable fields on API resources that don't get stored in
Sierra; in fact, you could create resources that merge data from a variety of
sources. Data that isn't sourced from Sierra can be merged when your export
jobs run.

- Accessing API resources _only_ accesses the data in Solr and Redis; it
doesn't hit the Sierra database at all. Thus, API performance is isolated from
performance of your Sierra database, and API usage has no impact on your
Sierra database. You don't have to worry about API users running up against
the concurrent connection limit in Sierra.

- [Celery](http://www.celeryproject.org/) provides an asynchronous task queue
and scheduler. Set up your exporters to run as often as you need so that your
API stays in synch with Sierra as data in Sierra is added, updated, or deleted.

- API resources can be grouped and completely compartmentalized into reusable
Django apps. New apps can expose new resources and/or override the default
base resources. (The _shelflist_ app provides an example of this.)


### Project Structure

There are three directories in the project root: `django`, `requirements`, and
`solrconf`.

The `requirements` directory simply contains pip requirements files for
various environments — dev, production, and tests.

The `django` directory contains the Django project and related
code, in `django\sierra`. The `manage.py` script for issuing Django
commands is located here, along with the apps for the project:

- `api`: Contains the Django REST Framework implementation of the default
API resources, which include apiusers, bibs, items, eresources,
itemstatuses, itemtypes, and locations.
- `base`: Contains the Django ORM models for Sierra, certain rulesets used in
data transformations, and search index definitions for Haystack.
- `export`: Contains code for exporters, including definitions of the
exporters themselves, models related to export jobs, changes to the Django
admin interface to allow you to manage and track export jobs, and tasks
for running export jobs through Celery.
- `shelflist`: Implements a shelflistitems resource in the API; contains
overrides for export classes and api classes that implement the resource
and add links to shelflistitems from item resources and location resources.
This provides an example of how you could create Django apps with self-
contained functionality for building new features onto existing API
resources.
- `sierra`: Contains configuration and settings for the project.
- `utils`: Not really a Django app, but contains miscellaneous tools and
utilities used throughout the project.

The `solrconf` directory contains necessary Solr configuration — core 
configuration for Discover (our Blacklight app whose indexes are maintained via
the Catalog API) and Haystack cores. This is designed so you can copy each
core/conf directory to your Solr server.

<a name="sierra-users"></a>Setting up Sierra Users
--------------------------------------------------

Before getting started, you should first take a moment to set up Sierra users.
The catalog-api requires access to Sierra to export data, and you must create
a new Sierra user for each instance of the project that will be running
(e.g., for each dev version, for staging, for production). Be sure that
each user has the _Sierra SQL Access_ application assigned in the Sierra
admin interface.


<a name="installation-docker"></a>Installation and Getting Started, Docker
--------------------------------------------------------------------------

The recommended setup for development is to use Docker and Docker Compose
to help automate building and managing the environment in which the catalog-api
code runs. It is simpler to manage than the manual method and is thus well-
suited for testing and development, but it is not meant for production
deployment.

The repository contains configuration files for Docker (`Dockerfile`) and
Docker Compose (`docker-compose.yml`) that define how to build, configure, and
run catalog-api processes. All processes that comprise the catalog-api system,
including databases, Solr, and Redis, run as services inside their own Docker
containers. They remain isolated from your host system, except insofar as they,
1. share a kernel, 2. use data volumes to store persistent data on the host,
and 3. map container ports to host ports to expose running services. Running
software in a container is otherwise similar to running it in a virtual machine.

If you are not familiar with Docker, we recommend that you at least run through
the basic [Docker Getting Started](https://docs.docker.com/get-started/)
tutorials and [Get Started with Docker Compose](https://docs.docker.com/compose/gettingstarted/)
before you proceed. Understanding images, containers, and services is
especially key to understanding how to troubleshoot.

### Requirements

- [Docker](https://www.docker.com)
- [Docker Compose](https://docks.docker.com/compose/)


### Setup Instructions

#### Install Docker and Docker Compose.

- [Download Docker here.](https://www.docker.com/pricing/#download)
If you're using Docker for Mac or Windows, Docker Compose is included.
- If you're using Docker on Linux or otherwise need to install Docker
Compose separately, you can
[view Docker Compose installation instructions here](https://docs.docker.com/compose/install/).

#### Clone the repository to your local machine.

Use the `git clone` command plus the appropriate URL to create a local copy of
the repository. For instance, to clone from GitHub, using SSH, into a local
`catalog-api` directory:

```bash
git clone git@github.com:unt-libraries/catalog-api.git catalog-api
```

#### Configure local settings.

For environment-specific settings, such as secrets and database connection
details, you should create a .env settings file. Use
[the instructions included below](#local-settings).
  
#### Build the Docker environment(s).
  
In the repository root, you can run

```bash
./init-dockerdata.sh all
```
 
It will take several minutes to finish, but it should complete these steps:
 
- Pull images needed for things like MariaDB, PostGreSQL, and Redis from
Docker Cloud.
- Build the catalog-api custom image from the supplied `Dockerfile` for the
various app-related services.
- Set up the `docker_data` directory and all necessary subdirectories,
setting the owning UID/GID to that of the current user.
- Create databases, as needed.
- Run database/Django migrations as needed to set up database schemas and
load initial data.

However, when running the build in a CI environment, we've found it
necessary to do an explicit pull, then a build, and then finally run the
init script. So, if you get errors while running the init script as above,
you may try doing this instead. E.g.:
    
```bash
./docker-compose.sh pull
./docker-compose.sh build
./init-dockerdata.sh all
```

#### (Optional) Run tests.

If you wish, you can try
[running Sierra database/model tests](#sierra-db-checks) to make sure that
Django is correctly reading from your production Sierra database.

You may also [run unit tests](#unit-tests).

#### Generate a new secret key for Django.

```bash
./docker-compose.sh run --rm manage-dev generate_secret_key
```

Copy/paste the new secret key into your `SECRET_KEY` environment variable.

#### Create a superuser account for Django.

```bash
./docker-compose.sh run --rm manage-dev createsuperuser
```

Go through the interactive setup. Remember your username and password, as
you'll use this to log into the Django admin screen for the first time.
(You can create additional users from there.)

#### Start the app.

There are two main Docker Compose services that you'll use during
development: one to control the app (i.e., to run the Django web server)
and one to control the Celery worker.

You can start them up individually like so:

```bash
./docker-compose.sh up -d app
./docker-compose.sh up -d celery-worker
```

Other services, such as your database, Solr, and Redis, are started
automatically if they aren't already running.

Note that the `-d` flag runs these as background processes, but you can run
them in the foreground (to write ouput to stdout) by ommitting the flag.

#### Check to make sure everything is up.

- Go to http://localhost:8000 (or use whatever port you've set up as the
`DJANGO_PORT` in your environment). It should redirect you to
http://localhost:8000/api/v1/ and display the Api Root resource.
- Go to http://localhost:8000/admin/export/. Log in with the username and
password you set up in step 6, and you should see a Django admin screen for
managing Sierra exports. Try clicking "View, Edit, and Add Export Types" to
make sure you see entries listed.
- Go to http://localhost:8983/solr/ (using whatever port you've set up as the
- `SOLR_PORT` in your environment). You should see an Apache Solr admin screen.
- Click the "core selector" dropdown and make sure you can select the
- discover-01, discover-02, and haystack cores and that they're all empty.
- Check your `docker_data/celery-worker/logs` directory, and view the
`celery-worker.log` file. Make sure that the last line has a timestamp and says
that the celery worker is ready. (Ignore any warning messages about using
settings.DEBUG.)

#### Check to make sure Sierra data exports work.

Follow the steps [in this section](#testing-exports) to make sure you can
export data from Sierra and view the results in the API.
   
#### Stop running services.

Whenever you're finished, you can stop all running catalog-api services
and remove all containers with one command.

```bash
./docker-compose.sh down
```

Even though containers are removed, data stored on data volumes remains.
Next time you start up the catalog-api services, your data should still be
there.

### More About the Docker Setup

#### Docker Compose Config File Version and Docker Swarm

Be aware that we have not tested our setup with a Docker swarm, even though the
`docker-compose.yml` file does conform to the version 3 specification.

#### Running Docker Compose Services, `docker-compose.sh`

Nearly everything you'll need to do — building images, running containers,
starting services, running tests, and running Django `manage.py`
commands — is implemented as a Docker Compose service.

Normally you'd run services by issuing `docker-compose` commands. But for this
project, you should use the provided shell script, `docker-compose.sh`,
instead. This script simply loads environment variables from your .env settings
file, effectively making those available to `docker-compse.yml`, before passing
your arguments on to `docker-compose`.

In other words, instead of issuing a command like
`docker-compose run --rm test`, you'd run `./docker-compose.sh run --rm test`.

#### Data Volumes

We store persistent data, such as database data, within data volumes created on
your host machine and mounted in the appropriate container(s).

For simplicity, all Docker volumes are created under a `docker_data` directory
within the root repository directory. This directory is ignored in
`.gitignore`.

Each service has its own subdirectory within `docker_data`. In some cases this
subdirectory is the mount point (such as with PostGreSQL and MariaDB), and in
other cases this directory contains additional children directories to separate
things like logs from data (as with the Redis and Solr services).

The containers/services that run the catalog-api code mount the root
catalog-api directory on your host machine as a data volume. Updating code
locally on your host also updates it inside the running containers — so,
you don't have to rebuild images with every code update.

#### Initializing Databases, `init-dockerdata.sh`

We've created an `init-dockerdata.sh` shell script to help make it easier to
initialize Docker data volumes so that services will run correctly. Database
migrations can be managed through this script, too. The setup instructions
above use this script to initialize data volumes during first-time setup, but
you can also use it to wipe out data for a particular service and start with a
clean slate.

Run

```bash
./init-dockerdata.sh -h
```

for help and usage information.


#### Tests and Test Data

The catalog-api project has complex testing needs, and Docker provides an ideal
way to meet those needs. Test instances of the default Django database, the
Sierra database, Solr, and Redis are implemented as their own Docker Compose
services. Running the `test` and `manage-test` services tie the catalog-api
code into these test instances before running tests by invoking Django using
the `sierra.settings.test` settings.

To ensure that tests run quickly, the test databases and some test data are
stored in data volumes and can be initialized alongside the development
databases using `init-dockerdata.sh`.

#### Building the catalog-api Image

The `Dockerfile` contains the custom build for the catalog-api services defined
in `docker-compose.yml`: app, celery-worker, test, manage-test, and manage-dev.
The first time you run any of these services, the image will be built, which
may take a few minutes. Subsequently, running these services will use the
cached image.

As mentioned above, changes to the catalog-api code do not require the image to
be rebuilt. However, changes to installed requirements *do*. For example, if
you need to update any of the requirements files, installed Python libraries
will not be updated in your containers until you issue a
`docker-compose.sh build` command. In other words, where you might otherwise
run `pip install` to install a new library in your local environment, you'll
instead update the requirements file with the new library name/version and then
rebuild the image to include the new library.

Official images for all other services are pulled from Docker Cloud.
  
<a name="installation-nondocker"></a>Installation and Getting Started, Non-Docker
---------------------------------------------------------------------------------
Steps below outline the manual, non-Docker setup. If you're creating a
production environment, these may be the basis for your setup but likely would
not result in the exact configuration you'd want. They are geared more toward
a pre-production environment, as they assume that everything will be installed
on the same machine, which would not be the case in a production environment.

We do include tips for production configuration, where possible.

### Requirements

- Python 3 >= 3.9
- [Redis](http://redis.io/)
- [Solr](https://solr.apache.org/)
- Whatever additional prerequisites are needed for your database software,
such as the mysql-development library and 
[mysqlclient](https://pypi.python.org/pypi/mysqlclient) if you're using MySQL
or MariaDB.

#### Install prerequisites.

##### Python 3 >= 3.9

Personally I like using [pyenv](https://github.com/pyenv/pyenv) for installing
and managing different Python versions. You can also install
[pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) if you want to
manage your virtualenvs using pyenv. Otherwise, you can just stick with the
`venv` tool that's part of Python now.

##### Requirements for psycopg2

In order for [psycopg2](https://pypi.org/project/psycopg2/) to build correctly,
you'll need to have the appropriate dev packages installed in your OS.

Ubuntu/Debian:

```bash
sudo apt-get install libpq-dev python-dev
```

Red Hat:

```bash
sudo yum install python-devel postgresql-devel
```

On Mac, with homebrew:
            
```bash
brew install postgresql
```

##### Redis

Redis is required to serve as a message broker for Celery. It's also used to
store some application data. You can follow the
[getting started guide](https://redis.io/docs/getting-started/) to get started,
but please make sure to set up your `redis.conf` file appropriately.

Default Redis settings only save your data periodically, so you'll want
to take a look at
[how Redis persistence works](https://redis.io/docs/management/persistence/).
I'd recommend RDB snapshots _and_ AOF persistence, but you'll have to turn
AOF on in your configuration file by setting `appendonly yes`. Note
that if you store the `dump.rdb` and/or `appendonly.aof` files anywhere
in the catalog-api project _and_ you rename them, you'll need to add
them to `.gitignore`.

***Production Notes***

The section _Install Redis more properly_ in the "getting started" guide
contains useful information for deploying Redis in a production environment.

You'll also want to be sure to take a look at the _Securing Redis_
section. It's HIGHLY recommended that you at least have Redis behind a firewall
and set a default user password for each of the two Redis instances that will
be running.

To configure your default user, include a line such as this in your Redis
`.conf` file.

```
user default on ~* &* +@all -@admin -@dangerous #PASSWORDHASH-SHA256
```

This line does a few things:

1. It gives the default user access to all keys (`~*`) and all channels (`&*`)
in that instance/database.
2. It gives the user permission to use all commands (`+@all`) EXCEPT admin
(`-@admin`) and dangerous (`-@dangerous`) ones.
3. It sets the password for the default user. Since the `conf` file is stored
in plain text, setting the SHA256 hash here is safer than storing the password,
although you can store the password itself and just write-protect the file.

Redis ACL settings are complex; you can configure multiple users with various
roles as needed. However, the Redis Python package does not yet have good
support for users besides the default one.

##### Solr

[Get Solr here](https://solr.apache.org/downloads.html). The Solr Reference
Guide has
[instructions for installing Solr](https://solr.apache.org/guide/solr/latest/deployment-guide/installing-solr.html).
If you're deploying for production, I'd recommend following
[the production deployment instructions](https://solr.apache.org/guide/solr/latest/deployment-guide/taking-solr-to-production.html)
instead, using the service installation script.

Once you've installed Solr, you must also install the necessary cores or
collections using the provided configuration files in
`solrconf/discover-01/conf`, `solrconf/discover-02/conf`, and
`solrconf/haystack/conf`. Exactly how you do this depends on whether or not you
are running Solr in SolrCloud mode.
[The configuration](https://solr.apache.org/guide/solr/latest/configuration-guide/configuration-files.html)
sections of the Solr Reference Guide can help you figure out where to put
these.

***Production Notes***

In a production environment, you will want Solr running on its own server(s).
Don't attempt to run it on the same server running the Catalog API.
[Solr architecture is a whole topic unto itself](https://solr.apache.org/guide/solr/latest/deployment-guide/cluster-types.html),
but the Catalog API supports running a standalone Solr server or using a
multi-server architecture.

When running Solr in standalone mode, you only need to configure the
`SOLR_PORT` and `SOLR_HOST` environment variables.

But when running Solr on multiple servers, you can use the
`SOLR_*_URL_FOR_UPDATE` and `SOLR_*_URL_FOR_SEARCH` environment variables to
control what server the Catalog API sends index updates to and what server it
searches. These could be URLs for a load-balancer that will forward your
request to an aviailable node. Or, in a user-managed cluster, you might send
updates to the leader and searches to a search-only follower.

Additionally, because the Catalog API is geared toward periodic batch updates
instead of near real-time updates, if you're using user-managed replication,
you may prefer that the Catalog API explicitly tell Solr to replicate ONLY
after an update happens rather than having followers poll the leader
needlessly. For this, set the `SOLR_*_MANUAL_REPLICATION` environment variables
to `True`. This tells the Catalog API to trigger replication for that core on
all followers whenever it commits to that core. I.e., when the
`utils.solr.commit` function is called, it issues a call to the appropriate
`SOLR_*_MANUAL_REPLICATION_HANDLER` (e.g., `replication`) for each of the
`SOLR_*_FOLLOWER_URLS` after it sends the `commit` command to Solr. Note that,
if you set this for a given core, you should disable polling in the
replication handler (in `solrconfig.xml`) by removing the `pollInterval`
setting.

##### Django Database

You'll need to have an RDBMS installed that you can use for the Django
database. PostGreSQL or MySQL/MariaDB are recommended.

#### Set up a virtual environment.

You should contain your instance of the Catalog API project in a disposable
virtual environment — never install development projects to your system Python.
At this point, setting up a virtualenv is just standard operating procedure
for any Python project.

If you aren't using pyenv-virtualenv, you can run, for example:

```bash
/path/to/py3/bin/python -m venv /path/to/new/venv
```

Now you can treat `/path/to/new/venv` like you have a copy of the Python from
`/path/to/py3`. Run the new Python with `/path/to/new/venv/bin/python`. Install
packages with `/path/to/new/venv/bin/pip`. It's fully self-contained and
doesn't affect `/path/to/py3` in any way.

See [here](https://docs.python.org/3/library/venv.html) for more information.

#### Clone the catalog-api to your local machine.

```bash
git clone git@content.library.unt.edu:catalog/catalog-api.git catalog-api
```

#### Install needed python requirements.

```bash
pip install -r requirements/requirements-base.txt \
            -r requirements/requirements-dev.txt \
            -r requirements/requirements-tests.txt \
            -r requirements/requirements-production.txt
```

Omit dev, tests, or production if not needed in a given environment.

#### Configure local settings.

Now you must set up a number of other environment-specific options, such as
secrets connection details for databases, Redis, Solr, etc. Follow
[the instructions included below](#local-settings).
    
#### Generate a new secret key for Django.

```bash
cd catalog-api/django/sierra
python -m manage.py generate_secret_key
```

Copy/paste the new secret key into your `SECRET_KEY` environment variable.

#### Run migrations and install fixtures.

Make sure that your database server is up and running, and then:

```bash
cd catalog-api/django/sierra
python -m manage.py migrate
```

This creates the default Django database and populates certain tables with
needed data.

#### Create a superuser account for Django.

```bash
cd catalog-api/django/sierra
python -m manage.py createsuperuser
```

Run through the interactive setup. Remember your username and password, as
you'll use this to log into the Django admin screen for the first time. (You
can create additional users from there.)

#### (Optional) Run tests.
    
If you wish, you can try
[running Sierra database/model tests](#sierra-db-checks) to make sure that
Django is reading correctly from your production Sierra database.
    
You may also try [running unit tests](#unit-tests), although setting these
up locally without using Docker requires a bit of work. 

#### Start services: Solr, Redis, Django Dev Server, and Celery.

All the services needed to run the Catalog API should now be installed and
ready to go. You'll want to start each of these and have them running to use
all features of the catalog-api software. (In the below instructions, replace
the referenced environment variables with the actual values you're using, as
needed.)

***Production Note***: In production you'll want all of these to run as
daemons, e.g. systemd services. They should always be running and start up
automatically when you reboot.

##### Your Django Database

If you've been following this guide, then you've already run migrations, so
your Django Database should already be running.

##### Solr

When you installed and configured Solr, you should have set what port it is
running on in the `solr.in.sh` file. Be sure this port matches the `SOLR_PORT`
environment variable and that the correct `SOLR_HOST` is set on the machine
where you're running the Catalog API.

Generally, you can start Solr using:

```bash
/path/to/solr/bin/solr start
```

##### Redis

We'll have two Redis processes running, each on a different port.

The first is for Celery:

```bash
/path/to/redis/redis-server /path/to/redis-celery.conf --port $REDIS_CELERY_PORT
```

The second is for the app data we need to store:

```bash
/path/to/redis/redis-server /path/to/redis-appdata.conf --port $REDIS_APPDATA_PORT
```

Make sure the `REDIS_CELERY_PORT`, and `REDIS_APPDATA_PORT` environment
variables are configured appropriately. If you have default user passwords set
in your `conf` files — and you should! — be sure that your
`REDIS_CELERY_PASSWORD` and `REDIS_APPDATA_PASSWORD` environment variables are
also set.

##### Django Development Web Server

For development, you can run Django using the built-in web server. This is
absolutely not meant for production!

```bash
cd catalog-api/django/sierra
python -m manage.py runserver 127.0.0.1:$DJANGO_PORT
```
        
If you didn't set the `$DJANGO_PORT` environment variable, replace
`$DJANGO_PORT` with `8000`.

If all goes well, you should see something like this:

```
System check identified no issues (0 silenced).

February 10, 2023 - 11:40:40
Django version 3.2, using settings 'sierra.settings.my_dev'
Starting development server at http://127.0.0.1:8000/
Quit the server with CONTROL-C.
```

Try going to `http://localhost:DJANGO_PORT/api/v1/` in a browser.
You should see a DJANGO REST Framework page displaying the API Root.

***Production Note***: For production, you must configure Django to work with a
real web server, like Apache. See the
[Django documentation](https://docs.djangoproject.com/en/3.2/howto/deployment/wsgi/modwsgi/)
for more details.

##### Celery

This command will start a Celery worker server that can run our project tasks.
Note that you must use `-c 4` to limit concurrency to 4 simultaneous tasks —
running more than four at once will run afoul of Sierra's limitation on
simultaneous database connections per user.

```bash
cd catalog-api/django/sierra
/path/to/venv/bin/celery -A sierra worker -l info -c 4
```

When you run this, you'll get some INFO logs, as well as a UserWarning about
not using the DEBUG setting in a production environment. Since this is
development, it's nothing to worry about. You should get a final log entry with
`celery@hostname ready`.

##### Celery Beat

Celery Beat is the task scheduler that's built into Celery. It's what lets you
schedule your export jobs to run at certain times. In development you generally
don't need this, it's mainly for scheduling production export jobs.

If you want to run it in development, use the following (while Celery is
running).

```bash
cd catalog-api/django/sierra
/path/to/venv/bin/celery -A sierra beat -S django
```

You should see a brief summary of your Celery configuration, and then
a couple of INFO log entries showing that Celery Beat has started.

***Production Note***: See the 
[Celery documentation](http://docs.celeryproject.org/en/latest/userguide/periodic-tasks.html)
for how to set up periodic tasks. In our production environment, we use
`django-celery-beat` and the Django DatabaseScheduler to store periodic-task
definitions in the Django database. These are then editable in the Django Admin
interface.

##### Convenience Scripts (Deprecated)

In the repository root we have some old shell scripts (`start_servers.sh`,
`stop_servers.sh`, and `start_celery.sh`) for starting/stopping the needed
catalog-api processes in a development environment, but these have not been
updated in a long time and are considered deprecated. Really, use the Docker
environment for development.
    
#### Check to make sure Sierra data exports work.

With all of your services running, follow the steps
[in this section](#testing-exports) to make sure you can export data from
Sierra and view the results in the API.


<a name="local-settings"></a>Configuring Local Settings
-------------------------------------------------------

You must configure local settings like database connection details for your
instance of the catalog-api. Where possible, we provide usable default values,
with simple ways of overriding them.

### Django Settings

You'll find Django settings for the catalog-api project in
`catalog-api/django/sierra/sierra/settings`. Here, the `base.py` module
contains overall global settings and defaults. The `dev.py`, `production.py`,
and `test.py` modules then import and override the base settings to provide
defaults tailored for particular types of environments.

You can set a default settings file to use via a `DJANGO_SETTINGS_MODULE`
environment variable. You can also specify a particular settings file when you
run a catalog-api command or service through `manage.py`, using the
`--settings` option.

In many cases it's perfectly reasonable to configure a Django project to run
locally by changing or creating a Django settings file. However, we've set up
the catalog-api to minimize this need by reading local settings from
environment variables.

**Under most circumstances, we recommend customizing your local environment by
setting environment variables, not by changing the Django settings files.**
If you're running the catalog-api using Docker, then this is especially true
(unless you're modifying the Docker configuration as well).

### Environment Variables

Set these up using one or both of two methods:

* Regular shell environment variables.
* An environment variable `.env` file, which is kept secret and out of version
control. This file must be located at
`<project_root>/django/sierra/sierra/settings/.env`.

These are not necessarily mutually exclusive. The set of variables defined in
the `.env` file will automatically merge with the set of variables in the
system environment, with system environment variables taking precedence if
any are set in both places.

***Production Note***: Use the `.env` file in production. Then you don't have
to mess with setting environment variables in whatever process is running your
WSGI server (e.g., Apache mod_wsgi). Just be sure to protect it! If your WSGI
process runs as `capi:capi`, chown the file to `root:capi` and chmod it to e.g.
`0440`.

***Docker Notes***

- With Docker, the shell environment variables in your host do not carry over
to your containers, so it's best to use only the `.env` file method to ensure
that the container running your catalog-api instance will have access to all of
the needed environment variables.

- Your environment variables will do double duty so that you don't have to
configure the same settings in two places. Docker Compose will use your
environment variables to pull details that are needed by both Django and
Docker, such as database usernames and passwords.

#### Configuring Environment Variables

First, take a look at the
`catalog-api/django/sierra/sierra/settings/.env.template` file. This
contains the complete set of environment variables used in the Django settings
that you may configure. Most are optional, where the settings file configures
a reasonable default if you do not set the environment variable. A few are
required, where setting a default does not make sense. Some are needed only if
you're deploying the project in a production environment. Note that many of
these are things you want to keep secret.

Assuming you're setting all of the variables in your .env file, you'd copy
`catalog-api/django/sierra/sierra/settings/.env.template` to
`catalog-api/django/sierra/sierra/settings/.env`. Update the variables you
want to update, and remove the ones you want to remove.

##### Required Settings

Your settings file won't load without these.

- `SECRET_KEY` — You'll generate this via `manage.py` as described elsewhere in
this README. Until then you can leave the default value provided in the
template.
- `DJANGO_SETTINGS_MODULE` — The settings module that you want Django to use in
the current environment, in Python path syntax (e.g., `sierra.settings.FILE`).
Unless you create new settings files that import from `base.py`, this will
either be `sierra.settings.dev` or `sierra.settings.production`.
- `SIERRA_DB_USER` — The username for the Sierra user you set up
[earlier](#sierra-users).
- `SIERRA_DB_PASSWORD` — Password for your Sierra user.
- `SIERRA_DB_HOST` — The hostname or IP for your Sierra database server.
- `DEFAULT_DB_USER` — The username for the default Django database user.
- `DEFAULT_DB_PASSWORD` — The password for the default Django database user.

When using the Docker setup, the default Django DB is created for you
automatically using the username and password you have in the `DEFAULT_`
environment variables. If not using the Docker setup, you must set up that
database yourself.
    
These last two variables are required only if you're not using the Docker
setup. In Docker, these are relative to the container and are overridden in
the `Dockerfile`. Outside Docker, they're of course relative to your
filesystem.

- `LOG_FILE_DIR` — The full path to the directory where you want Django log
files stored. You must create this directory if it does not already exist;
Django won't create it for you, and it will give you an error if it doesn't
exist.
- `MEDIA_ROOT` — Full path to the directory where downloads and user-uploaded
files are stored. Like `LOG_FILE_DIR`, you must create this directory if it
does not already exist.

##### Optional Settings, Development or Production

These are settings you may need to set in a development or production
environment, depending on circumstances. If the variable is not set, the
default value is used.

- `ADMINS` — A list of people who will be emailed if there are
errors. Entries are formatted as:
`Person One,person1@example.com;Person Two,person2@example.com`. Default
is an empty list.
- `EXPORTER_EMAIL_ON_ERROR` — true or false. If true, the Admins will be
emailed when an exporter program generates an error. Default is `True`.
- `EXPORTER_EMAIL_ON_WARNING` — true or false. If true, the Admins will be
emailed when an exporter program generates a warning. Default is `True`.
- `TIME_ZONE` — String representing the server timezone. Default is
`America/Chicago` (central timezone).
- `CORS_ORIGIN_REGEX_WHITELIST` — A space-separated list of regular expressions
that should match URLs for which you want to allow cross-domain JavaScript
requests to the API. If you're going to have JavaScript apps on other servers
making Ajax calls to your API, then you'll have to whitelist those domains
here. Default is an empty list.
- `EXPORTER_MAX_RC_CONFIG` and `EXPORTER_MAX_DC_CONFIG` — These two settings
allow you to set overrides for the `max_rec_chunk` and `max_del_chunk`
attributes of `Exporter` objects. They are totally optional; by default,
whatever value is set on the class is what will be used, if a specific override
is not set. However, depending on how your development, production, staging,
and testing environments are set up, you may need (e.g.) your development
settings scaled back compared to your staging and production settings. These
variables let you configure that on an env-specific basis. Do note that the
convention used for the settings as in your .env file looks like this:
`EXPORTER_MAX_RC_CONFIG="ItemsToSolr:1000,BibsToSolr:500"`

##### Production Settings

These are settings you'll probably only need to set in production. If your
development environment is very different than the default setup, then you may
need to set these there as well.

- `STATIC_ROOT` — Full path to the location where static files are put when you
run the `collectstatic` admin command. Note that you generally won't need this
in development: when the `DEBUG` setting is `True`, then static files are
discovered automatically. Otherwise, you need to make sure the static files are
available via a web-accessible URL. Default is `None`.
- `SITE_URL_ROOT` — The URL prefix for the site home. You'll need this if your
server is set to serve this application in anything but the root of the website
(like `/catalog/`). Default is `/`.
- `MEDIA_URL` — The URL where user-uploaded files can be accessed. Default is
`/media/`.
- `STATIC_URL` — The URL where static files can be accessed. Default is
`/static/`.
- `SOLR_PORT` — If running a single Solr instance, this is the port your Solr
instance is running on. Default is 8983.
- `SOLR_HOST` — If running a single Solr instance, this is the host where your
Solr instance is running. Default is `127.0.0.1`.
- `SOLR_*_URL_FOR_UPDATE` — If running Solr in SolrCloud mode or with
user-managed replication, you may be using different URLs for updating and for
searching the same core across the cluster. This is the URL to use for updating
a given core.
- `SOLR_*_URL_FOR_SEARCH` — If running Solr in SolrCloud mode or with
user-managed replication, you may be using different URLs for updating and for
searching the same core across the cluster. This is the URL to use for
searching a given core.
- `SOLR_*_MANUAL_REPLICATION` — true or false. If `True`, then anytime a commit
is made to this Solr core, the catalog-api code manually triggers replication
on each follower. The default is `False`. **IMPORTANT** — If you set this to
`True`, then you must NOT to set a `pollInterval` in your follower replication
handlers! This is intended to be used if you want to trigger replication
explicitly on each commit instead of making followers poll the leader.
- `SOLR_*_MANUAL_REPLICATION_HANDLER` — Defines the name of the Solr
replication handler for a given core. This is only needed if the corresponding
`MANUAL_REPLICATION` setting is `True`. Default is `replication`.
- `SOLR_*_FOLLOWER_URLS` - A comma-delimited list of Solr URLs that are
replication followers for a given core. This is only needed if the
corresponding `MANUAL_REPLICATION` setting is `True`. By default, it's assumed
that your `URL_FOR_UPDATE` is your leader and your `URL_FOR_SEARCH` is a
follower.
- `REDIS_CELERY_PORT` — The port where the Redis instance behind
Celery can be accessed. Default is 6379.
- `REDIS_CELERY_HOST` — The hostname of the Redis instance behaind
Celery. Default is `127.0.0.1`.
- `REDIS_CELERY_PASSWORD` — The password for the default user configured in
the Redis `.conf` file for this Redis instance.
- `REDIS_APPDATA_PORT` — The port where the Redis instance that
stores certain application data can be accessed. Default is 6380.
- `REDIS_APPDATA_HOST` — The hostname for the Redis instance that
stores certain application data. Default is `127.0.0.1`.
- `REDIS_APPDATA_DATABASE` — The number of the Redis database you're
using to store app data. Default is `0`.
- `REDIS_APPDATA_PASSWORD` — The password for the default user configured in
the Redis `.conf` file for this Redis instance.
- `ADMIN_ACCESS` — true or false. Enables (`True`) or disables (`False`) access
to the Django Admin interface. Default is `true`. We use this in production
because we run the Catalog API on two servers. One is a public app server that
only serves the API and has admin access disabled. The other is an internal
processing server where export jobs run, which has it enabled. Both servers
write to and read from the same Solr and Redis instances.
- `ALLOWED_HOSTS` — A space-separated list array of hostnames that represent
the domain names that this Django instance can serve. Whatever hostnames people
will access your app on need to be included. If you're using `dev` settings,
then `localhost` is added by default. Otherwise, it defaults to an empty list.
- `EXPORTER_AUTOMATED_USERNAME` — The name of the Django user that should be
tied to scheduled (automated) export jobs. Make sure that the Django user
actually exists (if it doesn't, create it). It can be helpful to have a unique
Django user tied to automated exports so that you can more easily differentiate
between scheduled exports and manually-run exports in the admin export
interface. Defaults to `django_admin`.

The four remaining variables are `DEFAULT_DB_ENGINE`, `DEFAULT_DB_NAME`,
`DEFAULT_DB_HOST`, and `DEFAULT_DB_PORT`. These, along with the
`DEFAULT_DB_USER` and `DEFAULT_DB_PASSWORD`, configure the default Django
database. Because the Docker setup is now the recommended development
setup, this defaults to using MySQL or MariaDB, running on 127.0.0.1:3306.

##### Test Settings

The `.env.template` file includes a section at the end for test settings. These
define configuration for test copies of the default database, the Sierra
database, Solr, and Redis. The variables prefixed with TEST correspond directly
with non-test settings (ones not prefixed with TEST).

If you will be running tests through Docker, then the only required
settings are `TEST_SIERRA_DB_USER`, `TEST_SIERRA_DB_PASSWORD`,
`TEST_DEFAULT_DB_USER`, and `TEST_DEFAULT_DB_PASSWORD`. Test databases
will be created for you automatically with these usernames/passwords.

If running tests outside of Docker, then you will have to configure all of
these test instances manually and include full configuration details in
your environment variables.

***Docker Note:*** If you're using Docker, you should note that all of the HOST
and PORT settings (except those associated with the live Sierra database)
define how services running in Docker containers map to your host machine. For
example, if `SOLR_HOST` is `127.0.0.1` and `SOLR_PORT` is `8983`, then when
you're running the `solr-dev` service via Docker Compose, you can access the
Solr admin screen from your host machine on http://localhost:8983/solr/. The
default settings are designed to expose all services locally on the host
machine, including test services, without raising port conflicts.

<a name="testing"></a>Testing
-----------------------------

### <a name="sierra-db-checks"></a>Running Sierra Database Checks

Early in development we implemented a series of tests using the built-in Django
test runner to do some simple sanity-checking to make sure the Django ORM
models for Sierra match the structures actually in the production database.
We have since converted these to run via pytest: see
`django/sierra/base/tests/test_database.py`.

When you run the full test suite, as [described below](#unit-tests), these run
against the test Sierra database — which is useful. But, there are times
that you'll want to run these tests against your live database to make sure the
models are accurate. For instance, systems may differ from institution to
institution based on what III products you have, so you may end up needing to
fork this project and update the models so they work with your own setup. It
may also be worth running these tests after Sierra upgrades so that you can
make sure there were no changes made to the database that break the models.

If using Docker, run _only_ the database tests using the following:

```bash
./docker-compose.sh run --rm live-db-test
```

If not using Docker, you can use the below command, instead. If applicable,
replace the value of the `--ds` option with whatever your DEV settings file is.

```bash
pytest --ds=sierra.settings.dev django/sierra/base/tests/test_database.py
```

Note: Some of these tests may fail simply because the models are generally more
restrictive than the live Sierra database. We are forcing ForeignKey-type
relationships on a lot of fields that don't seem to have actual
database-enforced keys in Sierra. E.g., from what I can gather, `id` fields are
usually proper keys, while `code` fields may not be — but `code` fields are
frequently used in a foreign-key-like capacity. I think this leads to a lot of
the invalid codes you have in Sierra, where you have a code in a record that
_should_ point to some entry in an administrative table (like a location), but
it doesn't because the administrative table entry was deleted and the record
was never updated. And there are other cases, as well. E.g., a code might use
the string `none` instead of a null value, but there is no corresponding entry
for `none` in the related table. Bib locations use the string `multi` to
indicate that they have multiple locations, but there is no corresponding
`multi` record in the location table. Etc.

Ultimately, even though these `code` relationships aren't database-enforced
keys, we do still want the ORM to handle the relationships for us in the
general case where you _can_ match a code with the entry that describes it.
Otherwise we'd have to do the matching manually, which would somewhat reduce
the utility of the ORM.


### <a name="unit-tests"></a>Running Unit(ish) Tests

We also have decent coverage with unit (or unit-ish) tests. Although it *is*
possible to run them outside of Docker if you're motivated enough, we
recommend using Docker.

If you followed the [Docker setup](#installation-docker), you can run all
available pytest tests with:

```bash
./docker-compose.sh run --rm test
```

If you didn't follow the Docker setup, then you should still be able to create
a comparable test environment:

- Create your own test sierra database (in PostGreSQL), your own test default
database, your own test Redis instance, and your own test Solr instance.
- Create users for your test sierra and default databases.
- Update your `.env` settings file with `TEST_` variables containg all of the
relevant connection details.
- Run migrations to load test data into your test databases:

```bash
cd catalog-api/django/sierra
python -m manage.py migrate --settings=sierra.settings.test --database=default
python -m manage.py migrate --settings=sierra.settings.test --database=sierra
```

Spin up all of the needed test databases, and then run:

```bash
pytest
```

### <a name="testing-exports"></a>Testing Sierra Exports Manually

A good final test to make sure everything is working once you have things set
up is to trigger a few record exports and make sure data shows up in the API.

- Start up the necessary services, including both the app and Celery worker.
- Go to http://localhost:8000/admin/export/ in a web browser. (Or, use
whatever hostname and port you've configured.)
- Log in using the superuser username and password you set up.
- Under the heading **Manage Export Jobs**, click _Trigger New Export_.
- The first thing to try is to export administrative metadata (like Location
codes, ITYPEs, and Item Statuses).
    - _Run this Export_: "Load ALL III administrative metadata-type data
into Solr."
    - _Filter Data By_: "None (Full Export)"
    - Click Go.
    - You'll see some activity in the Celery log, and the export should be
done within a second or two. Refresh your browser and you should
see a Status of _Successful_.
- Next, try exporting one or a few bib records and any attached items.
    - _Run this Export_: "Load bibs and attached records into Solr."
    - _Filter Data By_: "Record Range (by record number)."
    - Enter a small range of bib record IDs in the _From_ and _to_ fields. Be
    sure to omit the dot and check digit. E.g., from b4371440 to b4371450. 
    - Click Go.
    - You'll see activity in the Celery log, and the export should complete
within a few seconds. Refresh your browser and you should see a status of
_Successful_.
- Finally, try viewing the data you exported in the API.
    - Go to http://localhost:8000/api/v1/ in your browser.
    - Click the URL for the `bibs` resource, and make sure you see data for the
    bib records you loaded.
    - Navigate the various related resources in `_links`, such as `items`.
    - Anything that's linked should take you to the data for that resource.


<a name="license"></a>License
-----------------------------

See LICENSE.txt.


<a name="contributors"></a>Contributors
---------------------------------------

* [Jason Thomale](https://github.com/jthomale)
* [Jason Ellis](https://github.com/jason-ellis)
