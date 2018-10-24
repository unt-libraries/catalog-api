Catalog API [![Build Status](https://travis-ci.org/unt-libraries/catalog-api.svg?branch=master)](https://travis-ci.org/unt-libraries/catalog-api)
===========

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

* All 300+ Sierra database views are modeled using the Django ORM.

* [Django Rest Framework](http://www.django-rest-framework.org/) provides the
API implementation. Serializers and class-based views are easy to extend.

* The API layer has a built-in browseable API view, and content negotiation is
supported. Visit API URLs in a web browser and get nicely formatted, browseable
HTML; request resources in JSON format and get JSON.

* [HAL](http://stateless.co/hal_specification.html), or Hypertext Application
Language (hal+json), is the media type that is used to serve the built-in
resources. "HAL is a simple format that gives a consistent and easy way to
hyperlink between resources in your API." _But_ you are not restricted to using
HAL&mdash;you are free to implement the media types and formats that best fit
your use cases.

* The API supports a wide range of query filters, and more are planned: equals,
greater than, less than, in, range, regular expressions, keyword searches,
and more.

* Your API data is completely decoupled from your Sierra data. An extensible
_exporter_ Django app allows you to define custom 
[ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) processes. Solr
instances that store and index data for the API are included. Or, you can set
up your own data storage and tie your exporters and REST Framework views and
serializers into it.

* Although Sierra data is read-only, the API framework does allow you to
implement POST, PUT, PATCH, and DELETE methods along with GET. So, you can
create your own editable fields on API resources that don't get stored in
Sierra; in fact, you could create resources that merge data from a variety of
sources. Data that isn't sourced from Sierra can be merged when your export
jobs run.

* Accessing API resources _only_ accesses the data in Solr and Redis; it
doesn't hit the Sierra database at all. Thus, API performance is isolated from
performance of your Sierra database, and API usage has no impact on your
Sierra database. You don't have to worry about API users running up against
the concurrent connection limit in Sierra.

* [Celery](http://www.celeryproject.org/) provides an asynchronous task queue
and scheduler. Set up your exporters to run as often as you need so that your
API stays in synch with Sierra as data in Sierra is added, updated, or deleted.

* API resources can be grouped and completely compartmentalized into reusable
Django apps. New apps can expose new resources and/or override the default
base resources. (The _shelflist_ app provides an example of this.)


### Project Structure

There are two main directories in the project root: `django` and `solr`.

The `django` directory contains the Django project and related
code, in `django\sierra`. The `manage.py` script for issuing Django
commands is located here, along with the apps for the project:

* `api`: Contains the Django REST Framework implementation of the default
API resources, which include apiusers, bibs, items, eresources,
itemstatuses, itemtypes, locations, and marc.
* `base`: Contains the Django ORM models for Sierra.
* `export`: Contains code for exporters, including definitions of the
exporters themselves, models related to export jobs, changes to the Django
admin interface to allow you to manage and track export jobs, and tasks
for running export jobs through Celery.
* `sierra`: Contains configuration and settings for the project.
* `shelflist`: Implements a shelflistitems resource in the API; contains
overrides for export classes and api classes that implement the resource
and add links to shelflistitems from item resources and location resources.
This provides an example of how you could create Django apps with self-
contained functionality for building new features onto existing API
resources.

The `solr` directory contains the included Solr instance and a [fork
of SolrMarc](https://github.com/sul-dlss/solrmarc-sw) from Naomi Dushay of 
Stanford University Libraries, which is used for loading MARC data into
Solr. (See the
[SolrMarc documentation](https://code.google.com/archive/p/solrmarc/wikis)
for more information.)


<a name="sierra-users"></a>Setting up Sierra Users
--------------------------------------------------

Before getting started, you should first take a moment to set up Sierra users.
The catalog-api requires access to Sierra to export data, and you must create
a new Sierra user for each instance of the project that will be running
(e.g., for each dev version, for staging, for production). Be sure that
each user has the _Sierra SQL Access_ application assigned in the Sierra
admin interface.

You must also set each new Sierra user's `search_path` in PostgreSQL so that
the user can issue queries without specifying the `sierra_view` prefix.
Without doing this, the SQL generated by the Django models will not work.

For each Sierra user you created, log into the database as that user and
send the following query, replacing `user` with the name of that user:

    ALTER ROLE user SET search_path TO sierra_view;

<a name="installation-docker"></a>Installation and Getting Started, Docker
--------------------------------------------------------------------------

This is now the recommended setup for development. It uses Docker and Docker
Compose to help automate building and managing the environment in which the
catalog-api code runs. It is simpler to manage than the manual method and is
thus well-suited for testing and development, but it is not tailored for
production deployment.

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

* [Docker](https://www.docker.com) CE/EE >= 17.03 (earlier versions may work
but have not been tested)
* [Docker Compose](https://docks.docker.com/compose/) >= 1.11


### Setup Instructions

1. **Install Docker and Docker Compose.**

    * [View Docker CE download instructions here.](https://www.docker.com/community-edition/#download)
    If you're using Docker for Mac or Windows, Docker Compose is included in
    the Docker CE package.
      
    * If you're using Docker on Linux or otherwise need to install Docker
    Compose separately, you can
    [view Docker Compose installation instructions here](https://docs.docker.com/compose/install/).

2. **Fork and clone the repository to your local machine.**

    Generally, rather than simply cloning the repository, you'll want to fork
    it and then make modifications for your own institution. So,
    first&mdash;create a fork on GitHub. Then,
    
        git clone https://github.com/[your-github-account]/catalog-api.git
    
    Or, if you're authenticating via SSH:
    
        git clone git@github.com:[your-github-account]/catalog-api.git
    
    If you're new to git and/or GitHub, see the GitHub help pages about
    [how to fork](https://help.github.com/articles/fork-a-repo),
    [how to sync your fork with the original repository](https://help.github.com/articles/syncing-a-fork/),
    [managing branches](https://help.github.com/articles/managing-branches-in-your-repository/),
    and [how to submit pull requests](https://help.github.com/articles/using-pull-requests/)
    (for when you want to contribute back).
    
3. **Configure local settings.**

    For environment-specific settings, such as secrets and database connection
    details, you should create a .env settings file. Use
    [the instructions included below](#local-settings).
  
4. **Build the Docker environment(s).**
  
    In the repository root, you can run
  
        ./init-dockerdata.sh all
  
    It will take several minutes to finish, but it should complete these steps:
  
    * Pull images needed for things like MariaDB, PostGreSQL, and Redis from
    Docker Cloud.
    * Build the catalog-api custom image from the supplied `Dockerfile` for the
    various app-related services.
    * Set up the `docker_data` directory and all necessary subdirectories,
    setting the owning UID/GID to that of the current user.
    * Create databases, as needed.
    * Run database/Django migrations as needed to set up database schemas and
    load initial data.

    However, when running the build in a CI environment, we've found it
    necessary to do an explicit pull, then a build, and then finally run the
    init script. So, if you get errors while running the init script as above,
    you may try doing this instead. E.g.:
      
        ./docker-compose.sh pull
        ./docker-compose.sh build
        ./init-dockerdata.sh all
      
5. **(Optional) Run tests.**

    If you wish, you can try
    [running Sierra database/model tests](#sierra-db-checks) to make sure that
    Django is correctly reading from your production Sierra database.
    
    You may also [run unit tests](#unit-tests).

6. **Generate a new secret key for Django.**

        ./docker-compose.sh run --rm manage-dev generate_secret_key

    Copy/paste the new secret key into your `SECRET_KEY` environment variable.
  
7. **Create a superuser account for Django.**

        ./docker-compose.sh run --rm manage-dev createsuperuser

    Go through the interactive setup. Remember your username and password, as
    you'll use this to log into the Django admin screen for the first time.
    (You can create additional users from there.)
  
8. **Start the app.**

    There are two main Docker Compose services that you'll use during
    development: one to control the app (i.e., to run the Django web server)
    and one to control the Celery worker.

    You can start them up individually like so:

        ./docker-compose.sh up -d app
        ./docker-compose.sh up -d celery-worker
      
    Other services, such as your database, Solr, and Redis, are started
    automatically if they aren't already running.
      
    Note that the `-d` flag runs these as background processes, but you can run
    them in the foreground (to write ouput to stdout) by ommitting the flag.
      
9. **Check to make sure everything is up.**

    * Go to http://localhost:8000 (or use whatever port you've set up as the
    `DJANGO_PORT` in your environment). It should redirect you to
    http://localhost:8000/api/v1/ and display the Api Root resource.
    * Go to http://localhost:8000/admin/export/. Log in with the username and
    password you set up in step 6, and you should see a Django admin screen
    for managing Sierra exports. Try clicking "View, Edit, and Add Export
    Types" to make sure you see data.
    * Go to http://localhost:8983/solr/ (using whatever port you've set up as
    the `SOLR_PORT` in your environment). You should see an Apache Solr admin
    screen. Click the "core selector" dropdown and make sure you can select
    the bibdata, haystack, and marc cores and that they're all empty. If
    you've run tests and left the test Solr instance running, you will
    probably see a SolrCore initialization failure for the *libguides* core.
    Ignore this, as this core is not actually used by the catalog-api.
    * Check your `docker_data/celery-worker/logs` directory, and view the
    `celery-worker.log` file. Make sure that the last line has a timestamp
    and says that the celery worker is ready. (Ignore any warning messages
    about using settings.DEBUG.)

10. **Check to make sure Sierra data exports work.**

    Follow the steps [in this section](#testing-exports) to make sure you can
    export data from Sierra and view the results in the API.
   
11. **Stop running services.**
    
    Whenever you're finished, you can stop all running catalog-api services
    and remove all containers with one command.
        
        ./docker-compose.sh down
        
    Even though containers are removed, data stored on data volumes remains.
    Next time you start up the catalog-api services, your data should still be
    there.

### More About the Docker Setup

#### Docker Compose Config File Version and Docker Swarm

Be aware that we have not tested our setup with a Docker swarm, even though the
`docker-compose.yml` file does conform to the version 3 specification.

#### Running Docker Compose Services, `docker-compose.sh`

Nearly everything you'll need to do&mdash;building images, running containers,
starting services, running tests, and running Django `manage.py`
commands&mdash;is implemented as a Docker Compose service.

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
locally on your host also updates it inside the running containers&mdash;e.g.,
you don't have to rebuild images with every update.

#### Initializing Databases, `init-dockerdata.sh`

We've created an `init-dockerdata.sh` shell script to help make it easier to
initialize Docker data volumes so that services will run correctly. Database
migrations can be managed through this script, too. The setup instructions
above use this script to initialize data volumes during first-time setup, but
you can also use it to wipe out data for a particular service and start with a
clean slate.

Run

    ./init-dockerdata.sh -h

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
on the same machine, which likely would not be the case in a full production
environment.

We do include tips for production configuration, where possible.

### Requirements

* Python 2 >= 2.7.5, plus pip and virtualenv.
* Java >= 1.7.0_45.
* [Redis](http://redis.io/) >= 2.4.10.
* Whatever additional prerequisites are needed for your
database software, such as the mysql-development library and 
[mysqlclient](https://pypi.python.org/pypi/mysqlclient) if you're using MySQL
or MariaDB.


1. **Install prerequisites.**

    * **Python 2 >= 2.7.5**.

    * **Latest version of pip**. Python >=2.7.9 from [python.org](https://www.python.org)
    includes pip. Otherwise, go
    [here for installation instructions](https://pip.pypa.io/en/stable/installing/).

        Once pip is installed, be sure to update to the latest version:

            pip install -U pip
    
    * **virtualenv**. If you've already installed pip:

            pip install virtualenv
            
        Note: Virtualenv also includes pip, so you could install virtualenv
        first, without using pip.

    * **Requirements for psycopg2**. In order for 
    [psycopg2](http://initd.org/psycopg/) to build correctly, you'll need to
    have the appropriate dev packages installed.

        On Ubuntu/Debian:
    
            sudo apt-get install libpq-dev python-dev

        On Red Hat:

            sudo yum install python-devel postgresql-devel

        On Mac, with homebrew:
            
            brew install postgresql

    * **Java**.

        On Ubuntu/Debian:

            sudo apt-get install openjdk-8-jre

        On Red Hat:
    
            sudo yum install java-1.8.0-openjdk

    * **Redis** is required to serve as a message broker for Celery. It's also
    used to store some application data. You can follow the
    [quickstart guide](http://redis.io/topics/quickstart) to get started, but
    please make sure to set up your `redis.conf` file appropriately.

        Default Redis settings only save your data periodically, so you'll want
        to take a look at
        [how Redis persistence works](http://redis.io/topics/persistence). I'd
        recommend RDB snapshots _and_ AOF persistence, but you'll have to turn
        AOF on in your configuration file by setting `appendonly yes`. Note
        that if you store the `dump.rdb` and/or `appendonly.aof` files anywhere
        in the catalog-api project _and_ you rename them, you'll need to add
        them to `.gitignore`.

        You'll also want to be sure to take a look at the _Securing Redis_
        section in the quickstart guide.

        ***Production Note***: The quickstart section _Installing Redis more
        properly_ contains useful information for deploying Redis in a
        production environment.

    * **Your database of choice to serve as the Django database.** PostGreSQL
    or MySQL/MariaDB are recommended.

2. **Set up a virtual environment.**

    **virtualenv**

    [virtualenv](https://virtualenv.readthedocs.org/en/latest/) is commonly
    used with Python, and especially Django, projects. It allows you to isolate
    the Python environment for projects on the same machine from each other
    (and, importantly, from the system Python). Using virtualenv is not
    strictly required, but it is strongly recommended.

    **(Optional) virtualenvwrapper**

    [virtualenvwrapper](https://virtualenvwrapper.readthedocs.org/en/latest/)
    is very useful if you need to manage several different virtual environments
    for different projects. At minimum, it makes creation, management, and
    activation of virtualenvs easier. The instructions below assume that you
    are not using virtualenvwrapper.

    **Without virtualenvwrapper**

    First generate the virtual environment you're going to use for the
    project. Create a directory where it will live (&lt;DIR&gt;), and then:
    
        virtualenv <DIR>

    This creates a clean copy of whatever Python version you installed
    virtualenv on in that directory.

    Next, activate the new virtual environment.

        source <DIR>/bin/activate

    Once it's activated, any time you run Python, it will use the Python that's
    in the virtual environment. This means any pip installations or other
    modules you install (e.g., via a setup.py file) while this virtualenv is
    active will be stored and used only in this virtual environment. You can
    create multiple virtualenvs on the same machine for different projects in
    order to keep their requirements nicely separated.

    You can deactivate an active virtual environment with:

        deactivate

    You'll probably want to add the `source <DIR>/bin/activate` statement to
    your shell startup script, e.g. such as `~/.bash_profile` and/or `~/.bashrc`
    (if you're using bash), that will activate the appropriate environment on
    startup.

3. **Fork catalog-api on GitHub and clone to your local machine.**

     Generally, rather than simply cloning the repository, you'll want to fork
     it and then make modifications for your own institution. So,
     first&mdash;create a fork on GitHub. Then,
    
        git clone https://github.com/[your-github-account]/catalog-api.git
    
    Or, if you're authenticating via SSH:
    
        git clone git@github.com:[your-github-account]/catalog-api.git
    
    If you're new to git and/or GitHub, see the GitHub help pages about
    [how to fork](https://help.github.com/articles/fork-a-repo),
    [how to sync your fork with the original repository](https://help.github.com/articles/syncing-a-fork/),
    [managing branches](https://help.github.com/articles/managing-branches-in-your-repository/),
    and [how to submit pull requests](https://help.github.com/articles/using-pull-requests/)
    (for when you want to contribute back).

4. **Install all python requirements.**

        pip install -r requirements/requirements-base.txt \
                    -r requirements/requirements-dev.txt \
                    -r requirements/requirements-tests.txt \
                    -r requirements/requirements-production.txt

5. **Set environment variables.**

    * To `PATH`, add the path to your JRE `/bin` directory and the path to your
    Redis `/src` directory, where the `redis-server` binary lives. 
    Example: `/home/developer/jdk1.7.0_45/bin:/home/developer/redis-2.8.9/src`
    * `JAVA_HOME` &mdash; Should contain the path to your JRE.
    * `REDIS_CONF_PATH` &mdash; Contains the path to the `redis.conf` file you
    want to use for your Redis instance. This is not required, but it's
    strongly recommended. If blank, then when you run Redis with the
    `start_servers.sh` script, Redis will run using the built-in defaults. The
    main problem with this is that the built-in defaults don't provide very
    good persistence, and you will probably lose some data whenever you shut
    down or restart Redis.
    
    If adding environment variables to your `.bash_profile`, be sure to refresh
    it after you save changes:
    
        . ~/.bash_profile

    If using virtualenvwrapper, environment variables can be set each
    time a virtual environment is activated. See the [virtualenvwrapper
    documentation](https://virtualenvwrapper.readthedocs.org/en/latest/scripts.html#postactivate)
    for more details.

6. **Configure other local settings.** 

    Next, you must set up a number of other environment-specific options, such
    as secrets and database connection details. Follow
    [the instructions included below](#local-settings).
    
7. **Generate a new secret key for Django.**

        cd <project_root>/django/sierra
        manage.py generate_secret_key

    Copy/paste the new secret key into your `SECRET_KEY` environment variable.

8. **Run migrations and install fixtures.**

   Make sure that your database server is up and running, and then:

        cd <project_root>/django/sierra
        manage.py migrate

    This creates the default Django database and populates certain tables with
    needed data.

9. **Create a superuser account for Django.**

        cd <project_root>/django/sierra
        manage.py createsuperuser

    Run through the interactive setup. Remember your username and password, as
    you'll use this to log into the Django admin screen for the first time.
    (You can create additional users from there.)

10. **(Optional) Run tests.**
    
    If you wish, you can try
    [running Sierra database/model tests](#sierra-db-checks) to make sure that
    Django is reading correctly from your production Sierra database.
    
    You may also try [running unit tests](#unit-tests), although setting these
    up locally without using Docker requires a bit of work. 

11. **Start servers and processes: Solr, Redis, Django Development Web Server,
and Celery.**

    The services listed below should now be installed and ready to go. You'll
    want to start each of these and have them running to use all features of
    the catalog-api software. (In the below instructions, replace the
    referenced environment variables with the actual values you're using, as
    needed.)
    
    All of this assumes that your default database backend server is already
    running, which should be the case if you've already run migrations.

    * **Solr**

            cd <project_root>/solr/instances
            java -jar start.jar -Djetty.port=$SOLR_PORT

        Try going to `http://localhost:SOLR_PORT/solr/` in a Web browser.
        (Replace `SOLR_PORT` with the value of the `SOLR_PORT` environment
        variable, and, if testing from an external machine, replace `localhost`
        with your hostname.) You should see an Apache Solr admin screen.

        You can stop Solr with CTRL-C in the terminal where it's running in the
        foreground.

    * **Redis (for Celery)**

            cd <project_root>
            redis-server $REDIS_CONF_PATH --port $REDIS_CELERY_PORT
    
    * **Redis (for App Data)**

            cd <project_root>
            redis-server $REDIS_CONF_PATH --port $REDIS_APPDATA_PORT

    * **Django Development Web Server**

        Open another terminal to test Django.

            cd <project_root>/django/sierra
            manage.py runserver 0.0.0.0:$DJANGO_PORT
        
        (In this case, if you didn't set the `$DJANGO_PORT` environment
        variable, replace `$DJANGO_PORT` with `8000`.)

        If all goes well, you should see something like this:

            System check identified no issues (0 silenced).

            February 10, 2016 - 11:40:40
            Django version 1.7, using settings 'sierra.settings.my_dev'
            Starting development server at http://0.0.0.0:8000/
            Quit the server with CONTROL-C.

        Try going to `http://localhost:DJANGO_PORT/api/v1/` in a browser.
        (Replace `localhost` with your hostname if accessing from an external
        computer, and replace `DJANGO_PORT` with your `DJANGO_PORT` value.)
        You should see a DJANGO REST Framework page displaying the API Root.

        ***Production Note***: The Django Development Web Server is intended to
        be used only in development environments, not in production. You must
        configure Django to work with a real web server like Apache for
        production. See the [Django documentation](https://docs.djangoproject.com/en/1.7/howto/deployment/wsgi/modwsgi/)
        for more details.

    * **Celery**

        Open up another terminal to test Celery.

            cd <project_root>/django/sierra
            celery -A sierra worker -l info -c 4

        You'll get some INFO logs, as well as a UserWarning about not using
        the DEBUG setting in a production environment. Since this is
        development, it's nothing to worry about. You should get a final log
        entry with `celery@hostname ready`.

    * **Celery Beat**

        Celery Beat is the task scheduler that's built into Celery. It's what
        lets you schedule your export jobs to run at certain times.

        With Celery still running, open another terminal.

            cd <project_root>/django/sierra
            celery -A sierra beat -S djcelery.schedulers.DatabaseScheduler

        You should see a brief summary of your Celery configuration, and then
        a couple of INFO log entries showing that Celery Beat has started.

        ***Production Note***: See the 
        [Celery documentation](http://docs.celeryproject.org/en/latest/userguide/periodic-tasks.html)
        for how to set up periodic tasks. In our production environment, we use
        the DatabaseScheduler and store periodic-task definitions in the Django
        database. These are then editable in the Django Admin interface.

    Once you've confirmed each of the above processes runs, then you can stop
    them. (Ctrl-C in each of the running terminals.)

    **Convenience Scripts**
    
    Use of the below scripts for running catalog-api processes is now
    effectively deprecated, since you won't use them in production and you
    won't use them in the recommended development/testing environment. They are
    still useful for testing a non-Docker install, so they are not yet
    *officially* deprecated.

    * `start_servers.sh` &mdash; Starts Solr, Redis, and Django on the ports
    you've specified in your environment variables as background processes.
    Optionally, you can issue an argument, `django` or `solr` or `redis`, to
    run one of those as a foreground process (and direct output for that
    process to stdout). Often, in development, `start_servers.sh django` can be
    useful so that you get Django web server logs output to the console. Solr
    output and Redis output are often not as immediately useful. Solr output
    will still be logged to a file in `<project_root>/solr/instances/logs`, and
    Redis output will be logged based on how you've configured your Redis
    instance.

    * `stop_servers.sh` &mdash; Stops Solr, Redis, and Django (if they're
    currently running).

    * `start_celery.sh` &mdash; Starts Celery as a foreground process. Often,
    in development, you'll want Celery logged to the console so you can keep an
    eye on output. (Use CTRL-C to stop Celery.)

    ***Production Notes*: Daemonizing Processes for a Production Environment**

    In a production environment, you'll want to have all of these servers
    and processes daemonized using init.d scripts.

    * Redis ships with usable init scripts. See the
    [quickstart guide](http://redis.io/topics/quickstart) for more info.

    * For both Celery and Celery Beat, there are example init
    scripts available, although you'll have to edit some variables.
    See the
    [Celery documentation](http://docs.celeryproject.org/en/latest/tutorials/daemonizing.html)
    for details.

    * For Solr, you'll need to create your init.d file yourself, but there
    are a number of tutorials available on the Web. The Solr instance
    provided with this project uses a straightforward multi-core setup,
    so the init.d file should be straightforward.

12. **Check to make sure Sierra data exports work.**

     Follow the steps [in this section](#testing-exports) to make sure you can export data from Sierra and view the results in the API.


<a name="local-settings"></a>Configuring Local Settings
-------------------------------------------------------

You must configure local settings like database connection details for your
instance of the catalog-api. Where possible, we provide usable default values,
with simple ways of overriding them.

### Django Settings

You'll find Django settings for the catalog-api project in
`<project_root>/django/sierra/sierra/settings`. Here, the `base.py` module
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
setting environment variables&mdash;not by changing the Django settings
files.** If you're running the catalog-api using Docker, then this is
especially true (unless you're modifying the Docker configuration as well).

### Environment Variables

Set these up using one or both of two methods:

* Regular environment variables.
* An environment variable `.env` file, which is kept secret and out of version
control. This file must be located at
`<project_root>/django/sierra/sierra/settings/.env`.

These are not necessarily mutually exclusive. The set of variables defined in
the `.env` file will automatically merge with the set of variables in the
system environment, with system environment variables taking precedence if
any are set in both places.

***Docker Notes***

* With Docker, the environment variables in your host do not carry over to your
containers, so it's best to use only the `.env` file method to ensure that the
container running your catalog-api instance will have access to all of the
needed environment variables.

* Your environment variables will do double duty so that you don't have to
configure the same settings in two places. Docker Compose will use your
environment variables to pull details that are needed by both Django and
Docker, such as database usernames and passwords.

#### Configuring Environment Variables

First, take a look at the
`<project_root>/django/sierra/sierra/settings/.env.template` file. This
contains the complete set of environment variables used in the Django settings
that you may configure. Most are optional, where the settings file configures
a reasonable default if you do not set the environment variable. A few are
required, where setting a default does not make sense. Some are needed only if
you're deploying the project in a production environment. Note that many of
these are things you want to keep secret.

Assuming you're setting all of the variables in your .env file, you'd copy
`<project_root>/django/sierra/sierra/settings/.env.template` to
`<project_root>/django/sierra/sierra/settings/.env`. Update the variables you
want to update, and remove the ones you want to remove.

* Required Settings &mdash; Your settings file won't load without these.
    * `SECRET_KEY` &mdash; Leave the default value provided in the template
    as-is while you configure the rest of your settings. Then generate a new
    secret key according to the specific setup method you're using, and update
    the value in the environment variable.
    * `DJANGO_SETTINGS_MODULE` &mdash; The settings module that you want Django to
    use in the current environment, in Python path syntax
    (e.g., sierra.settings.FILE). Unless you create new settings files
    that import from `base.py`, this will either be `sierra.settings.dev`
    or `sierra.settings.production`.
    * `SIERRA_DB_USER` &mdash; The username for the Sierra user you set up
    [earlier](#sierra-users).
    * `SIERRA_DB_PASSWORD` &mdash; Password for your Sierra user.
    * `SIERRA_DB_HOST` &mdash; The hostname for your Sierra database server.
    * `DEFAULT_DB_USER` &mdash; The username for the default Django database
    user.
    * `DEFAULT_DB_PASSWORD` &mdash; The password for the default Django
    database user. 

    When using the Docker setup, the default Django DB is created for you
    automatically using the username and password you have in the `DEFAULT_`
    environment variables. Otherwise, you must set up that database yourself.
    
    These last two variables are required only if you're not using the Docker
    setup. In Docker, these are relative to the container and are overridden in
    the `Dockerfile`. Outside Docker, they're of course relative to your
    filesystem.

    * `LOG_FILE_DIR` &mdash; The full path to the directory where you want
    Django log files stored. You must create this directory if it does
    not already exist; Django won't create it for you, and it will error
    out if it doesn't exist.
    * `MEDIA_ROOT` &mdash; Full path to the directory where downloads and
    user-uploaded files are stored. MARC files that are generated (e.g.,
    to be loaded by SolrMarc) are stored here. Like `LOG_FILE_DIR`, you
    must create this directory if it does not already exist.

* Optional Settings, Development or Production &mdash; These are settings you
may need to set in a development or production environment, depending on
circumstances. If the variable is not set, the default value is used.

    * `ADMINS` &mdash; A list of people who will be emailed if there are
    errors. Entries are formatted as:
    `Person One,person1@example.com;Person Two,person2@example.com`. Default
    is an empty list.
    * `EXPORTER_EMAIL_ON_ERROR` &mdash; true or false. If true, the Admins will
    be emailed when an exporter program generates an error. Default is `True`.
    * `EXPORTER_EMAIL_ON_WARNING` &mdash; true or false. If true, the Admins
    will be emailed when an exporter program generates a warning. Default
    is `True`.
    * `TIME_ZONE` &mdash; String representing the server timezone. Default is
    `America/Chicago` (central timezone).
    * `CORS_ORIGIN_REGEX_WHITELIST` &mdash; A space-separated list of regular
    expressions that should match URLs for which you want to allow
    cross-domain JavaScript requests to the API. If you're going to have
    JavaScript apps on other servers making Ajax calls to your API, then
    you'll have to whitelist those domains here. Default is an empty list.
    * `SOLRMARC_CONFIG_FILE` &mdash; The name of the file that contains
    configuration settings for SolrMarc for a particular environment. This
    will match up with a `config.properties` file in
    `<project_root>/solr/solrmarc`. (See "SolrMarc Configuration," below,
    for more information.) Default is `dev_config.properties`.
* Production Settings &mdash; These are settings you'll probably only need to
set in production. If your development environment is very different than
the default setup, then you may need to set these there as well.
    * `STATIC_ROOT` &mdash; Full path to the location where static files are
    put when you run the `collectstatic` admin command. Note that you
    generally won't need this in development: when the `DEBUG` setting is
    `True`, then static files are discovered automatically. Otherwise,
    you need to make sure the static files are available via a
    web-accessible URL, which this helps you do. Default is `None`.
    * `SITE_URL_ROOT` &mdash; The URL prefix for the site home. You'll need
    this if your server is set to serve this application in anything but the
    root of the website (like `/catalog/`). Default is `/`.
    * `MEDIA_URL` &mdash; The URL where user-uploaded files can be accessed.
    Default is `/media/`.
    * `STATIC_URL` &mdash; The URL where static files can be accessed. Default
    is `/static/`.
    * `SOLR_PORT` &mdash; The port your Solr instance is running on. Default is
    8983.
    * `SOLR_HOST` &mdash; The host where your Solr instance is running. Default
    is `127.0.0.1`.
    * `SOLR_HAYSTACK_URL` &mdash; The URL pointing to your Solr instance where
    the `haystack` core can be accessed. Default is
    `http://{SOLR_HOST}:{SOLR_PORT}/solr/haystack`.
    * `SOLR_BIBDATA_URL` &mdash; The URL pointing to your Solr instance where
    the `bibdata` core can be accessed. Default is
    `http://{SOLR_HOST}:{SOLR_PORT}/solr/bibdata`.
    * `SOLR_MARC_URL` &mdash; The URL pointing to your Solr instance where
    the `marc` core can be accessed. Default is
    `http://{SOLR_HOST}:{SOLR_PORT}/solr/marc`.
    * `REDIS_CELERY_PORT` &mdash; The port where the Redis instance behind
    Celery can be accessed. Default is 6379.
    * `REDIS_CELERY_HOST` &mdash; The hostname of the Redis instance behaind
    Celery. Default is `127.0.0.1`.
    * `REDIS_APPDATA_PORT` &mdash; The port where the Redis instance that
    stores certain application data can be accessed. Default is 6380.
    * `REDIS_APPDATA_HOST` &mdash; The hostname for the Redis instance that
    stores certain application data. Default is `127.0.0.1`.
    * `REDIS_APPDATA_DATABASE` &mdash; The number of the Redis database you're
    using to store app data. Default is `0`.
    * `ADMIN_ACCESS` &mdash; true or false. Default is `true`, but you can set
    to `false` if you want to disable the Django Admin interface for
    a particular catalog-api instance.
    * `ALLOWED_HOSTS` &mdash; An space-separated list array of hostnames that
    represent the domain names that this Django instance can serve. This is a
    security measure that is required to be set in production. Defaults to an
    empty list.
    * `EXPORTER_AUTOMATED_USERNAME` &mdash; The name of the Django user that
    should be tied to scheduled (automated) export jobs. Make sure that the
    Django user actually exists (if it doesn't, create it). It can be
    helpful to have a unique Django user tied to automated exports so that
    you can more easily differentiate between scheduled exports and
    manually-run exports in the admin export interface. Defaults to
    `django_admin`.
    
    The four remaining variables are `DEFAULT_DB_ENGINE`, `DEFAULT_DB_NAME`,
    `DEFAULT_DB_HOST`, and `DEFAULT_DB_PORT`. These, along with the
    `DEFAULT_DB_USER` and `DEFAULT_DB_PASSWORD`, configure the default Django
    database. Because the Docker setup is now the recommended development
    setup, this defaults to using MySQL or MariaDB, running on 127.0.0.1:3306.
    
* Test Settings &mdash; The `.env.template` file includes a section at the end
for test settings. These define configuration for test copies of the default
database, the Sierra database, Solr, and Redis. The variables prefixed with
TEST correspond directly with non-test settings (ones not prefixed with TEST).

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

### SolrMarc Configuration

*Ignore this section if you're using the Docker setup.*

An older version of SolrMarc is currently used to index bib records in Solr.
The SolrMarc code is located in `<project_root>/solr/solrmarc/`.

SolrMarc uses its own configuration files that are completely separate from
Django and do not use environment variables. Like Django, you can create
multiple configuration files to  use in different environments.

These files include the following.

* `*_config.properties` &mdash; Contains settings for SolrMarc. There are two
settings here that are of immediate concern.

    * `solr.hosturl` &mdash; Should contain the URL for the Solr index that
    SolrMarc loads into.
    * `solrmarc.indexing.properties` &mdash; Points to the `*_index.properties`
    file used by your SolrMarc instance, described below.

* `*_index.properties` &mdash; Defines how MARC fields translate to fields in
your Solr index. You'd only change this file if you wanted to change how
bib API resources are created.

Primarly, we're concerned with making sure that the `solr.hosturl` is correct.
The provided `dev_config.properties` and `test_config.properties` files are
appropriate for the Docker setup, which is recommended for development and
testing. If you are not using Docker, then you must make a change to the
SolrMarc `config.properties`. Create a copy of
`<project_root>/solr/solrmarc/dev_config.properties`. In the copy, change the
value of `solr.hosturl` value to match the correct host and port.

Now set the `SOLRMARC_CONFIG_FILE` environment variable to the filename of the
`config.properties` file you just created.

***Production Note***: You'll likely want to keep the URL for your
production Solr instance out of GitHub. The `production_config.properties`
file is in `.gitignore` for that reason. There is a
`production_config.properties.template` file that you can copy over to
`production_config.properties` and fill in the `solr.hosturl` value.


<a name="testing"></a>Testing
-----------------------------

### <a name="sierra-db-checks"></a>Running Sierra Database Checks

Early in development we implemented a series of tests using the built-in Django
test runner to do some simple sanity-checking to make sure the Django ORM
models for Sierra match the structures actually in the production database.
We have since converted these to run via pytest: see
`django/sierra/base/tests/test_database.py`.

When you run the full test suite, as [described below](#unit-tests), these run
against the test Sierra database&mdash;which is useful. But, there are times
that you'll want to run these tests against your live database to make sure the
models are accurate. For instance, systems may differ from institution to
institution based on what products you have, so you may end up needing to fork
this project and update the models so they work with your own setup. It may
also be worth running these tests after Sierra upgrades so that you can make
sure there were no changes made to the database that break the models.

If using Docker, run _only_ the database tests using the following:

    ./docker-compose.sh run --rm live-db-test

If not using Docker, you can use the below command, instead. If applicable,
replace the value of the `--ds` option with whatever your DEV settings file is.

    cd <project_root>
    pytest --ds=sierra.settings.dev django/sierra/base/tests/test_database.py

Note: Some of these tests may fail simply because the models are generally more
restrictive than the live Sierra database. We are forcing ForeignKey-type
relationships on a lot of fields that don't seem to have actual
database-enforced keys in Sierra. E.g., from what I can gather, `id` fields are
usually proper keys, while `code` fields may not be&mdash;but `code` fields are
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

More recently we've been working on adding unit (and integration) tests that
run with pytest. We recommend running these tests via Docker, but it *is*
possible to run them outside of Docker if you're motivated enough.

If you followed the [Docker setup](#installation-docker),
      
     ./docker-compose.sh run --rm test

will run all available pytest tests.

If you didn't follow the Docker setup, then you should still be able to create
a comparable test environment:

* Create your own test sierra database (in PostGreSQL), your own test default
database, your own test Redis instance, and your own test Solr instance.
* Create users for your test sierra and default databases.
* Update your `.env` settings file with `TEST_` variables containg all of the
relevant connection details.
* Run migrations to load test data into your test databases:

      cd <project_root>/django/sierra
      ./manage.py migrate --settings=sierra.settings.test --database=default
      ./manage.py migrate --settings=sierra.settings.test --database=sierra

Spin up all of the needed test databases, and then run

    pytest


### <a name="testing-exports"></a>Testing Sierra Exports Manually

Until more complete integration tests are written, a good final test to make
sure everything is working once you have things set up is to trigger a few
record exports and make sure data shows up in the API.

* Start up the necessary services/servers, including both the app and Celery
worker.

* Go to http://localhost:8000/admin/export/ in a web browser (using the
appropriate hostname and port).

* Log in using the superuser username and password you set up.

* Under the heading **Manage Export Jobs**, click _Trigger New Export_.

* First thing we want to do is export administrative metadata (like Location
codes, ITYPEs, and Item Statuses).

    * _Run this Export_: "Load ALL III administrative metadata-type data
into Solr."
    * _Filter Data By_: "None (Full Export)"
    * Click Go.
    * You'll see some activity in the Celery log, and the export should be
done within a second or two. Refresh your browser and you should
see a Status of _Successful_.

* Next, try exporting one or a few bib records and any attached items.

    * _Run this Export_: "Load bibs and attached records into Solr."
    * _Filter Data By_: "Record Range (by record number)."
    * Enter a small range of bib record IDs in the _From_ and _to_ fields. Be
    sure to omit the dot and check digit. E.g., from b4371440 to b4371450. 
    * Click Go.
    * You'll see activity in the Celery log, and the export should complete
within a couple of seconds. Refresh your browser and you should see a
status of _Successful_.

* Finally, try viewing the data you exported in the API.

    * Go to http://localhost:8000/api/v1/ in your browser.
    * Click the URL for the `bibs` resource, and make sure you see data for the
    bib records you loaded.
    * Navigate the various related resources in `_links`, such as `marc` and
    `items`. Anything that's linked should take you to the data for that
    resource.


<a name="license"></a>License
-----------------------------

See LICENSE.txt.


<a name="contributors"></a>Contributors
---------------------------------------

* [Jason Thomale](https://github.com/jthomale)
* [Jason Ellis](https://github.com/jason-ellis)
