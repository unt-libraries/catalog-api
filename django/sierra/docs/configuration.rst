Configuration
=============

Environment
-----------

Ensure the following are on your ``PATH``:

* Java (``which java``)
* Redis (``which redis-server``)

The following environment variables should also be set:

* ``JAVA_HOME`` = Java install directory. `Instructions <http://docs.oracle.com/cd/E19182-01/820-7851/inst_cli_jdk_javahome_t/index.html>`_
* ``SIERRA_DB_USER`` = Sierra database username
* ``SIERRA_DB_PASSWORD`` = Sierra database password
* ``SIERRA_SECRET_KEY`` = `Django secret key <https://docs.djangoproject.com/en/1.8/ref/settings/#secret-key>`_. Salt used for hashing.
* ``DJANGO_SETTINGS_MODULE`` = `Settings module used by Django <https://docs.djangoproject.com/en/1.8/topics/settings/#envvar-DJANGO_SETTINGS_MODULE>`_. This should be the settings file you'd like to use in the current environment (production, dev, etc). The format is in Python path syntax (e.g. ``sierra.settings.ENVFILE``). See :ref:`django_app_settings_files` for more information.

Optional environment variables can be set for development cases where multiple instances of the project will run on the same server concurrently.

* ``DJANGO_PORT`` = Defaults to 8000.
* ``SOLR_PORT`` = Defaults to 8983.
* ``REDIS_PORT`` = Defaults to 6379.

*If using virtualenvwrapper, environment variables can be set each time the virtual environment is activated. See* :ref:`virtualenvwrapper-env-vars` *for more information*

Django app settings
-------------------

.. _django_app_settings_files:

Settings files
~~~~~~~~~~~~~~

``base.py`` contains common Django settings. A settings file specific to each environment should be invoked when starting the server. The environment settings file should import ``base.py`` and then override relevant settings.

For example, one may choose to create ``production.py`` for their production environment and ``dev.py`` for their development environment. In either case, the first line of each settings file should include::

    from .base import *

The settings file can be invoked in two ways. The recommended method is by setting the ``DJANGO_SETTINGS_MODULE`` environment variable. You may override ``DJANGO_SETTINGS_MODULE`` or specify a settings file when you start the Django server by using the ``--settings`` option. For example::

    $ python manage.py runserver --settings=sierra.settings.production

*As with all settings files, take care to avoid committing production environment keys and secrets in a public repository. Files containing this information should be excluded from version control or the information should be stored in environment variables.*

Settings
~~~~~~~~

This is a list of settings that may need to be changed in your base settings and overridden for your specific development and production environments.

.. todo::
    base, prod, or dev for each setting? clean this section up. Only include what may need to be changed.

* ``LOG_FILE_DIR`` = The log file directory ('logs') must exist prior to starting the server or an error will be encountered. By default, this directory is located in the user's home directory.
* ``SOLR_PORT`` = Default port that will be used for Solr if none is defined in the ``$SOLR_PORT`` environment variable.
* ``REDIS_PORT`` = Default port that will be used for Redis if none is defined in the ``$REDIS_PORT`` environment variable.
* ``ALLOWED_HOSTS`` = A list of strings representing the host/domain names that this Django site can serve. https://docs.djangoproject.com/en/1.8/ref/settings/#std:setting-ALLOWED_HOSTS
* ``ADMINS`` = A tuple that lists people who get code error notifications. https://docs.djangoproject.com/en/1.8/ref/settings/#std:setting-ADMINS
* ``MANAGERS`` = A tuple in the same format as ADMINS that specifies who should get broken link notifications when BrokenLinkEmailsMiddleware is enabled. https://docs.djangoproject.com/en/1.8/ref/settings/#managers
* ``DATABASES`` = A dictionary containing the settings for all databases to be used with Django. https://docs.djangoproject.com/en/1.8/ref/settings/#databases

.. todo::
    Provide more information on different Databases for this project. Separate document.

* ``TIME_ZONE`` = A string representing the time zone for this installation, or None. https://docs.djangoproject.com/en/1.8/ref/settings/#time-zone
* ``HAYSTACK_CONNECTIONS`` = Required for Haystack to connect to Solr.
* ``REST_FRAMEWORK`` = Controls REST defaults
* ``CORS_ORIGIN_REGEX_WHITELIST`` =
* ``BROKER_URL`` = Celery connection to Redis
* ``REDIS_CONNECTION`` = Required for Redis configuration.


SolrMarc settings
-----------------

SolrMarc is used to index bib records in Solr. The SolrMarc code is located in ``solr/solrmarc/``.

Because the stock version of SolrMarc doesn't work with Solr 4.x, this project includes a `fork <https://github.com/solrmarc/stanford-solr-marc>`_ of SolrMarc from Naomi Dushay of Stanford University Libraries.

The following files should be modified in ``solr/solrmarc``:

* ``*_config.properties`` - Contains some configuration settings for SolrMarc. These two settings should be changed:
    * ``solrmarc.hosturl`` - Should contain the URL for the Solr index that SolrMarc loads onto.
    * ``solrmarc.indexing.properties`` - points to the ``*_index.properties`` file described below.
* ``*_index.properties`` - Defines how MARC fields translate to fields in the Solr index.
* ``indexfile.sh`` - A bash script that runs a SolrMarc load on a file. The filename is provided as an argument to the script.
    * ``CONFIG`` - The ``*_config.properties`` file you will be using.

Sierra Settings
---------------

The Catalog API app requires access to Sierra to export data. It's recommended that a new user is created for each instance of the app that will be running.

After creating the user in Sierra, set the user's search path with::

    ALTER RULE {user} SET search_path TO sierra_view;

.. todo::
    Confirm statement above with Jason T