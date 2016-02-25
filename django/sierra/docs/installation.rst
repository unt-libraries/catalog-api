Installation
============

Requirements
------------

.. todo::
    Figure out version compatibility as some versions used are no longer available to download.

Python
~~~~~~

Version 2.7.5

Python 2.7.5 can be downloaded and installed from https://www.python.org/download/releases/2.7.5/

Java
~~~~

Version 1.7.0_45 of JDK and included JRE

Java JDK can be downloaded and installed from http://www.oracle.com/technetwork/java/javase/downloads/index.html

Solr
~~~~

Version 4.5.1

Solr 4.5.1 is included with the project. Solr can be downloaded from http://www.apache.org/dyn/closer.cgi/lucene/solr/5.1.0

PostgreSQL
~~~~~~~~~~

Version 8.4.20

PostgreSQL can be downloaded and installed from http://www.postgresql.org/download/

Redis
~~~~~

Version 2.8.9

Redis can be downloaded and installed from http://redis.io/download

Installing
----------

To install this project, first clone the repository

.. todo::
    Cloning instructions forthcoming

The project is divided into two main folders:

* ``django/`` - contains the bulk of the project
* ``solr/`` - contains all Solr related code

Python packages
~~~~~~~~~~~~~~~

The recommended method for installing Python dependencies is ``pip`` in a virtual environment. Instructions for installing ``pip``, ``virtualenv``, and ``virtualenvwrapper`` can be found :doc:`here </virtualenv>`.

With your virtual environment active, ``cd`` into the directory containing the project's ``requirements.txt`` file and run::

    $ [sudo] pip install -r requirements.txt

This will install each of the packages and versions in ``requirements.txt`` to your virtual environment.

*If you receive an error stating* ``Command "python setup.py egg_info" failed with error code 1`` *you are missing dependencies that can't be installed by pip. You may try running*:

Debian/Ubuntu::

    $ sudo apt-get install python-dev

Fedora::

    $ sudo yum install python-devel

*If you still receive the error, look at which file the error message states is missing. On Ubuntu, a missing* ``mysql_config`` *can be fixed by installing* ``libmysqlclient-dev``. ``pg_config`` *can be fixed by installing* ``libpq-dev``.