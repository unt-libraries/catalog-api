Virtualenv Installation
=======================

*Where indicated, this guide will provide instructions for installing to a user environment in instances where root permissions are not available. Instructions assume Linux environment. Windows and OS X instructions can be found at provided links.*

Install pip
-----------

`pip installation instructions <https://pip.pypa.io/en/latest/installing.html>`_

Python 2.7.9+ and 3.4+ include pip by default.

On Debian and Ubuntu::

    $ sudo apt-get install python-pip

On Fedora::

    $ sudo yum install python-pip

Without root::

    $ easy_install --user pip

*Installing without root installs pip to* ``~/.local/bin`` *, so be sure to add this location to your* ``PATH`` *.*

Install virtualenv
------------------

`virtualenv installation instructions <https://virtualenv.pypa.io/en/latest/installation.html>`_

Install virtualenv with pip::

    $ [sudo] pip install virtualenv

Or without root access::

    $ pip install --user virtualenv

*Installing without root installs virtualenv to* ``~/.local/bin``

OPTIONAL: Install virtualenvwrapper
-----------------------------------

Properly configuring virtualenvwrapper will make developing easier.

`virtualenv installation instructions <http://virtualenvwrapper.readthedocs.org/en/latest/install.html>`_

Install via pip with::

    $ [sudo] pip install virtualenvwrapper

Install via pip without root with::

    $ pip install --user virtualenvwrapper

*Installing without root installs virtualenvwrapper to* ``~/.local/bin``

Add three lines to your shell startup file (``.bashrc``, ``.profile``, etc.) to set the location where the virtual environments should live, the location of your development project directories, and the location of the script installed with this package::

    export WORKON_HOME=$HOME/.virtualenvs
    export PROJECT_HOME=$HOME/projects
    source /usr/local/bin/virtualenvwrapper.sh

*If you installed with the pip* ``--user`` *option,* ``virtualenvwrapper.sh`` *will be in* ``~/.local/bin``

After adding the lines above to your shell startup file, be sure to source it (``source ~/.bashrc`` etc) or log out and back in so that the changes are immediately applied.

*Helpful tips for using virtualenvwrapper can be found at* http://mrcoles.com/tips-using-pip-virtualenv-virtualenvwrapper/

Create a Virtual Environment
----------------------------

With **virtualenv**
~~~~~~~~~~~~~~~~~~~

``cd`` to your project folder and run::

    $ virtualenv [-p python_interpreter_path] ENVNAME

Virtualenv will create a folder in your current directory with Python executable files and a copy of pip. ``ENVNAME`` can be anything you'd like to identify the virtual environment. If you do not define a Python interpreter to use, virtualenv will use the first Python interpreter it finds on your ``PATH``.

To begin using your virtual environment, it needs to be activated from your project folder::

    $ source ENVNAME/bin/activate

Your prompt will change to indicate your current virtual environment.

To exit the virtual environment::

    $ deactivate

With **virtualenvwrapper**
~~~~~~~~~~~~~~~~~~~~~~~~~~

virtualenvwrapper stores all of your virtual environments in the directory specified in your ``$WORKON_HOME`` environment variable.

Create a virtual environment with::

    $ mkvirtualenv [-a project_path] [virtualenv options] ENVNAME

The ``-a`` option can be used to associate an existing project directory with the new environment. When you activate the virtual environment, it will ``cd`` you into the project directory.

**virtualenv's** ``-p`` **option should be used to specify the Python interpreter to use.**

The ``ENVNAME`` can be anything you desire to identify your virtual environment. Unlike virtualenv, all of your virtual environments will be stored in the same location and can be activated from anywhere with the ``workon ENVNAME`` command.

To begin using your virtual environment, activate it from any directory with::

    $ workon ENVNAME

To exit the virtual environment::

    $ deactivate

.. _virtualenvwrapper-env-vars:

Environment variables with virtualenvwrapper
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

virtualenvwrapper provides a method of setting environment variables when a virtual environment is activated. Official documentation can be found `here <https://virtualenvwrapper.readthedocs.org/en/latest/scripts.html>`_

To set environment variables for your virtual environment, locate the ``postactivate`` file in your virtual environment's ``/bin`` directory. This can be easily accessed from within the active virtualenv with::

    $ cdvirtualenv /bin

Add your desired environment variables to the ``postactivate`` file and save it. ``postactivate`` is sourced each time you activate your virtualenv with ``workon``. To activate your changes ``deactivate`` your virtualenv and activate it again with ``workon``.

***Please note that** ``postactivate`` **will set your environment variables, but it does not restore previous variables when leaving the environment. Take care not to overwrite important environment variables***

*You may consult the virtualenvwrapper documentation for further customization.*