"""
Contains test helpers for managing ORM test models and such.
"""

import os
from collections import OrderedDict

from django.db import models
from django.core import management
from django.db import connections, OperationalError

from django.conf import settings


class AppModelsEnvironment(object):
    """
    Class for creating and managing a set of model classes for testing.

    Create a model class using `make`. That class is added to
    self.models, an OrderedDict, and can be accessed there. Example:
    my ModelSet object is `modelset` and one of my classes is MyModel.
    I can access it via modelset.models['MyModel'].

    Destroy a model class, remove it from this ModelSet, and remove it
    from the Django apps registry using `delete`.

    Destroy all model classes on this ModelSet using `clear`.

    Sync the database using `migrate`. Migrations are made and the
    database is synced via admin commands (makemigrations, migrate),
    just depending on the current state of the models.

    Use `reset` to roll back and clear migrations for the entire app.

    Use `close` to completely roll back changes made by creating models
    this way--it calls `reset` and then `clear`.

    This is also a context manager object, so, when used in a `with`
    block, it calls `close` for you afterward.
    """

    def __init__(self, modulename, modulepath, modeltype=models.Model,
                 using='default'):
        self.models = OrderedDict()
        self.modulename = modulename
        self.modulepath = modulepath
        self.modeltype = modeltype
        self.connection = connections[using]

    def __enter__(self):
        self.reset()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _get_migration_file_info(self):
        """
        Get the filenames of all migration files for the app the models
        on this object belong to, along with the absolute path to the
        migration directory. Returns a tuple: (migfnames, migdirpath)
        """
        def is_migration(filename):
            if not filename.startswith('__'):
                if filename.endswith('.py') or filename.endswith('.pyc'):
                    return True
            return False

        moddirpath = os.path.join(*self.modulepath.split('.'))
        migdirpath = os.path.join(settings.PROJECT_DIR, moddirpath,
                                  'migrations')
        migfnames = [fn for fn in os.listdir(migdirpath) if is_migration(fn)]
        return (migfnames, migdirpath)

    def make(self, name, fields, modeltype=None, meta_options=None):
        """
        Return a new test model class. Pass in the test model's name
        via `name` and a dict of fields to create via `fields`. The
        model will be a base class of self.modeltype by default, or you
        may pass in a custom type via `modeltype`. Any special meta
        options you need can be passed as a dict via `meta_options`.
        """
        params = fields
        modeltype = modeltype or self.modeltype
        params['__module__'] = self.modulepath
        if meta_options:
            params['Meta'] = type('Meta', (object,), meta_options)
        new_model = type(name, (modeltype,), params)
        self.models[name] = new_model
        return new_model

    def delete(self, name):
        """
        Destroy one of the registered test models you created, by name.
        """
        model = self.models[name]
        apps = model._meta.apps
        model_name = model._meta.model_name
        try:
            del(apps.all_models[self.modulename][model_name])
        except KeyError:
            pass
        del(model)
        apps.clear_cache()
        del(self.models[name])

    def clear(self):
        """
        Destroy all registered test models created via this factory.
        """
        for name in self.models.keys():
            self.delete(name)

    def migrate(self):
        """
        Create and run migrations to sync the DB with the current model
        state.
        """
        management.call_command('makemigrations', self.modulename, verbosity=0,
                                interactive=False)
        management.call_command('migrate', self.modulename, verbosity=0,
                                interactive=False)

    def reset(self):
        """
        Reset migration states for the app this object represents.
        Resets migration history and deletes all migration files.
        """
        try:
            management.call_command('migrate', self.modulename, 'zero',
                                     verbosity=0, interactive=False)
        except OperationalError:
            # If DB tables from a previous run have been deleted, the
            # above attempt to migrate will error out. In that case,
            # just back the migration history up to zero.
            management.call_command('migrate', self.modulename, 'zero',
                                    verbosity=0, interactive=False, fake=True)

        migfnames, migdirpath = self._get_migration_file_info()
        for migfname in migfnames:
            os.remove(os.path.join(migdirpath, migfname))

    def close(self):
        self.reset()
        self.clear()
