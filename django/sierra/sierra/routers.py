from __future__ import absolute_import

from django.conf import settings
from django.db import DatabaseError


class SierraRouter(object):
    """
    The DB router used by the Catalog API project.
    """

    def db_for_read(self, model, **hints):
        """
        Routes all read attempts for base models to Sierra DB.
        """
        return 'sierra' if model._meta.app_label == 'base' else 'default'

    def db_for_write(self, model, **hints):
        """
        Raises an error for attempts to write to the live Sierra DB;
        otherwise, routes TEST access on `base` models to the test
        Sierra DB and all others to the default DB.
        """
        if model._meta.app_label == 'base':
            if settings.TESTING:
                return 'sierra'
            raise DatabaseError('Attempted to write to Sierra database.')
        else:
            return 'default'

    def is_sierra_model_and_is_sierra_test_db(self, db, app_label):
        return (app_label == 'base' and db == 'sierra' and
                settings.TESTING)

    def is_not_sierra_model_and_is_default_db(self, db, app_label):
        return app_label != 'base' and db == 'default'

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Allow migrations under 2 conditions: 1. we have Sierra (base
        app) models in a testing environment going into the sierra db,
        or 2. we have non-Sierra models going into the default db.
        """
        return (self.is_sierra_model_and_is_sierra_test_db(db, app_label) or
                self.is_not_sierra_model_and_is_default_db(db, app_label))
