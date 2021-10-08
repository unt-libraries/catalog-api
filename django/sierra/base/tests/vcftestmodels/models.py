"""
Contains factory to set up models to test base.fields.VirtualCompField.

The reason this file contains a model factory/management class, rather
than just having a set of static test models defined, is because we
need the setup--including making and running migrations--to be part of
the testing process. Other tests for this project require the test
database to be reused, but this needs the relevant tables and data to
be reconstructed each time. We need to catch errors when that setup
process fails. Therefore, we need to do the database setup/teardown
explicitly.

There may be a better way to do this, but Django doesn't make it easy
to test things like custom fields to ensure they work with the Django
infrastructure, e.g., migrations and such.
"""

from __future__ import absolute_import

from utils.test_helpers.orm import AppModelsEnvironment


def get_app_models_environment():
    return AppModelsEnvironment('vcftestmodels', 'base.tests.vcftestmodels')
