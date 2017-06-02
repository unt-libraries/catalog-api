"""
Generate a `load_data` function to use in a data migration.

Usage:

from django.db import models, migrations
from utils.load_data import load_data

class Migration(migrations.Migration):

    dependencies = [ ]  # dependencies
    operations = [
        migrations.RunPython(load_data('path/to/fixtures1.json')),
        migrations.RunPython(load_data('path/to/fixtures2.json')),
    ]


"""

from django.core.serializers import base, python
from django.core.management import call_command


def load_data(path_to_fixture, database='default'):
    """
    Create a function for loading fixture data.

    Rather than using the built-in `loaddata` command as-is, the
    returned function loads fixture data based on the current model
    state (in case a data migration needs to run in the middle of
    schema migrations).
    """
    def do_it(apps, schema_editor):
        if schema_editor.connection.alias == database:
            original_get_model = python._get_model

            try:
                # monkey-patch python_.get_model to use the apps argument
                # to get the version of a model at this point in the
                # migrations.
                def _get_model(model_identifier):
                    try:
                        return apps.get_model(model_identifier)
                    except (LookupError, TypeError):
                        msg = ('Invalid model identifier: \'{}\' '
                               ''.format(model_identifier))
                        raise base.DeserializationError(msg)

                python._get_model = _get_model
                call_command('loaddata', path_to_fixture, database=database)
                

            finally:
                python._get_model = original_get_model

    return do_it
