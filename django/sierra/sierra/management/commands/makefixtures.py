"""
Contains the `makefixtures` manage.py command. 
"""
import ujson

from django.core.management.base import BaseCommand, CommandError

from sierra.management.relationtrees import Orchard


class Command(BaseCommand):
    """
    Run a `makefixtures` command from manage.py.

    This command takes as an argument a path to a JSON configuration
    file that specifies exactly what fixtures to make.
    """
    args = '<config.json>'
    help = 'Generate fixture data according to a supplied json config file'

    def handle(self, *args, **options):
        try:
            trees = Orchard(self.read_config_file(args[0]))
        except Exception as e:
            msg = ('The supplied json configuration file is invalid. {}'
                   ''.format(str(e)))
            raise CommandError(msg)

        try:
            objects = trees.harvest()
        except Exception as e:
            raise CommandError(e)

    def read_config_file(self, filename):
        """
        Read the JSON configuration file and return as Python data.
        """
        with open(filename, 'r') as fh:
            data = ujson.loads(fh.read())
            if not isinstance(data, list):
                data = [data]
            return data

