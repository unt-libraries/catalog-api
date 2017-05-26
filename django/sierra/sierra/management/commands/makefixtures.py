"""
Contains the `makefixtures` manage.py command. 
"""
import ujson

from django.core.management.base import BaseCommand, CommandError
from django.core import serializers

from sierra.management import relationtrees


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
            confdata = self.read_config_file(args[0])
            config = Configuration(confdata)
        except Exception as e:
            msg = ('The supplied json configuration file is invalid. {}'
                   ''.format(str(e)))
            raise CommandError(msg)

        try:
            bucket = relationtrees.harvest(config.trees,
                                           tree_qsets=config.tree_qsets)
        except Exception as e:
            raise CommandError(e)

        self.stdout.write(serializers.serialize('json', bucket.dump()))

    def read_config_file(self, filename):
        """
        Read the JSON configuration file and return as Python data.
        """
        with open(filename, 'r') as fh:
            data = ujson.loads(fh.read())
            if not isinstance(data, list):
                data = [data]
            return data


class Configuration(object):
    """
    A list of configuration dicts (imported from a JSON file)
    is passed to __init__, and RelationTree members are generated.

    If errors result from parsing the config data, they are stored in
    self.config_errors and an exception is raised.
    """

    def __init__(self, confdata):
        self.config_errors = []
        self.trees, self.tree_qsets = self._get_trees(confdata)
        if self.config_errors:
            msg = ('Encountered the following errors: {}'
                   ''.format('\n'.join(self.config_errors)))
            raise ConfigError(msg)

    def _error(self, entry, errorstr):
        self.config_errors += ['entry {}: {}'.format(entry, errorstr)]

    def _get_trees(self, confdata):
        trees, tree_qsets = [], {}
        for i, datum in enumerate(confdata):
            tree, tree_qset = None, None
            model_string = datum['model']
            user_branches = datum.get('branches', [])
            trace_branches = datum.get('trace_branches', False)
            user_filter = datum.get('filter', None)
            try:
                root_model = self._get_root_model(model_string)
            except ConfigError as e:
                self._error(i, str(e))
            else:
                try:
                    tree = RelationTree(root_model, user_branches,
                                        trace_branches)
                except ConfigError as e:
                    self._error(i, str(e))

                try:
                    qset = self._get_qset(root_model, user_filter)
                except ConfigError as e:
                    self._error(i, str(e))

                trees += tree
                tree_qsets += qset
        return trees, tree_qsets

    def _get_root_model(self, model_string):
        try:
            return apps.get_model(*model_string.split('.'))
        except AttributeError:
            raise ConfigError('`model` is missing.')
        except ValueError:
            raise ConfigError('`model` is not formatted as "app.model".')
        except LookupError:
            raise ConfigError('`model` ({}) not found.'.format(model_string))

    def _get_qset(self, model, user_filter=None):
        try:
            return model.objects.filter(**user_filter)
        except TypeError:
            return model.objects.all()
        except FieldError:
            msg = ('`filter` {} could not be resolved into a valid field.'
                   ''.format(user_filter))
            raise ConfigError(msg)


