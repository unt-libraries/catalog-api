"""
Contains the `makefixtures` manage.py command. 
"""
import ujson

from django.core.management.base import BaseCommand, CommandError
from django.core.exceptions import FieldError
from django.apps import apps
from django.core import serializers

from sierra.management import relationtrees


INDENT = 2  # How much to indent JSON output from makefixtures.


class ConfigError(Exception):
    """
    Raise exception if provided config file is not valid.
    """
    pass


class Command(BaseCommand):
    """
    Run a `makefixtures` command from manage.py.

    This command takes as an argument a path to a JSON configuration
    file that specifies exactly what fixtures to make.
    """
    args = '<config.json>'
    help = 'Generate fixture data according to a supplied json config file'

    def add_arguments(self, parser):
        parser.add_argument('file', type=str)

    def handle(self, *args, **options):
        try:
            confdata = self.read_config_file(options['file'])
        except Exception as e:
            msg = ('There was a problem reading the supplied json config '
                   'file: {}'.format(str(e)))
            raise CommandError(msg)

        try:
            config = Configuration(confdata)
        except ConfigError as e:
            msg = ('The supplied json config file is invalid: {}'
                   ''.format(str(e)))
            raise CommandError(msg)

        try:
            bucket = relationtrees.harvest(config.trees,
                                           tree_qsets=config.tree_qsets)
        except Exception as e:
            raise CommandError(e)

        self.stdout.write(serializers.serialize('json', bucket.dump(),
                                                indent=INDENT))

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
    is passed to __init__, and RelationTrees are generated.

    If errors result from parsing the config data, they are stored in
    self.config_errors and an exception is raised.
    """

    def __init__(self, confdata):
        self.config_errors = []
        self.trees, self.tree_qsets = self.parse(confdata)
        if self.config_errors:
            msg = ('Encountered the following errors: {}'
                   ''.format('\n'.join(self.config_errors)))
            raise ConfigError(msg)

    def _error(self, entry, errorstr):
        self.config_errors += ['entry {}: {}'.format(entry, errorstr)]

    def parse(self, confdata):
        trees, tree_qsets = [], {}
        for i, datum in enumerate(confdata):
            tree, tree_qset = None, None
            model_string = datum.get('model', None)
            brfields = datum.get('branches', [])
            trace_branches = datum.get('trace_branches', False)
            qset_filter = datum.get('filter', None)
            try:
                root = get_model_from_string(model_string)
            except ConfigError as e:
                self._error(i, '`model`: {}'.format(str(e)))
            else:
                try:
                    tree = self._make_tree(root, brfields, trace_branches)
                except relationtrees.BadRelation as e:
                    self._error(i, '`branches`: {}'.format(str(e)))

                try:
                    qset = self._make_qset(root, qset_filter)
                except relationtrees.BadRelation as e:
                    self._error(i, '`filter`: {}'.format(str(e)))

                trees += [tree]
                tree_qsets[tree] = qset
        return trees, tree_qsets

    def _make_tree(self, root, field_lists, trace_branches):
        branches = []
        for fl in field_lists:
            rels = relationtrees.make_relation_chain_from_fieldnames(root, fl)
            branches += [relationtrees.RelationBranch(root, rels)]
        if trace_branches:
            branches += [b for b in relationtrees.trace_branches(root)
                         if b not in branches]
        return relationtrees.RelationTree(root, branches)

    def _make_qset(self, model, qset_filter=None):
        try:
            return model.objects.filter(**qset_filter)
        except TypeError:
            return model.objects.all()
        except FieldError:
            msg = ('{} could not be resolved into a valid field.'
                   ''.format(qset_filter))
            raise ConfigError(msg)


def get_model_from_string(model_string):
    try:
        return apps.get_model(*model_string.split('.'))
    except AttributeError:
        raise ConfigError('No model was provided.')
    except ValueError:
        raise ConfigError('Model string not formatted as "app.model".')
    except LookupError:
        raise ConfigError('Model {} not found.'.format(model_string))
