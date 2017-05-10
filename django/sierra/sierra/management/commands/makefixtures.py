"""
Contains the `makefixtures` manage.py command. 
"""
import ujson

from django.core.management.base import BaseCommand, CommandError
from django.core.exceptions import FieldError
from django.apps import apps


class ConfigIsInvalid(CommandError):
    """
    Raise exception if the makefixtures configuration file is invalid.
    """
    pass


class BadRelationPath(Exception):
    """
    Raise exception if any of the user's `paths` entries are invalid.
    """
    pass


class BadRelation(Exception):
    """
    Raise exception if any individual relation in a path is invalid.
    """
    pass


class Relation(object):

    def __init__(self, model, fieldname):
        try:
            self.model_name = model._meta.model_name
        except AttributeError:
            raise BadRelation('`model` arg must be a model object.')
        try:
            self.accessor = getattr(model, fieldname)
        except AttributeError:
            msg = '{} not found on {}'.format(fieldname, self.model_name)
            raise BadRelation(msg)
        if not (self._accessor_is_a_relation(self.accessor)):
            msg = ('{} is not a relation field on {}'
                   .format(fieldname, self.model_name))
            raise BadRelation(msg)
        self.model = model
        self.fieldname = fieldname

    def _accessor_is_a_relation(self, a):
        return (self.is_foreign_key(a) or self.is_many_to_many(a) or 
                self.is_indirect(a))

    def get_target_model(self):
        if self.is_foreign_key() or self.is_many_to_many():
            return self.accessor.field.rel.to
        elif self.is_indirect():
            return self.accessor.related.model

    def is_foreign_key(self, accessor=None):
        a = accessor or self.accessor
        return hasattr(a, 'field') and not hasattr(a, 'related_manager_cls')

    def is_many_to_many(self, accessor=None):
        a = accessor or self.accessor
        return hasattr(a, 'field') and hasattr(a, 'related_manager_cls')

    def is_indirect(self, accessor=None):
        a = accessor or self.accessor
        return not hasattr(a, 'field') and hasattr(a, 'related_manager_cls')

    def arrange_models(self, models=None):
        """
        Order a `models` list to satisfy this relation's dependencies.
        """
        models = models or []
        target_model = self.get_target_model()
        goes_first = self.model if self.is_indirect() else target_model
        goes_second = target_model if self.is_indirect() else self.model
        models += [goes_second] if goes_second not in models else []
        goes_second_index = models.index(goes_second)
        try:
            goes_first_index = models.index(goes_first)
            if goes_second_index < goes_first_index:
                models.remove(goes_first)
        except ValueError:
            pass

        if goes_first not in models:
            models.insert(goes_second_index, goes_first)

        return models


class RelationPath(object):

    def __init__(self, model, path_fields):
        self.model = model
        self.path_fields = path_fields
        self.relations = self.build_relations_from_path_fields(path_fields)

    def build_relations_from_path_fields(self, path_fields):
        model = self.model
        relations = []
        for i, fieldname in enumerate(path_fields):
            try:
                relation = Relation(model, fieldname)
            except BadRelation as e:
                msg = 'path_field {} is invalid: {}'.format(i, str(e))
                raise BadRelationPath(msg)
            relations += [relation]
            model = relation.get_target_model()
        return relations

    def get_selects_and_prefetches(self):
        selects, prefetch = [], []
        all_fks_so_far = True
        m = self.model
        for relation in self.relations:
            if relation.is_foreign_key() and all_fks_so_far:
                selects.append(relation.fieldname)
            else:
                all_fks_so_far = False
                prefetch.append(relation.fieldname)
            m = relation.get_target_model()
        return selects, prefetch

    def arrange_models(self, models=None):
        """
        Order a `models` list to satisfy all this path's dependencies.
        """
        for relation in self.relations:
            models = relation.arrange_models(models)
        return models


def trace_relations(model, onlyfk=False, first_model=None, pfields=None,
                    fieldcache=None):
    """
    Recursively trace all direct relationships from the given model.

    Returns an array of RelationPath objects.

    By default, both foreign-key and many-to-many relationships are
    traced. Indirect relationships (where there is a foreign key on
    another model pointing to the given model) are not traced. You may
    pass onlyfk=True to limit the tracing to foreign-key relationships
    only (i.e., one-to-one and many-to-one).
    """
    tracing = []
    first_model = first_model or model
    m2m_fields = model._meta.many_to_many
    fk_fields = [f for f in model._meta.fields if f.rel]
    rel_fields = fk_fields + (m2m_fields if not onlyfk else [])
    for field in rel_fields:
        fieldcache = fieldcache or []
        field_id = '{}.{}'.format(model._meta.model_name, field.name)
        if field_id not in fieldcache:
            next_model = field.rel.to
            pathcache = fieldcache + [field_id]
            next_pfields = (pfields or []) + [field.name]
            tracing += trace_relations(next_model, onlyfk, first_model,
                                       next_pfields, pathcache)
    if pfields and len(rel_fields) == 0:
        tracing.append(RelationPath(first_model, pfields))
    return tracing


class ConfigEntry(object):

    def __init__(self, data):
        self.errors = []
        model, qs, paths = None, None, None
        model_string = data.get('model', None)
        follow_relations = data.get('follow_relations', False)
        user_paths = data.get('paths', [])
        user_filter = data.get('filter', None)

        try:
            model = self.get_model(model_string)
        except ConfigIsInvalid as e:
            self.errors.append(str(e))

        if model is not None:
            try:
                qs = self.get_filtered_queryset(model, user_filter)
            except ConfigIsInvalid as e:
                self.errors.append(str(e))

        if qs is not None:
            try:
                paths = self.get_paths(model, user_paths, follow_relations)
            except ConfigIsInvalid as e:
                self.errors.append(str(e))

            if paths is not None:
                qs = self.prep_qs_relations(qs, model, paths)

        self.model, self.qs, self.paths = model, qs, paths

    def get_model(self, model_string):
        try:
            (app, model) = model_string.split('.')
            return apps.get_model(app, model)
        except AttributeError:
            raise ConfigIsInvalid('`model` is missing.')
        except ValueError:
            raise ConfigIsInvalid('`model` is not formatted as "app.model".')
        except LookupError:
            msg = ('`model` ({}) not found.'.format(model_string))
            raise ConfigIsInvalid(msg)

    def get_filtered_queryset(self, model, user_filter):
        try:
            return model.objects.filter(**user_filter)
        except TypeError:
            return model.objects.all()
        except FieldError:
            msg = ('`filter` {} could not be resolved into a valid field.'
                   ''.format(user_filter))
            raise ConfigIsInvalid(msg)

    def get_paths(self, model, user_paths, follow_relations):
        paths, errors = [], []
        for i, path_fields in enumerate(user_paths):
            try:
                paths += [RelationPath(model, path_fields)]
            except BadRelationPath as e:
                errors += 'path {}: {}'.format(i, str(e))
                break
        if errors:
            raise ConfigIsInvalid('`paths`: {}'.format('; '.join(errors)))
        if follow_relations:
            paths += trace_relations(model)
        return paths

    def prep_qs_relations(self, qs, model, paths):
        for path in paths:
            selects, prefetch = path.get_selects_and_prefetches()
            if selects:
                qs = qs.select_related('__'.join(selects))
            if prefetch:
                qs = qs.prefetch_related('__'.join(prefetch))
        return qs

    def arrange_models(self, models=None):
        models = models or []
        for path in self.paths:
            models = path.arrange_models(models)
        return models


class Config(object):

    def __init__(self, entry_data):
        self.errors = []
        self.entries = self.make_entries(entry_data)

    def make_entries(self, entry_data):
        """
        Process config entries.
        """
        entries = []
        for i, entry_datum in enumerate(entry_data):
            entry = ConfigEntry(entry_datum)
            for error in entry.errors:
                self.errors += ['entry {}: {}'.format(i, error)]
            entries += [entry]
        return entries

    def get_dependencies(self):
        models = []
        for entry in self.entries:
            models = entry.arrange_models(models)
        return models


class Command(BaseCommand):
    """
    Run a `makefixtures` command from manage.py.

    This command takes as an argument a path to a JSON configuration
    file that specifies exactly what fixtures to make.
    """
    args = '<config.json>'
    help = 'Generate fixture data according to a supplied json config file'

    def handle(self, *args, **options):
        config = Config(self.read_config_file(args[0]))
        if config.errors:
            msg = ('The supplied json configuration file is invalid. The '
                   'following errors were found: {}'
                   ''.format('\n'.join(config.errors)))
            raise ConfigIsInvalid(msg)

        # objects = { model_name: { obj.pk: <JSON obj> } }
        try:
            objects = self.fetch_objects(config)
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

    def fetch_objects(self, prepped_config):
        """
        Return a set of 
        """
        pass

