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
    """
    Access info about a relationship between two Django Model objects.

    The relationship this object represents is from the POV of the
    model provided on init--e.g., model.fieldname. It's mainly a
    simplified way to get information about that relationship.

    After initializing a Relation object, you can access the POV model
    on self.model, the model at the other end of the relationship on
    self.target_model, the POV model name on self.model_name, the name
    of the attribute containing the relationship on self.fieldname, and
    that attribute itself on self.accessor.

    Public methods:

    `is_foreign_key` returns True if the relationship from self.model
    to self.target_model uses a foreign key on self.model (one-to-one
    or many-to-one).

    `is_many_to_many` returns True if the relationship from self.model
    to self.target_model is many-to-many.

    `is_indirect` returns True if the relationship from self.model to
    self.target_model is an indirect one--if it's one-to-many, where a
    foreign key is present on self.target_model pointing to self.model.
    By default these are `fieldname_set` attributes.

    `arrange_models` returns a list containing self.model and
    self.target_model arranged in order based on which is dependent on
    the other. A model with an FK relationship to the target model is
    assumed to be dependent on that model--data for the target model
    should be loaded into the database before the FKs exist to use to
    populate data for the dependent model. Optionally, a model list
    may be supplied to this method--if it is, then models are arranged
    in place in the list if they appear there, and the full list is
    returned. If they don't appear, they are added or inserted as
    needed to satisfy dependencies.
    """

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
        self.target_model = self._get_target_model()
        self.fieldname = fieldname

    def _accessor_is_a_relation(self, a):
        return (self.is_foreign_key(a) or self.is_many_to_many(a) or 
                self.is_indirect(a))

    def _get_target_model(self):
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
        models = models or []
        goes_first = self.model if self.is_indirect() else self.target_model
        goes_second = self.target_model if self.is_indirect() else self.model
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


class RelationPath(list):
    """
    Work with multiple models connected via relationships.

    This is a child of the `list` type: on init, it takes a Django
    model object and a list of field names representing one path or
    branch of related models (`path_fields`). It converts these to
    Relation objects and initializes itself as a list of these.

    Public methods:

    `get_selects_and_prefetches` returns a tuple of two lists of
    field names. Each could be joined with '__' and passed respectively
    to the `selected_related` and `prefetch_related` methods of a
    self.model Queryset, in order to pre-cache the data necessary to
    traverse the branch via model instances.

    `arrange_models` returns a list of the models for this path, put
    in dependency order. You can supply an optional list of models to
    insert or rearrange--any models found already in the list will be
    rearranged if they're not already in order, and any models not in
    the list will be inserted in the most logical spot.
    """

    def __init__(self, model, path_fields):
        path = self._build_relations_from_path_fields(model, path_fields)
        super(RelationPath, self).__init__(path)
        self.model = model
        self.fieldnames = path_fields

    def _build_relations_from_path_fields(self, model, path_fields):
        relations = []
        for i, fieldname in enumerate(path_fields):
            try:
                relation = Relation(model, fieldname)
            except BadRelation as e:
                msg = 'path_field {} is invalid: {}'.format(i, str(e))
                raise BadRelationPath(msg)
            relations += [relation]
            model = relation.target_model
        return relations

    def get_selects_and_prefetches(self):
        selects, prefetch = [], []
        all_fks_so_far = True
        m = self.model
        for relation in self:
            if relation.is_foreign_key() and all_fks_so_far:
                selects.append(relation.fieldname)
            else:
                all_fks_so_far = False
                prefetch.append(relation.fieldname)
            m = relation.target_model
        return selects, prefetch

    def arrange_models(self, models=None):
        for relation in self:
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
    """
    Work with one entry in a makefixtures job configuration file.

    This class represents one complete entry in a makefixtures job
    config file, which should look something like this:

    {
        'model': 'testmodels.SelfReferentialNode',
        'follow_relations': False,
        'filter': {
            'referencenode__name': 'ref1',
        },
        'paths': [
            ['referencenode_set', 'end'],
            ['referencenode_set', 'srn', 'end'],
            ['referencenode_set', 'srn', 'parent', 'end'],
        ]
    }

    On init, the entry is parsed and validated via the four private
    `prep` methods. The Django model is found and stored in self.model.
    A QuerySet object is created, using the appropriate filter params,
    and stored in self.qs. Each supplied path is parsed, converted to a
    RelationPath object, and stored in self.paths. Based on the paths,
    the QuerySet's `select_related` and `prefetch_related` methods are
    used to pre-cache data as needed to cover all of the related models
    that will need to be accessed.

    If at any point any of these steps raise errors, then the error
    message(s) are appended to self.errors.

    After init, prepped config data can be accessed on self.model,
    self.qs, and self.paths.

    Public methods:

    `arrange_models` returns a list of all models referenced in this
    config entry in dependency order. An optional models list can be
    supplied--if supplied, it will first look for the models in the
    list and rearrange them if needed or insert them if they are not
    there.
    """

    def __init__(self, data):
        self.errors = []
        model, qs, paths = None, None, None
        model_string = data.get('model', None)
        follow_relations = data.get('follow_relations', False)
        user_paths = data.get('paths', [])
        user_filter = data.get('filter', None)

        try:
            model = self._prep_model(model_string)
        except ConfigIsInvalid as e:
            self.errors.append(str(e))
        else:
            try:
                paths = self._prep_paths(model, user_paths, follow_relations)
            except ConfigIsInvalid as e:
                self.errors.append(str(e))

            try:
                qs = self._prep_filtered_queryset(model, user_filter)
            except ConfigIsInvalid as e:
                self.errors.append(str(e))
            else:
                if paths is not None:
                    qs = self._prep_qs_relations(qs, model, paths)

        self.model, self.qs, self.paths = model, qs, paths

    def _prep_model(self, model_string):
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

    def _prep_filtered_queryset(self, model, user_filter):
        try:
            return model.objects.filter(**user_filter)
        except TypeError:
            return model.objects.all()
        except FieldError:
            msg = ('`filter` {} could not be resolved into a valid field.'
                   ''.format(user_filter))
            raise ConfigIsInvalid(msg)

    def _prep_paths(self, model, user_paths, follow_relations):
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

    def _prep_qs_relations(self, qs, model, paths):
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


class Config(list):
    """
    Work with a list of ConfigEntry objects.

    Config inherits from `list` and is mainly just a list of
    ConfigEntry objects. Configuration data (imported from a JSON file)
    is passed to __init__, and entries are generated from that.

    If errors result from `_make_entries`, they are stored in
    self.errors and an exception is raised.

    Dependencies for all models involved in the configuration are
    generated and stored in self.dependencies. This list can be used to
    order the output of model data so that dependencies are satisfied.
    """

    def __init__(self, entry_data):
        self.errors = []
        super(Config, self).__init__(self._make_entries(entry_data))
        if self.errors:
            msg = ('Config encountered the following errors: {}'
                   ''.format('\n'.join(self.errors)))
            raise ConfigIsInvalid(msg)

        self.dependencies = self._find_dependencies()

    def _make_entries(self, entry_data):
        entries = []
        for i, entry_datum in enumerate(entry_data):
            entry = ConfigEntry(entry_datum)
            for error in entry.errors:
                self.errors += ['entry {}: {}'.format(i, error)]
            entries += [entry]
        return entries

    def _find_dependencies(self):
        models = []
        for entry in self:
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
        try:
            config = Config(self.read_config_file(args[0]))
        except Exception as e:
            msg = ('The supplied json configuration file is invalid. {}'
                   ''.format(str(e)))
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

