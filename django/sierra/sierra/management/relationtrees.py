"""
Contains classes needed for custom sierra management commands.
"""

from django.core.exceptions import FieldError
from django.apps import apps


class ConfigError(Exception):
    """
    Raise exception if user-provide config data is invalid.
    """
    pass


class BadBranch(Exception):
    """
    Raise exception if a RelationBranch is invalid.
    """
    pass


class BadRelation(Exception):
    """
    Raise exception if a relation in a RelationBranch is invalid.
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
    self.target_model, the POV model name on self.model_name, the
    target model name on self.target_model_name, and the name of the
    attribute containing the relationship on self.fieldname, and that
    attribute itself on self.accessor.

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

    `fetch_target_model_objects` returns a list of Django Model object
    instances related to a set of source object instances via this
    relation.
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
        self.target_model_name = self.target_model._meta.model_name
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

    def fetch_target_model_objects(self, source_objects):
        all_related_objs = []
        for obj in source_objects:
            subset = getattr(obj, self.fieldname)
            if self.is_foreign_key():
                subset = [] if subset is None else [subset]
            else:
                subset = subset.all()
            all_related_objs += subset
        return list(set(all_related_objs))


class RelationBranch(list):
    """
    Work with a chain of models connected via relationships.
    """

    def __init__(self, root_model, branch_fields):
        branch = self._make_branch(root_model, branch_fields)
        super(RelationBranch, self).__init__(branch)
        self.root_model = root_model
        self.root_model_name = root_model._meta.model_name
        self.fieldnames = branch_fields

    def _make_branch(self, model, branch_fields):
        relations = []
        for i, fieldname in enumerate(branch_fields):
            try:
                relation = Relation(model, fieldname)
            except BadRelation as e:
                msg = 'field {} is invalid: {}'.format(i, str(e))
                raise BadBranch(msg)
            relations += [relation]
            model = relation.target_model
        return relations

    def prepare_qset(self, qset):
        selects, prefetches = self._get_selects_and_prefetches_for_qset()
        if selects:
            qset = qset.select_related(selects)
        if prefetches:
            qset = qset.prefetch_related(prefetches)
        return qset

    def _get_selects_and_prefetches_for_qset(self):
        selects, prefetches = [], []
        all_fks_so_far = True
        for relation in self:
            if relation.is_foreign_key() and all_fks_so_far:
                selects.append(relation.fieldname)
            else:
                all_fks_so_far = False
                prefetches.append(relation.fieldname)
        select_str = '__'.join(selects)
        prefetch_str = '__'.join(selects + prefetches) if prefetches else ''
        return select_str, prefetch_str

    def arrange_models(self, models=None):
        for relation in self:
            models = relation.arrange_models(models)
        return models

    def pick_into(self, objset, bucket):
        for relation in self:
            objset = relation.fetch_target_model_objects(objset)
            for obj in objset:
                bucket.put(relation.target_model, obj)
        return bucket


class RelationTree(list):
    """
    Work with the sets of relations that branch from a Django model.
    """

    def __init__(self, model, user_branches=None, trace_branches=False):
        self.root_model = model
        branches, errors = [], []
        for i, branch_fields in enumerate(user_branches):
            try:
                branches += [RelationBranch(model, branch_fields)]
            except BadBranch as e:
                errors += 'branch {}: {}'.format(i, str(e))
        if errors:
            raise ConfigError('`branches`: {}'.format('; '.join(errors)))
        if trace_branches:
            branches += self.trace_branches()
        super(RelationTree, self).__init__(branches)

    def trace_branches(self, only=None, model=None, brfields=None, cache=None):
        tracing = []
        model = model or self.root_model
        meta = model._meta
        m2ms = meta.many_to_many if not only == 'fk' else [] 
        fks = [f for f in meta.fields if f.rel] if not only == 'm2m' else []
        relfields = fks + m2ms

        for field in relfields:
            cache = cache or []
            field_id = '{}.{}'.format(meta.model_name, field.name)
            if field_id not in cache:
                next_model = field.rel.to
                branchcache = cache + [field_id]
                next_brfields = (brfields or []) + [field.name]
                tracing += self.trace_branches(only, next_model, next_brfields,
                                               branchcache)
        if brfields and len(relfields) == 0:
            tracing.append(RelationBranch(self.root_model, brfields))
        return tracing

    def prepare_qset(self, qset):
        for branch in self:
            qset = branch.prepare_qset(qset)
        return qset

    def arrange_models(self, models=None):
        models = models or []
        for branch in self:
            models = branch.arrange_models(models)
        return models

    def pick_into(self, bucket, qset=None):
        qset = self.prepare_qset(qset or self.root_model.objects.all())
        for obj in qset:
            bucket.put(self.root_model, obj)
        for branch in self:
            bucket = branch.pick_into(qset, bucket)
        return bucket


class ObjectBucket(dict):

    def __init__(self, compartments=None):
        self.compartments = compartments or []

    def put(self, compartment, obj):
        try:
            self[compartment][obj.pk] = obj
        except KeyError:
            self[compartment] = {obj.pk: obj}
            self.compartments += [compartment]

    def dump(self):
        objects = []
        for c in self.compartments:
            objects += sorted(self.get(c, {}).values(), key=lambda x: x.pk)
        return objects


class Orchard(object):
    """
    Make a set of RelationTree objects and harvest objects from it.

    Configuration data (imported from a JSON file) is passed to
    __init__, and RelationTree members are generated.

    If errors result from parsing the config data, they are stored in
    self.config_errors and an exception is raised.

    `harvest` returns ...
    """

    def __init__(self, confdata):
        self.config_errors = []
        self.plots = self._plant_trees(confdata)
        if self.config_errors:
            msg = ('Encountered the following errors: {}'
                   ''.format('\n'.join(self.config_errors)))
            raise ConfigError(msg)
        self.bucket = ObjectBucket(self._calculate_model_dependencies())

    def _error(self, entry, errorstr):
        self.config_errors += ['entry {}: {}'.format(entry, errorstr)]

    def _plant_trees(self, confdata):
        plots = []
        for i, datum in enumerate(confdata):
            plot = {'tree': None, 'qset': None}
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
                    plot['tree'] = RelationTree(root_model, user_branches,
                                                trace_branches)
                except ConfigError as e:
                    self._error(i, str(e))

                try:
                    plot['qset'] = self._get_qset(root_model, user_filter)
                except ConfigError as e:
                    self._error(i, str(e))

            plots += [plot]
        return plots

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

    def _calculate_model_dependencies(self):
        models = []
        for plot in self.plots:
            models = plot['tree'].arrange_models(models)
        return models

    def harvest(self):
        for plot in self.plots:
            plot['tree'].pick_into(self.bucket, qset=plot['qset'])
        return self.bucket.dump()

