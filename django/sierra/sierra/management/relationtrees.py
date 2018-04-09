"""
Contains classes, etc. needed for custom sierra management commands.
"""


class BadRelation(Exception):
    """
    Raise exception if a relation in a RelationBranch is invalid.
    """
    pass


class Bucket(dict):
    """
    Store and compartmentalize model obj instances of different types.
    """

    def __init__(self, compartments=None):
        super(Bucket, self).__init__({})
        self.update_compartments(compartments or [])

    def update_compartments(self, compartments):
        self.compartments = compartments
        for compartment in [c for c in compartments if c not in self]:
            self[compartment] = {}

    def put(self, objset):
        try:
            len(objset)
        except TypeError:
            objset = [objset]
        for obj in objset:
            compartment = obj._meta.model
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


class Relation(object):
    """
    Access info about a relationship between two Django Model objects.

    The relationship a given Relatiom object represents is from the POV
    of the model provided on init--e.g., model.fieldname. It's mainly a
    simplified way to get information about that relationship.

    After initializing a Relation object, you can access the POV model
    on self.model, the model at the other end of the relationship on
    self.target_model, the POV model name on self.model_name, the
    target model name on self.target_model_name, and the name of the
    attribute containing the relationship on self.fieldname, and that
    attribute itself on self.accessor.

    Public methods:

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
            model_name = model._meta.model_name
        except AttributeError:
            raise BadRelation('`model` arg must be a model object.')
        try:
            accessor = getattr(model, fieldname)
        except AttributeError:
            msg = '{} not found on {}'.format(fieldname, model_name)
            raise BadRelation(msg)

        self._describe(accessor)
        self.model = model
        self.fieldname = fieldname
        self.target_model = self._get_target_model(accessor)

    def __repr__(self):
        mname = '.'.join([self.model._meta.app_label,
                          self.model._meta.object_name])
        target_mname = '.'.join([self.target_model._meta.app_label,
                                 self.target_model._meta.object_name])
        kind = 'Direct' if self.is_direct else 'Indirect'
        kind = '{} {}'.format(kind, ' M2M' if self.is_m2m else ' FK')
        return '<{} on `{}` from {} to {}>'.format(kind, self.fieldname, mname,
                                                   target_mname)
        
    def _describe(self, acc):
        self.is_direct = True if hasattr(acc, 'field') else False
        self.is_multi = True if hasattr(acc, 'related_manager_cls') else False
        field = acc.field if self.is_direct else acc.related.field
        self.through = getattr(field.rel, 'through', None)
        self.is_m2m = False if self.through is None else True

    def _get_target_model(self, acc):
        return acc.field.rel.to if self.is_direct else acc.related.model

    def get_as_through_relations(self):
        meta = self.model._meta
        all_rels = [
            f for f in meta.get_fields()
            if (f.one_to_many or f.one_to_one) and f.auto_created and not f.concrete
        ]
        matching_rel = [rel for rel in all_rels if rel.related_model == self.through]
        try:
            through_name = matching_rel[0].get_accessor_name()
        except IndexError:
            msg = ('Models {} and {} have no `through` relation with each '
                   'other.'.format(self.model, self.target_model))
            raise BadRelation(msg)
        through_model = getattr(self.model, through_name).related.model
        rel_fs = [f for f in through_model._meta.get_fields() if f.related_model]

        try:
            thru_f = [f for f in rel_fs if f.related_model == self.target_model][0]
        except IndexError:
            msg = ('Field for relation from model {} to {} not found.'
                   ''.format(through_model, self.target_model))
            raise BadRelation(msg)

        return [Relation(self.model, through_name),
                Relation(through_model, thru_f.name)]

    def arrange_models(self, models=None):
        models = models or []
        goes_first = self.target_model if self.is_direct else self.model
        goes_second = self.model if self.is_direct else self.target_model
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
            if self.is_multi:
                subset = subset.all()
            else:
                subset = [] if subset is None else [subset]               
            all_related_objs += subset
        return list(set(all_related_objs))


class RelationBranch(tuple):
    """
    Work with a chain of models connected via relationships.
    """

    def __new__(cls, root, relations):
        return super(RelationBranch, cls).__new__(cls, tuple(relations))

    def __init__(self, root, relations):
        self.root = root
        self.root_name = root._meta.model_name
        self.fieldnames = [r.fieldname for r in relations]
        self._root_label = '{}.{}'.format(root._meta.app_label,
                                          root._meta.object_name)

    def __repr__(self):
        return '<RelationBranch from {} {}'.format(
            self._root_label, super(RelationBranch, self).__repr__())

    def __hash__(self):
        key = tuple([self._root_label] + list(self))
        return hash(key)

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
            if (relation.is_direct and not relation.is_m2m) and all_fks_so_far:
                selects.append(relation.fieldname)
            else:
                all_fks_so_far = False
                prefetches.append(relation.fieldname)
        select_str = '__'.join(selects)
        prefetch_str = '__'.join(selects + prefetches) if prefetches else ''
        return select_str, prefetch_str


class RelationTree(tuple):
    """
    Work with the sets of relations that branch from a Django model.
    """

    def __new__(cls, root, branches):
        return super(RelationTree, cls).__new__(cls, tuple(branches))

    def __init__(self, root, branches):
        self.root = root
        self._root_label = '{}.{}'.format(root._meta.app_label,
                                          root._meta.object_name)

    def __repr__(self):
        return '<RelationTree from {} {}'.format(
            self._root_label, super(RelationTree, self).__repr__())

    def __hash__(self):
        key = tuple([self._root_label] + list(self))
        return hash(key)

    def prepare_qset(self, qset):
        for branch in self:
            qset = branch.prepare_qset(qset)
        return qset

    def pick(self, into=None, qset=None):
        bucket = into or Bucket()
        qset = self.prepare_qset(qset or self.root.objects.all())
        bucket.put(qset)
        for branch in self:
            objset = qset
            for relation in branch:
                compartments = relation.arrange_models(bucket.compartments)
                bucket.update_compartments(compartments)
                objset = relation.fetch_target_model_objects(objset)
                bucket.put(objset)
        return bucket


# Factory and utility functions

def make_relation_chain_from_fieldnames(root, fieldnames):
    """
    Produce a list of Relation objs from a root model and field list.

    Use this to generate the Relation objects needed for a
    RelationBranch from a list of fieldname strings. Any many-to-many
    relationships are automatically converted to relations using the
    appropriate `through` model.
    """
    relations, model = [], root
    for i, fieldname in enumerate(fieldnames):
        relation = Relation(model, fieldname)
        if relation.is_m2m:
            relations += relation.get_as_through_relations()
        else:
            relations += [relation]
        model = relation.target_model
    return relations


def trace_branches(model, orig_model=None, brfields=None, cache=None):
    """
    Produce a list of all branches stemming from a given root model.

    Use this to generate a full set of branches for creating a
    RelationTree object. Recursively follows all *direct* relationships
    from the given model. Does not follow indirect relationships,
    unless the indirect relationship is part of a `through` model from
    a direct many-to-many relationship.
    """
    tracing, orig_model = [], orig_model or model
    meta = model._meta
    relfields = [f for f in meta.fields if f.rel] + [f for f in meta.many_to_many]

    for field in relfields:
        cache = cache or []
        field_id = '{}.{}'.format(meta.model_name, field.name)
        if field_id not in cache:
            next_model = field.rel.to
            branchcache = cache + [field_id]
            next_brfields = (brfields or []) + [field.name]
            tracing += trace_branches(next_model, orig_model,
                                      next_brfields, branchcache)
    if brfields and len(relfields) == 0:
        relations = make_relation_chain_from_fieldnames(orig_model, brfields)
        tracing += [RelationBranch(orig_model, relations)]
    return tracing


def harvest(trees, into=None, tree_qsets=None):
    """
    Harvest data from a set (list/tuple) of RelationTree objects.

    Optionally pass a Bucket object you want to use to collect the
    harvested data. If none is provided, a new one is created for you.
    Also, optionally pass a dict with any particular QuerySets you want
    to use for filtering the data from a particular tree (use the tree
    object as the key and the qset as the value).
    """
    tree_qsets = tree_qsets or {}
    bucket = into or Bucket()
    for tree in trees:
        bucket = tree.pick(into=bucket, qset=tree_qsets.get(tree, None))
    return bucket
