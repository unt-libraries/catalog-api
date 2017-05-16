"""
Tests the relationtrees module used in custom sierra management commands.
"""

import pytest

from testmodels import models
from sierra.management import relationtrees

# FIXTURES AND TEST DATA

pytestmark = pytest.mark.django_db


@pytest.fixture(scope='module')
def make_object():
    def do_it(class_, args):
        try:
            return class_(*args)
        except Exception:
            return None
    return do_it


@pytest.fixture
def model_instances():
    def make_model_instances(modelname, inst_names):
        m = getattr(models, modelname)
        return m.objects.filter(name__in=inst_names).order_by('name')
    return make_model_instances


@pytest.fixture
def param_model_instances(model_instances, request):
    modelname, instance_names = request.param
    return model_instances(modelname, instance_names)


@pytest.fixture(scope='module')
def relation_params():
    return {
        'Invalid model': ('invalid', 'fieldname'),
        'Invalid fieldname': (models.EndNode, 'invalid'),
        'Fieldname is not a relation': (models.EndNode, 'name'),
        'ReferenceNode to EndNode': (models.ReferenceNode, 'end'),
        'ReferenceNode to ThroughNode': (models.ReferenceNode,
                                         'throughnode_set'),
        'ReferenceNode to ManyToManyNode': (models.ReferenceNode, 'm2m'),
        'ReferenceNode to SelfReferentialNode': (models.ReferenceNode, 'srn'),
        'ThroughNode to ReferenceNode': (models.ThroughNode, 'ref'),
        'ThroughNode to ManyToManyNode': (models.ThroughNode, 'm2m'),
        'ManyToManyNode to ThroughNode': (models.ManyToManyNode,
                                          'throughnode_set'),
        'ManyToManyNode to EndNode': (models.ManyToManyNode, 'end'),
        'ManyToManyNode to ReferenceNode': (models.ManyToManyNode,
                                            'referencenode_set'),
        'EndNode to ReferenceNode': (models.EndNode, 'referencenode_set'),
        'EndNode to ManyToManyNode': (models.EndNode, 'manytomanynode_set'),
        'EndNode to SelfReferentialNode': (models.EndNode,
                                           'selfreferentialnode_set'),
        'SelfReferentialNode to SelfReferentialNode':
            (models.SelfReferentialNode, 'parent'),
        'SelfReferentialNode to ReferenceNode': (models.SelfReferentialNode,
                                                 'referencenode_set'),
        'SelfReferentialNode to EndNode': (models.SelfReferentialNode, 'end'),
    }


@pytest.fixture(params=[key for key in relation_params().keys()])
def relation(make_object, relation_params, request):
    return make_object(relationtrees.Relation, relation_params[request.param])


@pytest.fixture(scope='module')
def branch_params():
    return {
        'No valid fieldnames':
            (models.ReferenceNode, ['invalid']),
        'Valid and invalid fieldnames':
            (models.ReferenceNode, ['end', 'invalid']),
        'Invalid relationship':
            (models.ReferenceNode, ['m2m', 'srn']),
        'ReferenceNode > end':
            (models.ReferenceNode, ['end']),
        'ReferenceNode > m2m, end':
            (models.ReferenceNode, ['m2m', 'end']),
        'ReferenceNode > throughnode_set, m2m, end':
            (models.ReferenceNode, ['throughnode_set', 'm2m', 'end']),
        'ReferenceNode > srn, parent, end':
            (models.ReferenceNode, ['srn', 'parent', 'end']),
        'ThroughNode > m2m, referencenode_set, end':
            (models.ThroughNode, ['m2m', 'referencenode_set', 'end']),
        'SelfReferentialNode > referencenode_set, m2m, end':
            (models.SelfReferentialNode, ['referencenode_set', 'm2m', 'end']),
        'ThroughNode > ref, m2m, end':
            (models.ThroughNode, ['ref', 'm2m', 'end']),
        'EndNode > m2m, ref, end':
            (models.EndNode, ['manytomanynode_set', 'referencenode_set',
                              'end'])
    }


@pytest.fixture(params=[key for key in branch_params().keys()])
def branch(make_object, branch_params, request):
    return make_object(relationtrees.RelationBranch,
                       branch_params[request.param])


@pytest.fixture(scope='module')
def badtree_params():
    return {
        'One invalid branch (1 node)':
            (models.ReferenceNode, [['invalid']], False),
        'One invalid branch (2 nodes)':
            (models.ReferenceNode, [['end', 'invalid']], False),
        'First branch valid, second branch invalid':
            (models.ReferenceNode, [['end'], ['invalid']], False),
        'First branch invalid, second branch valid':
            (models.ReferenceNode, [['invalid'], ['end']], False),
        'Both branches invalid':
            (models.ReferenceNode, [['end', 'invalid'], ['invalid']], False),
    }


@pytest.fixture(scope='module')
def tree_params():
    return {
        'No user_branches, trace_branches is False (EndNode)':
            (models.EndNode, None, False),
        'No user_branches, trace_branches is True (ReferenceNode)':
            (models.ReferenceNode, None, True),
        'Has user_branches, trace_branches is False (SelfReferentialNode)':
            (models.SelfReferentialNode,
                [
                    ['referencenode_set', 'end'],
                    ['referencenode_set', 'srn', 'end'],
                    ['referencenode_set', 'srn', 'parent', 'end']
                ], False),
        'Has user_branches, trace_branches is True (ReferenceNode)':
            (models.ReferenceNode,
                [
                    ['srn', 'parent', 'end'],
                    ['throughnode_set', 'm2m', 'end']
                ], True)
    }


@pytest.fixture(params=[key for key in badtree_params().keys()])
def badtree(make_object, badtree_params, request):
    return make_object(relationtrees.RelationTree,
                       badtree_params[request.param])


@pytest.fixture(params=[key for key in tree_params().keys()])
def tree(make_object, tree_params, request):
    return make_object(relationtrees.RelationTree, tree_params[request.param])


# TESTS

@pytest.mark.relation
@pytest.mark.parametrize('pkey', [
    'Invalid model',
    'Invalid fieldname',
    'Fieldname is not a relation'
])
def test_relation_init_raises_error_on_invalid_data(relation_params, pkey):
    """
    Relation.__init__ should raise a BadRelation error if the provided
    model/fieldname combo is not valid.
    """
    with pytest.raises(relationtrees.BadRelation):
        relationtrees.Relation(*relation_params[pkey])


@pytest.mark.relation
@pytest.mark.parametrize('relation, fk, m2m, indirect', [
    ('ReferenceNode to EndNode', True, False, False),
    ('ReferenceNode to ManyToManyNode', False, True, False),
    ('EndNode to ReferenceNode', False, False, True),
    ('ManyToManyNode to ReferenceNode', False, False, True)
], ids=[
    'fk',
    'direct m2m',
    'indirect 1-many',
    'indirect m2m',
], indirect=['relation'])
def test_relation_ismethods_return_right_bools(relation, fk, m2m, indirect):
    """
    All "is" methods on Relation objects should return the correct
    truth values for the type of relation that is represented.
    """
    assert (relation.is_foreign_key() == fk and
            relation.is_many_to_many() == m2m and
            relation.is_indirect() == indirect)


@pytest.mark.relation
@pytest.mark.parametrize('relation, target', [
    ('ReferenceNode to SelfReferentialNode', models.SelfReferentialNode),
    ('ReferenceNode to ManyToManyNode', models.ManyToManyNode),
    ('EndNode to ReferenceNode', models.ReferenceNode),
    ('ManyToManyNode to ReferenceNode', models.ReferenceNode)
], ids=[
    'fk',
    'direct m2m',
    'indirect 1-many',
    'indirect m2m',
], indirect=['relation'])
def test_relation_targetmodel_has_right_model(relation, target):
    """
    Relation.target_model should contain whatever model is on the other
    end of the relation relative to Relation.model.
    """
    assert relation.target_model == target


@pytest.mark.relation
@pytest.mark.parametrize('relation, models, result', [
    ('ReferenceNode to SelfReferentialNode', None,
     [models.SelfReferentialNode, models.ReferenceNode]),
    ('ReferenceNode to SelfReferentialNode', [],
     [models.SelfReferentialNode, models.ReferenceNode]),
    ('ReferenceNode to SelfReferentialNode',
     [models.SelfReferentialNode],
     [models.SelfReferentialNode, models.ReferenceNode]),
    ('ReferenceNode to SelfReferentialNode',
     [models.ReferenceNode],
     [models.SelfReferentialNode, models.ReferenceNode]),
    ('ReferenceNode to SelfReferentialNode',
     [models.ReferenceNode, models.SelfReferentialNode],
     [models.SelfReferentialNode, models.ReferenceNode]),
    ('ReferenceNode to SelfReferentialNode',
     [models.SelfReferentialNode, models.ReferenceNode],
     [models.SelfReferentialNode, models.ReferenceNode]),
    ('ReferenceNode to SelfReferentialNode',
     [models.EndNode, models.ReferenceNode, models.SelfReferentialNode],
     [models.EndNode, models.SelfReferentialNode, models.ReferenceNode]),
    ('ReferenceNode to SelfReferentialNode',
     [models.ReferenceNode, models.EndNode, models.SelfReferentialNode],
     [models.SelfReferentialNode, models.ReferenceNode, models.EndNode]),
    ('ReferenceNode to SelfReferentialNode',
     [models.ReferenceNode, models.SelfReferentialNode, models.EndNode],
     [models.SelfReferentialNode, models.ReferenceNode, models.EndNode]),
], ids=[
    'models list is None',
    'models list is empty',
    'first model is in models list and second is not',
    'second model is in models list and first is not',
    'both models (only) are in models list, out of order',
    'both models (only) are in models list, in order',
    'multiple models, non-relevant model is before first and second',
    'multiple models, non-relevant model is between first and second',
    'multiple models, non-relevant model is after first and second'
],
indirect=['relation'])
def test_relation_arrangemodels_order(relation, models, result):
    """
    Relation.arrange_models should return models in dependency order,
    optionally utilizing a supplied "models" list.
    """
    assert relation.arrange_models(models) == result


@pytest.mark.relation
@pytest.mark.parametrize('relation, source, result', [
    ('ReferenceNode to EndNode', ['ReferenceNode', []], ['EndNode', []]),
    ('EndNode to ReferenceNode', ['EndNode', ['end1']], ['ReferenceNode', []]),
    ('ReferenceNode to EndNode', ['ReferenceNode', ['ref0']],
     ['EndNode', ['end0']]),
    ('ReferenceNode to EndNode', ['ReferenceNode', ['ref0', 'ref1']],
     ['EndNode', ['end0', 'end2']]),
    ('EndNode to ReferenceNode', ['EndNode', ['end0']],
     ['ReferenceNode', ['ref0']]),
    ('EndNode to ReferenceNode', ['EndNode', ['end0', 'end2']],
     ['ReferenceNode', ['ref0', 'ref1', 'ref2']]),
    ('ReferenceNode to ManyToManyNode', ['ReferenceNode', ['ref0']],
     ['ManyToManyNode', ['m2m0', 'm2m1']]),
    ('ReferenceNode to ManyToManyNode', ['ReferenceNode', ['ref0', 'ref1']],
     ['ManyToManyNode', ['m2m0', 'm2m1', 'm2m2']]),
], ids=[
    'zero source objects returns empty list',
    'zero target objects returns empty list',
    'fk relation, one source object returns target',
    'fk relation, multiple source objects returns all targets',
    'indirect relation, one source object returns all targets',
    'indirect relation, multiple source objects returns all targets',
    'm2m relation, one source object returns all targets',
    'm2m relation, multiple source objects returns all targets (unique)',
], indirect=['relation'])
def test_relation_fetchtargetmodelobjects_results(relation, source, result,
                                                  model_instances):
    """
    Relation.fetch_target_model_objects should return all of the
    expected objects (based on the source object(s) and relation's
    target_model).
    """
    source_objs = list(model_instances(*source))
    expected_objs = list(model_instances(*result))
    actual_objs = relation.fetch_target_model_objects(source_objs)
    assert sorted(actual_objs, key=lambda x: x.name) == expected_objs


@pytest.mark.branch
@pytest.mark.parametrize('branch, fieldnames', [
    ('ReferenceNode > end', ['end']),
    ('ReferenceNode > m2m, end', ['m2m', 'end']),
    ('ReferenceNode > throughnode_set, m2m, end',
     ['throughnode_set', 'm2m', 'end']),
    ('ReferenceNode > srn, parent, end', ['srn', 'parent', 'end']),
    ('EndNode > m2m, ref, end',
     ['manytomanynode_set', 'referencenode_set', 'end'])
], ids=[
    'root -> foreign-key',
    'root -> many-to-many (direct) -> foreign-key',
    'root -> indirect (one-to-many) -> foreign-key -> foreign-key',
    'root -> foreign-key -> foreign-key (self-referential) -> foreign-key',
    'root -> indirect (one-to-many) -> indirect (many-to-many) -> foreign-key'
], indirect=['branch'])
def test_relationbranch_init_creates_correct_relations(branch, fieldnames):
    """
    Initializing a RelationBranch object should result in a tuple
    containing Relation objects equivalent to the branch field names
    passed to __init__.
    """
    assert all([isinstance(rel, relationtrees.Relation) for rel in branch])
    assert [rel.fieldname for rel in branch] == fieldnames


@pytest.mark.branch
@pytest.mark.parametrize('branch, exp_select, exp_prefetch', [
    ('ReferenceNode > end', 'end', ''),
    ('ReferenceNode > srn, parent, end', 'srn__parent__end', ''),
    ('ReferenceNode > m2m, end', '', 'm2m__end'),
    ('ReferenceNode > throughnode_set, m2m, end', '',
     'throughnode_set__m2m__end'),
    ('ThroughNode > m2m, referencenode_set, end', 'm2m',
     'm2m__referencenode_set__end'),
    ('ThroughNode > ref, m2m, end', 'ref', 'ref__m2m__end'),
], ids=[
    'select_related with one fk',
    'select_related with multiple fks',
    'prefetch_related starts with first m2m relation',
    'fks after 1st indirect create prefetch_related',
    'fks up to 1st indirect create selected_related, then prefetch after that',
    'fks up to 1st m2m create selected_related, then prefetch after that'
], indirect=['branch'])
def test_relationbranch_prepareqset_precaches_correctly(branch, exp_select,
                                                        exp_prefetch):
    """
    RelationBranch.add_selects_and_prefetches_to_qset should call
    the select_related and prefetch_related methods of QuerySet
    appropriately, based on what object relations are in the branch.
    """
    qset = branch.root_model.objects.all()
    selects, prefetches = branch._get_selects_and_prefetches_for_qset()
    qset = branch.prepare_qset(qset)
    assert selects == exp_select
    assert prefetches == exp_prefetch
    assert len(qset) > 0


#@pytest.mark.branch
@pytest.mark.parametrize('branch, models, result', [], indirect=True)
def test_relationbranch_arrangemodels_order(branch, models, result):
    """
    RelationBranch.arrange_models should return models in dependency order,
    optionally utilizing a supplied "models" list.
    """
    pass


#@pytest.mark.branch
@pytest.mark.parametrize('branch, result', [], indirect=True)
def test_relationbranch_fetchobjects_results(branch, result):
    """
    RelationBranch.fetch_objects should return all of the expected
    objects in an objects dictionary.
    """
    pass


#@pytest.mark.tree
@pytest.mark.parametrize('badtree, numerrors', [
    ('One invalid branch (1 node)', 1),
    ('One invalid branch (2 nodes)', 1),
    ('First branch valid, second branch invalid', 1),
    ('First branch invalid, second branch valid', 1),
    ('Both branches invalid', 2)
], indirect=True)
def test_relationtree_init_produces_errors_on_bad_branches(badtree, numerrors):
    """
    RelationTree.__init__ should record one or more errors in
    RelationTree.config_errors if one or more invalid branches are
    passed.
    """
    assert len(badtree.config_errors) == numerrors


#@pytest.mark.tree
def test_relationtree_init_returns_valid_tree(tree):
    """
    RelationTree.__init___ should return a valid tree object, with
    root_model, qset, and branches attributes populated as appropriate.
    """
    assert len(tree.config_errors) == 0
    assert tree.branches is not None
    assert tree.root_model
    assert len(tree.qset)


#@pytest.mark.tree
@pytest.mark.parametrize('model, exp', [
    (models.ReferenceNode, [['end'], ['srn', 'end'], ['srn', 'parent', 'end'],
                            ['m2m', 'end']]),
    (models.ThroughNode, [['ref', 'end'], ['ref', 'srn', 'end'],
                          ['ref', 'srn', 'parent', 'end'],
                          ['ref', 'm2m', 'end'], ['m2m', 'end']]),
    (models.EndNode, []),
    (models.SelfReferentialNode, [['end'], ['parent', 'end']]),
    (models.ManyToManyNode, [['end']])
])
def test_relationtree_tracebranches_returns_correct_paths(model, exp):
    """
    trace_relations should correctly follow direct many-to-many
    and foreign-key relationships on the given model, resulting in the
    expected list of paths (exp).
    """
    result = [p.fieldnames for p in relationtrees.trace_relations(model)]
    for expected_path in exp:
        assert expected_path in result
    assert len(result) == len(exp)


