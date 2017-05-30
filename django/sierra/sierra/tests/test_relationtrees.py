"""
Tests the relationtrees module used in custom sierra management commands.
"""

import pytest

from testmodels import models as m
from sierra.management import relationtrees

# FIXTURES AND TEST DATA

pytestmark = pytest.mark.django_db


@pytest.fixture
def instname_to_model():
    def do_it(inst_name):
        nameprefix_to_model = {
            'ref': m.ReferenceNode,
            'srn': m.SelfReferentialNode,
            'end': m.EndNode,
            'm2m': m.ManyToManyNode,
            'thr': m.ThroughNode
        }
        return nameprefix_to_model[inst_name[:3]]
    return do_it


@pytest.fixture
def get_model_instances(instname_to_model):
    def do_it(inst_names):
        if not isinstance(inst_names, (list, tuple)):
            inst_names = [inst_names]
        try:
            model = instname_to_model(inst_names[0])
        except IndexError:
            return []
        return model.objects.filter(name__in=inst_names).order_by('name')
    return do_it


@pytest.fixture
def assert_all_objset_calls():
    def do_it(mock, exp_objsets):
        calls = mock.call_args_list
        actual_objsets = []
        for call in calls:
            for arg in (list(call[0]) + call[1].values()):
                try:
                    arg[0]._meta
                except Exception:
                    pass
                else:
                    actual_objsets += [sorted([obj for obj in arg],
                                              key=lambda x: x.name)]
        for exp_objset in exp_objsets:
            expected = [obj for obj in exp_objset]
            assert expected in actual_objsets
            actual_objsets.remove(expected)
        assert actual_objsets == []
    return do_it


@pytest.fixture(scope='module')
def bad_relation_params():
    return {
        'Invalid model': ('invalid', 'fieldname'),
        'Invalid fieldname': (m.EndNode, 'invalid'),
        'Fieldname is not a relation': (m.EndNode, 'name'),
    }


@pytest.fixture(scope='module')
def relation_params():
    return {
        'ReferenceNode to EndNode': (m.ReferenceNode, 'end'),
        'ReferenceNode to ThroughNode': (m.ReferenceNode, 'throughnode_set'),
        'ReferenceNode to ManyToManyNode': (m.ReferenceNode, 'm2m'),
        'ReferenceNode to SelfReferentialNode': (m.ReferenceNode, 'srn'),
        'ThroughNode to ReferenceNode': (m.ThroughNode, 'ref'),
        'ThroughNode to ManyToManyNode': (m.ThroughNode, 'm2m'),
        'ManyToManyNode to ThroughNode': (m.ManyToManyNode, 'throughnode_set'),
        'ManyToManyNode to EndNode': (m.ManyToManyNode, 'end'),
        'ManyToManyNode to ReferenceNode':
            (m.ManyToManyNode, 'referencenode_set'),
        'EndNode to ReferenceNode': (m.EndNode, 'referencenode_set'),
        'EndNode to ManyToManyNode': (m.EndNode, 'manytomanynode_set'),
        'EndNode to SelfReferentialNode':
            (m.EndNode, 'selfreferentialnode_set'),
        'SelfReferentialNode to SelfReferentialNode':
            (m.SelfReferentialNode, 'parent'),
        'SelfReferentialNode to ReferenceNode':
            (m.SelfReferentialNode, 'referencenode_set'),
        'SelfReferentialNode to EndNode': (m.SelfReferentialNode, 'end'),
    }


@pytest.fixture(params=[key for key in bad_relation_params().keys()])
def make_bad_relation(bad_relation_params, request):
    def do_it():
        relationtrees.Relation(*bad_relation_params[request.param])
    return do_it


@pytest.fixture(params=[key for key in relation_params().keys()])
def relation(relation_params, request):
    return relationtrees.Relation(*relation_params[request.param])


@pytest.fixture(scope='module')
def bad_branch_params():
    return {
        'No valid fieldnames': (m.ReferenceNode, ['invalid']),
        'Valid and invalid fieldnames': (m.ReferenceNode, ['end', 'invalid']),
        'Invalid relationship': (m.ReferenceNode, ['m2m', 'srn'])
    }


@pytest.fixture(scope='module')
def branch_params():
    return {
        'ReferenceNode > end': (m.ReferenceNode, ['end']),
        'ReferenceNode > m2m, end': (m.ReferenceNode, ['m2m', 'end']),
        'ReferenceNode > throughnode_set, m2m, end':
            (m.ReferenceNode, ['throughnode_set', 'm2m', 'end']),
        'ReferenceNode > srn, parent, end':
            (m.ReferenceNode, ['srn', 'parent', 'end']),
        'ThroughNode > m2m, referencenode_set, end':
            (m.ThroughNode, ['m2m', 'referencenode_set', 'end']),
        'SelfReferentialNode > referencenode_set, m2m, end':
            (m.SelfReferentialNode, ['referencenode_set', 'm2m', 'end']),
        'ThroughNode > ref, m2m, end': (m.ThroughNode, ['ref', 'm2m', 'end']),
        'EndNode > m2m, ref, end':
            (m.EndNode, ['manytomanynode_set', 'referencenode_set', 'end'])
    }


@pytest.fixture(params=[key for key in bad_branch_params().keys()])
def make_bad_branch(bad_branch_params, request):
    def do_it():
        relationtrees.RelationBranch(*bad_branch_params[request.param])
    return do_it


@pytest.fixture(params=[key for key in branch_params().keys()])
def branch(branch_params, request):
    return relationtrees.RelationBranch(*branch_params[request.param])


@pytest.fixture(scope='module')
def badtree_params():
    return {
        'One invalid branch (1 node)':
            (m.ReferenceNode, [['invalid']], False),
        'One invalid branch (2 nodes)':
            (m.ReferenceNode, [['end', 'invalid']], False),
        'First branch valid, second branch invalid':
            (m.ReferenceNode, [['end'], ['invalid']], False),
        'First branch invalid, second branch valid':
            (m.ReferenceNode, [['invalid'], ['end']], False),
        'Both branches invalid':
            (m.ReferenceNode, [['end', 'invalid'], ['invalid']], False),
    }


@pytest.fixture(scope='module')
def tree_params():
    return {
        'No user_branches, trace_branches is False (EndNode)':
            (m.EndNode, None, False),
        'No user_branches, trace_branches is True (ReferenceNode)':
            (m.ReferenceNode, None, True),
        'Has user_branches, trace_branches is False (SelfReferentialNode)':
            (m.SelfReferentialNode,
                [
                    ['referencenode_set', 'end'],
                    ['referencenode_set', 'srn', 'end'],
                    ['referencenode_set', 'srn', 'parent', 'end']
                ], False),
        'Has user_branches, trace_branches is True (ReferenceNode)':
            (m.ReferenceNode,
                [
                    ['srn', 'parent', 'end'],
                    ['throughnode_set', 'm2m', 'end']
                ], True)
    }


@pytest.fixture(params=[key for key in badtree_params().keys()])
def make_badtree(badtree_params, request):
    def do_it():
        relationtrees.RelationTree(*badtree_params[request.param])
    return do_it


@pytest.fixture(params=[key for key in tree_params().keys()])
def tree(tree_params, request):
    return relationtrees.RelationTree(*tree_params[request.param])


@pytest.fixture
def all_trees(tree_params):
    return {k: relationtrees.RelationTree(*v)
            for k, v in tree_params.iteritems()}


# TESTS

@pytest.mark.bucket
@pytest.mark.parametrize('oldcmps, newcmps', [
    ([], [m.ReferenceNode, m.EndNode]),
    ([m.ReferenceNode], [m.ReferenceNode, m.EndNode]),
    ([m.ReferenceNode, m.EndNode], [m.ReferenceNode, m.EndNode]),
    ([m.EndNode, m.ReferenceNode], [m.ReferenceNode, m.EndNode]),
    ([m.ReferenceNode, m.SelfReferentialNode], [m.ReferenceNode, m.EndNode])
], ids=[
    'empty bucket',
    'one existing compartment, in new compartments',
    'old compartments == new compartments',
    'old compartments in different order than new compartments',
    'one old compartment missing from new compartments'
])
def test_bucket_updatecompartments_updates_correctly(oldcmps, newcmps):
    """
    Bucket.update_compartments should correctly update the
    `compartments` attribute of the bucket AND should ensure dict
    elements for the new compartments exist on the bucket.
    """
    bucket = relationtrees.Bucket(oldcmps)
    bucket.update_compartments(newcmps)
    assert bucket.compartments == newcmps
    for newcmp in newcmps:
        assert newcmp in bucket


@pytest.mark.bucket
def test_bucket_updatecompartments_doesnt_change_data(get_model_instances):
    """
    Bucket.update_compartments should not change any existing data.
    """
    bucket = relationtrees.Bucket([m.ReferenceNode])
    test_instance = get_model_instances('ref0')[0]
    bucket.put(test_instance)
    bucket.update_compartments([m.EndNode, m.ReferenceNode])
    assert len(bucket[m.ReferenceNode]) == 1
    assert bucket[m.ReferenceNode][test_instance.pk] == test_instance


@pytest.mark.bucket
@pytest.mark.parametrize('oldcmps, objlists, newcmps', [
    ([m.ReferenceNode], [['ref0']], [m.ReferenceNode]),
    ([m.ReferenceNode], [['ref0', 'ref2']], [m.ReferenceNode]),
    ([m.ReferenceNode, m.EndNode], [['ref0', 'ref1'], ['end0']],
     [m.ReferenceNode, m.EndNode]),
    ([], [['ref0']], [m.ReferenceNode]),
    ([], [['ref0', 'ref0'], ['ref0']], [m.ReferenceNode]),

], ids=[
    'single object',
    'multiple objects, same type',
    'multiple objects, different types',
    'object without an existing compartment',
    'duplicate objects'
])
def test_bucket_put_puts_objs_into_compartments(oldcmps, objlists, newcmps,
                                                get_model_instances,
                                                instname_to_model):
    """
    Bucket.put should put objects into the correct compartments using
    the correct keys; objects should be deduplicated, and compartments
    should be added if they don't already exist.
    """
    bucket = relationtrees.Bucket(oldcmps)
    for objlist in objlists:
        bucket.put(get_model_instances(objlist))
    assert bucket.compartments == newcmps
    for exp_names in objlists:
        model = instname_to_model(exp_names[0])
        unique_expnames = list(set(exp_names))
        actual_names = [obj.name for obj in bucket[model].values()]
        assert len(bucket[model]) == len(unique_expnames)
        assert all([name in actual_names for name in unique_expnames])


@pytest.mark.bucket
def test_bucket_dump_returns_objs_in_compartment_order(get_model_instances):
    """
    Bucket.dump should return a list of objects that have been "put"
    into the bucket in compartment and then PK order.
    """
    bucket = relationtrees.Bucket([m.EndNode, m.ReferenceNode])
    bucket.put(get_model_instances(['ref2', 'ref0']))
    bucket.put(get_model_instances(['end0', 'end1', 'end2']))
    exp = (list(get_model_instances(['end0', 'end1', 'end2'])) +
           list(get_model_instances(['ref0', 'ref2'])))
    assert bucket.dump() == exp


@pytest.mark.relation
def test_relation_init_raises_error_on_invalid_data(make_bad_relation):
    """
    Relation.__init__ should raise a BadRelation error if the provided
    model/fieldname combo is not valid.
    """
    with pytest.raises(relationtrees.BadRelation):
        make_bad_relation()


@pytest.mark.relation
@pytest.mark.parametrize('relation, m2m, multi, direct', [
    ('ReferenceNode to EndNode', False, False, True),
    ('ReferenceNode to ManyToManyNode', True, True, True),
    ('EndNode to ReferenceNode', False, True, False),
    ('ManyToManyNode to ReferenceNode', True, True, False)
], ids=[
    'direct foreign-key',
    'direct m2m',
    'indirect 1-many',
    'indirect m2m',
], indirect=['relation'])
def test_relation_isattrs_return_right_bools(relation, m2m, multi, direct):
    """
    All "is" attributes on Relation objects should return the correct
    truth values for the type of relation that is represented.
    """
    assert relation.is_m2m == m2m
    assert relation.is_multi == multi
    assert relation.is_direct == direct


@pytest.mark.relation
@pytest.mark.parametrize('relation, target', [
    ('ReferenceNode to SelfReferentialNode', m.SelfReferentialNode),
    ('ReferenceNode to ManyToManyNode', m.ManyToManyNode),
    ('EndNode to ReferenceNode', m.ReferenceNode),
    ('ManyToManyNode to ReferenceNode', m.ReferenceNode)
], ids=[
    'direct foreign-key',
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
@pytest.mark.parametrize('relation, exp', [
    ('ReferenceNode to ManyToManyNode',
     ['ReferenceNode to ThroughNode', 'ThroughNode to ManyToManyNode']),
    ('ManyToManyNode to ReferenceNode',
     ['ManyToManyNode to ThroughNode', 'ThroughNode to ReferenceNode'])
], ids=[
    'direct m2m',
    'indirect m2m'
], indirect=['relation'])
def test_relation_getasthroughrelations_m2m_returns_expected(relation, exp,
                                                             relation_params):
    """
    Relation.get_as_through_relations should return the expected list
    of Relation objects, if the relationship is many-to-many.
    """
    exp_objs = [relationtrees.Relation(*relation_params[exp[0]]),
                relationtrees.Relation(*relation_params[exp[1]])]
    result = relation.get_as_through_relations()
    exp_params = [[r.model, r.fieldname, r.target_model] for r in exp_objs]
    res_params = [[r.model, r.fieldname, r.target_model] for r in result]
    assert res_params == exp_params


@pytest.mark.relation
@pytest.mark.parametrize('relation', [
    'ReferenceNode to SelfReferentialNode',
    'EndNode to ReferenceNode'
], ids=[
    'direct foreign-key',
    'indirect 1-many'
], indirect=['relation'])
def test_relation_getasthroughrelations_not_m2m_returns_error(relation):
    """
    Relation.get_as_through_relations should raise a BadRelation error
    if the relation is not an m2m relation.
    """
    with pytest.raises(relationtrees.BadRelation):
        relation.get_as_through_relations()


@pytest.mark.relation
@pytest.mark.parametrize('relation, models, result', [
    ('ReferenceNode to SelfReferentialNode', None,
     [m.SelfReferentialNode, m.ReferenceNode]),
    ('SelfReferentialNode to ReferenceNode', None,
     [m.SelfReferentialNode, m.ReferenceNode]),
    ('ReferenceNode to ManyToManyNode', None,
     [m.ManyToManyNode, m.ReferenceNode]),
    ('ManyToManyNode to ReferenceNode', None,
     [m.ManyToManyNode, m.ReferenceNode]),
    ('ReferenceNode to SelfReferentialNode', [],
     [m.SelfReferentialNode, m.ReferenceNode]),
    ('ReferenceNode to SelfReferentialNode',
     [m.SelfReferentialNode],
     [m.SelfReferentialNode, m.ReferenceNode]),
    ('ReferenceNode to SelfReferentialNode',
     [m.ReferenceNode],
     [m.SelfReferentialNode, m.ReferenceNode]),
    ('ReferenceNode to SelfReferentialNode',
     [m.ReferenceNode, m.SelfReferentialNode],
     [m.SelfReferentialNode, m.ReferenceNode]),
    ('ReferenceNode to SelfReferentialNode',
     [m.SelfReferentialNode, m.ReferenceNode],
     [m.SelfReferentialNode, m.ReferenceNode]),
    ('ReferenceNode to SelfReferentialNode',
     [m.EndNode, m.ReferenceNode, m.SelfReferentialNode],
     [m.EndNode, m.SelfReferentialNode, m.ReferenceNode]),
    ('ReferenceNode to SelfReferentialNode',
     [m.ReferenceNode, m.EndNode, m.SelfReferentialNode],
     [m.SelfReferentialNode, m.ReferenceNode, m.EndNode]),
    ('ReferenceNode to SelfReferentialNode',
     [m.ReferenceNode, m.SelfReferentialNode, m.EndNode],
     [m.SelfReferentialNode, m.ReferenceNode, m.EndNode]),
], ids=[
    'direct fk, 1-many; second (many) should be first',
    'indirect many-1; first (many) should be first',
    'direct m2m; second should be first',
    'indirect m2m; first should be first',
    'models list is empty',
    'first model is in models list and second is not',
    'second model is in models list and first is not',
    'both models (only) are in models list, out of order',
    'both models (only) are in models list, in order',
    'multiple models, non-relevant model is before first and second',
    'multiple models, non-relevant model is between first and second',
    'multiple models, non-relevant model is after first and second',
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
    ('ReferenceNode to EndNode', [], []),
    ('EndNode to ReferenceNode', ['end1'], []),
    ('ReferenceNode to EndNode', ['ref0'], ['end0']),
    ('ReferenceNode to EndNode', ['ref0', 'ref1'], ['end0', 'end2']),
    ('EndNode to ReferenceNode', ['end0'], ['ref0']),
    ('EndNode to ReferenceNode', ['end0', 'end2'], ['ref0', 'ref1', 'ref2']),
    ('ReferenceNode to ManyToManyNode', ['ref0'], ['m2m0', 'm2m1']),
    ('ReferenceNode to ManyToManyNode', ['ref0', 'ref1'],
     ['m2m0', 'm2m1', 'm2m2']),
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
                                                  get_model_instances):
    """
    Relation.fetch_target_model_objects should return all of the
    expected objects (based on the source object(s) and relation's
    target_model).
    """
    source_objs = list(get_model_instances(source))
    expected_objs = list(get_model_instances(result))
    actual_objs = relation.fetch_target_model_objects(source_objs)
    assert sorted(actual_objs, key=lambda x: x.name) == expected_objs


@pytest.mark.branch
def test_relationbranch_init_raises_error_on_invalid_data(make_bad_branch):
    """
    RelationBranch.__init__ should raise a BadBranch error if any of
    the provided relationships are invalid.
    """
    with pytest.raises(relationtrees.BadBranch):
        make_bad_branch()


@pytest.mark.branch
@pytest.mark.parametrize('branch, fieldnames', [
    ('ReferenceNode > end', ['end']),
    ('ReferenceNode > m2m, end', ['throughnode_set', 'm2m', 'end']),
    ('ReferenceNode > throughnode_set, m2m, end',
     ['throughnode_set', 'm2m', 'end']),
    ('ReferenceNode > srn, parent, end', ['srn', 'parent', 'end']),
    ('EndNode > m2m, ref, end',
     ['manytomanynode_set', 'throughnode_set', 'ref', 'end'])
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
    ('ReferenceNode > m2m, end', '', 'throughnode_set__m2m__end'),
    ('ReferenceNode > throughnode_set, m2m, end', '',
     'throughnode_set__m2m__end'),
    ('ThroughNode > m2m, referencenode_set, end', 'm2m',
     'm2m__throughnode_set__ref__end'),
    ('ThroughNode > ref, m2m, end', 'ref', 'ref__throughnode_set__m2m__end'),
], ids=[
    'select_related with one fk',
    'select_related with multiple fks',
    'prefetch_related starts with first m2m relation',
    'fks after 1st indirect create prefetch_related',
    'fks up to 1st indirect create selected_related, then prefetch after that',
    'fks up to 1st m2m create selected_related, then prefetch after that'
], indirect=['branch'])
def test_relationbranch_prepareqset_precaches_correctly(branch, exp_select,
                                                        exp_prefetch, mocker):
    """
    RelationBranch.prepare_qset should call the select_related and
    prefetch_related methods of QuerySet appropriately, based on what
    object relations are in the branch. It should also return actual
    results.
    """
    real_qset = branch.root_model.objects.all()
    mock_qset = branch.root_model.objects.all()
    mocker.patch.object(mock_qset, 'select_related', return_value=mock_qset)
    mocker.patch.object(mock_qset, 'prefetch_related', return_value=mock_qset)
    real_qset = branch.prepare_qset(real_qset)
    mock_qset = branch.prepare_qset(mock_qset)
    if exp_select:
        mock_qset.select_related.assert_called_once_with(exp_select)
    else:
        mock_qset.select_related.assert_not_called()
    if exp_prefetch:
        mock_qset.prefetch_related.assert_called_once_with(exp_prefetch)
    else:
        mock_qset.prefetch_related.assert_not_called()
    assert len(real_qset) > 0


@pytest.mark.tree
def test_relationtree_init_produces_error_on_bad_branches(make_badtree):
    """
    RelationTree.__init__ should raise a BadTree if it encounters
    invalid branches.
    """
    with pytest.raises(relationtrees.BadTree):
        make_badtree()


@pytest.mark.tree
def test_relationtree_init_returns_valid_tree(tree):
    """
    RelationTree.__init___ should return a valid tree object, with
    root_model attribute and member branches populated as appropriate.
    """
    assert tree.root_model
    assert all([isinstance(br, relationtrees.RelationBranch) for br in tree])


@pytest.mark.tree
@pytest.mark.parametrize('model, exp', [
    (m.ReferenceNode, [['end'], ['srn', 'end'], ['srn', 'parent', 'end'],
                            ['m2m', 'end']]),
    (m.ThroughNode, [['ref', 'end'], ['ref', 'srn', 'end'],
                          ['ref', 'srn', 'parent', 'end'],
                          ['ref', 'm2m', 'end'], ['m2m', 'end']]),
    (m.EndNode, []),
    (m.SelfReferentialNode, [['end'], ['parent', 'end']]),
    (m.ManyToManyNode, [['end']])
], ids=[
    'ReferenceNode',
    'ThroughNode',
    'EndNode',
    'SelfReferentialNode',
    'ManyToManyNode'
])
def test_relationtree_tracebranches_returns_correct_branches(model, exp):
    """
    RelationTree.trace_branches should correctly follow direct
    many-to-many and foreign-key relationships on the given model,
    resulting in the expected list of branches (exp).
    """
    tree = relationtrees.RelationTree(model)
    result = [branch.fieldnames for branch in tree.trace_branches(model)]
    for expected_branch in exp:
        assert expected_branch in result
    assert len(result) == len(exp)


@pytest.mark.tree
def test_relationtree_prepareqset_calls_branches_prepareqset(mocker):
    """
    RelationTree.prepare_qset should return a Queryset object that has
    had each branch's prepare_qset called on it.
    """
    qset = m.ReferenceNode.objects.all()
    tree = relationtrees.RelationTree(m.ReferenceNode)
    for branch in tree:
        mocker.patch.object(branch, 'prepare_qset', return_value=branch)
    qset = tree.prepare_qset(qset)
    for branch in tree:
        branch.prepare_qset.assert_called_once_with(qset)


@pytest.mark.tree
@pytest.mark.parametrize('qset, exp_qset', [
    (m.ReferenceNode.objects.filter(name='ref1'),
     m.ReferenceNode.objects.filter(name='ref1')),
    (None, m.ReferenceNode.objects.all())
], ids=[
    'qset kwarg provided, should use provided qset',
    'qset kwarg not provided, should use default full `objects.all` qset'
])
def test_relationtree_pick_calls_prepareqset(qset, exp_qset, mocker,
                                             assert_all_objset_calls):
    """
    RelationTree.pick should call RelationTree.prepare_qset, using
    either the queryset passed via the qset kwarg or using the full
    queryset for the root model of that tree.
    """
    tree = relationtrees.RelationTree(m.ReferenceNode)
    mocker.patch.object(tree, 'prepare_qset')
    bucket = tree.pick(qset=qset)
    tree.prepare_qset.assert_called_once()
    assert_all_objset_calls(tree.prepare_qset, [exp_qset])


@pytest.mark.tree
@pytest.mark.parametrize('tree, exp_comps', [
    ('No user_branches, trace_branches is False (EndNode)', [m.EndNode]),
    ('No user_branches, trace_branches is True (ReferenceNode)',
     [m.EndNode, m.SelfReferentialNode, m.ReferenceNode, m.ManyToManyNode,
      m.ThroughNode]),
    ('Has user_branches, trace_branches is False (SelfReferentialNode)',
     [m.EndNode, m.SelfReferentialNode, m.ReferenceNode]),
], ids=[
    'No branches, only the root model is part of the tree',
    'Full set of branches',
    'Partial set of branches'
], indirect=['tree'])
def test_relationtree_pick_arranges_bucket_compartments(tree, exp_comps):
    """
    RelationTree.pick should return a bucket with compartments arranged
    in the correct order.
    """
    bucket = tree.pick()
    assert bucket.compartments == exp_comps


@pytest.mark.tree
@pytest.mark.parametrize('tree, qset, exp_obj_lists', [
    ('No user_branches, trace_branches is False (EndNode)', None,
     [['end0', 'end1', 'end2']]),
    ('No user_branches, trace_branches is True (ReferenceNode)', None,
     [['ref0', 'ref1', 'ref2'], ['end0', 'end2'],
      ['srn0', 'srn1', 'srn2'], ['end0', 'end2'],
      ['srn0', 'srn1', 'srn2'], ['srn1'],
      ['thr0', 'thr1', 'thr2', 'thr3'], ['m2m0', 'm2m1', 'm2m2'],
      ['end0', 'end1', 'end2']]),
    ('Has user_branches, trace_branches is False (SelfReferentialNode)', None,
     [['srn0', 'srn1', 'srn2'],
      ['ref0', 'ref1', 'ref2'], ['end0', 'end2'],
      ['ref0', 'ref1', 'ref2'], ['srn0', 'srn1', 'srn2'], ['end0', 'end2'],
      ['ref0', 'ref1', 'ref2'], ['srn0', 'srn1', 'srn2'], ['srn1']]),
    ('No user_branches, trace_branches is True (ReferenceNode)',
     m.ReferenceNode.objects.filter(name='ref1'),
     [['ref1'], ['end2'], ['srn2'], ['end2'], ['srn2'], ['srn1'],
      ['thr2', 'thr3'], ['m2m0', 'm2m2'], ['end0', 'end1']]),
], ids=[
    'No branches, only the root model is part of the tree',
    'Full set of branches',
    'Partial set of branches',
    'Full branches with user-specified queryset'
], indirect=['tree'])
def test_relationtree_pick_puts_correct_qsets(tree, qset, exp_obj_lists,
                                              mocker, assert_all_objset_calls,
                                              get_model_instances):
    """
    RelationTree.pick should call Bucket.put with each of the expected
    sets of objects.
    """
    exp_objsets = [get_model_instances(objnames) for objnames in exp_obj_lists]
    mocker.patch.object(relationtrees.Bucket, 'put')
    bucket = relationtrees.Bucket()
    bucket = tree.pick(into=bucket, qset=qset)
    assert_all_objset_calls(bucket.put, exp_objsets)


@pytest.mark.harvest
def test_harvest_picks_trees_into_bucket_using_qset(all_trees, mocker):
    """
    The `harvest` function should call `tree.pick` once for each of the
    trees provided as an argument; it should also pick using the
    provided `into` and `tree_qsets` values.
    """
    bucket = relationtrees.Bucket()
    qset_tree_key = 'No user_branches, trace_branches is True (ReferenceNode)'
    qsets = {
        all_trees[qset_tree_key]: m.ReferenceNode.objects.filter(name='ref1')
    }
    for tree in all_trees.values():
        mocker.patch.object(tree, 'pick', return_value=bucket)
    relationtrees.harvest(all_trees.values(), into=bucket, tree_qsets=qsets)
    for tree in all_trees.values():
        tree.pick.assert_called_once_with(into=bucket,
                                          qset=qsets.get(tree, None))

