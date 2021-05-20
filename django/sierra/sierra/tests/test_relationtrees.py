"""
Tests the relationtrees module used in custom sierra management commands.
"""

from __future__ import absolute_import
import pytest

from .testmodels import models as m
from sierra.management import relationtrees
import six

# FIXTURES AND TEST DATA

pytestmark = pytest.mark.django_db


BAD_RELATION_PARAMS = {
    'Invalid model': ('invalid', 'fieldname'),
    'Invalid fieldname': (m.EndNode, 'invalid'),
    'Fieldname is not a relation': (m.EndNode, 'name'),
}


RELATION_PARAMS = {
    'ReferenceNode to EndNode': (m.ReferenceNode, 'end'),
    'ReferenceNode to ThroughNode': (m.ReferenceNode, 'throughnode_set'),
    'ReferenceNode to ManyToManyNode': (m.ReferenceNode, 'm2m'),
    'ReferenceNode to SelfReferentialNode': (m.ReferenceNode, 'srn'),
    'ReferenceNode to OneToOneNode': (m.ReferenceNode, 'one'),
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
    'OneToOneNode to ReferenceNode': (m.OneToOneNode, 'referencenode'),
    'SelfReferentialNode to SelfReferentialNode':
        (m.SelfReferentialNode, 'parent'),
    'SelfReferentialNode to ReferenceNode':
        (m.SelfReferentialNode, 'referencenode_set'),
    'SelfReferentialNode to EndNode': (m.SelfReferentialNode, 'end'),
}


BRANCH_PARAMS = {
    'ReferenceNode > end': (m.ReferenceNode, ['end']),
    'ReferenceNode > one': (m.ReferenceNode, ['one']),
    'ReferenceNode > m2m, end': (m.ReferenceNode, ['m2m', 'end']),
    'ReferenceNode > throughnode_set, m2m, end':
        (m.ReferenceNode, ['throughnode_set', 'm2m', 'end']),
    'ReferenceNode > srn, parent, end':
        (m.ReferenceNode, ['srn', 'parent', 'end']),
    'ThroughNode > ref, one': (m.ThroughNode, ['ref', 'one']),
    'ThroughNode > m2m, referencenode_set, end':
        (m.ThroughNode, ['m2m', 'referencenode_set', 'end']),
    'SelfReferentialNode > referencenode_set, m2m, end':
        (m.SelfReferentialNode, ['referencenode_set', 'm2m', 'end']),
    'ThroughNode > ref, m2m, end': (m.ThroughNode, ['ref', 'm2m', 'end']),
    'EndNode > m2m, ref, end':
        (m.EndNode, ['manytomanynode_set', 'referencenode_set', 'end']),
    'OneToOneNode > referencenode, m2m, end':
        (m.OneToOneNode, ['referencenode', 'm2m', 'end']),
}


TREE_PARAMS = {
    'No branches (EndNode)':
        (m.EndNode, []),
    'Has branches (SelfReferentialNode)':
        (m.SelfReferentialNode, [
            ['referencenode_set', 'one'],
            ['referencenode_set', 'end'],
            ['referencenode_set', 'srn', 'end'],
            ['referencenode_set', 'srn', 'parent', 'end']
        ])
}


@pytest.fixture
def instname_to_model():
    def do_it(inst_name):
        nameprefix_to_model = {
            'ref': m.ReferenceNode,
            'srn': m.SelfReferentialNode,
            'end': m.EndNode,
            'm2m': m.ManyToManyNode,
            'thr': m.ThroughNode,
            'one': m.OneToOneNode
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
            for arg in (list(call[0]) + list(call[1].values())):
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


@pytest.fixture(params=[key for key in BAD_RELATION_PARAMS.keys()])
def make_bad_relation(request):
    def do_it():
        relationtrees.Relation(*BAD_RELATION_PARAMS[request.param])
    return do_it


@pytest.fixture(params=[key for key in RELATION_PARAMS.keys()])
def relation(request):
    return relationtrees.Relation(*RELATION_PARAMS[request.param])


@pytest.fixture(scope='module')
def make_branch():
    def do_it(model, fnames):
        rels = relationtrees.make_relation_chain_from_fieldnames(model, fnames)
        return relationtrees.RelationBranch(model, rels)
    return do_it


@pytest.fixture(params=[key for key in BRANCH_PARAMS.keys()])
def branch(make_branch, request):
    return make_branch(*BRANCH_PARAMS[request.param])


@pytest.fixture(scope='module')
def make_tree(make_branch):
    def do_it(model, flists):
        branches = [make_branch(model, fl) for fl in flists]
        return relationtrees.RelationTree(model, branches)
    return do_it


@pytest.fixture(params=[key for key in TREE_PARAMS.keys()])
def tree(make_tree, request):
    return make_tree(*TREE_PARAMS[request.param])


@pytest.fixture
def all_trees(make_tree):
    return {k: make_tree(*v) for k, v in six.iteritems(TREE_PARAMS)}


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
    ('ReferenceNode to OneToOneNode', False, False, True),
    ('ReferenceNode to EndNode', False, False, True),
    ('ReferenceNode to ManyToManyNode', True, True, True),
    ('OneToOneNode to ReferenceNode', False, False, False),
    ('EndNode to ReferenceNode', False, True, False),
    ('ManyToManyNode to ReferenceNode', True, True, False)
], ids=[
    'direct 1-1',
    'direct foreign-key',
    'direct m2m',
    'indirect 1-1',
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
    ('ReferenceNode to OneToOneNode', m.OneToOneNode),
    ('ReferenceNode to SelfReferentialNode', m.SelfReferentialNode),
    ('ReferenceNode to ManyToManyNode', m.ManyToManyNode),
    ('OneToOneNode to ReferenceNode', m.ReferenceNode),
    ('EndNode to ReferenceNode', m.ReferenceNode),
    ('ManyToManyNode to ReferenceNode', m.ReferenceNode)
], ids=[
    'direct 1-1',
    'direct foreign-key',
    'direct m2m',
    'indirect 1-1',
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
def test_relation_getasthroughrelations_m2m_returns_expected(relation, exp):
    """
    Relation.get_as_through_relations should return the expected list
    of Relation objects, if the relationship is many-to-many.
    """
    exp_objs = [relationtrees.Relation(*RELATION_PARAMS[exp[0]]),
                relationtrees.Relation(*RELATION_PARAMS[exp[1]])]
    result = relation.get_as_through_relations()
    exp_params = [[r.model, r.fieldname, r.target_model] for r in exp_objs]
    res_params = [[r.model, r.fieldname, r.target_model] for r in result]
    assert res_params == exp_params


@pytest.mark.relation
@pytest.mark.parametrize('relation', [
    'ReferenceNode to OneToOneNode',
    'ReferenceNode to SelfReferentialNode',
    'OneToOneNode to ReferenceNode',
    'EndNode to ReferenceNode'
], ids=[
    'direct 1-1',
    'direct foreign-key',
    'indirect 1-1',
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
    ('ReferenceNode to OneToOneNode', None,
     [m.OneToOneNode, m.ReferenceNode]),
    ('OneToOneNode to ReferenceNode', None,
     [m.OneToOneNode, m.ReferenceNode]),
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
    'direct 1-1; second (indirect) should be first',
    'indirect 1-1; first (indirect) should be first',
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
    ('ReferenceNode to OneToOneNode', ['ref2'], []),
    ('OneToOneNode to ReferenceNode', ['one2'], []),
    ('SelfReferentialNode to EndNode', ['srn1'], []),
    ('EndNode to ReferenceNode', ['end1'], []),
    ('ReferenceNode to OneToOneNode', ['ref0'], ['one0']),
    ('OneToOneNode to ReferenceNode', ['one1'], ['ref1']),
    ('ReferenceNode to EndNode', ['ref0'], ['end0']),
    ('ReferenceNode to EndNode', ['ref0', 'ref1'], ['end0', 'end2']),
    ('EndNode to ReferenceNode', ['end0'], ['ref0']),
    ('EndNode to ReferenceNode', ['end0', 'end2'], ['ref0', 'ref1', 'ref2']),
    ('ReferenceNode to ManyToManyNode', ['ref0'], ['m2m0', 'm2m1']),
    ('ReferenceNode to ManyToManyNode', ['ref0', 'ref1'],
     ['m2m0', 'm2m1', 'm2m2']),
], ids=[
    'zero source objects returns empty list',
    'direct 1-1 relation, zero target objects returns empty list',
    'indirect 1-1 relation, zero target objects returns empty list',
    'direct fk relation, zero target objects returns empty list',
    'indirect fk relation, zero target objects returns empty list',
    'direct 1-1 relation, one source object returns target',
    'indirect 1-1 relation, one source object returns target',
    'direct fk relation, one source object returns target',
    'direct fk relation, multiple source objects returns all targets',
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
@pytest.mark.parametrize('branch, exp_select, exp_prefetch', [
    ('ReferenceNode > one', 'one', ''),
    ('ReferenceNode > end', 'end', ''),
    ('ReferenceNode > srn, parent, end', 'srn__parent__end', ''),
    ('ThroughNode > ref, one', 'ref__one', ''),
    ('ReferenceNode > m2m, end', '', 'throughnode_set__m2m__end'),
    ('ReferenceNode > throughnode_set, m2m, end', '',
     'throughnode_set__m2m__end'),
    ('OneToOneNode > referencenode, m2m, end', '',
     'referencenode__throughnode_set__m2m__end'),
    ('ThroughNode > m2m, referencenode_set, end', 'm2m',
     'm2m__throughnode_set__ref__end'),
    ('ThroughNode > ref, m2m, end', 'ref', 'ref__throughnode_set__m2m__end'),
], ids=[
    'select_related with one 1-1',
    'select_related with one fk',
    'select_related with multiple fks',
    'select_related with mix of 1-1 and 1-many fks',
    'prefetch_related starts with first m2m relation',
    'fks after 1st indirect create prefetch_related',
    'fks after 1st indirect 1-1 create prefetch_related',
    'fks up to 1st indirect create selected_related, then prefetch after that',
    'fks up to 1st m2m create selected_related, then prefetch after that',
], indirect=['branch'])
def test_relationbranch_prepareqset_precaches_correctly(branch, exp_select,
                                                        exp_prefetch, mocker):
    """
    RelationBranch.prepare_qset should call the select_related and
    prefetch_related methods of QuerySet appropriately, based on what
    object relations are in the branch. It should also return actual
    results.
    """
    real_qset = branch.root.objects.all()
    mock_qset = branch.root.objects.all()
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
def test_relationtree_prepareqset_calls_branches_prepareqset(mocker):
    """
    RelationTree.prepare_qset should return a Queryset object that has
    had each branch's prepare_qset called on it.
    """
    qset = m.ReferenceNode.objects.all()
    tree = relationtrees.RelationTree(m.ReferenceNode, [])
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
    tree = relationtrees.RelationTree(m.ReferenceNode, [])
    mocker.patch.object(tree, 'prepare_qset')
    bucket = tree.pick(qset=qset)
    tree.prepare_qset.assert_called_once()
    assert_all_objset_calls(tree.prepare_qset, [exp_qset])


@pytest.mark.tree
@pytest.mark.parametrize('tree, exp_comps', [
    ('No branches (EndNode)', [m.EndNode]),
    ('Has branches (SelfReferentialNode)',
     [m.EndNode, m.SelfReferentialNode, m.OneToOneNode, m.ReferenceNode]),
], ids=[
    'No branches, only the root model is part of the tree',
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
    ('No branches (EndNode)', None,
     [['end0', 'end1', 'end2']]),
    ('Has branches (SelfReferentialNode)', None,
     [['srn0', 'srn1', 'srn2'],
      ['ref0', 'ref1', 'ref2'], ['one0', 'one1'],
      ['ref0', 'ref1', 'ref2'], ['end0', 'end2'],
      ['ref0', 'ref1', 'ref2'], ['srn0', 'srn1', 'srn2'], ['end0', 'end2'],
      ['ref0', 'ref1', 'ref2'], ['srn0', 'srn1', 'srn2'], ['srn1']]),
], ids=[
    'No branches, only the root model is part of the tree',
    'Partial set of branches'
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


@pytest.mark.utilities
@pytest.mark.parametrize('bp_key, fieldnames', [
    ('ReferenceNode > end', ['end']),
    ('ReferenceNode > one', ['one']),
    ('OneToOneNode > referencenode, m2m, end',
     ['referencenode', 'throughnode_set', 'm2m', 'end']),
    ('ReferenceNode > m2m, end', ['throughnode_set', 'm2m', 'end']),
    ('ReferenceNode > throughnode_set, m2m, end',
     ['throughnode_set', 'm2m', 'end']),
    ('ReferenceNode > srn, parent, end', ['srn', 'parent', 'end']),
    ('EndNode > m2m, ref, end',
     ['manytomanynode_set', 'throughnode_set', 'ref', 'end'])
], ids=[
    'root -> foreign-key',
    'root -> one-to-one',
    'root -> one-to-one (indirect) -> many-to-many (direct) -> foreign-key',
    'root -> many-to-many (direct) -> foreign-key',
    'root -> indirect (one-to-many) -> foreign-key -> foreign-key',
    'root -> foreign-key -> foreign-key (self-referential) -> foreign-key',
    'root -> indirect (one-to-many) -> indirect (many-to-many) -> foreign-key'
])
def test_makerelationchainfromfieldnames(bp_key, fieldnames):
    """
    The `make_relation_chain_from_fieldnames` factory should generate
    the correct list of relations based on the fields passed to it.
    """
    model, fnames = BRANCH_PARAMS[bp_key]
    rels = relationtrees.make_relation_chain_from_fieldnames(model, fnames)
    assert all([isinstance(rel, relationtrees.Relation) for rel in rels])
    assert [rel.fieldname for rel in rels] == fieldnames


@pytest.mark.utilities
@pytest.mark.parametrize('model, exp', [
    (m.ReferenceNode, [['end'], ['srn', 'end'], ['srn', 'parent', 'end'],
                            ['throughnode_set', 'm2m', 'end'], ['one']]),
    (m.ThroughNode, [['ref', 'end'], ['ref', 'srn', 'end'], ['ref', 'one'],
                          ['ref', 'srn', 'parent', 'end'],
                          ['ref', 'throughnode_set', 'm2m', 'end'],
                          ['m2m', 'end']]),
    (m.EndNode, []),
    (m.OneToOneNode, []),
    (m.SelfReferentialNode, [['end'], ['parent', 'end']]),
    (m.ManyToManyNode, [['end']])
], ids=[
    'ReferenceNode',
    'ThroughNode',
    'EndNode',
    'OneToOneNode',
    'SelfReferentialNode',
    'ManyToManyNode'
])
def test_tracebranches_returns_correct_branches(model, exp):
    """
    The `trace_branches` factory should correctly follow direct
    1-1, many-to-many, and foreign-key relationships on the given
    model, resulting in the expected list of branches (exp).
    """
    branches = relationtrees.trace_branches(model)
    result = [branch.fieldnames for branch in branches]
    for expected_branch in exp:
        assert expected_branch in result
    assert len(result) == len(exp)


@pytest.mark.utilities
def test_harvest_picks_trees_into_bucket_using_qset(all_trees, mocker):
    """
    The `harvest` function should call `tree.pick` once for each of the
    trees provided as an argument; it should also pick using the
    provided `into` and `tree_qsets` values.
    """
    bucket = relationtrees.Bucket()
    qskey = 'Has branches (SelfReferentialNode)'
    qsets = {
        all_trees[qskey]: m.SelfReferentialNode.objects.filter(name='srn2')
    }
    for tree in all_trees.values():
        mocker.patch.object(tree, 'pick', return_value=bucket)
    relationtrees.harvest(list(all_trees.values()), into=bucket, tree_qsets=qsets)
    for tree in all_trees.values():
        tree.pick.assert_called_once_with(into=bucket,
                                          qset=qsets.get(tree, None))

