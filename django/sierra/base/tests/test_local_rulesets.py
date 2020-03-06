"""
Tests the base.ruleset classes and functions.
"""

from __future__ import unicode_literals
import pytest

from base import local_rulesets as lr

# FIXTURES / TEST DATA

@pytest.fixture
def item_rules():
    return lr.ITEM_RULES


@pytest.fixture
def bib_rules():
    return lr.BIB_RULES


# TESTS

@pytest.mark.parametrize('loc_code, expected', [
    ('czwww', True),
    ('gwww', True),
    ('lawww', True),
    ('lwww', True),
    ('mwww', True),
    ('xwww', True),
    ('w1', False),    
    ('xdoc', False),
    ('w4m', False),
    ('czm', False),
    ('law', False),
])
def test_itemrules_isonline(loc_code, expected, item_rules, mocker):
    """
    Our local ITEM_RULES['is_online'] rule should return the expected
    boolean for items with the given location code, indicating whether
    or not that item is an online copy.
    """
    item = mocker.Mock(location_id=loc_code)
    assert item_rules['is_online'].evaluate(item) == expected


@pytest.mark.parametrize('loc_code, expected', [
    ('czm', True),
    ('czmrf', True),
    ('frsco', True),
    ('jlf', True),
    ('kmatt', True),
    ('r', True),
    ('rzzrs', True),
    ('rmak', True),
    ('s', True),
    ('sdoc', True),
    ('w', True),
    ('w1inf', True),
    ('w4m', True),
    ('unt', False),
    ('ill', False),
    ('czwww', False),
    ('gwww', False),
    ('lawww', False),
    ('lwww', False),
    ('mwww', False),
    ('xwww', False),
    ('x', False),
    ('xmus', False),
    ('xdoc', False),
])
def test_itemrules_isatpubliclocation(loc_code, expected, item_rules, mocker):
    """
    Our local ITEM_RULES['is_at_public_location'] rule should return
    the expected boolean for items with the given location code,
    indicating whether or not that item exists at a (physical) location
    that can be accessed by the public.
    """
    item = mocker.Mock(location_id=loc_code)
    assert item_rules['is_at_public_location'].evaluate(item) == expected


@pytest.mark.parametrize('loc_code, expected', [
    ('czm', 'czm'),
    ('czmrf', 'czm'),
    ('czmrs', 'czm'),
    ('frsco', 'frsco'),
    ('r', 'r'),
    ('rzzrs', 'r'),
    ('s', 's'),
    ('sdoc', 's'),
    ('sdzrs', 's'),
    ('w', 'w'),
    ('w1inf', 'w'),
    ('w3', 'w'),
    ('w4m', 'w'),
    ('x', 'x'),
    ('xmus', 'x'),
    ('xdoc', 'x'),
    ('jlf', 'x'),
    ('spec', None),
    ('xprsv', None),
    ('xts', None),
    ('unt', None),
    ('ill', None),
    ('czwww', None),
    ('gwww', None),
    ('lawww', None),
    ('lwww', None),
    ('mwww', None),
])
def test_itemrules_buildinglocation(loc_code, expected, item_rules, mocker):
    """
    Our local ITEM_RULES['building_location'] rule should return
    the location code value of the physical building an item belongs
    in, if any.
    """
    item = mocker.Mock(location_id=loc_code)
    assert item_rules['building_location'].evaluate(item) == expected


@pytest.mark.parametrize('loc_code, expected', [
    ('czm', ['Media Library']),
    ('czmrf', ['Media Library']),
    ('czwww', ['Media Library']),
    ('jlf', ['General Collection']),
    ('lwww', ['General Collection']),
    ('w', ['General Collection']),
    ('w3', ['General Collection']),
    ('x', ['General Collection']),
    ('xmic', ['General Collection']),
    ('gwww', ['Government Documents']),
    ('sd', ['Government Documents']),
    ('sdtx', ['Government Documents']),
    ('xdoc', ['Government Documents']),
    ('xdmic', ['Government Documents']),
    ('mwww', ['Music Library']),
    ('w4m', ['Music Library']),
    ('xmus', ['Music Library']),
    ('w4spe', ['Special Collections']),
    ('xspe', ['Special Collections']),
    ('rmak', ['The Factory (Makerspace)']),
    ('w1mak', ['The Factory (Makerspace)']),
    ('r', ['Discovery Park Library']),
    ('rfbks', ['Discovery Park Library']),
    ('rst', ['Discovery Park Library']),
])
def test_itemrules_incollections(loc_code, expected, item_rules, mocker):
    """
    Our local ITEM_RULES['in_collections'] rule should return a set of
    names/labels of the collections an item belongs to.
    """
    item = mocker.Mock(location_id=loc_code)
    assert item_rules['in_collections'].evaluate(item) == set(expected)


@pytest.mark.parametrize('loc_code, itype_id, item_status_id, expected', [
    ('czm', 1, '-', True),
    ('czmrf', 1, '-', False),
    ('czm', 7, '-', False),
    ('czm', 20, '-', False),
    ('czm', 29, '-', False),
    ('czm', 69, '-', False),
    ('czm', 74, '-', False),
    ('czm', 112, '-', False),
    ('xmus', 1, '-', True),
    ('xmus', 7, '-', True),
    ('czm', 1, 'e', False),
    ('czm', 1, 'f', False),
    ('czm', 1, 'i', False),
    ('czm', 1, 'j', False),
    ('czm', 1, 'm', False),
    ('czm', 1, 'n', False),
    ('czm', 1, 'o', False),
    ('czm', 1, 'p', False),
    ('czm', 1, 'w', False),
    ('czm', 1, 'y', False),
    ('czm', 1, 'z', False),
    ('jlf', 1, '-', False),
    ('w4mr1', 1, '-', False),
    ('w4mr2', 1, '-', False),
    ('w4mr3', 1, '-', False),
    ('w4mrb', 1, '-', False),
    ('w4mrx', 1, '-', False),
    ('w4spe', 1, '-', False),
])
def test_itemrules_isrequestablethroughcatalog(loc_code, itype_id,
                                               item_status_id, expected,
                                               item_rules, mocker):
    """
    Our local ITEM_RULES['is_requestable_through_catalog'] rule should
    return True if an item is available to be requested via the online
    catalog; False if not.
    """
    item = mocker.Mock(location_id=loc_code, itype_id=itype_id,
                       item_status_id=item_status_id)
    result = item_rules['is_requestable_through_catalog'].evaluate(item)
    assert result == expected


@pytest.mark.parametrize('loc_code, expected', [
    ('czm', False),
    ('jlf', False),
    ('w4mr1', True),
    ('w4mr2', True),
    ('w4mr3', True),
    ('w4mrb', True),
    ('w4mrx', True),
    ('w4spe', True),
])
def test_itemrules_isrequestablethroughaeon(loc_code, expected, item_rules,
                                            mocker):
    """
    Our local ITEM_RULES['is_requestable_through_aeon'] rule should
    return True if an item is available to be requested via Aeon; False
    if not.
    """
    item = mocker.Mock(location_id=loc_code)
    result = item_rules['is_requestable_through_aeon'].evaluate(item)
    assert result == expected


@pytest.mark.parametrize('loc_code, expected', [
    ('czm', False),
    ('jlf', True),
    ('w4mr1', False),
    ('w4mr2', False),
    ('w4mr3', False),
    ('w4mrb', False),
    ('w4mrx', False),
    ('w4spe', False),
])
def test_itemrules_isatjlf(loc_code, expected, item_rules, mocker):
    """
    Our local ITEM_RULES['is_at_jlf'] rule should return True if an
    item is available to be requested via ILLiad from the JLF.
    """
    item = mocker.Mock(location_id=loc_code)
    assert item_rules['is_at_jlf'].evaluate(item) == expected


@pytest.mark.parametrize('bcode2, cns, f008_26, bib_locations, expected', [
    ('-', None, None, None, 'unknown'),
    ('a', None, None, None, 'book'),
    ('b', None, None, None, 'database'),
    ('c', None, None, None, 'score'),
    ('e', None, None, None, 'map'),
    ('g', None, None, None, 'video'),
    ('i', None, None, None, 'audiobook'),
    ('j', None, None, None, 'recording'),
    ('k', None, None, None, 'graphic'),
    ('m', [], ' ', [''], 'computer_file'),
    ('m', ['CD-ROM 1234'], 'b', ['czm'], 'computer_file'),
    ('m', ['Game 1234'], 'g', ['czm'], 'video_game'),
    ('m', ['Game 1234'], ' ', ['czm'], 'video_game'),
    ('m', ['CD-ROM 1234'], 'g', ['sd'], 'video_game'),
    ('m', ['CD-ROM 1234'], 'd', ['sd'], 'eresource'),
    ('m', ['MT 1234 S32'], 'h', ['sd'], 'recording'),
    ('n', None, None, None, 'ebook'),
    ('o', None, None, None, 'kit'),
    ('p', None, None, None, 'archival_collection'),
    ('q', None, None, None, 'print_journal'),
    ('r', [], ' ', [''], 'equipment'),
    ('r', ['MT 1234 S32'], ' ', ['w4spe'], 'object'),
    ('r', ['Boardgame 1234'], ' ', ['czm'], 'board_game'),
    ('s', None, None, None, 'score_thesis'),
    ('t', None, None, None, 'manuscript'),
    ('y', None, None, None, 'ejournal'),
    ('z', None, None, None, 'thesis'),
])
def test_bibrules_resourcetype(bcode2, cns, f008_26, bib_locations, expected,
                               bib_rules, mocker):
    """
    Our local BIB_RULES['resource_type'] and
    BIB_RULES['resource_type_label'] rules should return the expected
    type and label.
    """
    bib = mocker.Mock(bcode2=bcode2)
    if cns is not None:
        rval = [mocker.Mock(**{'display_field_content.return_value': cn})
                for cn in cns]
        bib.record_metadata.varfield_set.filter.return_value = rval
    if f008_26 is not None:
        f008 = '{}{}'.format(' ' * 26, f008_26)
        rval = [mocker.Mock(**{'get_data.return_value': f008})]
        bib.record_metadata.controlfield_set.filter.return_value = rval
    if bib_locations is not None:
        rval = [mocker.Mock(code=bl) for bl in bib_locations]
        bib.locations.all.return_value = rval
    assert bib_rules['resource_type'].evaluate(bib) == expected
