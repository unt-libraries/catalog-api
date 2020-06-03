"""
Tests for custom behavior on base.models (i.e., Sierra models).
"""

import pytest

from base import models as m
from base import fields as f


# FIXTURES AND TEST DATA
# External fixtures used below can be found in
# django/sierra/conftest.py:
#    model_instance

pytestmark = pytest.mark.django_db


def get_attached_name_models():
    """
    Find and return all models derived from ModelWithAttachedName, so
    we can test them.
    """
    modelset = []
    for attrname in dir(m):
        thing = getattr(m, attrname)
        if m.ModelWithAttachedName in getattr(thing, '__bases__', []):
            modelset.append(thing)
    return modelset


# TESTS

@pytest.mark.parametrize('prop_model', get_attached_name_models())
def test_modelattachedname_getname(prop_model, model_instance, settings):
    """
    The ModelWithAttachedName `get_name` method should return the
    plain-language string representing the given property's name, using
    either the default language (set in the Django settings) or the
    provided language code argument.
    """
    settings.III_LANGUAGE_CODE = 'eng'
    eng, spi = (m.IiiLanguage.objects.get(code=l) for l in ('eng', 'spi'))
    name_model = getattr(m, '{}Name'.format(prop_model._meta.object_name))
    name_accessor = '{}name_set'.format(prop_model._meta.model_name)
    prop_attname = getattr(prop_model, name_accessor).related.field.name
    name_attname = prop_model._name_attname
    lang_attname = prop_model._language_attname
    test_property = model_instance(prop_model, pk='999999')
    eng_params = {
        name_attname: '__object name',
        lang_attname: eng,
        prop_attname: test_property
    }
    spi_params = {
        name_attname: '__nombre de objeto',
        lang_attname: spi,
        prop_attname: test_property
    }
    if not isinstance(name_model._meta.pk, f.VirtualCompField):
        eng_params['pk'] = '999998'
        spi_params['pk'] = '999999'
    test_name_eng = model_instance(name_model, **eng_params)
    test_name_spi = model_instance(name_model, **spi_params)
    assert test_property.get_name() == '__object name'
    assert test_property.get_name('spi') == '__nombre de objeto'


@pytest.mark.parametrize('fields, expected', [
    ([('d', '099', '|awrong vf tag')], []),
    ([('c', '050', '|alc cn')], [('lc cn', 'lc')]),
    ([('c', '055', '|alc cn')], [('lc cn', 'lc')]),
    ([('c', '086', '|asudoc num')], [('sudoc num', 'sudoc')]),
    ([('c', '090', '|alc cn')], [('lc cn', 'lc')]),
    ([('c', '092', '|adewey cn')], [('dewey cn', 'dewey')]),
    ([('c', '099', '|alocal cn')], [('local cn', 'other')]),
    ([('c', '999', '|alocal cn')], [('local cn', 'other')]),
    ([('c', None, '|alocal cn')], [('local cn', 'other')]),
    ([('g', '999', '|asudoc num')], [('sudoc num', 'sudoc')]),
    ([('g', None, '|asudoc num')], [('sudoc num', 'sudoc')]),
    ([('g', None, '|asudoc|bnum|znum')], [('sudoc num', 'sudoc')]),
    ([('c', '050', '|afirst'),
      ('c', '050', '|asecond'),
      ('c', '999', '|athird'),
      ('d', '050', '|askip'),
      ('g', None, '|alast')],
     [('first', 'lc'), ('second', 'lc'), ('third', 'other'),
      ('last', 'sudoc')]),
])
def test_itemrecord_getcallnumbers(fields, expected, model_instance):
    """
    The ItemRecord model `get_call_numbers` method should return the
    expected call_number_tuple value.
    """
    item_md = model_instance(m.RecordMetadata, id=9999999999)
    varfields = [model_instance(m.Varfield, occ_num=i, varfield_type_code=t[0],
                                marc_tag=t[1], field_content=t[2],
                                id=99999999 + i, record=item_md)
                 for i, t in enumerate(fields)]
    item = model_instance(m.ItemRecord, id=9999999999, record_metadata=item_md)
    assert item.get_call_numbers() == expected


@pytest.mark.parametrize('fields, expected', [
    ([('d', '099', '|awrong vf tag')], []),
    ([('c', '050', '|alc cn')], [('lc cn', 'lc')]),
    ([('c', '055', '|alc cn')], [('lc cn', 'lc')]),
    ([('c', '086', '|asudoc num')], [('sudoc num', 'sudoc')]),
    ([('c', '090', '|alc cn')], [('lc cn', 'lc')]),
    ([('c', '092', '|adewey cn')], [('dewey cn', 'dewey')]),
    ([('c', '099', '|alocal cn')], [('local cn', 'other')]),
    ([('c', '999', '|alocal cn')], []),
    ([('c', None, '|alocal cn')], []),
    ([('g', '999', '|asudoc num')], []),
    ([('g', None, '|asudoc num')], []),
    ([('g', '086', '|asudoc|bnum|znum')], [('sudoc num', 'sudoc')]),
    ([('c', '050', '|afirst'),
      ('c', '050', '|asecond'),
      ('c', '092', '|athird'),
      ('d', '050', '|askip'),
      ('g', '086', '|alast')],
     [('first', 'lc'), ('second', 'lc'), ('third', 'dewey'),
      ('last', 'sudoc')]),
])
def test_bibrecord_getcallnumbers(fields, expected, model_instance):
    """
    The BibRecord model `get_call_numbers` method should return the
    expected call_number_tuple value.
    """
    bib_md = model_instance(m.RecordMetadata, id=9999999999)
    varfields = [model_instance(m.Varfield, occ_num=i, varfield_type_code=t[0],
                                marc_tag=t[1], field_content=t[2],
                                id=99999999 + i, record=bib_md)
                 for i, t in enumerate(fields)]
    bib = model_instance(m.BibRecord, id=9999999999, record_metadata=bib_md)
    assert bib.get_call_numbers() == expected
