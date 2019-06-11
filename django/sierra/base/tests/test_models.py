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


