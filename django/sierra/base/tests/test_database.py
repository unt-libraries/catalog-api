"""
This contains very basic sanity tests to test Sierra base models
against the Sierra database. This will run as part of the test suite
when running pytest (against your test Sierra database) -- but its
main utility is running it against your live Sierra database to make
sure that your live DB matches up against the provided models. These
tests should be run again after every Sierra update and changes to the
models made as needed.
"""

import re
import warnings

import pytest

from django.core.exceptions import ObjectDoesNotExist

from base import models


# FIXTURES AND TEST DATA

pytestmark = pytest.mark.django_db

def get_sierra_models():
    potential_models = [getattr(models, m) for m in dir(models)]
    return [m for m in potential_models
            if hasattr(m, '_meta') and not m._meta.abstract]


def get_model_related_fields(all_models):
    model_related_fields = []
    for model in all_models:
        for field in model._meta.get_fields():
            if field.is_relation:
                try:
                    fieldname = field.get_accessor_name()
                except AttributeError:
                    fieldname = field.name
                model_related_fields.append((model, fieldname))
    return model_related_fields


ALL_MODELS = get_sierra_models()
MODEL_RELATED_FIELDS = get_model_related_fields(ALL_MODELS)

# TESTS

@pytest.mark.parametrize('model', ALL_MODELS)
def test_model_instance_against_database(model):
    """
    This test tries accessing one instance of each model, forcing a
    database query to be sent. It fails on DatabaseError (which means
    there's something in the model that doesn't match up with the DB),
    but passes otherwise.
    """
    try:
        model.objects.all()[0]
    except IndexError as e:
        # An IndexError just means there are no records in the
        # database for this table.
        pass
    assert True


@pytest.mark.parametrize('model, fieldname', MODEL_RELATED_FIELDS)
def test_model_related_field_against_database(model, fieldname):
    """
    This test tries to catch basic related-fields problems on each
    model. Note that this just uses the first row in the database table
    to test each field. It's not meant to test data integrity, just to
    catch basic problems with the models.
    """
    try:
        test_row = model.objects.all()[0]
        getattr(test_row, fieldname)
    except IndexError as e:
        pass
    except ObjectDoesNotExist as e:
        if re.match(r'\w+ matching query does not exist', str(e)) is not None:
            warnings.warn('{} The Sierra database may have changed.'.format(str(e)))


@pytest.mark.parametrize('many_model, one_model, field_name', [
    (models.IiiUser, models.IiiUserGroup, 'iii_user_group'),
    (models.ItemRecord, models.AgencyProperty, 'agency'),
    (models.PatronRecord, models.AgencyProperty, 'patron_agency'),
    (models.Pblock, models.AgencyProperty, 'ptype_agency'),
    (models.RecordMetadata, models.AgencyProperty, 'agency'),
    (models.PatronRecord, models.FirmProperty, 'firm')
])
def test_specific_m2one_relation(many_model, one_model, field_name):
    """
    Checks the table on the one side of a one-to-many relationship to
    see if it has any records in it (or not) and the table on the many
    side to see if it has a relationship defined in the model or just a
    character or integer 'code' field. Test fails if the two things--
    relationship and empty table OR non-relationship and populated
    table--don't match up.
    """
    objects_exist = one_model.objects.all()
    relationship_exists = hasattr(many_model, field_name)
    assert relationship_exists or not objects_exist
    assert objects_exist or not relationship_exists


