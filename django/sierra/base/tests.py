'''
Contains unit tests for the base Sierra app. Since we want to test
each model against the live database, and there are a lot of models, we
have test generators attached to each TestCase child class as class
methods, which are triggered when this file is imported. The test
generators create individual test methods as appropriate and then
attach them to the class using setattr().
'''
import re
import warnings
from inspect import isclass

from django.test import TestCase
from django.db import DatabaseError
from django.core.exceptions import ObjectDoesNotExist
from django.db.models.fields.related import RelatedField

from .benchmarks import timeit
from . import models


class BaseModelMapTests(TestCase):
    '''
    Tests all Sierra models to make sure they map to existing database
    columns and views and can be read. We test this because it's
    possible that III will change the structure of the public Sierra
    views when they update their software, which will break our models.
    A given model may not be implemented in any other way, so we can't
    necessarily rely on other functional tests. These tests should be
    run again after every Sierra update.
    '''
    @classmethod
    def create_map_test(self, model):
        '''
        Generates and returns a test function that can be used to test
        the specified model_name against the database. The test tries
        accessing one instance of the model, forcing a database query
        to be sent. It fails on DatabaseError (which means there's
        something in the model that doesn't match up with the DB), but
        passes otherwise.
        '''
        def do_test(self):
            try:
                model.objects.all()[0]
            except DatabaseError as e:
                self.fail(e)
            except IndexError as e:
                # An IndexError just means there are no records in the
                # database for this table.
                pass
        do_test.__name__ = ('test_model_{}_maps_to_'
                            'database'.format(model.__name__))
        return do_test

    @classmethod
    def create_field_test(self, model, field_name):
        '''
        Generates and returns a test function that accesses a related
        field on a model and fails on any sort of exception. Used to
        provide a basic sanity check to catch basic related-fields
        problems on a given model. Note that this just uses the first
        row in the database table to test each field. It's not meant
        to test data integrity, just to catch basic problems with the
        models.
        '''
        def do_test(self):
            try:
                test_row = model.objects.all()[0]
                getattr(test_row, field_name)
            except IndexError as e:
                pass
            except ObjectDoesNotExist as e:
                if re.match(r'\w+ matching query does not exist', str(e)) is not None:
                    warnings.warn('{} The Sierra database may have changed.'.format(str(e)))
                else:
                    self.fail(e)
            except Exception as e:
                self.fail(e)
        do_test.__name__ = ('test_model_{}_related_field_{}_sanity'
                            '_check'.format(model.__name__, field_name))
        return do_test

    @classmethod
    def generate_tests(self):
        '''
        Generates model -> database mapping tests. One for each model,
        and one for each related field on each model.
        '''
        for model_name in dir(models):
            model = getattr(models, model_name)
            if hasattr(model, '_meta') and not model._meta.abstract:
                map_test = self.create_map_test(model)
                setattr(self, map_test.__name__, map_test)
                for field in model._meta.fields:
                    if isinstance(field, RelatedField):
                        field_test = self.create_field_test(model, field.name)
                        setattr(self, field_test.__name__, field_test)


class BaseModelDbConfigTests(TestCase):
    '''
    Specific tests to confirm that certain quirks in a Sierra DB are
    similarly quirky across installations and across Sierra versions.
    Some of these may depend on having certain III products or features
    enabled.

    Right now this just runs a few tests to check certain many-to-one
    relationships, where the relationship hinges on a numeric code
    field. The code field may have a 0, while there are actually no
    related records. If a relationship is actually specified, Django
    tries to match the 0 to a non-existent record and throws an error.
    These tests make sure that these particular fields match up. Use
    the model_relationships class member to specify parameters for new
    tests.
    '''
    model_relationships = [
        # many_model, one_model, related_field_name
        [models.IiiUser, models.IiiUserGroup, 'iii_user_group'],
        [models.ItemRecord, models.AgencyProperty, 'agency'],
        [models.PatronRecord, models.AgencyProperty, 'patron_agency'],
        [models.Pblock, models.AgencyProperty, 'ptype_agency'],
        [models.RecordMetadata, models.AgencyProperty, 'agency'],
        [models.PatronRecord, models.FirmProperty, 'firm'],
    ]

    @classmethod
    def create_db_config_test(self, model_many, model_one, field_name):
        '''
        Returns a test function that checks the table on the one side
        of a one-to-many relationship to see if it has any records in
        it (or not) and the table on the many side to see if it has a
        relationship defined in the model or just a character or
        integer 'code' field. Test fails if the two things--
        relationship and empty table OR non-relationship and populated
        table--don't match up.
        '''
        def do_test(self):
            objects_exist = model_one.objects.all()
            relationship_exists = hasattr(model_many, field_name)
            object_name = model_one._meta.object_name
            self.assertFalse(objects_exist and not relationship_exists,
                             '{} objects exist but no corresponding '
                             'relationship {}.'.format(object_name,
                                                       field_name))
            self.assertFalse(not objects_exist and relationship_exists,
                             '{} model has {} field but no corresponding '
                             'objects exist.'.format(object_name, field_name))
        do_test.__name__ = ('test_sierra_db_config_relationship'
                            '_{}_{}'.format(model_many.__name__,
                                            model_one.__name__))
        return do_test

    @classmethod
    def generate_tests(self):
        '''
        Generates DB config tests.
        '''
        for (model_many, model_one, field_name) in self.model_relationships:
            db_config_test = self.create_db_config_test(model_many, model_one,
                                                        field_name)
            setattr(self, db_config_test.__name__, db_config_test)


# Generate the tests
BaseModelMapTests.generate_tests()
BaseModelDbConfigTests.generate_tests()

