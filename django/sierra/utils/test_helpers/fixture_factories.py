"""
Provides object factories for use in pytest fixtures.
"""

from __future__ import absolute_import
import importlib
from datetime import datetime
import pysolr

from django.conf import settings

from utils.solr import format_datetime_for_solr
from . import solr_factories as sf


class FactoryTracker(object):
    """
    Class used to wrap a factory object and provide caching/tracking
    of objects created via that factory. Can be used as a context
    manager, as well, to clear the factory upon entering and exiting.
    This is meant to be used in a pytest fixture, to return a callable
    factory for whatever kinds of objects you need to create for
    testing, to take care of setup/cleanup automatically for you.

    To use: create a factory class that defines how to `make` and
    `unmake` certain types of objects. Whatever you want to make via
    a pytest fixture. (Such as, a factory for creating ORM model
    instances.) Wrap an instance of that factory class inside a call
    to initialize a FactoryTracker object, like this:

    make = FactoryTracker(MyTestObjectFactory())

    Now call `make` directly, passing whatever args and kwargs to the
    `MyTestObjectFactory` class you need to to make an object. When
    you're finished, call make.clear() to delete all of the objects
    that were made via that FactoryTracker object.

    Use it as a context manager to take care of this automatically. In
    a pytest fixture, you can use `yield` to make this simple:

    @pytest.fixture(scope='function')
    def obj_factory():
        with FactoryTracker(MyTestObjectFactory()) as make:
            yield make

    Your test would then take this fixture and use it to create your
    test objects:

    def test_something(obj_factory):
        test_obj = obj_factory('test_thing', *args, **kwargs)
        etc.

    For a function-scoped fixture, tests that use that fixture will
    have a blank slate upon start and stop of each test. For module-
    level fixtures, data will be generated once at the start of the
    module's tests and then deleted when the module's tests complete.

    Your factory class must implement a `make` and an `unmake`
    method. `make` should take some kind of `type` argument plus any
    other args and kwargs, and it should return an object. (The
    FactoryTracker caches the object before returning it.) `unmake`
    should take an object created via `make` (e.g., one that's been
    cached) and do whatever it needs to do to destroy or delete that
    object. (Or delete it from a database, Solr, etc.)
    """

    def __init__(self, factory):
        self.factory = factory
        self.obj_cache = []

    def __enter__(self):
        self.clear()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.clear()

    def __call__(self, objtype, *args, **kwargs):
        obj = self.factory.make(objtype, *args, **kwargs)
        self.obj_cache.append(obj)
        return obj

    def clear(self):
        for obj in reversed(self.obj_cache):
            self.factory.unmake(obj)
        self.obj_cache = []


# The rest of the classes defined here are `factory` classes, each of
# which is suitable to use with FactoryTracker.

class TestInstanceFactory(object):
    """
    Factory for making ORM model instances. Works with our Sierra
    (`base` app) models as well as other types of models.
    """

    def _set_write_override(self, model, value):
        """
        Sierra models use a `_write_override` attribute to force models
        to be writable during tests. This method sets that attribute to
        `value` (True or False) on the supplied `model`.
        """
        if hasattr(model, '_write_override'):
            model._write_override = value

    def make(self, _model, *args, **kwargs):
        """
        Make an instance of the `_model` using the supplied `args` and
        `kwargs`. This method will first try to get a model instance
        matching the the args and kwargs and return that, just in case.
        If this is a User model, it tries creating the instance using
        the `create_user` helper method.
        """
        self._set_write_override(_model, True)
        try:
            obj = _model.objects.get(*args, **kwargs)
        except Exception:
            try:
                obj = _model.objects.create_user(*args, **kwargs)
            except AttributeError:
                obj = _model.objects.create(*args, **kwargs)
        self._set_write_override(_model, False)
        return obj

    def unmake(self, obj):
        model = type(obj)
        self._set_write_override(model, True)
        try:
            obj.delete()
        except AssertionError:
            pass
        self._set_write_override(model, False)


class TestInstanceCallerFactory(TestInstanceFactory):
    """
    Factory for making ORM model instances, where the `make` method
    utilizes *any* callable object to generate the test instance.
    """

    def make(self, callable_object, *args, **kwargs):
        """
        Make an ORM model instance using the supplied `args` and
        `kwargs`. The instance is created by calling the supplied
        `callable_object` using the supplied args and kwargs.
        """
        return callable_object(*args, **kwargs)


class SolrTestDataAssemblerFactory(object):
    """
    Factory for making SolrTestDataAssembler objects, which
    coordinate multiple solr_factories.SolrFixtureFactory objects for
    generating and tracking Solr test data (e.g., multiple record sets
    implemented via multiple profiles).
    """

    class SolrTestDataAssembler(object):
        """
        Class for wrangling multiple SolrProfile objects to create,
        manage, save, and delete multiple record sets.

        Initialize an assembler instance by passing parameters that
        define the collection of Solr profiles you need. Each profile
        is identified via a record type name (`rectype`) string that
        is the key for each `profile_definitions` dict entry. Then,
        profiles and recordsets are referenced via that key for the
        life of the assembler, to make, save, access, and clear
        records.
        """

        def __init__(self, solr_types, global_unique_fields, gen_factory,
                     profile_definitions):
            """
            Initialize a SolrTestDataAssembler object. Arguments
            include:

            `solr_types` is a dictionary that maps Solr field types to
            python types and solr_factories.DataEmitter types. Same as
            the solr_factories.SolrProfile `solr_types` arg.

            `global_unique_fields` is a list of field names that appear
            in your Solr schemas that should be treated as unique. Note
            that having fields listed here that appear in some schemas
            but not all is okay; ones that don't apply are ignored.
            Same as the solr_factories.SolrProfile `unique_fields` arg.

            `gen_factory` is the solr_factories.SolrDataGenFactory obj
            you want to use for creating test data "gen" functions.

            `profile_definitions` is a dictionary that maps recordtype
            id strings to profile-specific args for creating the
            assembler's solr_factories.SolrProfile objects. E.g.:
            { 'location': {
                'conn': pysolr.conn('solr_core'),
                'user_fields': ('id', 'name', 'code', 'label'),
                'inclusive': True,
                'field_gens': ( 'code', code_gen_function,
                                'label', label_gen_function ) },
              'item': { # etc.
            } }

            The rectype id strings you use in the `profile_definitions`
            arg become the id strings you use to create and access
            records using each profile in the other object methods.
            """
            self.gen_factory = gen_factory
            self.default_solr_types = solr_types
            self.default_unique_fields = global_unique_fields
            self.records = {}
            self.profiles = {
                rectype: self.make_profile(rectype, **profile_def)
                for rectype, profile_def in profile_definitions.items()
            }

        def make_profile(self, rectype, conn=None, user_fields=None,
                         unique_fields=None, gen_factory=None,
                         solr_types=None, field_gens=None):
            """
            Generate a solr_factories.SolrProfile object using the
            provided kwargs.
            """
            unique_fields = unique_fields or self.default_unique_fields
            gen_factory = gen_factory or self.gen_factory
            solr_types = solr_types or self.default_solr_types
            field_gens = field_gens or tuple()
            profile = sf.SolrProfile(
                rectype, conn, user_fields=user_fields,
                unique_fields=unique_fields, solr_types=solr_types,
                gen_factory=gen_factory, default_field_gens=field_gens
            )
            self.records[rectype] = self.records.get(rectype, tuple())
            return profile

        def make(self, rectype, number, context=None, **field_gens):
            """
            Make a set of records via a
            solr_factories.SolrFixtureFactory for the given `rectype`
            profile. `number` is an integer defining how many records
            to create. `context` is an optional keyword arg providing
            a list or tuple of exisiting records that are part of the
            same set as the ones you want to make (e.g. for determining
            uniqueness). `field_gens` are the kwargs for field_gen
            overrides you want passed to the SolrFixtureFactory.

            This method returns the record set you created via this
            method call AND adds the records to self.records[rectype].
            E.g.:

                set1 = assembler.make('location', 10)
                set2 = assembler.make('location', 5, context=set1)
                assert len(set1) == 10
                assert len(set2) == 5
                assert set1 + set2 == assembler.records['location']

            Note that calling `make` DOES NOT save the records to Solr.
            """
            context = context or tuple()
            fixture_factory = sf.SolrFixtureFactory(self.profiles[rectype])
            records = tuple(fixture_factory.make_more(context, number,
                                                      **field_gens))
            self.records[rectype] += records
            return records

        def save(self, rectype):
            """
            Save this assembler's `rectype` recordset to Solr.
            """
            recs = []
            for old_rec in self.records.get(rectype, []):
                rec = {}
                for k, v in old_rec.items():
                    if isinstance(v, datetime):
                        rec[k] = format_datetime_for_solr(v)
                    elif isinstance(v, (list, tuple)):
                        rec[k] = []
                        for sub_v in v:
                            if isinstance(sub_v, datetime):
                                rec[k].append(format_datetime_for_solr(sub_v))
                            else:
                                rec[k].append(sub_v)
                    else:
                        rec[k] = v
                recs.append(rec)
            self.profiles[rectype].conn.add(recs)

        def save_all(self):
            """
            Save ALL recordsets you've created via this assembler to
            Solr.
            """
            for rectype in self.records.keys():
                self.save(rectype)

        def load_static_test_data(self, rectype, test_data, id_field='id',
                                  context=None):
            """
            Helper method. Load a set of test records of the specified
            `rectype` containing static data into Solr, using this
            assembler. Returns the newly created records.

            `test_data` should be a list or tuple of tuples, where the
            first member of the inner tuple is an ID value and the
            second is a dictionary of field/value pairs for the static
            data to load. E.g.:

              ( ('ID1', { 'field1': 'VAL 1', 'field2': 'VAL A' }),
                ('ID2', { 'field1': 'VAL 2', 'field2': 'VAL B' })
              )

            `id_field` is the name of the Solr field that you're using
            as the ID. Default is `id`, but it can be any field. The
            point is to be able to identify your test records later.

            `context` is an optional keyword arg providing a list or
            tuple of exisiting records that are part of the same set as
            the ones you want to make (e.g. for determining
            uniqueness).
            """
            context = context or tuple()
            records = tuple()
            gens = self.profiles[rectype].gen_factory
            for rec_id, record in test_data:
                datagens = {k: gens.static(v) for k, v in record.items()}
                datagens[id_field] = gens.static(rec_id)
                records += self.make(rectype, 1, context + records, **datagens)
            self.save(rectype)
            return records

        def clear_all(self):
            """
            Delete all records you've created via this assembler from
            Solr. Note that this does not delete the records from the
            assembler object--you could issue a `save_all()` after
            `clear_all()` to add them back to Solr.
            """
            for rectype, recset in self.records.items():
                profile = self.profiles[rectype]
                conn, key = profile.conn, profile.key_name
                try:
                    conn.delete(id=[r[key] for r in recset])
                except ValueError:
                    pass
                self.records[rectype] = tuple()

    def make(self, solr_types, unique_fields, gen_factory, defs):
        return type(self).SolrTestDataAssembler(solr_types, unique_fields,
                                                gen_factory, defs)

    def unmake(self, assembler):
        assembler.clear_all()


class TestSolrConnectionFactory(object):
    """
    Factory for making Pysolr connection objects, where the `unmake`
    method deletes all records in the associated Solr core. When
    wrapped in a FactoryTracker object, this ensures that the Solr core
    is cleared automatically before and after use.
    """

    def make(self, conn_name, *args, **kwargs):
        url = settings.HAYSTACK_CONNECTIONS[conn_name]['URL']
        return pysolr.Solr(url, always_commit=True, **kwargs)

    def unmake(self, conn):
        conn.delete(q='*:*')


class TestClassRegistry(object):
    """
    "Factory" that dynamically registers a class as an attribute on an
    existing module, and then unregisters and deletes the class after
    use. If the test class has the same name as an existing class, then
    the original class is overridden during the test and then restored
    afterward.

    This is useful when you need a test or fixture to create a mock or
    disposable class for testing, but you need to register the class on
    on an actual module so the code under test has a pathway to access
    the test class, when/if you can't just use dependency injection.

    (If you actually need a mock object, use the pytest_mock fixtures
    instead. This is mainly for creating test classes that need
    functionality beyond what a mock can supply.)
    """

    def make(self, testclass, modpath):
        mod = importlib.import_module(modpath)
        oldclass = getattr(mod, testclass.__name__, None)
        setattr(mod, testclass.__name__, testclass)
        return (modpath, testclass, oldclass)

    def unmake(self, cached_classtuple):
        modpath, testclass, oldclass = cached_classtuple
        mod = importlib.import_module(modpath)
        if oldclass is None:
            delattr(mod, testclass.__name__)
        else:
            setattr(mod, testclass.__name__, oldclass)
        del(testclass)
