from __future__ import absolute_import

import logging
from collections import OrderedDict
from collections.abc import Sequence

import django.db.models.query
from django.conf import settings
from utils import camel_case, helpers

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


class SimpleSerializerException(Exception):
    pass


class SimpleField(object):
    """
    A simplified base field type for SimpleSerializer.

    Subclass this to implement various field/data types for
    SimpleSerializer-type serializers. At the very least, you should
    specify the `data_type` attribute in your subclass, so that it's
    clear what the underlying Python data type should be for value
    conversions (from source data, from client-provided data, etc.).

    If you need specialized code for converting to the underlying
    Python type (beyond just trying to call `cls.data_type(value)`,
    then override the `cast_one_to_python` method.

    This class provides some flexibility for how a given serializer
    field should map to the source data, but it's optimized for the
    common case where there's a one-to-one mapping. (The field name in
    the source data may or may not be the same as the field name on the
    serializer). By default it allows for each field instance to have a
    separate main source field (for getting/saving data), order source
    field (for sorting), keyword source field (for keyword or text-
    based searches), and filter field (for non-keyword filters). If you
    need more granularity than that, such as if your serializer field
    values are derived from multiple source fields, you should look at
    overriding the `get_from_source`, `convert_to_source`,
    `apply_filter_to_qset`, and `emit_orderby_criteria` methods.

    The `present_direct_value` attribute controls whether or not the
    `present` method will call `cast_to_python`, i.e. when serializing
    a value from the source object. If True, it bypasses the casting,
    which is a bit expensive, and returns the value directly from the
    object. For very simple and common data types (like strings and
    numeric types) where you're sure the source object already uses the
    correct type, this avoids a performance penalty that may be small
    but can add up if you have a lot of fields. (And if you need to
    override the `present` method, then you can make it work however
    you want.)
    """
    class ValidationError(SimpleSerializerException):
        pass

    data_type = None
    present_direct_value = True

    def __init__(self, name, derived=False, source=None, order_source=None,
                 filter_source=None, keyword_source=None, writeable=False,
                 orderable=False, filterable=False, present_direct_value=None):
        """
        Initialize a field instance.

        Provide a `name` for the field; by default this will serve both
        as the public fieldname (in the serialized data) and the name
        used in your code to refer to that field. If you want to render
        the fieldname as camelCase but refer to it using snake_case,
        then just provide the snake case version using `name` and set
        the `public_name` attribute on the object to be the
        `camelcase_name`. (The `SimpleSerializer` class takes care of
        this automatically if you've set the
        `REST_FRAMEWORK['CAMELCASE_FIELDNAMES']` setting to True.)

        If the field is writeable, pass `writeable=True`. If it can be
        used for ordering a resource listing, pass `orderable=True`. If
        it can be used for filtering a resource listing, pass
        `filterable=True`. All of these default to False.
        
        If you need to override the class attribute
        `present_direct_value` on an instance, then pass that as a
        kwarg set to True or False. Default is True.

        The `derived` and `*source` kwargs define what fields from the
        source object are used to implement various field features. If
        `derived` is True, then it's assumed the field does not tie
        directly to any fields on the source object. In that case
        you'll need to make sure to implement a custom `present` method
        or `present_{field}` method on the serializer. Otherwise, if
        `derived` is False: `source` is the name of this field on the
        source object; `order_source` is the name of the source obj
        field you want to use when sorting a list of resources on this
        field; `keyword_source` is the name of the source obj field you
        want to use for 'keywords' filters (i.e. text searches) on this
        field; and `filter_source` is the name of the source obj field
        you want to use for non-keyword filters on this field.

        Note that you aren't prevented from passing conflicting values
        for the `*able` and `*source` kwargs. I.e., you can set
        `orderable=False` but also set an `order_source`. In these
        cases it's up to the serializer how to handle it (though, by
        default, the `SimpleSerializer` class assumes the `*able` value
        takes precedence). Source fields are also optional and have
        sensible defaults: if `derived` is False and `source` is not
        provided, `source` defaults to `name`. (So if your serializer
        field and source field share a name, you don't have to repeat
        it.) If e.g. `orderable` is True and `order_source` is not
        provided, `order_source` defaults to `source`. `filter_source`
        behaves like `order_source`, but `keyword_source` defaults to
        `filter_source`, if not provided.
        """
        self.name = name
        self.camelcase_name = self.name_to_camelcase(name)
        self.public_name = name
        source = source or (name if not derived else None)
        order_source = order_source or (source if orderable else None)
        filter_source = filter_source or (source if filterable else None)
        keyword_source = keyword_source or filter_source
        self.sources = {
            'main': source, 
            'order': order_source,
            'filter': filter_source,
            'keyword': keyword_source
        }
        self.writeable = writeable
        self.orderable = orderable
        self.filterable = filterable
        if present_direct_value is not None:
            self.present_direct_value = present_direct_value

    def name_to_camelcase(self, name):
        """
        Convert the given str (`name`) to camelCase.
        """
        if name.startswith('_'):
            suffix = name.lstrip('_')
            cc_suffix = camel_case.render.underscoreToCamel(suffix)
            return ''.join(['_' * (len(name) - len(suffix)), cc_suffix])
        return camel_case.render.underscoreToCamel(name)

    @classmethod
    def cast_one_to_python(cls, value):
        """
        Cast one atomic value to the correct Python type.

        Default behavior is to try casting to the type in the
        `data_type` attribute.

        Override this in a subclass if you need custom behavior for a
        given field or type of field.
        """
        return cls.data_type(value) if callable(cls.data_type) else value

    @classmethod
    def cast_to_python(cls, value):
        """
        Cast a value (may be one or many) to the correct Py type.
        """
        err_msg = ("Could not convert value '{{}}' to type "
                   "{}.".format(cls.data_type))
        if isinstance(value, (list, tuple)):
            try:
                return [cls.cast_one_to_python(v) for v in value]
            except ValueError:
                for v in value:
                    try:
                        cls.cast_one_to_python(v)
                    except ValueError:
                        raise cls.ValidationError(err_msg.format(v))
        try:
            return cls.cast_one_to_python(value)
        except ValueError:
            raise cls.ValidationError(err_msg.format(value))

    def present(self, source_obj_data):
        """
        Present a data value for this field from the source obj data.

        Default behavior is to call `get_from_source` to get the value
        from the source object, and then, if the `present_direct_value`
        attribute is True, to return the value from the object,
        directly. Otherwise it tries to cast it to the appropriate
        type.

        Override this in a subclass if you need custom behavior for a
        given type of field.
        """
        value = self.get_from_source(source_obj_data)
        if value is not None and not self.present_direct_value:
            return self.cast_to_python(value)
        return value

    def get_from_source(self, source_obj_data):
        """
        Get data for this field from the source obj data dict.

        Default behavior is to pull the data from the `sources['main']`
        field.

        Override this in a subclass if you need custom behavior for a
        given type of field, such as if you're compiling a value from
        multiple source fields.
        """
        return source_obj_data.get(self.sources.get('main'))

    def parse_from_client(self, client_data):
        """
        Parse (and clean/validate) data for this field from the client.
        
        `client_data` is a dict or dict-like object containing all data
        from a client request. It will match the presentation format.
        Default behavior is to pull the value based on the
        `public_field` attribute and cast it to python.

        Override this in a subclass if you need custom behavior for a
        given type of field. (Raise a cls.ValidationError if the data
        is invalid.)
        """
        value = client_data.get(self.public_name)
        if value is not None:
            return self.cast_to_python(value)
        return value

    def convert_to_source(self, value, client_data):
        """
        Convert the given (client) value to the source data format.

        This is the inverse of `get_from_source`. The return value
        should be a dict mapping source fields to data values each
        should contain. Default behavior is to assume you're loading
        data back to the `source['main']` field.

        Override this in a subclass if you need custom behavior for a
        given type of field, such as if your serializer field converts
        to multiple source fields.
        """
        source = self.sources.get('main')
        return {} if source is None else {source: value}

    def apply_filter_to_qset(self, qval, op, negate, qset):
        """
        Apply a filter to the supplied queryset (`qset`).
        
        This is responsible for parsing the query value (`qval`) that
        the client provides based on the operator (`op` and `negate`).
        It should apply the filter using the django-style queryset
        interface and then return the filtered qset. `negate` is
        expected to be a boolean and should trigger an `exclude` type
        filter if True.

        Default behavior assumes that you're using the filter backend:
        `api.filters.SimpleQSetFilterBackend`. If you need different
        behavior for a given type of field, override this in a
        subclass.
        """
        which_source = 'keyword' if op == 'keywords' else 'filter'
        source = self.sources.get(which_source)
        if op == 'isnull':
            qval = helpers.cast_to_boolean(qval)
        elif op == 'matches':
            qval = str(qval)
        elif op != 'keywords':
            qval = self.cast_to_python(qval)
        if source is not None:
            criterion = {'__'.join((source, op)): qval}
            if negate:
                return qset.exclude(**criterion)
            return qset.filter(**criterion)
        return qset

    def emit_orderby_criteria(self, desc=False):
        """
        Return a list of orderby criteria for ordering this field.

        Assume the orderby criteria will be applied to a django-style
        queryset using the `orderby` method. Default behavior is to
        use the `sources['order']` field as the basis for ordering.

        Override this in a subclass if you need custom behavior for a
        given type of field.
        """
        source = self.sources.get('order')
        if source is not None:
            return [''.join(('-' if desc else '', source))]
        return []


class SimpleObjectInterface(object):
    """
    A simple internal-object interface for SimpleSerializer.
    """
    class DefaultType(dict):
        def save(self, *args, **kwargs):
            msg = ('SimpleObjectInterface.DefaultType does not implement '
                   'saving. Please specify a different type if you need to '
                   'be able to save the object.')
            raise NotImplementedError(msg)

    obj_type = DefaultType

    def __init__(self, obj_type=None):
        if obj_type is not None:
            self.obj_type = obj_type

    def get_obj_data(self, obj):
        data = obj if hasattr(obj, 'items') else getattr(obj, '__dict__', {})
        return data.copy()

    def make_obj_from_data(self, obj_data):
        if isinstance(self.obj_type, dict):
            return self.obj_type(obj_data)
        return self.obj_type(**obj_data)

    def obj_is_many(self, obj):
        many_types = (list, tuple, Sequence, django.db.models.query.QuerySet)
        return isinstance(obj, many_types)

    def save_obj(self, obj, save_args, save_kwargs):
       obj.save(*save_args, **save_kwargs)


class SimpleSerializer(object):
    """
    A "serializer" base class meant to simplify and streamline things
    for our use-cases, designed to be used with `SimpleField` fields
    and `SimpleView` views.

    Subclass this and set the `fields` class attribute to a list of
    `SimpleField` objects representing your serializer fields. Fields
    get serialized in the order you set.

    If needed, implement a custom SimpleObjectInterface class and set
    an instance of that to be the `obj_interface` attribute. Do this if
    you need to customize how the serializer interacts with objects for
    getting and saving data; if you have writeable fields and need to
    be able to save objects, you should at least override the default
    `obj_type` for your interface class to define how to save.

    If you need to override how any specific field instances handle
    data conversions, you should subclass the applicable `SimpleField`
    class(es) and override the appropriate method(s).

    Override `prepare_for_serialization` if you need to implement any
    pre-serialization code to prep object data before it's serialized.
    """
    fields = []
    camelcase_fieldnames = settings.REST_FRAMEWORK['CAMELCASE_FIELDNAMES']
    obj_interface = SimpleObjectInterface()

    def __init__(self, instance=None, data=None, context=None):
        self.object = instance
        self.raw_client_data = data
        self.context = context or {}
        self._data = None
        self.errors = []
        self.set_up_field_lookup()

    @classmethod
    def set_up_field_lookup(cls):
        if not hasattr(cls, 'field_lookup'):
            cls.field_lookup = {f.name: f for f in cls.fields}
            if cls.camelcase_fieldnames:
                for f in cls.fields:
                    f.public_name = f.camelcase_name
                    cls.field_lookup[f.camelcase_name] = f

    def try_field_data_from_client(self, f, client_data):
        old_ser_data = self.data or {}
        old_val = f.parse_from_client(old_ser_data)
        new_val = f.parse_from_client(client_data)
        if new_val != old_val:
            if not f.writeable:
                logger.info('{}|{}|{}'.format(f.name, old_val, new_val))
                msg = '{} is not a writeable field.'.format(f.public_name)
                raise f.ValidationError(msg)
            return f.convert_to_source(new_val, client_data)

    def prepare_for_serialization(self, obj_data):
        """
        Run preparation or setup for serializing obj_data.

        This runs once for each object before serializing fields.
        Override in a subclass if you need to do special setup.
        """
        return obj_data

    def to_representation(self, obj):
        """
        Serializes an object (or sequence of objects) based on field
        specifications.
        """
        if self.obj_interface.obj_is_many(obj):
            data = []
            for o in obj:
                data.append(self.to_representation(o))
            return data

        data = OrderedDict()
        if obj is not None:
            obj_data = self.obj_interface.get_obj_data(obj)
            obj_data = self.prepare_for_serialization(obj_data)
            for f in self.fields:
                data[f.public_name] = f.present(obj_data)
        return data

    def prevalidate_client_data(self, client_data):
        errors = []
        if client_data is None:
            errors.append('No input provided.')
        elif not hasattr(client_data, 'items'):
            msg = 'Input must be a single dict-like object.'
            if isinstance(client_data, (list, tuple)):
                msg = '{} (Batch additions/updates not supported.)'.format(msg)
            errors.append(msg)
        return errors

    def to_internal_value(self, client_data):
        obj_has_changed = False
        new_obj_data = {}
        self.errors = self.prevalidate_client_data(client_data)
        if len(self.errors) == 0:
            new_obj_data = self.obj_interface.get_obj_data(self.object)
            for f in self.fields:
                try:
                    new_vals = self.try_field_data_from_client(f, client_data)
                except f.ValidationError as e:
                    msg = 'Field `{}` did not validate: {}'.format(f.name, e)
                    self.errors.append(msg)
                else:
                    if len(self.errors) == 0 and new_vals is not None:
                        new_obj_data.update(new_vals)
                        obj_has_changed = True
        return new_obj_data if obj_has_changed else None

    def is_valid(self):
        num_errors = len(self.errors)
        if self.raw_client_data is not None:
            self.errors = []
            data = self.to_internal_value(self.raw_client_data)
            num_errors = len(self.errors)
            if num_errors == 0:
                if data is not None:
                    self.object = self.obj_interface.make_obj_from_data(data)
                self._data = None
                self.raw_client_data = None
        return num_errors == 0

    @property
    def data(self):
        if self._data is None:
            if self.object is not None:
                self._data = self.to_representation(self.object)
        return self._data

    def save(self, *args, **kwargs):
        self.obj_interface.save_obj(self.object, args, kwargs)

    def replace_data(self, data):
        self.raw_client_data = data
        self.errors = []


class SimpleSerializerWithLookups(SimpleSerializer):
    """
    Base class to be used to simplify and improve performance of
    serializers that require lookups of some sort on each item--
    either in a search index or a database. The goal is to minimize
    the number of lookups required to improve performance.

    To use, your child class should override the cache_all_lookups
    and cache_all_db_objects methods to specify how lookup values are
    derived. (Note that both are optional.)
    """
    _lookup_cache = {}
    _db_cache = {}

    def cache_all(self):
        self.cache_all_lookups()
        self.cache_all_db_objects()

    def cache_all_lookups(self):
        """
        Child classes should implement this method to load all lookup
        fields using self.cache_lookup
        """
        pass

    def cache_all_db_objects(self):
        """
        Child classes should implement this method to cache DB
        objects using self.cache_field.
        """
        pass

    def get_db_objects(self, model, key_field, match_field, prefetch):
        """
        In your cache_all_db_objects implementation, use this to get a
        set of DB objects from a particular model based on a key_field
        in the current model, & a match_field in the foreign-key model.
        """
        if isinstance(self.instance, (list, tuple)):
            objects = self.instance
        else:
            objects = [self.instance]
        keys = [getattr(obj, key_field) for obj in objects]
        qset = model.objects.filter(**{'{}__in'.format(match_field): keys})
        return qset.prefetch_related(*prefetch)

    def cache_lookup(self, fname, values):
        self._lookup_cache[fname] = values

    def get_lookup_value(self, fname, lookup_code):
        return self._lookup_cache.get(fname, {}).get(lookup_code, '')

    def cache_field(self, fname, pk, value):
        if fname in self._db_cache:
            self._db_cache[fname][str(pk)] = value
        else:
            self._db_cache[fname] = {str(pk): value}

    def get_db_field_value(self, fname, pk):
        if fname in self._db_cache and str(pk) in self._db_cache[fname]:
            return self._db_cache[fname][str(pk)]
        else:
            return None

    def to_representation(self, obj):
        if self.obj_interface.obj_is_many(obj):
            self.cache_all()
        return super().to_representation(obj)

