from __future__ import absolute_import

import logging
from collections import OrderedDict
from collections.abc import Sequence

import django.db.models.query
from django.conf import settings
from utils import camel_case, helpers
from utils.timer import TIMER

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


class SimpleSerializerException(Exception):
    pass


class SimpleField(object):
    """
    A simplified base field type for SimpleSerializer.
    """
    class ValidationError(SimpleSerializerException):
        pass

    data_type = None
    def __init__(self, name, derived=False, source=None, order_source=None,
                 filter_source=None, keyword_source=None, writeable=False,
                 orderable=False, filterable=False):
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

    def name_to_camelcase(self, name):
        if name.startswith('_'):
            return name
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

        Default behavior is to try to get the value from the obj data
        (dict) using the `source` attribute, and then to cast that to
        the appropriate type.

        Override this in a subclass if you need custom behavior for a
        given field or type of field.
        """
        value = source_obj_data.get(self.sources['main'])
        if value is not None:
            return self.cast_to_python(value)
        return value

    def parse_from_client(self, value, client_data):
        """
        Parse (and clean/validate) the given value from the client.

        Data from the client will match the presentation format.

        Default behavior is to cast the supplied value to python.

        Override this in a subclass if you need custom behavior for a
        given field or type of field. Raise a cls.ValidationError if
        the data is invalid.
        """
        return self.cast_to_python(value)

    def compile_source_fields(self, value, client_data):
        source = self.sources.get('main')
        if source is not None:
            return {source: value}
        return {}

    def apply_filter_to_qset(self, qval, op, negate, qset):
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
        return obj if hasattr(obj, 'items') else getattr(obj, '__dict__', {})

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
    A simplified "serializer" base class that works quickly with ...
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

    def do_present_field(self, f, obj_data):
        method = 'present_{}'.format(f.name)
        present = getattr(self, method, f.present)
        return present(obj_data)

    def do_parse_field_from_client(self, f, client_data):
        method = 'parse_{}_from_client'.format(f.name)
        parse = getattr(self, method, f.parse_from_client)
        return parse(client_data.get(f.public_name), client_data)

    def do_convert_field_to_internal(self, f, client_data):
        method = 'convert_{}_to_internal_fields'.format(f.name)
        to_internal = getattr(self, method, f.compile_source_fields)
        return to_internal(new_val, client_data)

    def do_emit_field_orderby_criteria(self, f, desc=False):
        method = 'emit_{}_orderby_criteria'.format(f.name)
        emit = getattr(self, method, f.emit_orderby_criteria)
        return emit(desc)

    def do_apply_field_filter_to_qset(self, f, qval, op, negate, qset):
        method = 'apply_{}_filter_to_qset'.format(f.name)
        apply_filter = getattr(self, method, f.apply_filter_to_qset)
        return apply_filter(qval, op, negate, qset)

    def do_apply_orderby_to_qset(self, fields, direction, qset):
        method = 'apply_{}_orderby_to_qset'.format(f.name)
        apply_orderby = getattr(self, method, f.apply_orderby_to_qset)
        return apply_orderby(direction, qset)

    def try_field_data_from_client(self, f, client_data):
        old_ser_data = self.data or {}
        old_val = old_ser_data.get(f.public_name)
        new_val = self.do_parse_field_from_client(f, client_data)
        if new_val != old_val:
            if not f.writable:
                logger.info('{}|{}|{}'.format(f.name, old_val, new_val))
                msg = '{} is not a writable field.'.format(f.public_name)
                raise f.ValidationError(msg)
            return self.do_convert_field_to_internal(f, new_val, client_data)

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
            TIMER.start('SERIALIZE LIST')
            data = []
            for o in obj:
                data.append(self.to_representation(o))
            TIMER.end('SERIALIZE LIST')
            return data

        data = OrderedDict()
        if obj is not None:
            obj_data = self.obj_interface.get_obj_data(obj)
            obj_data = self.prepare_for_serialization(obj_data)
            for f in self.fields:
                data[f.public_name] = self.do_present_field(f, obj_data)
        return data

    def prevalidate_client_data(self, client_data):
        errors = []
        if client_data is None:
            errors.append('No input provided.')
        elif not hasattr(data, 'items'):
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
            new_obj_data = self.obj_interface.get_obj_data(self.object).copy()
            for f in self.fields:
                try:
                    new_vals = self.try_field_data_from_client(f, client_data)
                except f.ValidationError as e:
                    msg = 'Field `{}` did not validate: {}'.format(fn, e)
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
        return num_errors > 0

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

