from __future__ import absolute_import
from collections import OrderedDict, Sequence

import django.db.models.query

from utils import solr

import logging
import six

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


class SimpleSerializer(object):
    """
    A simplified "serializer" base class that works quickly with basic
    dict and dict-like objects that don't require a lot of fuss.
    Subclass this class, define a dict of valid fields to be serialized
    (where keys are fieldnames and values are dicts of settings) plus
    optional process_[fieldname] methods to parse the field values for
    rendering. If you want your fields always to appear in the specifed
    order when serialized, be sure fields is an OrderedDict.

    Since my use-case involves serialization straight to/from Solr,
    that's what this class assumes, but it shouldn't be hard to tweak
    it to work with other backends.

    A word about the dicts of settings: You can set up whatever
    settings you want based on your needs. Here are the few that I'm
    using.

    type: the Python type for the object stored in the field. Used in
    the Haystack filter to do basic type-based normalization operations
    on incoming data.

    source: specifies the name of the field in the source data, in case
    you want to have a fieldname in the serialized output that differs
    from the source. If this is not included, it defaults to using the
    fields dict element as the name of the field in the source data.

    writable: boolean value that indicates whether or not the field is
    writable. When de-serializing data (from client input to source),
    it checks the provided data against the source to make sure there
    are no differences for fields where writable is False. If writable
    is not provided, it defaults to False.

    derived: boolean value that indicates whether or not the field is
    stored on the object. If True, the field is derived and therefore
    not stored on the object. When True, you should specify a "process"
    method for the field on the serializer that derives the value. This
    is essentially equivalent to DRF serializers' "method" fields. If
    derived is not provided, it defaults to False.
    """
    fields = OrderedDict()

    def __init__(self, instance=None, data=None, context=None):
        self.context = context or {}
        self.object = instance
        self.init_data = data
        self._data = None
        self._errors = None

    def render_field_name(self, field_name):
        """
        Override this method to render field names differently than
        they are stored and referenced in/on the serializer class; for
        instance, you may want to render field names as camel case for
        JSON output but otherwise store and reference field names using
        snake case.
        """
        return field_name

    def restore_field_name(self, field_name):
        """
        Override this method to specify the reverse function for
        restoring a field name from the version rendered via the
        render_file_name method. For instance, restoring a field
        provided in camel case back to snake case.
        """
        return field_name

    def obj_is_sequence(self, obj):
        return isinstance(obj, (list, tuple, Sequence,
                                django.db.models.query.QuerySet))

    def to_native(self, obj=None):
        """
        Serializes an object (or sequence of objects) based on field
        specifications.
        """
        obj = obj or self.object
        data = obj

        if self.obj_is_sequence(obj):
            data = []
            for o in obj:
                data.append(self.to_native(o))
            return data

        data = OrderedDict()
        if obj is not None:
            # obj could be dict-like or have attributes
            if hasattr(obj, 'iteritems'):
                obj_dict = obj
            else:
                obj_dict = getattr(obj, '__dict__', {})

            for fname, fsettings in six.iteritems(self.fields):
                obj_fname = fsettings.get('source', fname)
                dtype = fsettings.get('type', None)
                derived = fsettings.get('derived', False)
                value = None if derived else obj_dict.get(obj_fname, None)

                process_type = getattr(self, 'process_{}_type'
                                             ''.format(dtype), None)
                if process_type is not None:
                    value = process_type(value, obj)

                process = getattr(self, 'process_{}'.format(fname), None)
                if process is not None:
                    value = process(value, obj)

                data[self.render_field_name(fname)] = value
        return data

    def perform_validation(self, data):
        """
        Runs all validation routines on client-provided data. Note that
        your class may provide validate_{} and validate_type_{} methods
        to validate field data and field type data, respectively. These
        methods should write errors to self._errors and return
        validated and cleaned data, appropriate for plugging back into
        the data used to create the deserialized object.
        """
        obj = self.object or {}

        for fname, fsettings in six.iteritems(self.fields):
            old_val = self.data.get(self.render_field_name(fname))
            new_val = data.get(self.render_field_name(fname))
            writable = fsettings.get('writable', False)
            ftype = fsettings.get('type', None)
            type_validator = getattr(self, 'validate_type_{}'
                                           ''.format(ftype), None)
            validator = getattr(self, 'validate_{}'.format(fname), None)

            if new_val is not None:
                if type_validator is not None:
                    new_val = type_validator(data, obj, fsettings)
                if validator is not None:
                    new_val = validator(data, obj, fsettings)
                data[fname] = new_val

            if not writable and new_val != old_val:
                logger.info('{}|{}|{}'.format(fname, old_val, new_val))
                self._errors.append('{} is not a writable field.'
                                    ''.format(self.render_field_name(fname)))
        if not self._errors:
            return data

    def restore_object(self, data, instance=None):
        """
        The data parameter should be a dictionary of attributes that
        needs to be converted into an object instance, useful for
        saving/storing. Override this method to control how
        deserialized objects get instantiated.

        Note that the method provided here assumes you're going from an
        abstract resource to Solr, where you may have fields in Solr
        that aren't on the object. Fields not on the object need to be
        added from the Solr doc before it's written back to Solr so
        that no data is lost.
        """
        if not hasattr(data, 'iteritems'):
            self._errors.append('Input must be a single object.')
            data = None
        else:
            new_obj_data = {}
            old_obj = self.object
            
            # obj could be dict-like or have attributes
            if hasattr(old_obj, 'iteritems'):
                old_obj_dict = old_obj
            else:
                old_obj_dict = getattr(old_obj, '__dict__', {})

            for fname, fsettings in six.iteritems(self.fields):
                # we only want to populate the field on the object if
                # it's not a derived field
                if not fsettings.get('derived', False):
                    obj_fname = fsettings.get('source', fname)
                    new_val = data.get(self.render_field_name(fname), None)
                    new_obj_data[obj_fname] = new_val
            
            populated_fields = list(new_obj_data.keys())
            for obj_fname, obj_val in six.iteritems(old_obj_dict):
                if obj_fname not in populated_fields:
                    new_obj_data[obj_fname] = obj_val

        return solr.Result(new_obj_data)

    def from_native(self, data):
        self._errors = []
        if data is not None:
            attrs = self.perform_validation(data)
        else:
            self._errors.append('No input provided.')

        if not self._errors:
            return self.restore_object(attrs, instance=getattr(self, 'object',
                                                               None))

    def is_valid(self):
        return not self.errors

    @property
    def data(self):
        if self._data is None:
            self._data = self.to_native(self.object)
        return self._data

    @property
    def errors(self):
        if self._errors is None:
            errors = []
            data = getattr(self, 'init_data', self.data)
            if isinstance(data, (list, tuple)):
                errors.append('Batch additions/updates not supported. Can '
                              'only add/update one object at a time.')
            self.object = self.from_native(data)
            errors.extend(self._errors)
        return self._errors

    def save(self, **kwargs):
        """
        The object your saving should have a save method on it.
        Override this as needed based on whatever type of object you're
        serializing.
        """
        self._data = None
        self.object.save(**kwargs)

    def replace_data(self, data):
        self.init_data = data


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
        return model.objects.filter(
                **{'{}__in'.format(match_field): 
                    keys}).prefetch_related(*prefetch)

    def cache_lookup(self, fname, values):
        self._lookup_cache[fname] = values

    def get_lookup_value(self, fname, lookup_code):
        try:
            ret_val = self._lookup_cache[fname][lookup_code]
        except KeyError:
            ret_val = ''
        return ret_val

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

    def to_native(self, obj=None):
        if self.obj_is_sequence(obj):
            self.cache_all()
        return super(SimpleSerializerWithLookups, self).to_native(obj)
