"""
Custom field types used by base.models.

Currently this only contains an implementation of a virtual composite
key field (and a few helper classes), to get around the fact that
Django does not implement composite keys in any shape or form.

Note that this NOT a proper composite key implementation, so, generally
speaking, you shouldn't actually try to *use* fields of this type for
anything. They exist mainly to satisfy models' PK requirements,
specifically for Sierra models where the underlying DB table has no
proper PK and we can't add one ourselves. In order to build the test
database for Sierra based on the Sierra models, we need a simulated PK
that works with existing DB columns.

Notably, this isn't designed to be used as a foreign key in a
relationship. The Sierra tables will never need to do that, anyway:
foreign keys in Sierra are always implemented as single database
columns.
"""
from __future__ import absolute_import

import re

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import functions
from django.utils.functional import cached_property
from six import text_type


class CompositeColumn(models.expressions.Col):
    """
    This is what the VirtualCompField.get_col method returns. It's
    a Col subtype, but the `as_sql` method creates a Concat expression
    based on the target field's `partfields`. This is of course
    horribly inefficient in lookup queries, since it isn't indexed, but
    it does work.

    Mainly this is meant to prevent errors when something absolutely
    requires some SQL expression representing the column and there's no
    other way to override that.
    """

    def as_sql(self, compiler, connection):
        """
        Returns tuple: sql, params -- where %s is used in place of
        caller/user-supplied values in the SQL, and `params` is that
        list of values.

        In this case, we want a Concat function that will produce the
        desired composite value from the subparts and separator defined
        in the main field. Fortunately, Django has an existing Concat
        function expression class we can use. We just have to turn
        *everything* into an `Expression` class first.
        """
        sep_exp = models.expressions.Value(self.target.separator)
        concat_exps = []
        for i, field in enumerate(self.target.partfields):
            if i > 0:
                concat_exps.append(sep_exp)
            concat_exps.append(field.get_col(self.alias))
        return_exp = functions.Concat(*concat_exps)
        return return_exp.as_sql(compiler, connection)


class CompositeValueTuple(tuple):
    """
    Custom tuple type that handles value conversions and validation for
    VirtualCompField. Each instance is tied to the field that created
    it and can be validated against it. Instances can be created
    directly, or using `from_raw`. Casting to `str` will create a
    composite string value, using the appropriate field separator; you
    can round-trip the conversion by passing the string to `from_raw`.
    """

    def __new__(cls, values, field=None):
        self = super(CompositeValueTuple, cls).__new__(cls, tuple(values))
        self.field = field
        return self

    def __str__(self):
        return self.to_string()

    def __unicode__(self):
        return text_type(self.to_string())

    @classmethod
    def from_raw(cls, value, field):
        """
        Factory method. Attempts to produce the most valid
        CompositeValueTuple possible given the `field` and the raw
        `value`. If the raw value is a composite string and can be
        split using the field separator, then it casts each part to the
        appropriate type using the `each_partfield_run_to_python` method
        on the field. Otherwise, it creates a value using an invalid
        (or questionably valid) set of values.
        """
        try:
            cv_tuple = tuple((None if v == '' else text_type(v)
                              for v in value.split(field.separator)))
        except AttributeError:
            # Not a string.
            try:
                cv_tuple = tuple(value)
            except TypeError:
                # Not a sequence--wrap in a tuple.
                cv_tuple = (value,)
        try:
            return cls(field.each_partfield_run_to_python(cv_tuple), field)
        except Exception:
            return cls(cv_tuple, field)

    def to_string(self, re_encode=False):
        """
        Convert this tuple to a composite string by joining on the
        field separator value. If the string is for a regex lookup,
        set `re_encode` to True. Default is False.
        """
        if re_encode:
            separator = re.escape(self.field.separator)
        else:
            separator = self.field.separator
        strings = [str('' if s is None else s) for s in self]
        return str(separator.join(strings))

    def validate(self):
        """
        Validate this tuple value against the `field` attribute.
        Raises a Django ValidationError if the value is invalid.
        """
        value_length, expected_length = len(self), len(self.field.partfields)
        if value_length != expected_length:
            msg = ('instance has {} elements; expected {} ({} for field {}'
                   ''.format(value_length, expected_length,
                             self.field.partfield_names, self.field))
            raise ValidationError(msg)
        try:
            self.field.each_partfield_run_to_python(self)
        except Exception as e:
            msg = 'elements {} fail type conversion: {}'.format(self, e)
            raise ValidationError(msg)
        return True


class VirtualCompField(models.Field):
    """
    Use this to add a virtual or calculated composite field (one
    composed of other DB fields that is NOT backed by a DB column). Can
    be used as a PK.

    Use this in your model's class definition like you would other
    Field-types. When defining an instance, you must pass a
    `partfield_names` kwarg (tuple of strings) that tells it which
    attributes on this model to use to compose the field, in order.
    Accessing the field on a model instance returns a tuple of the
    values that compose it.

    The optional `separator` kwarg is the string to use as a separator
    when operations require that the composite field value be converted
    to a string, such as for serialization and certain DB operations.
    Default is "|_|".
    """
    description = ('A calculated or virtual field that can act as a composite '
                   'field, e.g. to act as a PK')
    default_separator = '|_|'

    def __init__(self, partfield_names=None, separator=default_separator,
                 *args, **kwargs):
        self.separator = separator
        self.partfield_names = tuple(partfield_names)
        super(VirtualCompField, self).__init__(*args, **kwargs)

    @property
    def partfields(self):
        """
        Custom property that returns a tuple containing the Field
        objects (ON the reference model) that compose this field.
        """
        return tuple([self.model._meta.get_field(f)
                      for f in self.partfield_names])

    @property
    def partfield_bases(self):
        """
        Custom property that returns a tuple containing Field objects
        that compose this field. Relationships are followed to arrive
        at the base Field object.
        """
        pf_list = []
        for pf in self.partfield_names:
            try:
                pf_list.append(self.model._meta.get_field(
                    pf).remote_field.model._meta.pk)
            except AttributeError:
                pf_list.append(self.model._meta.get_field(pf))
        return tuple(pf_list)

    def each_partfield_run_to_python(self, cvalue):
        """
        On the provided composite value (`cvalue`), run the `to_python`
        method for each partfield that makes up this composite field.
        Returns the results as a tuple.
        """
        return tuple([getattr(pf, 'to_python')(cvalue[i])
                      for i, pf in enumerate(self.partfield_bases)])

    def _make_field_value_property(self):
        """
        Private method that generates a property object that can be
        attached to a model to serve as the getter/setter for this
        field on model instances.
        """
        def _get(instance):
            values = []
            for pfname in self.partfield_names:
                pf = instance._meta.get_field(pfname)
                acc = '{}_id'.format(pf.name) if pf.is_relation else pf.name
                values.append(getattr(instance, acc, None))
            return CompositeValueTuple(values, self)

        def _set(instance, value):
            if value is None:
                try:
                    instance.refresh_from_db()
                except instance.DoesNotExist:
                    # In this case, the object has already been deleted from
                    # the database--probably via the instance's `delete`
                    # method, so we do nothing and don't raise an error.
                    return True
            original_value = getattr(instance, self.name)
            if original_value != value:
                msg = ('On model {}, tried to change value for virtual field '
                       '`{}`. Original value was {}, changed value was {}. '
                       '(Virtual fields cannot be changed directly.)'
                       '').format(self.model, self.name, original_value, value)
                raise NotImplementedError(msg)
        return property(_get, _set)

    def _set_up_unique_together(self, cls):
        """
        Private method that sets the `unique_together` _meta attribute
        for the model this field is associated with. (Used by the
        `contribute_to_class` method.)
        """
        curr_ut = list(cls._meta.unique_together)
        orig_ut = list(cls._meta.original_attrs.get('unique_together', []))
        new_ut_entry = tuple(self.partfield_names)
        new_c_ut, new_o_ut = (tuple(ut + [new_ut_entry])
                              for ut in (curr_ut, orig_ut))
        cls._meta.unique_together = new_c_ut
        cls._meta.original_attrs['unique_together'] = new_o_ut

    def contribute_to_class(self, cls, name, *args, **kwargs):
        """
        This method is called by the ModelBase metaclass' __new__
        method when new Model classes and objects are created. It does
        several things that the parent class' `contribute_to_class`
        doesn't: 1) it sets the `virtual_only` kwarg (`private_only` in
        later Django versions) but still does PK setup if this is a PK;
        2) it adds a property to the model class for accessing
        the computed composite value on model instances; 3) adds an
        appropriate `unique_together` constraint to the Meta object if
        this is a PK; and 4) sets up an appropriate `natural_key` on
        the model class if this is a PK.

        WARNING: `unique_together` fails for NULL values; you can
        create duplicates when one or more of the column values is
        NULL. There is not an easy way to work around this without
        creating custom database constraints, which is way outside the
        scope of what we're trying to do here. Just be mindful of that
        limitation.
        """
        try:
            cls._meta.private_fields
        except AttributeError:
            kwargs['virtual_only'] = True
        else:
            kwargs['private_only'] = True

        super(VirtualCompField, self).contribute_to_class(cls, name, *args,
                                                          **kwargs)
        setattr(cls, name, self._make_field_value_property())
        if self.primary_key:
            cls._meta.setup_pk(self)
            self._set_up_unique_together(cls)
            setattr(cls, 'natural_key', lambda my: tuple(my.pk))

    def get_attname_column(self):
        """
        There is no actual DB column underlying this field, so we
        override this method to provide just the attribute name,
        not the column. This forces self.concrete to be set to False
        via the `set_attributes_from_name` method.
        """
        attname = self.get_attname()
        return attname, None

    def get_col(self, alias, output_field=None):
        """
        Normally this method returns a Col expression object that
        represents the DB column that this field represents. For this
        field type, we must return a CompositeColumn instance instead
        so that proper SQL is generated in queries/filters.
        """
        if output_field is None:
            output_field = self
        if alias != self.model._meta.db_table or output_field != self:
            return CompositeColumn(alias, self, output_field)
        else:
            return self.cached_col

    @cached_property
    def cached_col(self):
        return CompositeColumn(self.model._meta.db_table, self)

    def deconstruct(self):
        """
        This method is required for all Field objects, so Django knows
        how to serialize and then reconstruct instances of this field.
        """
        name, path, args, kwargs = super(VirtualCompField, self).deconstruct()
        if self.separator != DEFAULT_VALUE_SEPARATOR:
            kwargs['separator'] = self.separator
        kwargs['partfield_names'] = self.partfield_names
        return name, path, args, kwargs

    def db_type(self, connection):
        """
        Just another measure to ensure that instances of this field are
        not treated as concrete.
        """
        return None

    def get_prep_value(self, value):
        """
        Default prep to convert a Python value to the form needed to do
        queries. In most cases that will be a string with each value
        joined using the appropriate separator value.
        """
        cv_tuple = CompositeValueTuple.from_raw(value, self)
        return cv_tuple.to_string()

    def to_python(self, value):
        """
        Use CompositeValueTuple to handle deserialization.
        """
        if value is None:
            return value
        return CompositeValueTuple.from_raw(value, self)


@VirtualCompField.register_lookup
class CompositeExact(models.lookups.Exact):
    """
    Exact match Lookup for VirtualCompField. Tries to break the
    provided lookup value into its constituent parts and create a
    multi-part, ANDed together WHERE statement for a nice efficient
    lookup.
    """

    def get_prep_lookup(self):
        """
        Prep the user lookup value (self.rhs) for the DB query.

        As of Django 1.10 this is no longer done as part of the custom
        Field object. Instead it's done on the Lookup object. In order
        for our exact matches to work as intended, we want to prep each
        individual value that comprises the composite value using the
        appropriate `get_prep_lookup` method for that field and its
        designated lookup.
        """
        if hasattr(self.rhs, '_prepare'):
            return self.rhs._prepare(self.lhs.output_field)

        field = self.lhs.output_field
        cv_tuple = CompositeValueTuple.from_raw(self.rhs, field)
        try:
            cv_tuple.validate()
        except ValidationError as e:
            msg = ('Problem converting {} to an exact match lookup value '
                   'for field {}: {}'.format(self.rhs, field, e))
            raise ValueError(msg)
        prepped = []
        for i, pf in enumerate(field.partfield_bases):
            col = pf.get_col(self.lhs.alias)
            lookup = pf.get_lookup('exact')
            if lookup and hasattr(lookup, 'get_prep_lookup'):
                prepped.append(lookup(col, cv_tuple[i]).get_prep_lookup())
            else:
                prepped.append(pf.get_prep_value(cv_tuple[i]))
        return prepped

    def _partfield_as_sql(self, field, value, compiler, connection):
        """
        Return exact-match lookup `as_sql` result for the given field
        and value. Lookup is converted to `isnull` if value is None.
        """
        p_col = field.get_col(self.lhs.alias)
        if value is None:
            lookup_type, value = ('isnull', True)
        else:
            lookup_type, value = ('exact', value)
        lookup = field.get_lookup(lookup_type)(p_col, value)
        return lookup.as_sql(compiler, connection)

    def as_sql(self, compiler, connection):
        """
        Return an SQL tuple: sql, params -- where %s is used in place
        of caller/user-supplied values in the SQL, and `params` is that
        list of values.

        Normally lookups return a `left op right` style SQL expression,
        such as "table.column = 1". But, here, we get a series of these
        expressions ANDed together, by decomposing the user-provided
        value (`self.rhs`), matching each value part with the
        corresponding part field, using the part field to generate the
        needed lookup SQL, and manually compiling the WHERE clause.
        """
        if self.rhs_is_direct_value():
            sql_list, params_list = [], []
            _, rhs_params = self.get_db_prep_lookup(self.rhs, connection)
            assert len(rhs_params) == 1
            part_values = rhs_params[0]
            try:
                for i, part in enumerate(self.lhs.target.partfields):
                    sql, params = self._partfield_as_sql(part, part_values[i],
                                                         compiler, connection)
                    params_list.extend(params)
                    sql_list.append(sql)
            except (IndexError, TypeError, KeyError) as e:
                raise ValueError(e)
            return ' AND '.join(sql_list), params_list
        else:
            # if self.rhs isn't a concrete value, we have to fall back
            # to the default (parent class) behavior--which is the much
            # less efficient method of concatenating the lhs child
            # columns via the CompositeColumn.as_sql method.
            return super(CompositeExact, self).as_sql(compiler, connection)


@VirtualCompField.register_lookup
class CompositeRegex(models.lookups.Regex):
    prepare_rhs = True

    def get_prep_lookup(self):
        """
        Prep the user lookup value (self.rhs) for the DB query.

        As of Django 1.10 this is no longer done as part of the custom
        Field object. Instead it's done on the Lookup object.
        """
        field = self.lhs.output_field
        if hasattr(self.rhs, '_prepare'):
            return self.rhs._prepare(field)

        cv_tuple = CompositeValueTuple.from_raw(self.rhs, field)
        return cv_tuple.to_string(True)


@VirtualCompField.register_lookup
class CompositeIRegex(CompositeRegex):
    lookup_name = 'iregex'


@VirtualCompField.register_lookup
class CompositeIExact(models.lookups.IExact):
    prepare_rhs = True


@VirtualCompField.register_lookup
class CompositeContains(models.lookups.Contains):
    prepare_rhs = True


@VirtualCompField.register_lookup
class CompositeIContains(models.lookups.IContains):
    prepare_rhs = True


@VirtualCompField.register_lookup
class CompositeIStartsWith(models.lookups.IStartsWith):
    prepare_rhs = True


@VirtualCompField.register_lookup
class CompositeStartsWith(models.lookups.StartsWith):
    prepare_rhs = True


@VirtualCompField.register_lookup
class CompositeIEndsWith(models.lookups.IEndsWith):
    prepare_rhs = True


@VirtualCompField.register_lookup
class CompositeEndsWith(models.lookups.EndsWith):
    prepare_rhs = True
