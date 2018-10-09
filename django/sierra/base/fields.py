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
from django.db import models
from django.db.models import functions
from django.utils.functional import cached_property
from django.core.exceptions import ValidationError


class VirtualCompField(models.Field):
    """
    Use this to add a virtual or calculated composite field (one
    composed of other DB fields that is NOT backed by a DB column). Can
    be used as a PK.

    The underlying purpose here is to fix some of the Sierra views
    where a single PK is lacking. The original hack (to use a non-
    unique field as the PK) doesn't work for generating our test DB
    and related fixtures.

    Use this in your model's class definition like you would other
    Field-types. When defining an instance, you must pass a
    `part_field_names` kwarg (tuple of strings) that tells it which
    attributes on this model to use to compose the field. Values are
    constructed in part_field_name order. You may pass an optional
    `separator` (string) kwarg to tell it what string to use as a
    separator between values. Default is underscore (_).
    """
    description = ('A calculated or virtual field that can act as a composite '
                   'field, e.g. to act as a PK')

    def __init__(self, separator='_', part_field_names=None, *args, **kwargs):
        self.separator = separator
        self.part_field_names = tuple(part_field_names)
        kwargs['serialize'] = False
        super(VirtualCompField, self).__init__(*args, **kwargs)

    @property
    def part_fields(self):
        """
        This is a custom property that returns a list of the Field
        objects that compose the composite key.
        """
        return [self.model._meta.get_field(f) for f in self.part_field_names]

    def _field_value_property(self):
        """
        Generates a property object that can be attached to a model to
        serve as the accessor for this field. The property calculates
        the composite key by pulling the values from the appropriate
        model attributes.
        """
        pfnames = [name for name in self.part_field_names]
        sep = self.separator
        def _get(instance):
            raw_vals = [getattr(instance, pfname, None) for pfname in pfnames]
            str_vals = []
            for raw_val in raw_vals:
                # If this is a relation field, we want the PK value
                val = getattr(raw_val, 'pk', raw_val)
                str_vals.append(unicode('' if val is None else val))
            return sep.join(str_vals)
        return property(_get)

    def contribute_to_class(self, cls, name, *args, **kwargs):
        """
        This method is called by the ModelBase metaclass' __new__
        method when new Model classes and objects are created. It does
        two things that the parent class' `contribute_to_class` method
        doesn't: 1) it adds a property to the model class for accessing
        the computed composite value on model instances, and 2) if this
        is a PK, then it adds an appropriate `unique_together`
        constraint to the Meta object.

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
        setattr(cls, name, self._field_value_property())
        if self.primary_key:
            cls._meta.setup_pk(self)
            curr_ut = list(cls._meta.unique_together)
            orig_ut = list(cls._meta.original_attrs.get('unique_together', []))
            new_ut_entry = tuple(self.part_field_names)
            new_c_ut, new_o_ut = (tuple(ut + [new_ut_entry])
                                  for ut in (curr_ut, orig_ut))
            cls._meta.unique_together = new_c_ut
            cls._meta.original_attrs['unique_together'] = new_o_ut

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
        if self.separator != '_':
            kwargs['separator'] = self.separator
        kwargs['part_field_names'] = self.part_field_names
        return name, path, args, kwargs

    def db_type(self, connection):
        """
        Just another measure to ensure that instances of this field are
        not treated as concrete.
        """
        return None

    def decompose_value(self, value, silent=True):
        """
        Custom method used to decompose the provided value into its
        constituent parts. Mainly used for exact match database
        lookups. Use silent=True if you want to suppress errors, e.g.
        so lookups on invalid values return no results instead of
        raising errors.
        """
        num_part_fields = len(self.part_field_names)
        decomposed = []
        try:
            try:
                values = value.split(self.separator)
            except AttributeError:
                msg = 'Value to decompose must be a str or unicode type.'
                raise ValidationError(msg)
            if len(values) != num_part_fields:
                msg = ('Value to decompose has incorrect number of parts or '
                       'incorrect separator. Expected {} parts and `{}` '
                       'separator.'.format(num_part_fields, self.separator))
                raise ValidationError(msg)
            if '' in values:
                values = [None if v == '' else v for v in values]
            for i, field in enumerate(self.part_fields):
                try:
                    to_python = field.rel.to._meta.pk.to_python
                except AttributeError:
                    to_python = field.to_python
                try:
                    decomposed.append(to_python(values[i]))
                except ValidationError as e:
                    msg = ('Composite value corresponding to field `{}` is '
                           'invalid: {}'.format(field.name, e))
                    raise ValidationError(msg)
        except ValidationError:
            if silent:
                decomposed = [None] * num_part_fields
            else:
                raise
        return decomposed


class CompositeColumn(models.expressions.Col):
    """
    This is what the VirtualCompField.get_col method returns. It's
    a Col subtype, but the `as_sql` method creates a Concat expression
    based on the target field's `part_fields`. This is of course
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
        exps_nested = [[f.get_col(self.alias), sep_exp]
                       for f in self.target.part_fields]
        exps_flattened = [item for sublist in exps_nested for item in sublist]
        return_exp = functions.Concat(*exps_flattened[:-1])
        return return_exp.as_sql(compiler, connection)


@VirtualCompField.register_lookup
class CompositeExact(models.lookups.Exact):
    """
    Exact match Lookup for VirtualCompField. Tries to break the
    provided lookup value into its constituent parts and create a
    multi-part, ANDed together WHERE statement for a nice efficient
    lookup.
    """

    def _partfield_to_sql(self, field, value, compiler, connection):
        """
        Private helper method that returns a tuple -- sql, params --
        for each individual partfield that composes the composite field
        along with the portion of the user-provided lookup value that
        corresponds with that partfield.

        The trick is that parts of a value can be empty; such values
        correspond with blank or NULL in the database and translate to,
        e.g., "(column IS NULL OR column = '')". If the field is not
        character data, then creating a blank lookup will fail, and the
        IS NULL lookup will be used by itself.

        If the value is not empty, then a straightforward exact-match
        lookup is used.
        """
        sql, params = None, None
        p_col = field.get_col(self.lhs.alias)
        if value is None:
            null_lookup = field.get_lookup('isnull')(p_col, True)
            null_sql, params = null_lookup.as_sql(compiler, connection)
            try:
                blank_lookup = field.get_lookup('exact')(p_col, '')
            except ValueError:
                sql = null_sql
            else:
                blank_sql, params = blank_lookup.as_sql(compiler, connection)
                sql = '( {} OR {} )'.format(null_sql, blank_sql)
        else:
            exact_lookup = field.get_lookup('exact')(p_col, value)
            sql, params = exact_lookup.as_sql(compiler, connection)
        return sql, params

    def as_sql(self, compiler, connection):
        """
        Returns tuple: sql, params -- where %s is used in place of
        caller/user-supplied values in the SQL, and `params` is that
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
            p_values = self.lhs.target.decompose_value(rhs_params[0])
            for i, part in enumerate(self.lhs.target.part_fields):
                sql, params = self._partfield_to_sql(part, p_values[i],
                                                     compiler, connection)
                params_list.extend(params)
                sql_list.append(sql)
            return ' AND '.join(sql_list), params_list
        else:
            # if self.rhs isn't a concrete value, we have to fall back
            # to the default (parent class) behavior--which is the much
            # less efficient method of concatenating the lhs child
            # columns via the CompositeColumn.as_sql method.
            return super(CompositeExact, self).as_sql(compiler, connection)

