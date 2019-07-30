"""
Contains factories for generating test data for Solr.
"""
import ujson

import datetime
import pytz
import random
import fnmatch
from collections import OrderedDict


class DataEmitter(object):
    """
    Class containing emitter methods for generating randomized data.
    """

    default_emitter_defaults = {
        'string': {'mn': 10, 'mx': 20, 'alphabet': None},
        'text': {'mn_words': 2, 'mx_words': 10, 'mn_word_len': 1,
                 'mx_word_len': 20, 'alphabet': None},
        'int': {'mn': 0, 'mx': 9999999999},
        'date': {'mn': (2015, 1, 1, 0, 0), 'mx': None}
    }

    def __init__(self, alphabet=None, emitter_defaults=None):
        """
        On initialization, you can set up defaults via the two kwargs.
        If a default is NOT set here, the value(s) in the class
        attribute `default_emitter_defaults` will be used. All defaults
        can be overridden via the `emit` method.

        `alphabet` should be a list of characters that string/text
        emitter types will use by default. (This can be set separately
        for different types via `emitter_defaults`, but it's included
        as a single argument for convenience.)

        `emitter_defaults` is a nested dict that should be structured
        like the `default_emitter_defaults` class attribute. But, the
        class attribute is copied and then updated with user-provided
        overrides, so you don't have to provide the entire dictionary
        if you only want to override a few values.
        """
        user_defaults = emitter_defaults or {}
        combined_defaults = {}
        for k, v in type(self).default_emitter_defaults.items():
            combined_defaults[k] = v.copy()
            combined_defaults[k].update(user_defaults.get(k, {}))
            if 'alphabet' in v and combined_defaults[k]['alphabet'] is None:
                alphabet = alphabet or self.make_unicode_alphabet()
                combined_defaults[k]['alphabet'] = alphabet
        self.emitter_defaults = combined_defaults

    @staticmethod
    def make_unicode_alphabet(uchar_ranges=None):
        """
        Generate a list of characters to use for initializing a new
        DataEmitters object. Pass a nested list of tuples representing 
        the character ranges to include via `char_ranges`.
        """
        if uchar_ranges is None:
            uchar_ranges = [
                (0x0021, 0x0021), (0x0023, 0x0026), (0x0028, 0x007E),
                (0x00A1, 0x00AC), (0x00AE, 0x00FF)
            ]
        return [
            unichr(code) for this_range in uchar_ranges
                for code in range(this_range[0], this_range[1] + 1)
        ]

    def _emit_string(self, mn=0, mx=0, alphabet=None):
        """
        Generate a random unicode string with length between `mn` and
        `mx`.
        """
        length = random.randint(mn, mx)
        return ''.join(random.choice(alphabet) for _ in range(length))

    def _emit_text(self, mn_words=0, mx_words=0, mn_word_len=0,
                  mx_word_len=0, alphabet=None):
        """
        Generate random unicode multi-word text.

        The number of words is between `mn_words` and `mx_words` (where
        words are separated by spaces). Each word has a length between
        `mn_word_len` and `mx_word_len`.
        """
        text_length = random.randint(mn_words, mx_words)
        words = [self._emit_string(mn_word_len, mx_word_len, alphabet)
                 for n in range(0, text_length)]
        return ' '.join(words)

    def _emit_int(self, mn=0, mx=0):
        """
        Generate a random int between `mn` and `mx`.
        """
        return random.randint(mn, mx)

    def _emit_date(self, mn=(2000, 1, 1, 0, 0), mx=None):
        """
        Generate a random UTC date between `mn` and `mx`. If `mx` is
        None, the default is now. Returns a timezone-aware
        datetime.datetime obj.
        """
        min_date = datetime.datetime(*mn, tzinfo=pytz.utc)
        if mx is None:
            max_date = datetime.datetime.now(pytz.utc)
        else:
            max_date = datetime.datetime(*mx, tzinfo=pytz.utc)
        min_td = (min_date - datetime.datetime(1970, 1, 1, tzinfo=pytz.utc))
        max_td = (max_date - datetime.datetime(1970, 1, 1, tzinfo=pytz.utc))
        min_ts, max_ts = min_td.total_seconds(), max_td.total_seconds()
        new_ts = min_ts + (random.random() * (max_ts - min_ts))
        new_date = datetime.datetime.utcfromtimestamp(new_ts)
        return new_date.replace(tzinfo=pytz.utc)

    def _emit_boolean(self):
        """
        Generate a random boolean value.
        """
        return True if random.randint(0, 1) else False

    def _calculate_emitter_params(self, emtype, **user_params):
        """
        Return complete parameters for the given emitter type; default
        parameters with user_param overrides are returned.
        """
        params = self.emitter_defaults.get(emtype, {}).copy()
        params.update(user_params)
        return params

    def determine_max_unique_values(self, emtype, **user_params):
        """
        Return the maximum number of unique values possible for a given
        emitter type with the given user_params. This is mainly just so
        we can prevent infinite loops when generating unique values.
        E.g., the maximum number of unique values you can generate for
        an int range of 1 to 100 is 100, so it would be impossible to
        use those parameters for that emitter for a unique field. This
        really only applies to integers and strings. Dates and text
        values are either unlikely to repeat or unlikely ever to need
        to be unique.
        """
        params = self._calculate_emitter_params(emtype, **user_params)
        if emtype == 'int':
            return params['mx'] - params['mn'] + 1
        if emtype == 'string':
            mn, mx, alphabet = params['mn'], params['mx'], params['alphabet']
            return sum((len(alphabet) ** n for n in range(mn, mx + 1)))
        if emtype == 'boolean':
            return 2
        return None

    def get_type_emitter(self, emtype):
        return getattr(self, '_emit_{}'.format(emtype))

    def emit(self, emtype, **user_params):
        """
        Generate and emit a value using the given `emtype` and
        `user_params`.
        """
        params = self._calculate_emitter_params(emtype, **user_params)
        emitter = self.get_type_emitter(emtype)
        return emitter(**params)


class SolrDataGenFactory(object):
    """
    Factory for creating "gen" data generation functions.
    """

    default_emitter = DataEmitter()

    def __init__(self, emitter=None):
        """
        Pass an optional `emitter` obj for custom emitter methods.
        """
        self.emitter = emitter or type(self).default_emitter

    def __call__(self, function, max_unique=None):
        """
        Convert the given function to a "gen" (data generator) function
        for use in `SolrProfile` and `SolrFixtureFactory` objects.

        A `max_unique` value is only needed if you're making a custom
        gen function to be used with unique fields that can only create
        a small number of unique values.
        """
        def gen(record):
            return function(record)
        gen.max_unique = max_unique
        return gen

    def _make_choice_function(self, values, repeatable):
        choices = [v for v in values]
        random.shuffle(choices)
        def _choice_function(record):
            if repeatable:
                return random.choice(choices)
            return choices.pop() if choices else None
        return _choice_function

    def choice(self, values, repeatable=True):
        """
        Return a gen function that chooses randomly from the choices in
        the given `values` arg. Pass `repeatable=False` if choices
        cannot be repeated.
        """
        func = self._make_choice_function(values, repeatable)
        max_unique = len(values)
        return self(func, max_unique)

    def multi_choice(self, values, counter, repeatable=True):
        """
        Return a gen function that makes multiple random choices from
        the given `values` arg and returns the list of chosen values.
        `counter` is a function whose return value determines how many
        items are chosen. Pass `repeatable=False` if choices cannot be
        repeated.
        """
        ch = self._make_choice_function(values, repeatable)
        max_unique = len(values)
        return self(lambda r: [v for v in [ch(r) for _ in range(0, counter())]
                               if v is not None], max_unique)

    def type(self, emtype, **params):
        """
        Return an emitter gen function for the given emtype using the
        given params.
        """
        em = self.emitter.emit
        max_unique = self.emitter.determine_max_unique_values(emtype, **params)
        return self(lambda r: em(emtype, **params), max_unique)

    def multi_type(self, emtype, counter, **params):
        """
        Return a multi-value gen function for the given emitter using
        the given params. `counter` is a function whose return value
        determines how many values are generated.
        """
        em = self.emitter.emit
        max_unique = self.emitter.determine_max_unique_values(emtype, **params)
        return self(lambda r: [em(emtype, **params)
                               for _ in range(0, counter())], max_unique)

    def static(self, value):
        """
        Return a gen function that generates the given static value.
        """
        return self(lambda r: value, max_unique=1)

    # Following are `counter` methods--utility methods for generating
    # counter functions to use with `multi_choice` and `multi_type`.

    @staticmethod
    def static_counter(num):
        """
        Create a counter function that always returns the number passed
        in (`num`).
        """
        return lambda: num

    @staticmethod
    def random_counter(mn=0, mx=10):
        """
        Create a counter function that returns a random integer between
        `mn` and `mx`.
        """
        return lambda: random.randint(mn, mx)

    @staticmethod
    def precise_distribution_counter(total_into, total_from, mn, mx):
        """
        Create a counter function that attempts to evenly, but
        randomly, distribute a larger number (`total_from`) into a
        smaller one (`total_into`). Use this if, e.g., you have a
        number of children options you want assigned to a number of
        parent records, and you want to ensure every child is assigned
        to a parent.
        """
        counters = {'into_left': total_into, 'from_left': total_from}
        def _counter():
            if counters['into_left'] == 0:
                return 0
            if counters['into_left'] == 1:
                number = counters['from_left']
            else:
                upper_limit = counters['from_left'] - counters['into_left'] + 1
                local_max = upper_limit if upper_limit < mx else mx
                number = random.randint(mn, local_max)
            counters['into_left'] -= 1
            counters['from_left'] -= number
            return number
        return _counter


class SolrProfile(object):
    """
    Class used for creating objects that represent a Solr profile, i.e.
    a subset of fields from a particular schema.
    """

    class SchemaTypesError(Exception):
        """
        Exception raised when you try using a field in your profile
        that has a Solr type not included in the `solr_types` data
        structure used during initialization.
        """
        pass

    DEFAULT_SOLR_FIELD_TYPE_MAPPING = {
        'string': {'pytype': unicode, 'emtype': 'string'},
        'text_en': {'pytype': unicode, 'emtype': 'text'},
        'long': {'pytype': int, 'emtype': 'int'},
        'int': {'pytype': int, 'emtype': 'int'},
        'date': {'pytype': datetime.datetime, 'emtype': 'date'},
        'boolean': {'pytype': bool, 'emtype': 'boolean'}
    }

    def __init__(self, name, conn=None, schema=None, user_fields=None,
                 unique_fields=None, solr_types=None, gen_factory=None,
                 default_field_gens=None):
        """
        Initialize a `SolrProfile` object. Lots of options.

        `conn`, `schema`: The first is the pysolr connection object for
        the Solr index your profile covers; the second is a schema
        dataset you want to force. Provide one or the other; you don't
        need both. Normally you'll provide the `conn` and the schema
        will be grabbed automatically; `schema` overrides `conn` if
        both are provided. 

        `user_fields`: A list of field names (each of which should
        match a field name (whether static or dynamic) in the schema).
        Using the default of None assumes you want ALL the
        [non-dynamic] fields in the schema.

        `unique_fields`: A list or tuple of field names (each of which
        should match with a field name in the schema) where values
        should be unique in a given record set. Whatever field is the
        uniqueKey in your schema is already unique; you can include it
        or not.

        `solr_types`: A dict structure that tells the profile object
        how Solr schema types work, mapping each Solr type to a Python
        type and a DataEmitter type. See the
        DEFAULT_SOLR_FIELD_TYPE_MAPPING class attribute for an example.
        This is used as the default mapping if you don't provide one.

        `gen_factory`: The SolrDataGenFactory object you want to use
        for auto gen fields. Defaults to a plain object that uses a
        plain DataEmitter object.

        `default_field_gens`: A list (or tuple) of specific, non-auto
        gens that you want to use for specific fields in this profile.
        Each tuple item should be a (field_name, gen) tuple. This gets
        passed to the `set_field_gens` method, so see that for more
        info. You can also set (reset) the default field gens by
        calling that method directly after the profile object is
        initialized.
        """
        unique_fields = unique_fields or []
        solr_types = solr_types or type(self).DEFAULT_SOLR_FIELD_TYPE_MAPPING
        self.gen_factory = gen_factory or SolrDataGenFactory()
        self.conn = conn
        schema = schema or self.fetch_schema(conn)
        self.key_name = schema['uniqueKey']
        filtered_fields = self._filter_schema_fields(schema, user_fields)
        self._check_schema_types(filtered_fields, solr_types)
        self.fields = {}
        for sf in filtered_fields:
            field = type(self).Field({
                'name': sf['name'],
                'is_key': sf['name'] == self.key_name,
                'type': sf['type'],
                'emtype': solr_types[sf['type']]['emtype'],
                'pytype': solr_types[sf['type']]['pytype'],
                'multi': sf.get('multiValued', False),
                'unique': sf['name'] in list(unique_fields) + [self.key_name]
            }, gen_factory)
            self.fields[field['name']] = field
        self.name = name
        self.set_field_gens(*(default_field_gens or tuple()))

    @staticmethod
    def fetch_schema(conn):
        """
        Fetch the Solr schema in JSON format via the provided pysolr
        connection object (`conn`).
        """
        jsn = conn._send_request('get', 'schema?wt=json')
        return ujson.loads(jsn)['schema']

    def _get_schema_field(self, schema_fields, name):
        """
        Return a dict from the Solr schema for a field matching `name`.
        Returns the first match found, or None.
        """
        for field in schema_fields:
            if fnmatch.fnmatch(name, field['name']):
                field = field.copy()
                field['name'] = name
                return field
        return None

    def _filter_schema_fields(self, schema, user_fields):
        if not user_fields:
            return schema['fields']

        schema_fields = schema['fields'] + schema.get('dynamicFields', [])
        return_fields = []
        for ufname in user_fields:
            field = self._get_schema_field(schema_fields, ufname)
            if field is not None:
                return_fields.append(field)
        return return_fields

    def _check_schema_types(self, schema_fields, solr_types):
        schema_types = set([field['type'] for field in schema_fields])
        unknown_types = schema_types - set(solr_types.keys())
        if len(unknown_types) > 0:
            msg = ('Found field types in Solr schema that do not have '
                   'matching entries in the defined Solr field type mapping '
                   '(`solr_types` arg). {}').format(', '.join(unknown_types))
            raise type(self).SchemaTypesError(msg)

    def set_field_gens(self, *field_gens):
        """
        Set the default list of field_gen tuples to use for generating
        data via the fixture factory. Each field_gen is a (field_name,
        gen) tuple.

        Note that field gens will get called in the order specified,
        so make sure any gens that rely on existing field data are
        listed after the fields they depend on. You can set the `gen`
        portion of the tuple to the string 'auto' if you want to use
        the default generator, or None if you just want a placeholder.
        """
        field_gens = OrderedDict(field_gens)
        for fname, field in self.fields.items():
            if field_gens.get(fname, 'auto') == 'auto':
                field_gens[fname] = field.auto_gen
        self.field_gens = field_gens

    class Field(dict):
        """
        Internal class used for individual fields in the `fields`
        attribute of a `SolrProfile` object. Subclass of `dict`. The
        field attributes (like `multi` and `unique`) are simply normal
        dict members. Also provides methods for generating and
        converting data values.
        """

        class ViolatesUniqueness(Exception):
            """
            Exception raised when it's impossible to generate a unique
            value for a given record set (e.g. if all unique values are
            used up).
            """
            pass

        def __init__(self, params, gen_factory):
            super(SolrProfile.Field, self).__init__(params)
            if self['multi']:
                if self['unique']:
                    msg = ('Uniqueness for multivalued fields is not '
                           'implemented.')
                    raise NotImplementedError(msg)
                counter = gen_factory.random_counter(1, 10)
                self.auto_gen = gen_factory.multi_type(self['emtype'], counter)
            else:
                self.auto_gen = gen_factory.type(self['emtype'])

        def to_python(self, val):
            """
            Force the given value to the right Python type.
            """
            def dtype(val):
                _type = self['pytype']
                return val if isinstance(val, _type) else _type(val)

            if isinstance(val, (list, tuple, set)):
                vals = [dtype(v) for v in val if v is not None]
                if vals:
                    return vals if self['multi'] else vals[0]
            else:
                val = dtype(val) if val is not None else None
                return [val] if (self['multi'] and val is not None) else val

        def _do_gen(self, gen, record):
            return self.to_python(gen(record))

        def _do_unique_gen(self, gen, record, records):
            value = self._do_gen(gen, record)
            fieldvals = set([r.get(self['name'], None) for r in records])

            if gen.max_unique is not None:
                if value in fieldvals and len(records) >= gen.max_unique:
                    raise type(self).ViolatesUniqueness
            
            while value in fieldvals:
                value = self._do_gen(gen, record)
            return value

        def gen_value(self, gen=None, record=None, records=None):
            """
            Generate a value for this field type, optionally using the
            provided `gen`. A default auto gen is used, otherwise.
            `record` and `records` are optional, strictly speaking, but
            needed when generating values for a set of records.
            """
            record = record or {}
            records = records or []
            gen = gen or self.auto_gen
            if self['unique']:
                return self._do_unique_gen(gen, record, records)
            return self._do_gen(gen, record)


class SolrFixtureFactory(object):
    """
    Class that creates record data to be used in Solr-related test
    fixtures. Data created conforms to a particular `SolrProfile`
    object, passed in on initialization.
    """

    def __init__(self, profile):
        """
        Initialize a `SolrFixtureFactory` object. `profile` is the
        `SolrProfile` object that this will use to generate data.
        """
        self.profile = profile
        self.conn = self.profile.conn

    def _make_record(self, records, field_gen_overrides):
        rec = {}
        for fname, default_gen in self.profile.field_gens.items():
            gen = field_gen_overrides.get(fname, default_gen)
            if gen is not None:
                field = self.profile.fields[fname]
                value = field.gen_value(gen, rec, records)
                if value is not None:
                    rec[fname] = value
        return rec

    def make_more(self, rset_one, number, **field_gen_overrides):
        """
        Make a new set (list) of records that are a subset of some
        existing set.

        `rset_one`: The list of existing records. These are used when
        determining uniqueness for the new records but is NOT modified.

        `number`: How many new records you want to create.

        `field_gen_overrides`: Set of kwargs for the gens you want to
        use to generate this batch of records. E.g., fieldname=gen.
        These override whatever defaults you set on the profile.
        """
        rset_two = []
        rset_combined = [record for record in rset_one]
        for _ in range(0, number):
            try:
                record = self._make_record(rset_combined, field_gen_overrides)
            except self.profile.Field.ViolatesUniqueness:
                break
            else:
                rset_combined.append(record)
                rset_two.append(record)
        return rset_two

    def make(self, number, **field_gen_overrides):
        """
        Make a new set (list) of records. Calls the `make_more` method
        using an empty initial list. (See `make_more` for more info.)
        """
        return self.make_more([], number, **field_gen_overrides)
