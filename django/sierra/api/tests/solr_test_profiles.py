"""
Contains data structures for making Solr test data.
"""

import itertools
import random
import datetime

import pytest

from utils.test_helpers import solr_factories as sf
from utils.helpers import NormalizedCallNumber


GLOBAL_UNIQUE_FIELDS = ('django_id', 'code', 'id', 'record_number')
SOLR_TYPES = {
  'string': {'pytype': unicode, 'emtype': 'string'},
  'alphaOnlySort': {'pytype': unicode, 'emtype': 'string'},
  'text_en': {'pytype': unicode, 'emtype': 'text'},
  'text': {'pytype': unicode, 'emtype': 'text'},
  'textNoStem': {'pytype': unicode, 'emtype': 'text'},
  'long': {'pytype': int, 'emtype': 'int'},
  'slong': {'pytype': int, 'emtype': 'int'},
  'int': {'pytype': int, 'emtype': 'int'},
  'date': {'pytype': datetime.datetime, 'emtype': 'date'},
  'boolean': {'pytype': bool, 'emtype': 'boolean'},
}


# Multipurpose "gen" functions for generating Solr field data for
# certain fields or kinds of fields.

GENS = sf.SolrDataGenFactory()

def join_fields(fields, sep=' '):
    def gen(record):
        values = []
        for fname in fields:
            val = record.get(fname, None)
            if val is not None:
                if not isinstance(val, (list, tuple, set)):
                    val = [val]
                values.extend(val)
        return sep.join([unicode(v) for v in values])
    return gen


def copy_field(fname):
    return lambda rec: rec[fname]


def auto_increment(prefix='', start=0):
    increment = itertools.count()
    return lambda rec: '{}{}'.format(prefix, next(increment))


def auto_iii_recnum(typecode, start=0):
    increment = itertools.count(start)
    return lambda r: '{}{}'.format(typecode, next(increment))


# SolrProfile configuration (one profile for each API resource)

# CODES (includes Location, Itype, and ItemStatus)

CODE_FIELDS = ('django_ct', 'django_id', 'haystack_id', 'code', 'label',
               'type', 'text')
LOCATION_GENS = (
    ('django_ct', GENS.static('base.location')),
    ('django_id', GENS(auto_increment())),
    ('haystack_id', GENS(join_fields(('django_ct', 'django_id'), '.'))),
    ('code', GENS.type('string', mn=1, mx=5,
                       alphabet=list('abcdefghijklmnopqrstuvwxyz'))),
    ('label', 'auto'),
    ('type', GENS.static('Location')),
    ('text', GENS(join_fields(('code', 'label', 'type')))),
)
ITYPE_GENS = (
    ('django_ct', GENS.static('base.itypeproperty')),
    ('django_id', GENS(auto_increment())),
    ('haystack_id', GENS(join_fields(('django_ct', 'django_id'), '.'))),
    ('code', GENS.type('int', mn=1, mx=300)),
    ('label', 'auto'),
    ('type', GENS.static('Itype')),
    ('text', GENS(join_fields(('code', 'label', 'type')))),
)
ITEMSTATUS_GENS = (
    ('django_ct', GENS.static('base.itemstatusproperty')),
    ('django_id', GENS(auto_increment())),
    ('haystack_id', GENS(join_fields(('django_ct', 'django_id'), '.'))),
    ('code', GENS.type('string', mn=1, mx=1,
                        alphabet=list('abcdefghijklmnopqrstuvwxyz!$*'))),
    ('label', 'auto'),
    ('type', GENS.static('ItemStatus')),
    ('text', GENS(join_fields(('code', 'label', 'type')))),
)


# ITEMS

ITEM_FIELDS = ('django_ct', 'django_id', 'id', 'haystack_id', 'type',
    'suppressed', 'record_revision_number', 'record_number',
    'call_number_type', 'call_number', 'call_number_search',
    'call_number_sort', 'copy_number', 'volume', 'volume_sort',
    'barcode', 'local_code1', 'location_code', 'item_type_code', 'status_code',
    'public_notes', 'long_messages', 'price', 'copy_use_count',
    'internal_use_count', 'iuse3_count', 'number_of_renewals',
    'total_renewal_count', 'total_checkout_count',
    'last_year_to_date_checkout_count', 'year_to_date_checkout_count',
    'record_creation_date', 'record_last_updated_date', 'last_checkin_date',
    'due_date', 'checkout_date', 'recall_date', 'overdue_date',
    'parent_bib_id', 'parent_bib_record_number', 'parent_bib_title',
    'parent_bib_main_author', 'parent_bib_publication_year', 'text')

# Item-field specific gen functions

def lc_call_number(record):
    emitter = sf.DataEmitter(alphabet='ABCDEFGHIJKLMNOPQRSTUVWXYZ')
    lcclass = '{}{}'.format(emitter.emit('string', mn=1, mx=2),
                           emitter.emit('int', mn=1, mx=9999))
    cutter = '{}{}'.format(emitter.emit('string', mn=1, mx=2),
                           emitter.emit('int', mn=1, mx=999))
    date = emitter.emit('int', mn=1950, mx=2018)
    if random.choice(['decimal', 'nodecimal']) == 'decimal':
        class_decimal = emitter.emit('string', mn=1, mx=3,
                                     alphabet='0123456789')
        lcclass = '{}.{}'.format(lcclass, class_decimal)
    return '{} .{} {}'.format(lcclass, cutter, date)


def dewey_call_number(record):
    emitter = sf.DataEmitter(alphabet='ABCDEFGHIJKLMNOPQRSTUVWXYZ')
    class_num = emitter.emit('int', mn=100, mx=999)
    class_decimal = emitter.emit('string', mn=1, mx=3, alphabet='0123456789')
    cutter = '{}{}'.format(emitter.emit('string', mn=1, mx=2),
                           emitter.emit('int', mn=1, mx=999))
    date = emitter.emit('int', mn=1950, mx=2018)
    return '{}.{} {} {}'.format(class_num, class_decimal, cutter, date)


def sudoc_call_number(record):
    emitter = sf.DataEmitter(alphabet='ABCDEFGHIJKLMNOPQRSTUVWXYZ')
    stem = '{} {}.{}'.format(emitter.emit('string', mn=1, mx=2),
                             emitter.emit('int', mn=1, mx=99),
                             emitter.emit('int', mn=1, mx=999))
    if random.choice(['series', 'noseries']) == 'series':
        stem = '{}/{}'.format(stem, emitter.emit('string', mn=1, mx=1,
                                                 alphabet='ABCD123456789'))
        if random.choice(['dash', 'nodash']) == 'dash':
            stem = '{}-{}'.format(stem, emitter.emit('int', mn=1, mx=9))
    book_choice = random.choice(['', 'book_num', 'cutter'])
    if book_choice == 'book_num':
        book = emitter.emit('int', mn=1, mx=999)
    elif book_choice == 'cutter':
        book = '{} {}'.format(emitter.emit('string', mn=1, mx=2),
                              emitter.emit('int', mn=1, mx=999))
    else:
        book = book_choice
    if book and random.choice(['edition', 'noedition']) == 'edition':
        edition = emitter.emit('int', mn=1, mx=999)
        '{}/{}'.format(book, edition)
    return '{}:{}'.format(stem, book)


def other_call_number(record):
    emitter = sf.DataEmitter(alphabet='ABCDEFGHIJKLMNOPQRSTUVWXYZ')
    if random.choice(['just_text', 'structured']) == 'just_text':
        return emitter.emit('text')
    prefix = emitter.emit('string', mn=3, mx=6)
    number = emitter.emit('int', mn=1, mx=999999999)
    return '{} {}'.format(prefix, number)


def random_call_number(record):
    cntype = record['call_number_type']
    
    if cntype == 'lc':
        return lc_call_number(record)

    if cntype == 'dewey':
        return dewey_call_number(record)

    if cntype == 'sudoc':
        return sudoc_call_number(record)

    if cntype == 'other':
        return other_call_number(record)


def call_number_for_search(record):
    cn = record['call_number']
    return NormalizedCallNumber(cn, 'search').normalize()


def call_number_for_sort(record):
    cn = record['call_number']
    cntype = record['call_number_type']
    return NormalizedCallNumber(cn, cntype).normalize()


def random_volume(record):
    if random.choice(['has_volume'] + ['no_volume'] * 4) == 'has_volume':
        label = random.choice(['v', 'volume', 'vol', 'vol.', 'V'])
        number = random.randint(1, 1000)
        return '{}{}{}'.format(label, random.choice(['', ' ']), number)


def volume_for_sort(record):
    if record.get('volume', None):
        return NormalizedCallNumber(record['volume'], 'default').normalize()


def price(record):
    emitter = sf.DataEmitter(alphabet='0123456789')
    return '{}.{}'.format(emitter.emit('string', mn=1, mx=3),
                          emitter.emit('string', mn=1, mx=6))


def use_or_circ_count(record):
    return random.choice([0] * 9 + [1] * 4 + [2] * 3 + [3] * 2 + [4, 5, 6, 7] +
                         [random.randint(8, 50)])


def lytd_checkout_count(record):
    return random.randint(0, record['total_checkout_count'])


def ytd_checkout_count(record):
    total_field = 'total_checkout_count'
    lytd_field = 'last_year_to_date_checkout_count'
    return random.randint(0, record[total_field] - record[lytd_field])


def sequential_date(datefield, chance=100):
    seq = ['overdue', 'due', 'recall', 'checkout', 'last_checkin',
           'record_last_updated', 'record_creation']
    seq_index = seq.index(datefield)
    datefields = seq[seq_index + 1:]
    if chance < 100:
        choices = [True] * chance + [False] * (100 - chance)
    else:
        choices = None

    def should_emit(record):
        if seq_index <= 4 and record['total_checkout_count'] == 0:
            return False
        if datefield == 'last_checkin' and record['total_checkout_count'] > 1:
            return True
        if (datefield == 'checkout' and record.get('last_checkin_date', None)
             and record['total_checkout_count'] == 1):
            return False
        if seq_index <= 2 and not record.get('checkout_date', None):
            return False
        if datefield == 'due' and record.get('checkout_date', None):
            return True
        if choices is None:
            return True
        return random.choice(choices)

    def gen(record):
        if should_emit(record):
            emitter, mn = sf.DataEmitter(), None
            for cmpfield in datefields:
                mn = record.get('{}_date'.format(cmpfield), None)
                if mn:
                    break
            return emitter.emit('date', mn=(mn.year, mn.month, mn.day, mn.hour,
                                            mn.minute))
    return gen


ITEM_GENS = (
    ('django_ct', GENS.static('base.itemrecord')),
    ('django_id', GENS(auto_increment())),
    ('haystack_id', GENS(join_fields(('django_ct', 'django_id'), '.'))),
    ('type', GENS.static('Item')),
    ('suppressed', GENS.static(False)),
    ('record_revision_number', GENS.static(1)),
    ('record_number', GENS(auto_iii_recnum('i', 10000001))),
    ('call_number_type', GENS.choice(['lc'] * 5 + ['sudoc'] * 2 +
                                      ['dewey'] * 1 + ['other'] * 2)),
    ('call_number', GENS(random_call_number)),
    ('call_number_search', GENS(call_number_for_search)),
    ('call_number_sort', GENS(call_number_for_sort)),
    ('copy_number', GENS.type('int', mn=1, mx=9)),
    ('volume', GENS(random_volume)),
    ('volume_sort', GENS(volume_for_sort)),
    ('barcode', GENS.type('int', mn=1000000001, mx=1999999999)),
    ('local_code1', GENS.type('int', mn=0, mx=299)),
    ('location_code', None),
    ('item_type_code', None),
    ('status_code', None),
    ('public_notes', 'auto'),
    ('long_messages', 'auto'),
    ('price', GENS(price)),
    ('copy_use_count', GENS(use_or_circ_count)),
    ('internal_use_count', GENS(use_or_circ_count)),
    ('iuse3_count', GENS(use_or_circ_count)),
    ('number_of_renewals', GENS(use_or_circ_count)),
    ('total_renewal_count', GENS(use_or_circ_count)),
    ('total_checkout_count', GENS(use_or_circ_count)),
    ('last_year_to_date_checkout_count', GENS(lytd_checkout_count)),
    ('year_to_date_checkout_count', GENS(ytd_checkout_count)),
    ('record_creation_date', 'auto'),
    ('record_last_updated_date', GENS(sequential_date('record_last_updated'))),
    ('last_checkin_date', GENS(sequential_date('last_checkin', 50))),
    ('checkout_date', GENS(sequential_date('checkout', 25))),
    ('recall_date', GENS(sequential_date('recall', 10))),
    ('due_date', GENS(sequential_date('due'))),
    ('overdue_date', GENS(sequential_date('overdue', 33))),
    ('parent_bib_id', None),
    ('parent_bib_record_number', None),
    ('parent_bib_title', None),
    ('parent_bib_main_author', None),
    ('parent_bib_publication_year', None),
    ('text', GENS(join_fields(['parent_bib_record_number', 'call_number',
                               'parent_bib_main_author', 'location_code',
                               'public_notes', 'parent_bib_publication_year',
                               'record_number', 'parent_bib_title'])))
)


# ERESOURCES

ERES_FIELDS = ('django_ct', 'django_id', 'id', 'haystack_id', 'type',
               'eresource_type', 'suppressed', 'record_revision_number',
               'record_number', 'title', 'alternate_titles', 'subjects',
               'summary', 'publisher', 'alert', 'internal_notes', 'holdings',
               'record_creation_date', 'record_last_updated_date', 'text')


ERES_GENS = (
    ('django_ct', GENS.static('base.resourcerecord')),
    ('django_id', GENS(auto_increment())),
    ('haystack_id', GENS(join_fields(('django_ct', 'django_id'), '.'))),
    ('type', GENS.static('eResource')),
    ('suppressed', GENS.static(False)),
    ('eresource_type', 'auto'),
    ('record_revision_number', GENS.static(1)),
    ('record_number', GENS(auto_iii_recnum('e', 10000001))),
    ('title', 'auto'),
    ('alternate_titles', 'auto'),
    ('subjects', 'auto'),
    ('summary', 'auto'),
    ('publisher', 'auto'),
    ('alert', 'auto'),
    ('internal_notes', 'auto'),
    ('holdings', 'auto'),
    ('record_creation_date', 'auto'),
    ('record_last_updated_date', GENS(sequential_date('record_last_updated'))),
    ('text', GENS(join_fields(['title', 'subjects', 'eresource_type',
                              'record_number', 'publisher',
                              'alternate_titles'])))
)


# BIBS -- TODO
