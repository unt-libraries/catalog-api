"""
Contains data structures for making Solr test data.
"""

import itertools
import random
import datetime
import re
import ujson

import pytest

from utils.test_helpers import solr_factories as sf
from utils.helpers import NormalizedCallNumber


GLOBAL_UNIQUE_FIELDS = ('django_id', 'code', 'id', 'record_number')
SOLR_TYPES = {
  'string': {'pytype': unicode, 'emtype': 'string'},
  'norm_string': {'pytype': unicode, 'emtype': 'string'},
  'alphaOnlySort': {'pytype': unicode, 'emtype': 'string'},
  'text_en': {'pytype': unicode, 'emtype': 'text'},
  'text': {'pytype': unicode, 'emtype': 'text'},
  'textNoStem': {'pytype': unicode, 'emtype': 'text'},
  'stem_text': {'pytype': unicode, 'emtype': 'text'},
  'long': {'pytype': int, 'emtype': 'int'},
  'slong': {'pytype': int, 'emtype': 'int'},
  'int': {'pytype': int, 'emtype': 'int'},
  'integer': {'pytype': int, 'emtype': 'int'},
  'date': {'pytype': datetime.datetime, 'emtype': 'date'},
  'boolean': {'pytype': bool, 'emtype': 'boolean'},
}

LETTERS_UPPER = list('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
LETTERS_LOWER = list('abcdefghijklmnopqrstuvwxyz')
NUMBERS = list('0123456789')
INNER_WORD_PUNCTUATION = list('-/@')
INNER_SENTENCE_PUNCTUATION = list(',;')
END_SENTENCE_PUNCTUATION = list('.?!')
WRAP_PUNCTUATION = [list('()'), list('[]'), list('{}'), list("''"), list('""')]


# Multipurpose "gen" functions and gen function makers, for generating
# Solr field data.

GENS = sf.SolrDataGenFactory()

def join_fields(fnames, sep=' '):
    """
    Make a gen function that will join values from multiple fields into
    one string using a separator (`sep`). `fnames` is the list of field
    names to join. Multi-valued fields are also flattened and joined.
    """
    def gen(record):
        values = []
        for fname in fnames:
            val = record.get(fname, None)
            if val is not None:
                if not isinstance(val, (list, tuple, set)):
                    val = [val]
                values.extend(val)
        return sep.join([unicode(v) for v in values])
    return gen


def copy_field(fname):
    """
    Make a gen function that copies the contents of one field to
    another. `fname` is the name of the field you're copying from.
    """
    return lambda rec: rec.get(fname, None)


def auto_increment(prefix='', start=0):
    """
    Make an auto-increment counter function, for generating IDs and
    such. You can provide an optional `prefix` string that gets
    prepended to each ID number, and the counter starts at 0 unless you
    provide a different `start` value. Note that the gen function you
    create retains the counter's state (i.e. the last-used ID).
    """
    increment = itertools.count(start)
    return lambda rec: '{}{}'.format(prefix, next(increment))


def chance(gen_function, chance=100):
    """
    Wrap a gen function so that it has an X percent chance (`chance`)
    of returning a value; returns None otherwise.
    """
    def gen(record):
        val = gen_function(record)
        if chance < 100:
            val = random.choice([None] * (100 - chance) + [val] * chance)
        return val
    return gen


def multi(fmt_function, mn, mx):
    """
    Wrap a gen function that normally returns a single value so that it
    generates and returns a random number of values between `mn` and
    `mx`.
    """
    def gen(record):
        num = random.randint(mn, mx)
        return [fmt_function(record) for _ in range(0, num)] or None
    return gen


# *_like gen functions, below, for generating random data that kind of
# looks like a certain type of thing (name, place, title, etc.)

def year_like(record):
    return random.randint(1850, 2018)


def year_range_like(record):
    years = [year_like(record), random.choice([year_like(record), ''])]
    return  '-'.join([unicode(year) for year in sorted(years)])


def place_like(record):
    emitter = sf.DataEmitter(alphabet=LETTERS_LOWER)
    words = emitter.emit('text', mn_words=1, mx_words=4,
                         mn_word_len=3).split(' ')
    comma_index = random.randint(0, len(words) - 1)
    if comma_index < len(words) - 1:
        words[comma_index] = '{},'.format(words[comma_index])
    return ' '.join(words).title()


def _make_person_name_parts():
    emitter = sf.DataEmitter(alphabet=LETTERS_LOWER)
    first = emitter.emit('string', mn=1, mx=8)
    middle = emitter.emit('string', mn=3, mx=8)
    last = emitter.emit('string', mn=3, mx=10)
    middle = random.choice(['', middle[0], middle])
    first = '{}.'.format(first) if len(first) == 1 else first
    middle = '{}.'.format(middle) if len(middle) == 1 else middle
    return tuple(part.capitalize() for part in (first, middle, last))


def person_name_like(record):
    first, middle, last = _make_person_name_parts()
    return ' '.join([part for part in (first, middle, last) if part])


def person_name_heading_like(record):
    first, middle, last = _make_person_name_parts()
    years = year_range_like(record)
    last_first = ', '.join((last, first))
    return ' '.join([part for part in (last_first, middle, years) if part])


def org_name_like(record):
    emitter = sf.DataEmitter(alphabet=LETTERS_LOWER)
    return emitter.emit('text', mn_words=1, mx_words=4).title()


def sentence_like(record):
    emitter = sf.DataEmitter(alphabet=LETTERS_LOWER)
    words = emitter.emit('text', mn_words=1, mx_words=9).split(' ')
    noun_choice = random.choice([None] * 4 + [person_name_like] * 3 +
                                [place_like, org_name_like])
    year_choice = random.choice([None] * 4 + [year_like])
    wrap_choice = random.choice([None] * 4 + WRAP_PUNCTUATION)
    inner_choices = [random.choice([None] * 2 + INNER_SENTENCE_PUNCTUATION)
                     for _ in range(0, int(len(words) / 2))]
    end_choice = random.choice(END_SENTENCE_PUNCTUATION)

    for choice in (noun_choice, year_choice):
        if choice is not None:
            words.insert(random.randint(0, len(words)),
                         unicode(choice(record)))

    if wrap_choice is not None:
        start, end = wrap_choice
        wrap_index = random.randint(0, len(words) - 1)
        words[wrap_index] = '{}{}{}'.format(start, words[wrap_index], end)

    punct_pos = random.sample(range(0, len(words) - 1), len(inner_choices))
    for i, inner_punct in enumerate(inner_choices):
        if inner_punct is not None:
            punct_index = punct_pos[i]
            words[punct_index] = '{}{}'.format(words[punct_index], inner_punct)
    words[-1] = '{}{}'.format(words[-1], end_choice)
    words[0] = words[0].capitalize()
    return ' '.join(words)


def title_like(record):
    return sentence_like(record)[:-1]


def keyword_like(record):
    emitter = sf.DataEmitter(alphabet=LETTERS_LOWER)
    return emitter.emit('text', mn_words=1, mx_words=4).capitalize()


def url_like(record):
    emitter = sf.DataEmitter(alphabet=LETTERS_LOWER)
    host_parts = random.randint(1, 3)
    num_subdirs = random.randint(0, 2)
    host = [emitter.emit('string', mn=3, mx=10) for _ in range(0, host_parts)]
    domain_extension = random.choice(['net', 'com', 'edu'])
    subdirs = [emitter.emit('string', mn=4, mx=10)
               for _ in range(0, num_subdirs)]
    host_string = '.'.join(host + [domain_extension])
    return 'https://{}'.format('/'.join([host_string] + subdirs))


# SolrProfile configuration (one profile for each API resource)

# CODES (includes Location, Itype, and ItemStatus)

CODE_FIELDS = ('django_ct', 'django_id', 'haystack_id', 'code', 'label',
               'type', 'text')
LOCATION_GENS = (
    ('django_ct', GENS.static('base.location')),
    ('django_id', GENS(auto_increment())),
    ('haystack_id', GENS(join_fields(('django_ct', 'django_id'), '.'))),
    ('code', GENS.type('string', mn=1, mx=5, alphabet=LETTERS_LOWER)),
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
    ('code', GENS.type('string', mn=1, mx=1, alphabet=LETTERS_LOWER)),
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

def lc_cn(record):
    emitter = sf.DataEmitter(alphabet=LETTERS_UPPER)
    lcclass = '{}{}'.format(emitter.emit('string', mn=1, mx=2),
                           emitter.emit('int', mn=1, mx=9999))
    cutter = '{}{}'.format(emitter.emit('string', mn=1, mx=2),
                           emitter.emit('int', mn=1, mx=999))
    date = emitter.emit('int', mn=1950, mx=2018)
    if random.choice(['decimal', 'nodecimal']) == 'decimal':
        class_decimal = emitter.emit('string', mn=1, mx=3,
                                     alphabet=NUMBERS)
        lcclass = '{}.{}'.format(lcclass, class_decimal)
    return '{} .{} {}'.format(lcclass, cutter, date)


def dewey_cn(record):
    emitter = sf.DataEmitter(alphabet=LETTERS_UPPER)
    class_num = emitter.emit('int', mn=100, mx=999)
    class_decimal = emitter.emit('string', mn=1, mx=3, alphabet=NUMBERS)
    cutter = '{}{}'.format(emitter.emit('string', mn=1, mx=2),
                           emitter.emit('int', mn=1, mx=999))
    date = emitter.emit('int', mn=1950, mx=2018)
    return '{}.{} {} {}'.format(class_num, class_decimal, cutter, date)


def sudoc_cn(record):
    emitter = sf.DataEmitter(alphabet=LETTERS_UPPER)
    stem = '{} {}.{}'.format(emitter.emit('string', mn=1, mx=2),
                             emitter.emit('int', mn=1, mx=99),
                             emitter.emit('int', mn=1, mx=999))
    if random.choice(['series', 'noseries']) == 'series':
        stem = '{}/{}'.format(stem, emitter.emit('string', mn=1, mx=1,
                                                 alphabet=NUMBERS))
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


def other_cn(record):
    emitter = sf.DataEmitter(alphabet=LETTERS_UPPER)
    if random.choice(['just_text', 'structured']) == 'just_text':
        return emitter.emit('text')
    prefix = emitter.emit('string', mn=3, mx=6)
    number = emitter.emit('int', mn=1, mx=999999999)
    return '{} {}'.format(prefix, number)


def random_cn(record):
    cntype = record['call_number_type']
    
    if cntype == 'lc':
        return lc_cn(record)

    if cntype == 'dewey':
        return dewey_cn(record)

    if cntype == 'sudoc':
        return sudoc_cn(record)

    if cntype == 'other':
        return other_cn(record)


def cn_for_search(fieldname):
    def gen(record):
        cn = record.get(fieldname, None)
        if cn is not None:
            return NormalizedCallNumber(cn, 'search').normalize()
    return gen


def cn_for_sort(fieldname, cntype=None):
    def gen(record):
        cn = record.get(fieldname, None)
        if cn is not None:
            cnumtype = cntype or record.get('call_number_type', 'other')
            return NormalizedCallNumber(cn, cnumtype).normalize()
    return gen


def random_volume(record):
    if random.choice(['has_volume'] + ['no_volume'] * 4) == 'has_volume':
        label = random.choice(['v', 'volume', 'vol', 'vol.', 'V'])
        number = random.randint(1, 1000)
        return '{}{}{}'.format(label, random.choice(['', ' ']), number)


def volume_for_sort(record):
    if record.get('volume', None):
        return NormalizedCallNumber(record['volume'], 'default').normalize()


def price(record):
    emitter = sf.DataEmitter(alphabet=NUMBERS)
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


class Link(object):
    """
    Helper / container class for creating links between two different
    record types.
    """

    @staticmethod
    def link(source_val, target_record, target_fname, multi=False):
        """
        Create a link between two records by adding the `source_val`
        value(s) to the `target_record` at the field described by the
        `target_fname` (e.g. target field name). If the target field
        is multi-valued (`multi` is True), then it adds the value(s)
        to the existing value list. If the source is multi-valued but
        the target isn't, it grabs the first value from the source.
        """
        sv = source_val if isinstance(source_val, list) else [source_val]
        tv = (target_record.get(target_fname, []) + sv) if multi else sv[0]
        target_record[target_fname] = tv

    @classmethod
    def from_item_to_bib(cls, item, bib):
        cls.link(item['id'], bib, 'item_ids', multi=True)
        cls.link(item['record_number'], bib, 'item_record_numbers', multi=True)

    @classmethod
    def from_bib_to_item(cls, bib, item):
        cls.link(bib['id'], item, 'parent_bib_id')
        cls.link(bib['record_number'], item, 'parent_bib_record_number')
        cls.link(bib['full_title'], item, 'parent_bib_title')
        cls.link(bib['creator'], item, 'parent_bib_main_author')
        if bib.get('publication_dates', []):
            pub_year = bib['publication_dates'][0]
            cls.link(pub_year, item, 'parent_bib_publication_year')

    @classmethod
    def link_bib_and_item(cls, bib, item):
        cls.from_item_to_bib(item, bib)
        cls.from_bib_to_item(bib, item)


def choose_and_link_to_parent_bib(bib_rec_pool):
    """
    This is will create a gen function that you can use to relate items
    to parent bib records.

    After you've created a group of test bib records, use this function
    to make the gen for your item `parent_bib_id` field, and pass the
    group of bib records in as the `bib_rec_pool` param.

    When the gen runs, it will randomly select one bib record from the
    provided bib_rec_pool set to be the parent bib for the item in
    question. It will automatically create all needed links between
    that bib record and the item record. I.e., it wil add the item's
    'id' field value to the bib's 'item_ids' list and the item's
    'record_number' field value to the bib's 'item_record_numbers'
    list; it will pull values for each 'parent_' field from the bib and
    copy it into the corresponding field in the item (e.g. bib
    'full_title' to item 'parent_bib_title').

    A couple of things to note.

    First, the assignment of bibs to items is purely random. Depending
    on the size of your bib and item record sets, you could very well
    have some bibs that never get chosen and therefore don't have any
    items. It is possible (though rare) to have bibs without items in
    our live data, so it's not a problem. At the same time, bib records
    can (and often do) have multiple items. If your bib record set is
    smaller than your item record set, then this should work out well.
    (E.g., 100 bibs to 200 items.)

    Second, when your batch of item records are made, the gen will
    update records in the bib set, too. So, if you've added the bib
    record set to Solr before the item-make process runs, then you'll
    have to add those records to Solr again afterward to make sure
    they're updated.
    """
    bib_choices = tuple(br['id'] for br in bib_rec_pool)

    def gen(item):
        parent_id = random.choice(bib_choices)
        bib = [br for br in bib_rec_pool if br['id'] == parent_id][0]
        Link.link_bib_and_item(bib, item)
        return bib['id']

    return gen



ITEM_GENS = (
    ('django_ct', GENS.static('base.itemrecord')),
    ('id', GENS(auto_increment())),
    ('django_id', GENS(copy_field('id'))),
    ('haystack_id', GENS(join_fields(('django_ct', 'django_id'), '.'))),
    ('type', GENS.static('Item')),
    ('suppressed', GENS.static(False)),
    ('record_revision_number', GENS.static(1)),
    ('record_number', GENS(auto_increment('i', 10000001))),
    ('call_number_type', GENS.choice(['lc'] * 5 + ['sudoc'] * 2 +
                                      ['dewey'] * 1 + ['other'] * 2)),
    ('call_number', GENS(random_cn)),
    ('call_number_search', GENS(cn_for_search('call_number'))),
    ('call_number_sort', GENS(cn_for_sort('call_number'))),
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
    ('parent_bib_record_number', None),
    ('parent_bib_title', None),
    ('parent_bib_main_author', None),
    ('parent_bib_publication_year', None),
    ('parent_bib_id', None),
    ('text', GENS(join_fields(['parent_bib_record_number', 'call_number',
                               'parent_bib_main_author', 'location_code',
                               'public_notes', 'parent_bib_publication_year',
                               'record_number', 'parent_bib_title'])))
)


# ERESOURCES

ERES_FIELDS = ('django_ct', 'django_id', 'id', 'haystack_id', 'type',
    'eresource_type', 'suppressed', 'record_revision_number', 'record_number',
    'title', 'alternate_titles', 'subjects', 'summary', 'publisher', 'alert',
    'internal_notes', 'holdings', 'record_creation_date',
    'record_last_updated_date', 'text')


ERES_GENS = (
    ('django_ct', GENS.static('base.resourcerecord')),
    ('id', GENS(auto_increment())),
    ('django_id', GENS(copy_field('id'))),
    ('haystack_id', GENS(join_fields(('django_ct', 'django_id'), '.'))),
    ('type', GENS.static('eResource')),
    ('suppressed', GENS.static(False)),
    ('eresource_type', 'auto'),
    ('record_revision_number', GENS.static(1)),
    ('record_number', GENS(auto_increment('e', 10000001))),
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


# BIBS

BIB_FIELDS = ('django_ct', 'django_id', 'id', 'haystack_id', 'suppressed',
    'record_number', 'material_type', 'formats', 'languages', 'isbn_numbers',
    'issn_numbers', 'lccn_number', 'oclc_numbers', 'dewey_call_numbers',
    'loc_call_numbers', 'sudoc_numbers', 'other_call_numbers',
    'main_call_number', 'main_call_number_sort', 'main_title', 'subtitle',
    'statement_of_responsibility', 'full_title', 'title_sort',
    'alternate_titles', 'uniform_title', 'related_titles', 'corporations',
    'meetings', 'people', 'creator', 'creator_sort', 'contributors',
    'author_title_search', 'imprints', 'publishers', 'publication_country',
    'publication_dates', 'publication_places', 'physical_characteristics',
    'context_notes', 'summary_notes', 'toc_notes', 'era_terms', 'form_terms',
    'general_terms', 'genre_terms', 'geographic_terms', 'other_terms',
    'topic_terms', 'full_subjects', 'series', 'series_exact',
    'series_creators', 'urls', 'url_labels', 'timestamp', 'item_ids',
    'item_record_numbers', 'text')


# Bib-field specific gen functions

def isbn_number(record):
    return random.randint(100000000000, 9999999999999)


def issn_number(record):
    mn, mx = 1000, 9999
    return '{}-{}'.format(random.randint(mn, mx), random.randint(mn, mx))


def oclc_number(record):
    return random.randint(10000000, 9999999999)


def pick_main_call_number(record):
    cntypes = ('dewey_call', 'loc_call', 'sudoc', 'other_call')
    cns = {}
    for cntype in cntypes:
        for cn in record.get('{}_numbers'.format(cntype), []):
            cns[cn] = cntype
    if cns:
        main_cn = random.choice(cns.keys())
        main_cntype = cns[main_cn].split('_')[0]
        if main_cntype == 'loc':
            main_cntype = 'lc'
        record['__main_cntype'] = main_cntype
        return main_cn


def main_call_number_sort(record):
    if record.get('main_call_number', None):
        val = cn_for_sort('main_call_number', record['__main_cntype'])(record)
        del(record['__main_cntype'])
        return val


def full_title(record):
    main_title, subtitle = record['main_title'], record.get('subtitle', '')
    return ' : '.join([part for part in (main_title, subtitle) if part])


def sortable_text_field(fieldname):
    def gens(record):
        try:
            val = record[fieldname]
        except KeyError:
            return None
        no_punct = re.sub(r'[`~!@#$%^&*()-=_+{}\[\]|\\;\',./:"<>?]', '', val)
        return re.sub(r'\s+', ' ', no_punct)
    return gens


def agent(name_gen, name_type):
    add_to = 'people' if name_type == 'person' else '{}s'.format(name_type)
    def gen(record):
        name = name_gen(record)
        record[add_to] = record.get(add_to, None) or []
        record[add_to].append(name)
        return name
    return gen


def random_agent(person_weight=8, corp_weight=1, meeting_weight=1):
    def gen(record):
        ntype = random.choice(['person'] * person_weight +
                              ['corporation'] * corp_weight +
                              ['meeting'] * meeting_weight)
        ngen = person_name_heading_like if ntype == 'person' else org_name_like
        return agent(ngen, ntype)(record)
    return gen


def statement_of_resp(record):
    return 'by {}'.format(record.get('creator', 'Unknown'))


def imprints(record):
    fieldnames = ('publishers', 'publication_dates', 'publication_places')
    pub_parts = [record.get(fname, []) for fname in fieldnames]
    num_imprints = max(*[len(p) for p in pub_parts])
    imprints = []
    for i in range(0, num_imprints):
        name, date, place = (p[i] if i < len(p) else None for p in pub_parts)
        name_date = ', '.join([p for p in (name, date) if p])
        imprints.append(' : '.join([p for p in (place, name_date) if p]))
    return imprints


def subjects(record):
    fieldnames = ('topic_terms', 'general_terms', 'other_terms',
                  'geographic_terms', 'form_terms', 'era_terms')
    subj_parts = (record.get(fname, []) for fname in fieldnames)
    num_subjects = max(*[len(p) for p in subj_parts])
    subjects = []
    for i in range(0, num_subjects):
        terms = [p[i] if i < len(p) else None for p in subj_parts]
        subjects.append(' -- '.join([term for term in terms if term]))
    return subjects


BIB_GENS = (
    ('django_ct', GENS.static('base.bibrecord')),
    ('id', GENS(auto_increment())),
    ('django_id', GENS(copy_field('id'))),
    ('haystack_id', GENS(join_fields(('django_ct', 'django_id'), '.'))),
    ('suppressed', GENS.static(False)),
    ('record_number', GENS(auto_increment('b', 10000001))),
    ('material_type', GENS.type('string', mn=5, mx=15,
                                alphabet=LETTERS_UPPER)),
    ('formats', 'auto'),
    ('languages', 'auto'),
    ('isbn_numbers', GENS(chance(multi(isbn_number, 1, 5), 66))),
    ('issn_numbers', GENS(chance(multi(issn_number, 1, 5), 33))),
    ('lccn_number', GENS(chance(GENS.type('int', mn=100000, mx=999999999999),
                                80))),
    ('oclc_numbers', GENS(chance(multi(oclc_number, 1, 2), 75))),
    ('dewey_call_numbers', GENS(chance(multi(dewey_cn, 1, 2), 20))),
    ('loc_call_numbers', GENS(chance(multi(lc_cn, 1, 2), 80))),
    ('sudoc_numbers', GENS(chance(multi(sudoc_cn, 1, 2), 20))),
    ('other_call_numbers', GENS(chance(multi(other_cn, 1, 2), 30))),
    ('main_call_number', GENS(pick_main_call_number)),
    ('main_call_number_sort', GENS(main_call_number_sort)),
    ('main_title', GENS(title_like)),
    ('subtitle', GENS(chance(title_like, 40))),
    ('full_title', GENS(full_title)),
    ('title_sort', GENS(sortable_text_field('full_title'))),
    ('alternate_titles', GENS(chance(multi(title_like, 1, 3), 20))),
    ('uniform_title', GENS(chance(title_like), 30)),
    ('related_titles', GENS(chance(multi(title_like, 1, 5), 50))),
    ('corporations', None),
    ('meetings', None),
    ('people', None),
    ('creator', GENS(random_agent(8, 1, 1))),
    ('creator_sort', GENS(sortable_text_field('creator'))),
    ('contributors', GENS(chance(multi(random_agent(6, 3, 1), 1, 5), 75))),
    ('statement_of_responsibility', GENS(chance(statement_of_resp, 80))),
    ('author_title_search', GENS(join_fields(['creator', 'main_title']))),
    ('publishers', GENS(chance(multi(org_name_like, 1, 3), 70))),
    ('publication_dates', GENS(chance(multi(year_like, 1, 3), 90))),
    ('publication_places', GENS(chance(multi(place_like, 1, 3), 60))),
    ('publication_country', GENS(chance(place_like), 60)),
    ('imprints', GENS(imprints)),
    ('physical_characteristics', GENS(multi(sentence_like, 1, 4))),
    ('context_notes', GENS(chance(multi(sentence_like, 1, 4), 50))),
    ('summary_notes', GENS(chance(multi(sentence_like, 1, 4), 50))),
    ('toc_notes', GENS(chance(multi(sentence_like, 1, 4), 50))),
    ('era_terms', GENS(chance(multi(year_range_like, 1, 2), 25))),
    ('form_terms', GENS(chance(multi(keyword_like, 1, 2), 25))),
    ('general_terms', GENS(chance(multi(keyword_like, 1, 2), 25))),
    ('genre_terms', GENS(chance(multi(keyword_like, 1, 2), 40))),
    ('geographic_terms', GENS(chance(multi(place_like, 1, 2), 30))),
    ('other_terms', GENS(chance(multi(keyword_like, 1, 2), 25))),
    ('topic_terms', GENS(multi(keyword_like, 1, 5))),
    ('full_subjects', GENS(subjects)),
    ('series', GENS(chance(multi(title_like, 1, 3), 50))),
    ('series_exact', GENS(copy_field('series'))),
    ('series_creators', GENS(chance(multi(person_name_heading_like, 1, 3),
                                    50))),
    ('urls', GENS(chance(multi(url_like, 1, 3), 75))),
    ('url_labels', None),
    ('timestamp', 'auto'),
    ('item_ids', None),
    ('item_record_numbers', None),
    ('text', GENS(join_fields(['haystack_id', 'record_number', 'material_type',
                              'formats', 'languages', 'isbn_numbers',
                              'issn_numbers', 'lccn_number', 'oclc_numbers',
                              'dewey_call_numbers', 'loc_call_numbers',
                              'sudoc_numbers', 'other_call_numbers',
                              'main_call_number',
                              'statement_of_responsibility', 'full_title',
                              'alternate_titles', 'uniform_title',
                              'related_titles', 'corporations', 'meetings',
                              'people', 'imprints', 'publishers',
                              'publication_country', 'publication_dates',
                              'publication_places', 'physical_characteristics',
                              'context_notes', 'summary_notes', 'toc_notes',
                              'full_subjects', 'series', 'series_creators',
                              'url_labels'])))
)


# MARC -- Note that this does not currently generate realistic data for
# the MARC index, only basic stub records. Generating realistic data is
# going to be difficult and time-consuming, and at this point the MARC
# index is still not really used for any public services, so it's not
# super important.
#
# The major things that are missing for this data to be considered
# complete are: 1) searchable dynamic fields for MARC fields and
# subfields, and 2) basing the data on an existing BIB record in the
# bib index.

MARC_FIELDS = ('django_ct', 'django_id', 'id', 'haystack_id', 'record_number',
               'json', 'timestamp')


def marc_json(record):
    first, middle, last = _make_person_name_parts()
    years = year_range_like(record)
    data = {
        'fields': {
            '001': random.randint(10000, 99999999),
            '003': 'OCoLC',
            '050': {
                'subfields': [
                    { 'a': lc_cn(record) }
                ],
                'ind1': ' ',
                'ind2': ' '
            },
            '100': {
                'subfields': [
                    { 'a': '{}, {} {},'.format(last, first, middle) },
                    { 'd': '{}.'.format(years) }
                ],
                'ind1': str(random.randint(1, 9)),
                'ind2': str(random.randint(1, 9))
            },
            '245': {
                'subfields': [
                    { 'a': '{} :'.format(title_like(record)) },
                    { 'b': '{} /'.format(title_like(record)) },
                    { 'c': 'by {} {} {}.'.format(first, middle, last)}
                ],
                'ind1': str(random.randint(1, 9)),
                'ind2': str(random.randint(1, 9))
            }
        }
    }
    return ujson.dumps(data)


MARC_GENS = (
    ('django_ct', GENS.static('base.bibrecord')),
    ('id', GENS(auto_increment())),
    ('django_id', GENS(copy_field('id'))),
    ('haystack_id', GENS(join_fields(('django_ct', 'django_id'), '.'))),
    ('record_number', GENS(auto_increment('b', 10000001))),
    ('json', GENS(marc_json)),
    ('timestamp', 'auto'),
)
