"""
Contains structures needed to make Solr test data for `blacklight` app.
"""

import hashlib
import base64

from utils.test_helpers import solr_test_profiles as tp


SOLR_TYPES = tp.SOLR_TYPES
SOLR_TYPES['reverse_number'] = {'pytype': unicode, 'emtype': 'string'}
GLOBAL_UNIQUE_FIELDS = ('code', 'id', 'record_number')
GENS = tp.GENS


ALPHASOLRMARC_FIELDS = (
    'id', 'timestamp_of_last_solr_update', 'suppressed', 'date_added',
    'resource_type', 'items_json', 'has_more_items', 'more_items_json',
    'thumbnail_url', 'urls_json', 'publication_year_display',
    'creation_display', 'publication_display', 'distribution_display',
    'manufacture_display', 'copyright_display', 'publication_sort',
    'publication_decade_facet', 'publication_year_facet', 'access_facet',
    'building_facet', 'shelf_facet', 'collection_facet', 'resource_type_facet',
    'metadata_facets_search', 'publication_places_search', 'publishers_search',
    'publication_dates_search',
    # OLD FIELDS ARE BELOW
    'game_facet', 'formats', 'languages', 'isbn_numbers',
    'issn_numbers', 'lccn_number', 'oclc_numbers', 'dewey_call_numbers',
    'loc_call_numbers', 'sudoc_numbers', 'other_call_numbers',
    'main_call_number', 'main_call_number_sort', 'main_title', 'subtitle',
    'statement_of_responsibility', 'full_title', 'title_sort',
    'alternate_titles', 'uniform_title', 'related_titles', 'corporations',
    'meetings', 'people', 'creator', 'creator_sort', 'contributors',
    'author_title_search', 'physical_characteristics',
    'context_notes', 'summary_notes', 'toc_notes', 'era_terms', 'form_terms',
    'general_terms', 'genre_terms', 'geographic_terms', 'other_terms',
    'topic_terms', 'full_subjects', 'series', 'series_exact',
    'series_creators',
    'public_title_facet', 'public_author_facet', 'public_series_facet',
    'meetings_facet', 'public_subject_facet', 'geographic_terms_facet',
    'era_terms_facet', 'public_genre_facet', 'text'
)

# AlphaSolrmarc field specific gen functions

def _combine_fields(record, fields):
    """
    Combine values from multiple fields into one list of values;
    handles multi- and non-multi-valued fields, and deduplicates
    values.
    """
    values = set()
    for field in fields:
        val = record.get(field, None)
        if val is not None:
            if isinstance(val, (list, set, tuple)):
                values |= set(val)
            else:
                values.add(val)
    return list(values) if values else None


def public_title_facet(record):
    fields = ('uniform_title', 'main_title', 'related_titles')
    return _combine_fields(record, fields)


def public_author_facet(record):
    fields = ('creator', 'contributors', 'series_creators')
    return _combine_fields(record, fields)


def author_title_search(record):
    author_titles = []
    titles = record.get('related_titles', [])
    for i, author in enumerate(record.get('contributors', [])):
        try:
            author_titles.append('{}. {}'.format(author, titles[i]))
        except IndexError:
            break
    return author_titles or None


ALPHASOLRMARC_GENS = (
    ('id', GENS(tp.auto_increment('b', 10000001))),
    ('items_json', None),
    ('has_more_items', None),
    ('more_items_json', None),
    ('game_facet', GENS(tp.multi(GENS.type('string', mn=6, mx=10,
                                           alphabet=tp.LETTERS_LOWER + 
                                                    tp.NUMBERS), 1, 3))),
    ('formats', 'auto'),
    ('languages', 'auto'),
    ('isbn_numbers', GENS(tp.chance(tp.multi(tp.isbn_number, 1, 5), 66))),
    ('issn_numbers', GENS(tp.chance(tp.multi(tp.issn_number, 1, 5), 33))),
    ('lccn_number', GENS(tp.chance(GENS.type('int', mn=100000,
                                             mx=999999999999), 80))),
    ('oclc_numbers', GENS(tp.chance(tp.multi(tp.oclc_number, 1, 2), 75))),
    ('dewey_call_numbers', GENS(tp.chance(tp.multi(tp.dewey_cn, 1, 2), 20))),
    ('loc_call_numbers', GENS(tp.chance(tp.multi(tp.lc_cn, 1, 2), 80))),
    ('sudoc_numbers', GENS(tp.chance(tp.multi(tp.sudoc_cn, 1, 2), 20))),
    ('other_call_numbers', GENS(tp.chance(tp.multi(tp.other_cn, 1, 2), 30))),
    ('main_call_number', GENS(tp.pick_main_call_number)),
    ('main_call_number_sort', GENS(tp.main_call_number_sort)),
    ('main_title', GENS(tp.title_like)),
    ('subtitle', GENS(tp.chance(tp.title_like, 40))),
    ('full_title', GENS(tp.full_title)),
    ('title_sort', GENS(tp.sortable_text_field('full_title'))),
    ('alternate_titles', GENS(tp.chance(tp.multi(tp.title_like, 1, 3), 20))),
    ('uniform_title', GENS(tp.chance(tp.title_like), 30)),
    ('related_titles', GENS(tp.chance(tp.multi(tp.title_like, 1, 5), 50))),
    ('corporations', None),
    ('meetings', None),
    ('people', None),
    ('creator', GENS(tp.random_agent(8, 1, 1))),
    ('creator_sort', GENS(tp.sortable_text_field('creator'))),
    ('contributors', GENS(tp.chance(tp.multi(tp.random_agent(6, 3, 1), 1, 5),
                                    75))),
    ('statement_of_responsibility', GENS(tp.chance(tp.statement_of_resp, 80))),
    ('author_title_search', GENS(author_title_search)),
    ('physical_characteristics', GENS(tp.multi(tp.sentence_like, 1, 4))),
    ('context_notes', GENS(tp.chance(tp.multi(tp.sentence_like, 1, 4), 50))),
    ('summary_notes', GENS(tp.chance(tp.multi(tp.sentence_like, 1, 4), 50))),
    ('toc_notes', GENS(tp.chance(tp.multi(tp.sentence_like, 1, 4), 50))),
    ('era_terms', GENS(tp.chance(tp.multi(tp.year_range_like, 1, 2), 25))),
    ('form_terms', GENS(tp.chance(tp.multi(tp.keyword_like, 1, 2), 25))),
    ('general_terms', GENS(tp.chance(tp.multi(tp.keyword_like, 1, 2), 25))),
    ('genre_terms', GENS(tp.chance(tp.multi(tp.keyword_like, 1, 2), 40))),
    ('geographic_terms', GENS(tp.chance(tp.multi(tp.place_like, 1, 2), 30))),
    ('other_terms', GENS(tp.chance(tp.multi(tp.keyword_like, 1, 2), 25))),
    ('topic_terms', GENS(tp.multi(tp.keyword_like, 1, 5))),
    ('full_subjects', GENS(tp.subjects)),
    ('series', GENS(tp.chance(tp.multi(tp.title_like, 1, 3), 50))),
    ('series_exact', GENS(tp.copy_field('series'))),
    ('series_creators', GENS(tp.chance(tp.multi(tp.person_name_heading_like,
                                                1, 3), 50))),
    ('timestamp_of_last_solr_update', 'auto'),
    ('public_title_facet', GENS(public_title_facet)),
    ('public_author_facet', GENS(public_author_facet)),
    ('public_series_facet', GENS(tp.copy_field('series'))),
    ('meetings_facet', GENS(tp.copy_field('meetings'))),
    ('public_subject_facet', GENS(tp.copy_field('full_subjects'))),
    ('geographic_terms_facet', GENS(tp.copy_field('geographic_terms'))),
    ('era_terms_facet', GENS(tp.copy_field('era_terms'))),
    ('public_genre_facet', GENS(tp.copy_field('genre_terms'))),
    ('suppressed', GENS.static(False)),
    ('text', GENS(tp.join_fields([
        'id', 'formats', 'languages', 'isbn_numbers', 'issn_numbers',
        'lccn_number', 'oclc_numbers', 'dewey_call_numbers',
        'loc_call_numbers', 'sudoc_numbers', 'other_call_numbers',
        'main_call_number', 'statement_of_responsibility', 'full_title',
        'alternate_titles', 'uniform_title', 'related_titles', 'corporations',
        'meetings', 'people', 'physical_characteristics', 'context_notes',
        'summary_notes', 'toc_notes', 'full_subjects', 'series',
        'series_creators'
    ])))
)


BLSUGGEST_FIELDS = (
    'id', 'heading', 'heading_display', 'heading_keyphrases', 'heading_sort',
    'heading_variations', 'seefrom_variations', 'more_context',
    'this_facet_values', 'heading_type', 'thing_type', 'record_count',
    'bib_location_codes', 'item_location_codes', 'material_type', 'languages',
    'publication_dates_facet', 'public_author_facet', 'public_title_facet',
    'public_series_facet', 'meetings_facet', 'public_genre_facet',
    'public_subject_facet', 'geographic_terms_facet', 'era_terms_facet',
    'game_facet'
)


# BL-Suggest field specific gen functions

def hash_id(record):
    id_ = '{}|{}'.format(record['heading_type'], record['heading'])
    id_ = base64.b64encode(hashlib.md5(id_.encode('utf-8')).digest())
    return id_


def heading(record):
    htype = record['heading_type']

    if htype == 'title':
        return tp.title_like(record)

    if htype == 'author':
        return tp.person_name_heading_like(record)

    if htype in ('subject', 'genre'):
        return tp.keyword_like(record)

    if htype == 'call_number':
        return tp.lc_cn(record)

    if htype == 'sudoc':
        return tp.sudoc_cn(record)


def this_facet_values(record):
    existing = record.get('this_facet_values', [])
    htype_to_facets = {
        'title': 'public_title_facet',
        'author': 'public_author_facet',
        'subject': 'public_subject_facet',
        'genre': 'public_genre_facet',
        'call_number': 'call_number',
        'sudoc': 'sudoc'
    }
    facet = htype_to_facets[record['heading_type']]
    return existing + ['{}:{}'.format(facet, record['heading'])]


BLSUGGEST_GENS = (
    ('heading_type', GENS.choice(['title'] * 5 + ['author'] * 3 +
                                 ['subject'] * 3 + ['genre'] * 3 +
                                 ['call_number'] * 2 + ['sudoc'])),
    ('heading', GENS(heading)),
    ('id', GENS(hash_id)),
    ('heading_display', GENS(tp.copy_field('heading'))),
    ('heading_keyphrases', GENS(tp.copy_field('heading'))),
    ('heading_sort', GENS(tp.copy_field('heading'))),
    ('heading_variations', GENS(tp.copy_field('heading'))),
    ('seefrom_variations', None),
    ('more_context', None),
    ('this_facet_values', GENS(this_facet_values)),
    ('thing_type', GENS(tp.copy_field('heading_type'))),
    ('record_count', GENS.type('int', mn=1, mx=1000)),
    ('bib_location_codes', GENS(tp.multi(GENS.type('string', mn=1, mx=5,
                                                   alphabet=tp.LETTERS_LOWER),
                                         1, 3))),
    ('item_location_codes', GENS(tp.multi(GENS.type('string', mn=1, mx=5,
                                                    alphabet=tp.LETTERS_LOWER),
                                          1, 3))),
    ('game_facet', GENS(tp.multi(GENS.type('string', mn=6, mx=10,
                                           alphabet=tp.LETTERS_LOWER + 
                                                    tp.NUMBERS), 1, 3))),
    ('material_type', GENS.type('string', mn=5, mx=15,
                                alphabet=tp.LETTERS_UPPER)),
    ('languages', 'auto'),
    ('publication_dates_facet', GENS(tp.chance(tp.multi(tp.year_like, 1, 5),
                                               90))),
    ('public_author_facet', GENS(tp.multi(tp.person_name_heading_like, 1, 5))),
    ('public_title_facet', GENS(tp.multi(tp.title_like, 1, 5))),
    ('public_series_facet', GENS(tp.chance(tp.multi(tp.title_like, 1, 5),
                                           50))),
    ('meetings_facet', GENS(tp.chance(tp.multi(tp.org_name_like, 1, 5),
                                             50))),
    ('public_genre_facet', GENS(tp.chance(tp.multi(tp.keyword_like, 1, 5),
                                          40))),
    ('public_subject_facet', GENS(tp.subjects)),
    ('geographic_terms_facet', GENS(tp.chance(tp.multi(tp.place_like, 1, 2),
                                              30))),
    ('era_terms_facet', GENS(tp.chance(tp.multi(tp.year_range_like, 1, 2),
                                       25))),
)
