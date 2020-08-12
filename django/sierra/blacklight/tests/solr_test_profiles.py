"""
Contains structures needed to make Solr test data for `blacklight` app.
"""

import hashlib
import base64
import random

from utils.test_helpers import solr_test_profiles as tp


SOLR_TYPES = tp.SOLR_TYPES
SOLR_TYPES['reverse_number'] = {'pytype': unicode, 'emtype': 'string'}
GLOBAL_UNIQUE_FIELDS = ('code', 'id', 'record_number')
GENS = tp.GENS


ALPHASOLRMARC_FIELDS = (
    'id', 'timestamp_of_last_solr_update', 'suppressed', 'date_added',
    'resource_type', 'items_json', 'has_more_items', 'more_items_json',
    'thumbnail_url', 'urls_json', 'call_numbers_display', 'sudocs_display',
    'isbns_display', 'issns_display', 'lccns_display', 'oclc_numbers_display',
    'isbn_numbers', 'issn_numbers', 'lccn_number', 'oclc_numbers',
    'other_standard_numbers_display', 'other_control_numbers_display',
    'publication_year_display', 'creation_display', 'publication_display',
    'distribution_display', 'manufacture_display', 'copyright_display',
    'publication_sort', 'publication_decade_facet', 'publication_year_facet',
    'access_facet', 'building_facet', 'shelf_facet', 'collection_facet',
    'resource_type_facet', 'media_type_facet', 'metadata_facets_search',
    'call_numbers_search', 'sudocs_search', 'standard_numbers_search',
    'control_numbers_search', 'publication_places_search', 'publishers_search',
    'publication_dates_search', 'publication_date_notes', 'author_json',
    'contributors_json', 'meetings_json', 'author_sort',
    'author_contributor_facet', 'meeting_facet', 'author_search',
    'contributors_search', 'meetings_search', 'responsibility_search',
    'responsibility_display', 'title_display', 'non_truncated_title_display',
    'included_work_titles_json', 'related_work_titles_json',
    'related_series_titles_json', 'variant_titles_notes', 'main_title_search',
    'included_work_titles_search', 'related_work_titles_search',
    'related_series_titles_search', 'variant_titles_search',
    'title_series_facet', 'title_sort', 'summary_notes', 'toc_notes',
    'physical_description', 'physical_medium', 'geospatial_data',
    'audio_characteristics', 'projection_characteristics',
    'video_characteristics', 'digital_file_characteristics',
    'graphic_representation', 'performance_medium', 'performers',
    'language_notes', 'dissertation_notes', 'notes',
    # OLD FIELDS ARE BELOW
    'game_facet', 'languages', 'era_terms', 'form_terms',
    'general_terms', 'genre_terms', 'geographic_terms', 'other_terms',
    'topic_terms', 'full_subjects',
    'public_subject_facet',
    'geographic_terms_facet', 'era_terms_facet', 'public_genre_facet', 'text'
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


def title_series_facet(record):
    fields = ('included_work_titles_search', 'related_work_titles_search',
              'related_series_titles_search')
    return _combine_fields(record, fields)


def author_contributor_facet(record):
    fields = ('author_search', 'contributors_search')
    return _combine_fields(record, fields)


def random_agent(person_weight=8, corp_weight=1, meeting_weight=1):
    def gen(record):
        rval = ''
        nametype = random.choice(['person'] * person_weight +
                                 ['corporation'] * corp_weight +
                                 ['meeting'] * meeting_weight)
        if nametype == 'person':
            rval = tp.person_name_heading_like(record)
        else:
            rval = tp.org_name_like(record)
        print rval
        return rval
    return gen


ALPHASOLRMARC_GENS = (
    ('id', GENS(tp.auto_increment('b', 10000001))),
    ('items_json', None),
    ('has_more_items', None),
    ('more_items_json', None),
    ('game_facet', GENS(tp.multi(GENS.type('string', mn=6, mx=10,
                                           alphabet=tp.LETTERS_LOWER + 
                                                    tp.NUMBERS), 1, 3))),
    ('languages', 'auto'),
    ('isbn_numbers', GENS(tp.chance(tp.multi(tp.isbn_number, 1, 5), 66))),
    ('issn_numbers', GENS(tp.chance(tp.multi(tp.issn_number, 1, 5), 33))),
    ('lccn_number', GENS(tp.chance(GENS.type('int', mn=10000, mx=99999), 80))),
    ('oclc_numbers', GENS(tp.chance(tp.multi(tp.oclc_number, 1, 2), 75))),
    ('isbns_display', GENS(tp.copy_field('isbn_numbers'))),
    ('issns_display', GENS(tp.copy_field('issn_numbers'))),
    ('lccns_display', GENS(tp.copy_field('lccn_number'))),
    ('oclc_numbers_display', GENS(tp.copy_field('oclc_numbers'))),
    ('other_standard_numbers_display', GENS(tp.chance(tp.multi(tp.isbn_number,
                                                               1, 2), 20))),
    ('other_control_numbers_display', GENS(tp.chance(tp.multi(tp.oclc_number,
                                                              1, 2), 20))),
    ('standard_numbers_search', GENS(tp.copy_field('isbns_display'))),
    ('control_numbers_search', GENS(tp.copy_field('oclc_numbers_display'))),
    ('call_numbers_display', GENS(tp.multi(tp.lc_cn, 1, 2))),
    ('sudocs_display', GENS(tp.chance(tp.multi(tp.sudoc_cn, 1, 2), 20))),
    ('call_numbers_search', GENS(tp.copy_field('call_numbers_display'))),
    ('sudocs_search', GENS(tp.copy_field('sudocs_display'))),
    ('title_display', GENS(tp.title_like)),
    ('main_title_search', GENS(tp.copy_field('title_display'))),
    ('non_truncated_title_display',
        GENS(tp.chance(tp.copy_field('title_display'), 20))),
    ('included_work_titles_json', None),
    ('related_work_titles_json', None),
    ('related_series_titles_json', None),
    ('included_work_titles_search', GENS(tp.chance(tp.multi(tp.title_like, 1,
                                                            3), 50))),
    ('related_work_titles_search', GENS(tp.chance(tp.multi(tp.title_like, 1, 3),
                                                   20))),
    ('related_series_titles_search', GENS(tp.chance(tp.multi(tp.title_like, 1,
                                                             3), 20))),
    ('variant_titles_notes', GENS(tp.chance(tp.multi(tp.title_like, 1, 3),
                                            20))),
    ('variant_titles_search', GENS(tp.copy_field('variant_titles_search'))),
    ('title_sort', GENS(tp.sortable_text_field('title_display'))),
    ('author_json', None),
    ('contributors_json', None),
    ('meetings_json', None),
    ('author_search', GENS(random_agent(8, 1, 1))),
    ('author_sort', GENS(lambda r: r['author_search'][0].lower())),
    ('contributors_search', GENS(tp.chance(tp.multi(random_agent(6, 3, 1),
                                                    1, 5), 75))),
    ('meetings_search', GENS(tp.chance(tp.multi(tp.org_name_like, 1, 3), 25))),
    ('responsibility_display', GENS(tp.chance(tp.statement_of_resp, 80))),
    ('responsibility_search', GENS(tp.copy_field('responsibility_display'))),
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
    ('timestamp_of_last_solr_update', 'auto'),
    ('title_series_facet', GENS(title_series_facet)),
    ('author_contributor_facet', GENS(author_contributor_facet)),
    ('meeting_facet', GENS(tp.copy_field('meetings_search'))),
    ('public_subject_facet', GENS(tp.copy_field('full_subjects'))),
    ('geographic_terms_facet', GENS(tp.copy_field('geographic_terms'))),
    ('era_terms_facet', GENS(tp.copy_field('era_terms'))),
    ('public_genre_facet', GENS(tp.copy_field('genre_terms'))),
    ('suppressed', GENS.static(False)),
    ('text', GENS(tp.join_fields([
        'id', 'languages', 'summary_notes', 'toc_notes', 'full_subjects'
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
