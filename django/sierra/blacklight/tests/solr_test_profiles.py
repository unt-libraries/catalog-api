"""
Contains structures needed to make Solr test data for `blacklight` app.
"""
from __future__ import absolute_import
from __future__ import print_function
import hashlib
import base64
import random

from six import text_type

from utils.test_helpers import solr_test_profiles as tp


SOLR_TYPES = tp.SOLR_TYPES
SOLR_TYPES['sint'] = {'pytype': int, 'emtype': 'int'}
SOLR_TYPES['cn_norm'] = {'pytype': text_type, 'emtype': 'string'}
SOLR_TYPES['heading_term_text'] = {'pytype': text_type, 'emtype': 'string'}
SOLR_TYPES['heading_term_text_stem'] = {'pytype': text_type, 'emtype': 'string'}
SOLR_TYPES['full_heading_text'] = {'pytype': text_type, 'emtype': 'string'}
GLOBAL_UNIQUE_FIELDS = ('code', 'id', 'record_number')
GENS = tp.GENS


DISCOVER_FIELDS = (
    'id', 'timestamp_of_last_solr_update', 'suppressed', 'date_added',
    'resource_type', 'items_json', 'has_more_items', 'more_items_json',
    'thumbnail_url', 'urls_json', 'call_numbers_display', 'sudocs_display',
    'isbns_display', 'issns_display', 'lccns_display', 'oclc_numbers_display',
    'isbn_numbers', 'issn_numbers', 'lccn_number', 'oclc_numbers',
    'all_standard_numbers', 'all_control_numbers',
    'other_standard_numbers_display', 'other_control_numbers_display',
    'publication_year_display', 'creation_display', 'publication_display',
    'distribution_display', 'manufacture_display', 'copyright_display',
    'publication_sort', 'publication_year_range_facet', 'access_facet',
    'building_facet', 'shelf_facet', 'collection_facet', 'resource_type_facet',
    'media_type_facet', 'metadata_facets_search', 'games_ages_facet',
    'games_duration_facet', 'games_players_facet', 'call_numbers_search',
    'sudocs_search', 'standard_numbers_search', 'control_numbers_search',
    'publication_places_search', 'publishers_search',
    'publication_dates_search', 'publication_date_notes', 'author_json',
    'contributors_json', 'meetings_json', 'author_sort',
    'author_contributor_facet', 'meeting_facet', 'author_search',
    'contributors_search', 'meetings_search', 'responsibility_search',
    'responsibility_display', 'title_display', 'non_truncated_title_display',
    'included_work_titles_json', 'related_work_titles_json',
    'related_series_titles_json', 'variant_titles_notes', 'main_title_search',
    'main_work_title_json', 'included_work_titles_search',
    'related_work_titles_search', 'related_series_titles_search',
    'variant_titles_search', 'title_series_facet', 'title_sort',
    'summary_notes', 'toc_notes', 'physical_description', 'physical_medium',
    'geospatial_data', 'audio_characteristics', 'projection_characteristics',
    'video_characteristics', 'digital_file_characteristics',
    'graphic_representation', 'performance_medium', 'performers',
    'language_notes', 'dissertation_notes', 'notes', 'subject_headings_json',
    'genre_headings_json', 'subject_heading_facet', 'genre_heading_facet',
    'topic_facet', 'era_facet', 'region_facet', 'genre_facet',
    'subjects_search_exact_headings', 'subjects_search_main_terms',
    'subjects_search_all_terms', 'genres_search_exact_headings',
    'genres_search_main_terms', 'genres_search_all_terms', 'languages',
    'record_boost', 'serial_continuity_linking_json',
    'related_resources_linking_json', 'editions_display', 'editions_search',
    'library_has_display', 'audience', 'creator_demographics',
    'curriculum_objectives', 'arrangement_of_materials', 'system_details'
)

# Discover field specific gen functions

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


def subjects_search_all_terms(record):
    fields = ('topic_facet', 'region_facet', 'era_facet')
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
        print(rval)
        return rval
    return gen


DISCOVER_GENS = (
    ('id', GENS(tp.auto_increment('b', 10000001))),
    ('items_json', None),
    ('has_more_items', None),
    ('more_items_json', None),
    ('games_ages_facet', 'auto'),
    ('games_duration_facet', 'auto'),
    ('games_players_facet', 'auto'),
    ('languages', 'auto'),
    ('publication_year_range_facet',
        GENS(tp.multi(GENS.type('int', mn=1000, mx=9999), 1, 5))),
    ('isbn_numbers', GENS(tp.chance(tp.multi(tp.isbn_number, 1, 5), 66))),
    ('issn_numbers', GENS(tp.chance(tp.multi(tp.issn_number, 1, 5), 33))),
    ('lccn_number', GENS(tp.chance(GENS.type('int', mn=10000, mx=99999), 80))),
    ('oclc_numbers', GENS(tp.chance(tp.multi(tp.oclc_number, 1, 2), 75))),
    ('isbns_display', GENS(tp.copy_field('isbn_numbers'))),
    ('issns_display', GENS(tp.copy_field('issn_numbers'))),
    ('lccns_display', GENS(tp.copy_field('lccn_number'))),
    ('oclc_numbers_display', GENS(tp.copy_field('oclc_numbers'))),
    ('all_standard_numbers', GENS(tp.copy_field('isbn_numbers'))),
    ('all_control_numbers', GENS(tp.copy_field('oclc_numbers'))),
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
    ('subject_headings_json', None),
    ('genre_headings_json', None),
    ('subject_heading_facet', GENS(tp.subjects)),
    ('genre_heading_facet',
        GENS(tp.chance(tp.multi(tp.keyword_like, 1, 2), 40))),
    ('topic_facet', GENS(tp.multi(tp.keyword_like, 1, 5))),
    ('era_facet', GENS(tp.chance(tp.multi(tp.year_range_like, 1, 2), 25))),
    ('region_facet', GENS(tp.chance(tp.multi(tp.keyword_like, 1, 2), 25))),
    ('genre_facet', GENS(tp.copy_field('genre_heading_facet'))),
    ('subjects_search_exact_headings',
        GENS(tp.copy_field('subject_heading_facet'))),
    ('subjects_search_main_terms',
        GENS(tp.copy_field('topic_facet'))),
    ('subjects_search_all_terms', GENS(subjects_search_all_terms)),
    ('genres_search_exact_headings',
        GENS(tp.copy_field('genre_heading_facet'))),
    ('genres_search_main_terms',
        GENS(tp.copy_field('genre_heading_facet'))),
    ('genres_search_all_terms', GENS(tp.copy_field('genre_heading_facet'))),
    ('timestamp_of_last_solr_update', 'auto'),
    ('title_series_facet', GENS(title_series_facet)),
    ('author_contributor_facet', GENS(author_contributor_facet)),
    ('meeting_facet', GENS(tp.copy_field('meetings_search'))),
    ('suppressed', GENS.static(False)),
)
