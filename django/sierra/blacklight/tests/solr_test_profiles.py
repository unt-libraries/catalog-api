"""
Contains structures needed to make Solr test data for `blacklight` app.
"""

from utils.test_helpers import solr_test_profiles as tp


SOLR_TYPES = tp.SOLR_TYPES
GLOBAL_UNIQUE_FIELDS = ('code', 'id', 'record_number')
GENS = tp.GENS


ALPHASOLRMARC_FIELDS = (
    'id', 'suppressed', 'bib_location_codes', 'item_location_codes',
    'game_facet', 'material_type', 'formats', 'languages', 'isbn_numbers',
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
    'series_creators', 'urls', 'url_labels', 'timestamp', 'text'
)


ALPHASOLRMARC_GENS = (
    ('id', GENS(tp.auto_increment('b', 10000001))),
    ('suppressed', GENS.static(False)),
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
    ('author_title_search', GENS(tp.join_fields(['creator', 'main_title']))),
    ('publishers', GENS(tp.chance(tp.multi(tp.org_name_like, 1, 3), 70))),
    ('publication_dates', GENS(tp.chance(tp.multi(tp.year_like, 1, 3), 90))),
    ('publication_places', GENS(tp.chance(tp.multi(tp.place_like, 1, 3), 60))),
    ('publication_country', GENS(tp.chance(tp.place_like), 60)),
    ('imprints', GENS(tp.imprints)),
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
    ('urls', GENS(tp.chance(tp.multi(tp.url_like, 1, 3), 75))),
    ('url_labels', None),
    ('timestamp', 'auto'),
    ('item_ids', None),
    ('item_record_numbers', None),
    ('text', GENS(tp.join_fields([
        'id', 'material_type', 'bib_location_codes', 'item_location_codes',
        'formats', 'languages', 'isbn_numbers', 'issn_numbers', 'lccn_number',
        'oclc_numbers', 'dewey_call_numbers', 'loc_call_numbers',
        'sudoc_numbers', 'other_call_numbers', 'main_call_number',
        'statement_of_responsibility', 'full_title', 'alternate_titles',
        'uniform_title', 'related_titles', 'corporations', 'meetings',
        'people', 'imprints', 'publishers', 'publication_country',
        'publication_dates', 'publication_places', 'physical_characteristics',
        'context_notes', 'summary_notes', 'toc_notes', 'full_subjects',
        'series', 'series_creators', 'url_labels'
    ])))
)
