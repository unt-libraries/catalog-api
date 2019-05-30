"""
Contains integration tests for the `api` app.
"""

from datetime import datetime
from pytz import utc

import pytest
from django.contrib.auth.models import User
from api.models import APIUser

from utils.test_helpers import solr_test_profiles as tp

# FIXTURES AND TEST DATA
# ---------------------------------------------------------------------
# External fixtures used below can be found in
# django/sierra/conftest.py:
#     api_solr_env
#     basic_solr_assembler
#     api_client
#     pick_reference_object_having_link
#     assert_obj_fields_match_serializer
#     get_linked_view_and_objects
#     assemble_test_records
#     do_filter_search
#     get_found_ids


# API_ROOT: Base URL for the API we're testing.
API_ROOT = '/api/v1/'

# RESOURCE_METADATA: Lookup dict for mapping API resources to various
# parameters for setting up tests.
RESOURCE_METADATA = {
    'bibs': {
        'profile': 'bib',
        'id_field': 'record_number',
        'links': { 'items': 'items' }
    },
    'items': {
        'profile': 'item',
        'id_field': 'record_number',
        'links': { 'bibs': 'parentBib', 'locations': 'location',
                   'itemtypes': 'itemtype', 'itemstatuses': 'itemstatus' }
    },
    'eresources': {
        'profile': 'eresource',
        'id_field': 'record_number',
        'links': None
    },
    'itemstatuses': {
        'profile': 'itemstatus',
        'id_field': 'code',
        'links': { 'items': 'items' }
    },
    'itemtypes': {
        'profile': 'itype',
        'id_field': 'code',
        'links': { 'items': 'items' }
    },
    'locations': {
        'profile': 'location',
        'id_field': 'code',
        'links': { 'items': 'items' }
    }
}


# PARAMETERS__* constants contain parametrization data for certain
# tests. Each should be a tuple, where the first tuple member is a
# header string that describes the parametrization values (such as
# what you'd pass as the first arg to pytest.mark.parametrize); the
# others are single-entry dictionaries where the key is the parameter-
# list ID (such as what you'd pass to pytest.mark.parametrize via its
# `ids` kwarg) and the value is the list of parameters for that ID.

# PARAMETERS__FILTER_TESTS__INTENDED: Parameters for testing API filter
# behavior that works as intended. The provided `search` query string
# matches the `test_data` record(s) they're supposed to match.
PARAMETERS__FILTER_TESTS__INTENDED = (
    'resource, test_data, search, expected',
    # EXACT (`exact`) filters should match exactly the text or value
    # passed to them. This is the default operator, if the client does
    # not specify one.
    { 'exact text (bibs/creator) | no operator specified => exact match':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator=Person, Test A. 1900-', ['TEST1'])
    }, { 'exact text (bibs/creator) | one match':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[exact]=Person, Test B. 1900-', ['TEST2']),
    }, { 'exact text (bibs/creator) | multiple matches':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test A. 1900-'}),
            ('TEST3', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[exact]=Person, Test A. 1900-', ['TEST1', 'TEST2']),
    }, { 'exact text (bibs/creator) | no matches':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[exact]=Test A. Person', None),
    }, { 'exact text (bibs/creator) | negated, one match':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[-exact]=Person, Test B. 1900-', ['TEST1']),
    }, { 'exact string (locations/label) | one match':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1'}),
            ('TEST2', {'label': 'TEST LABEL 2'}),
         ), 'label[exact]=TEST LABEL 1', ['TEST1']),
    }, { 'exact string (locations/label) | multiple matches':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1'}),
            ('TEST2', {'label': 'TEST LABEL 2'}),
            ('TEST3', {'label': 'TEST LABEL 1'}),
            ('TEST4', {'label': 'TEST LABEL 2'}),
         ), 'label[exact]=TEST LABEL 2', ['TEST2', 'TEST4']),
    }, { 'exact string (locations/label) | case does not match: no match':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1'}),
            ('TEST2', {'label': 'TEST LABEL 2'}),
         ), 'label[exact]=Test Label 2', None),
    }, { 'exact string (locations/label) | punct. does not match: no match':
        ('locations', (
            ('TEST1', {'label': 'TEST-LABEL 1'}),
            ('TEST2', {'label': 'TEST-LABEL 2'}),
         ), 'label[exact]=TEST LABEL 2', None),
    }, { 'exact string (locations/label) | negated, one match':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1'}),
            ('TEST2', {'label': 'TEST LABEL 2'}),
         ), 'label[-exact]=TEST LABEL 1', ['TEST2']),
    }, { 'exact int (items/copy_number) | one match':
        ('items', (
            ('TEST1', {'copy_number': 54}),
            ('TEST2', {'copy_number': 12}),
         ), 'copyNumber[exact]=54', ['TEST1']),
    }, { 'exact int (items/copy_number) | multiple matches':
        ('items', (
            ('TEST1', {'copy_number': 54}),
            ('TEST2', {'copy_number': 12}),
            ('TEST3', {'copy_number': 54}),
            ('TEST4', {'copy_number': 12}),
         ), 'copyNumber[exact]=54', ['TEST1', 'TEST3']),
    }, { 'exact int (items/copy_number) | no matches':
        ('items', (
            ('TEST1', {'copy_number': 54}),
            ('TEST2', {'copy_number': 12}),
         ), 'copyNumber[exact]=543', None),
    }, { 'exact int (items/copy_number) | negated, one match':
        ('items', (
            ('TEST1', {'copy_number': 54}),
            ('TEST2', {'copy_number': 12}),
         ), 'copyNumber[-exact]=54', ['TEST2']),
    }, { 'exact date (items/due_date) | one match':
        ('items', (
            ('TEST1', {'due_date': datetime(2018, 11, 30, 5, 0, 0,
                                            tzinfo=utc)}),
            ('TEST2', {'due_date': datetime(2018, 12, 13, 9, 0, 0,
                                            tzinfo=utc)}),
         ), 'dueDate[exact]=2018-11-30T05:00:00Z', ['TEST1']),
    }, { 'exact date (items/due_date) | multiple matches':
        ('items', (
            ('TEST1', {'due_date': datetime(2018, 11, 30, 5, 0, 0,
                                            tzinfo=utc)}),
            ('TEST2', {'due_date': datetime(2018, 11, 30, 5, 0, 0,
                                            tzinfo=utc)}),
            ('TEST3', {'due_date': datetime(2018, 12, 13, 9, 0, 0,
                                            tzinfo=utc)}),
         ), 'dueDate[exact]=2018-11-30T05:00:00Z', ['TEST1', 'TEST2']),
    }, { 'exact date (items/due_date) | no matches':
        ('items', (
            ('TEST1', {'due_date': datetime(2018, 11, 30, 5, 0, 0,
                                            tzinfo=utc)}),
            ('TEST2', {'due_date': datetime(2018, 12, 13, 9, 0, 0,
                                            tzinfo=utc)}),
         ), 'dueDate[exact]=1990-01-01T08:00:00Z', None),
    }, { 'exact date (items/due_date) | negated, one match':
        ('items', (
            ('TEST1', {'due_date': datetime(2018, 11, 30, 5, 0, 0,
                                            tzinfo=utc)}),
            ('TEST2', {'due_date': datetime(2018, 12, 13, 9, 0, 0,
                                            tzinfo=utc)}),
         ), 'dueDate[-exact]=2018-11-30T05:00:00Z', ['TEST2']),
    }, { 'exact bool (bibs/suppressed) | one match':
        ('bibs', (
            ('TEST1', {'suppressed': True}),
            ('TEST2', {'suppressed': False}),
         ), 'suppressed[exact]=true', ['TEST1']),
    }, { 'exact bool (bibs/suppressed) | multiple matches':
        ('bibs', (
            ('TEST1', {'suppressed': True}),
            ('TEST2', {'suppressed': False}),
            ('TEST3', {'suppressed': False}),
         ), 'suppressed[exact]=false', ['TEST2', 'TEST3']),
    }, { 'exact bool (bibs/suppressed) | no matches':
        ('bibs', (
            ('TEST1', {'suppressed': False}),
            ('TEST2', {'suppressed': False}),
         ), 'suppressed[exact]=true', None),
    }, { 'exact bool (bibs/suppressed) | negated, one match':
        ('bibs', (
            ('TEST1', {'suppressed': True}),
            ('TEST2', {'suppressed': False}),
         ), 'suppressed[-exact]=true', ['TEST2']),
    },
    # Note that we don't do extensive testing on multi-valued fields.
    # For any operator, only one of the multiple values in a given
    # field must match for the record to match.
    { 'exact multi (bibs/sudoc_numbers) | only 1 value must match':
        ('bibs', (
            ('TEST1', {'sudoc_numbers': ['Sudoc 1', 'Sudoc 2']}),
            ('TEST2', {'sudoc_numbers': ['Sudoc 3']}),
         ), 'sudocNumbers[exact]=Sudoc 1', ['TEST1']),
    },

    # STRING OPERATORS: `contains`, `startswith`, `endswith`, and
    # `matches`. These operators only work 100% correctly with string
    # fields. Due to tokenization during indexing, text fields behave
    # as though they are multi-valued fields containing individual
    # words, not complete strings. So, with text fields, you can filter
    # by what's in a single word, but you can't filter across multiple
    # words. Normalization (e.g. removing punctuation) affects things,
    # as well. Cases where filtering a text field does return what
    # you'd expect are here, but PARAMETERS__FILTER_TESTS__STRANGE
    # contains test cases that demonstrate the odd behavior. Dates,
    # integers, and boolean values don't work with string operators.

    # CONTAINS (`contains`) should return records where the query text
    # appears inside the field value, like a LIKE "%text%" SQL query.
    { 'contains text (bibs/creator) | one word, no punct.':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[contains]=A', ['TEST1']),
    }, { 'contains text (bibs/creator) | partial word, numeric':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
            ('TEST3', {'creator': 'Person, Test C. 2010-'}),
         ), 'creator[contains]=90', ['TEST1', 'TEST2']),
    }, { 'contains text (bibs/creator) | non-matching word: no match':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[contains]=Persona', None),
    }, { 'contains text (bibs/creator) | negated, one word, no punct.':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[-contains]=A', ['TEST2']),
    }, { 'contains string (locations/label) | full match':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1'}),
            ('TEST2', {'label': 'TEST LABEL 2'}),
         ), 'label[contains]=TEST LABEL 1', ['TEST1']),
    }, { 'contains string (locations/label) | multiple words, partial':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1-1'}),
            ('TEST2', {'label': 'TEST LABEL 2-2'}),
         ), 'label[contains]=BEL 1-', ['TEST1']),
    }, { 'contains string (locations/label) | multiple words, complete':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1-1'}),
            ('TEST2', {'label': 'TEST LABEL 2-2'}),
         ), 'label[contains]=LABEL 1-1', ['TEST1']),
    }, { 'contains string (locations/label) | single word, partial':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1'}),
            ('TEST2', {'label': 'TEST LABEL 2'}),
         ), 'label[contains]=LAB', ['TEST1', 'TEST2']),
    }, { 'contains string (locations/label) | single word, complete':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1'}),
            ('TEST2', {'label': 'TEST LABEL 2'}),
         ), 'label[contains]=LABEL', ['TEST1', 'TEST2']),
    }, { 'contains string (locations/label) | non-adjacent words: no match':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1'}),
            ('TEST2', {'label': 'TEST LABEL 2'}),
         ), 'label[contains]=TEST 1', None),
    }, { 'contains string (locations/label) | negated':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1'}),
            ('TEST2', {'label': 'TEST LABEL 2'}),
         ), 'label[-contains]=LABEL 1', ['TEST2']),
    },

    # STARTS WITH (`startswith`) returns records where the beginning of
    # the field value exactly matches the query text. Equivalent to a
    # LIKE "text%" SQL query.
    { 'startswith text (bibs/creator) | one word, no punct.':
        ('bibs', (
            ('TEST1', {'creator': 'Per, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[startswith]=Person', ['TEST2']),
    }, { 'startswith text (bibs/creator) | partial word':
        ('bibs', (
            ('TEST1', {'creator': 'Per, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[startswith]=Per', ['TEST1', 'TEST2']),
    }, { 'startswith text (bibs/creator) | negated':
        ('bibs', (
            ('TEST1', {'creator': 'Per, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[-startswith]=Person', ['TEST1']),
    }, { 'startswith string (locations/label) | full match':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1'}),
            ('TEST2', {'label': 'TEST LABEL 2'}),
         ), 'label[startswith]=TEST LABEL 1', ['TEST1']),
    }, { 'startswith string (locations/label) | partial match':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1'}),
            ('TEST2', {'label': 'TEST LABEL 2'}),
         ), 'label[startswith]=TEST LAB', ['TEST1', 'TEST2']),
    }, { 'startswith string (locations/label) | start mid-string: no match':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1'}),
            ('TEST2', {'label': 'TEST LABEL 2'}),
         ), 'label[startswith]=LABEL 1', None),
    }, { 'startswith string (locations/label) | partial non-match: no match':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1'}),
            ('TEST2', {'label': 'TEST LABEL 2'}),
         ), 'label[startswith]=TEST LB', None),
    }, { 'startswith string (locations/label) | negated':
        ('locations', (
            ('TEST1', {'label': 'TEST 1 LABEL'}),
            ('TEST2', {'label': 'TEST 2 LABEL'}),
         ), 'label[-startswith]=TEST 1', ['TEST2']),
    },

    # ENDS WITH (`endswith`) returns records where the end of the field
    # value exactly matches the query text. Equivalent to a LIKE
    # "%text" SQL query.
    { 'endswith text (bibs/creator) | one word, no punct.':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
         ), 'creator[endswith]=Beta', ['TEST2']),
    }, { 'endswith text (bibs/creator) | partial word':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
         ), 'creator[endswith]=pha', ['TEST1']),
    }, { 'endswith text (bibs/creator) | negated':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
         ), 'creator[-endswith]=Beta', ['TEST1']),
    }, { 'endswith string (locations/label) | full match':
        ('locations', (
            ('TEST1', {'label': 'TEST 1 LABEL'}),
            ('TEST2', {'label': 'TEST 2 LABEL'}),
         ), 'label[endswith]=TEST 1 LABEL', ['TEST1']),
    }, { 'endswith string (locations/label) | partial match':
        ('locations', (
            ('TEST1', {'label': 'TEST 1 LABEL'}),
            ('TEST2', {'label': 'TEST 2 LABEL'}),
         ), 'label[endswith]=1 LABEL', ['TEST1']),
    }, { 'endswith string (locations/label) | end mid-string: no match':
        ('locations', (
            ('TEST1', {'label': 'TEST 1 LABEL'}),
            ('TEST2', {'label': 'TEST 2 LABEL'}),
         ), 'label[endswith]=1 LAB', None),
    }, { 'endswith string (locations/label) | partial non-match: no match':
        ('locations', (
            ('TEST1', {'label': 'TEST 1 LABEL'}),
            ('TEST2', {'label': 'TEST 2 LABEL'}),
         ), 'label[endswith]=3 LABEL', None),
    }, { 'endswith string (locations/label) | negated':
        ('locations', (
            ('TEST1', {'label': 'TEST 1 LABEL'}),
            ('TEST2', {'label': 'TEST 2 LABEL'}),
         ), 'label[-endswith]=1 LABEL', ['TEST2']),
    },

    # MATCHES (`matches`) treats the query text as a regular expression
    # and attempts to find field values matching the regex. This is
    # still vaguly experimental--it isn't used in any of the production
    # systems that use the Catalog API, and it relies on Solr's regex
    # implementation, which is a little quirky. (Plus we're still using
    # an old version of Solr.) So, these tests aren't exhaustive--they
    # just demonstrate some of the things that do work.
    { 'matches text (bibs/creator) | match on a single word':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
         ), 'creator[matches]=.[Ee]ta', ['TEST2']),
    }, { 'matches text (bibs/creator) | match using pipe (e.g., or)':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
         ), 'creator[matches]=(Alpha|Beta)', ['TEST1', 'TEST2']),
    }, { 'matches text (bibs/creator) | negated':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
         ), 'creator[-matches]=.[Ee]ta', ['TEST1']),
    }, { 'matches string (locations/label) | ^ matches start of string':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1'}),
            ('TEST2', {'label': 'SECOND TEST LABEL'}),
         ), 'label[matches]=^TEST LABEL', ['TEST1']),
    }, { 'matches string (locations/label) | $ matches end of string':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1'}),
            ('TEST2', {'label': 'SECOND TEST LABEL'}),
         ), 'label[matches]=TEST LABEL$', ['TEST2']),
    }, { 'matches string (locations/label) | complex multi-word regex':
        ('locations', (
            ('TEST1', {'label': 'TEST LAB 1'}),
            ('TEST2', {'label': 'TEST LABEL 2'}),
         ), 'label[matches]=LAB(EL)? (1|2)$', ['TEST1', 'TEST2']),
    }, { 'matches string (locations/label) | no ^$ anchors':
        ('locations', (
            ('TEST1', {'label': 'TESTING LAB 1'}),
            ('TEST2', {'label': 'TEST LABEL 2'}),
         ), 'label[matches]=TEST(ING)? LAB', ['TEST1', 'TEST2']),
    },

    # KEYWORDS (`keywords`) is the only filter meant to be used mainly
    # with text fields. Essentially it just passes your query directly
    # to Solr, limited to whatever field you query, wrapped in
    # parentheses. Something like:
    # creator[keywords]="william shakespeare" OR "shakespeare, william"
    # is passed to Solr as:
    # fq=creator:("william shakespeare" OR "shakespeare, william")
    # Exact behavior of course depends on how the Solr text field is
    # set up (with what indexing processes, etc.). These tests show
    # that standard boolean keyword search behavior works as expected.
    { 'keywords text (bibs/creator) | single kw match':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
         ), 'creator[keywords]=Alpha', ['TEST1']),
    }, { 'keywords text (bibs/creator) | multiple kw matches':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
         ), 'creator[keywords]=Test Person Alpha', ['TEST1', 'TEST2']),
    }, { 'keywords text (bibs/creator) | kw phrase match':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
         ), 'creator[keywords]="Test Alpha"', ['TEST1']),
    }, { 'keywords text (bibs/creator) | kw phrase, wrong order: no matches':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
         ), 'creator[keywords]="Test Person Alpha"', None),
    }, { 'keywords text (bibs/creator) | kw match is case insensitive':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
         ), 'creator[keywords]="test alpha"', ['TEST1']),
    }, { 'keywords text (bibs/creator) | kw boolean AND':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
            ('TEST3', {'creator': 'Smith, Susan B.'}),
            ('TEST4', {'creator': 'Baker, Joseph'}),
         ), 'creator[keywords]=test AND person AND alpha', ['TEST1']),
    }, { 'keywords text (bibs/creator) | kw boolean OR':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
            ('TEST3', {'creator': 'Smith, Susan B.'}),
            ('TEST4', {'creator': 'Baker, Joseph'}),
         ), 'creator[keywords]=person OR smith', ['TEST1', 'TEST2', 'TEST3']),
    }, { 'keywords text (bibs/creator) | kw parenthetical groups':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
            ('TEST3', {'creator': 'Smith, Susan B.'}),
            ('TEST4', {'creator': 'Baker, Joseph'}),
         ), 'creator[keywords]=baker OR (test AND alpha)', ['TEST1', 'TEST4']),
    }, { 'keywords text (bibs/creator) | kw phrase with non-phrase':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
            ('TEST3', {'creator': 'Smith, Susan B.'}),
            ('TEST4', {'creator': 'Baker, Joseph'}),
         ), 'creator[keywords]="test alpha" smith', ['TEST1', 'TEST3']),
    }, { 'keywords text (bibs/creator) | right truncation':
        ('bibs', (
            ('TEST1', {'creator': 'Per, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
            ('TEST3', {'creator': 'Smith, Sonia B.'}),
            ('TEST4', {'creator': 'Baker, Joseph'}),
         ), 'creator[keywords]=per*', ['TEST1', 'TEST2']),
    }, { 'keywords text (bibs/creator) | left truncation':
        ('bibs', (
            ('TEST1', {'creator': 'Per, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
            ('TEST3', {'creator': 'Smith, Sonia B.'}),
            ('TEST4', {'creator': 'Baker, Joseph'}),
         ), 'creator[keywords]=*son', ['TEST2']),
    }, { 'keywords text (bibs/creator) | left and right truncation':
        ('bibs', (
            ('TEST1', {'creator': 'Per, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
            ('TEST3', {'creator': 'Smith, Sonia B.'}),
            ('TEST4', {'creator': 'Baker, Joseph'}),
         ), 'creator[keywords]=*so*', ['TEST2', 'TEST3']),
    }, { 'keywords text (bibs/creator) | negated':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
         ), 'creator[-keywords]=Alpha', ['TEST2']),
    },

    # NUMERIC OPERATORS: `gt`, `gte`, `lt`, `lte`, and `range`. These
    # work with integers, dates, and also strings. The best example of
    # a string field where these come in handy is call numbers, such as
    # filtering by call number range. HOWEVER, call numbers are special
    # because they don't strictly behave like strings and need to be
    # normalized to force proper behavior. E.g., MT 20 < MT 100 -- but
    # if not normalized to enforce that, MT 20 > MT 100 as a plain
    # string. (Call numbers are therefore NOT used in the below tests.)

    # GREATER THAN [OR EQUAL] (`gt`, `gte`)
    # LESS THAN [OR EQUAL] (`lt`, `lte`)
    # Return results where the value in the queried field is > (gt),
    # >= (gte), < (lt), or <= (lte) the query value.
    # Strings are compared like strings, from left to right:
    # "20" > "100"; "100" < "20"; "BC" > "ABC"; "ABC" < "BC".
    { 'gt int (items/copy_number) | field val > query val':
        ('items', (
            ('TEST50', {'copy_number': 50}),
            ('TEST51', {'copy_number': 51}),
            ('TEST52', {'copy_number': 52}),
            ('TEST53', {'copy_number': 53}),
            ('TEST54', {'copy_number': 54}),
            ('TEST55', {'copy_number': 55}),
         ), 'copyNumber[gt]=52', ['TEST53', 'TEST54', 'TEST55']),
    }, { 'gte int (items/copy_number) | field val >= query val':
        ('items', (
            ('TEST50', {'copy_number': 50}),
            ('TEST51', {'copy_number': 51}),
            ('TEST52', {'copy_number': 52}),
            ('TEST53', {'copy_number': 53}),
            ('TEST54', {'copy_number': 54}),
            ('TEST55', {'copy_number': 55}),
         ), 'copyNumber[gte]=52', ['TEST52', 'TEST53', 'TEST54', 'TEST55']),
    }, { 'lt int (items/copy_number) | field val < query val':
        ('items', (
            ('TEST50', {'copy_number': 50}),
            ('TEST51', {'copy_number': 51}),
            ('TEST52', {'copy_number': 52}),
            ('TEST53', {'copy_number': 53}),
            ('TEST54', {'copy_number': 54}),
            ('TEST55', {'copy_number': 55}),
         ), 'copyNumber[lt]=52', ['TEST50', 'TEST51']),
    }, { 'lte int (items/copy_number) | field val <= query val':
        ('items', (
            ('TEST50', {'copy_number': 50}),
            ('TEST51', {'copy_number': 51}),
            ('TEST52', {'copy_number': 52}),
            ('TEST53', {'copy_number': 53}),
            ('TEST54', {'copy_number': 54}),
            ('TEST55', {'copy_number': 55}),
         ), 'copyNumber[lte]=52', ['TEST50', 'TEST51', 'TEST52']),
    }, { 'gt int (items/copy_number) | negated':
        ('items', (
            ('TEST50', {'copy_number': 50}),
            ('TEST51', {'copy_number': 51}),
            ('TEST52', {'copy_number': 52}),
            ('TEST53', {'copy_number': 53}),
            ('TEST54', {'copy_number': 54}),
            ('TEST55', {'copy_number': 55}),
         ), 'copyNumber[-gt]=52', ['TEST50', 'TEST51', 'TEST52']),
    }, { 'gte int (items/copy_number) | negated':
        ('items', (
            ('TEST50', {'copy_number': 50}),
            ('TEST51', {'copy_number': 51}),
            ('TEST52', {'copy_number': 52}),
            ('TEST53', {'copy_number': 53}),
            ('TEST54', {'copy_number': 54}),
            ('TEST55', {'copy_number': 55}),
         ), 'copyNumber[-gte]=52', ['TEST50', 'TEST51']),
    }, { 'lt int (items/copy_number) | negated':
        ('items', (
            ('TEST50', {'copy_number': 50}),
            ('TEST51', {'copy_number': 51}),
            ('TEST52', {'copy_number': 52}),
            ('TEST53', {'copy_number': 53}),
            ('TEST54', {'copy_number': 54}),
            ('TEST55', {'copy_number': 55}),
         ), 'copyNumber[-lt]=52', ['TEST52', 'TEST53', 'TEST54', 'TEST55']),
    }, { 'lte int (items/copy_number) | negated':
        ('items', (
            ('TEST50', {'copy_number': 50}),
            ('TEST51', {'copy_number': 51}),
            ('TEST52', {'copy_number': 52}),
            ('TEST53', {'copy_number': 53}),
            ('TEST54', {'copy_number': 54}),
            ('TEST55', {'copy_number': 55}),
         ), 'copyNumber[-lte]=52', ['TEST53', 'TEST54', 'TEST55']),
    }, { 'gt date (items/due_date) | field val > query val':
        ('items', (
            ('TEST1', {'due_date': datetime(2018, 11, 30, 10, 0, 0,
                                            tzinfo=utc)}),
            ('TEST2', {'due_date': datetime(2018, 11, 30, 16, 0, 0,
                                            tzinfo=utc)}),
            ('TEST3', {'due_date': datetime(2018, 12, 01, 10, 0, 0,
                                            tzinfo=utc)}),
            ('TEST4', {'due_date': datetime(2018, 12, 02, 12, 0, 0,
                                            tzinfo=utc)}),
         ), 'dueDate[gt]=2018-11-30T16:00:00Z', ['TEST3', 'TEST4']),
    }, { 'gte date (items/due_date) | field val >= query val':
        ('items', (
            ('TEST1', {'due_date': datetime(2018, 11, 30, 10, 0, 0,
                                            tzinfo=utc)}),
            ('TEST2', {'due_date': datetime(2018, 11, 30, 16, 0, 0,
                                            tzinfo=utc)}),
            ('TEST3', {'due_date': datetime(2018, 12, 01, 10, 0, 0,
                                            tzinfo=utc)}),
            ('TEST4', {'due_date': datetime(2018, 12, 02, 12, 0, 0,
                                            tzinfo=utc)}),
         ), 'dueDate[gte]=2018-11-30T16:00:00Z', ['TEST2', 'TEST3', 'TEST4']),
    }, { 'lt date (items/due_date) | field val < query val':
        ('items', (
            ('TEST1', {'due_date': datetime(2018, 11, 30, 10, 0, 0,
                                            tzinfo=utc)}),
            ('TEST2', {'due_date': datetime(2018, 11, 30, 16, 0, 0,
                                            tzinfo=utc)}),
            ('TEST3', {'due_date': datetime(2018, 12, 01, 10, 0, 0,
                                            tzinfo=utc)}),
            ('TEST4', {'due_date': datetime(2018, 12, 02, 12, 0, 0,
                                            tzinfo=utc)}),
         ), 'dueDate[lt]=2018-11-30T16:00:00Z', ['TEST1']),
    }, { 'lte date (items/due_date) | field val <= query val':
        ('items', (
            ('TEST1', {'due_date': datetime(2018, 11, 30, 10, 0, 0,
                                            tzinfo=utc)}),
            ('TEST2', {'due_date': datetime(2018, 11, 30, 16, 0, 0,
                                            tzinfo=utc)}),
            ('TEST3', {'due_date': datetime(2018, 12, 01, 10, 0, 0,
                                            tzinfo=utc)}),
            ('TEST4', {'due_date': datetime(2018, 12, 02, 12, 0, 0,
                                            tzinfo=utc)}),
         ), 'dueDate[lte]=2018-11-30T16:00:00Z', ['TEST1', 'TEST2']),
    }, { 'gt string (locations/label) | numeric strings':
        ('locations', (
            ('TEST1', {'label': 'A 1'}),
            ('TEST10', {'label': 'A 10'}),
            ('TEST2', {'label': 'A 2'}),
            ('TEST20', {'label': 'A 20'}),
         ), 'label[gt]=A 10', ['TEST2', 'TEST20']),
    }, { 'gt string (locations/label) | alphabet strings':
        ('locations', (
            ('TEST_A1', {'label': 'A 1'}),
            ('TEST_A10', {'label': 'A 10'}),
            ('TEST_B1', {'label': 'B 1'}),
            ('TEST_B10', {'label': 'B 10'}),
         ), 'label[gt]=A 10', ['TEST_B1', 'TEST_B10']),
    }, { 'gt string (locations/label) | something > nothing':
        ('locations', (
            ('TEST_A', {'label': 'A'}),
            ('TEST_AB', {'label': 'AB'}),
            ('TEST_ABC', {'label': 'ABC'}),
            ('TEST_ABCD', {'label': 'ABCD'}),
         ), 'label[gt]=AB', ['TEST_ABC', 'TEST_ABCD']),
    }, { 'gt string (locations/label) | alphanumeric characters > formatting':
        ('locations', (
            ('TEST_A1', {'label': 'A-1'}),
            ('TEST_A2', {'label': 'A-2'}),
            ('TEST_AA', {'label': 'AA'}),
            ('TEST_AA1', {'label': 'AA-1'}),
         ), 'label[gt]=A-2', ['TEST_AA', 'TEST_AA1']),
    }, { 'gte string (locations/label) | field val >= query val':
        ('locations', (
            ('TEST1', {'label': 'A 1'}),
            ('TEST10', {'label': 'A 10'}),
            ('TEST2', {'label': 'A 2'}),
            ('TEST20', {'label': 'A 20'}),
         ), 'label[gte]=A 10', ['TEST10', 'TEST2', 'TEST20']),
    }, { 'lt string (locations/label) | field val < query val':
        ('locations', (
            ('TEST1', {'label': 'A 1'}),
            ('TEST10', {'label': 'A 10'}),
            ('TEST2', {'label': 'A 2'}),
            ('TEST20', {'label': 'A 20'}),
         ), 'label[lt]=A 10', ['TEST1']),
    }, { 'lte string (locations/label) | field val <= query val':
        ('locations', (
            ('TEST1', {'label': 'A 1'}),
            ('TEST10', {'label': 'A 10'}),
            ('TEST2', {'label': 'A 2'}),
            ('TEST20', {'label': 'A 20'}),
         ), 'label[lte]=A 10', ['TEST1', 'TEST10']),
    },

    # OPERATORS THAT TAKE ARRAYS: The next two operators we're testing
    # take arrays as arguments: `range` and `in`. Arrays are comma-
    # separated lists of values that are surrounded in square brackets,
    # such as: [1,2,3]. There are a few things to note about our array
    # syntax.
    # * Quotation marks can be used to surround any values, but they
    # are optional. If used, any commas appearing between the quotation
    # marks are interpreted literally, not as value separators. (Like
    # most CSV syntaxes.) E.g.: ["Smith, James","Jones, Susan"] is an
    # array containing two values, each of which contains a comma.
    # * A backslash character can be used to escape commas you want to
    # use literally (instead of using the quotation mark syntax). E.g.:
    # [Smith\, James, Jones\, Susan] is equivalent to the above.
    # * A backslash character escapes a quotation mark you need to use
    # literally in the query. [A book about \"something\"] (includes
    # the quotation marks as part of the query).
    # * Spaces included after commas are interpreted literally. E.g.,
    # with the array [1, 2, 3], the second value is " 2" and the third
    # is " 3".

    # RANGE (`range`) takes an array of two values -- [start,end] --
    # and returns results where the value in the queried field is in
    # the provided range. The range filter is inclusive: [1,3] matches
    # both 1 and 3 (and the range of values between).
    { 'range int (items/copy_number) | multi-value range':
        ('items', (
            ('TEST50', {'copy_number': 50}),
            ('TEST51', {'copy_number': 51}),
            ('TEST52', {'copy_number': 52}),
            ('TEST53', {'copy_number': 53}),
            ('TEST54', {'copy_number': 54}),
            ('TEST55', {'copy_number': 55}),
         ), 'copyNumber[range]=[52,54]', ['TEST52', 'TEST53', 'TEST54']),
    }, { 'range int (items/copy_number) | single-value range':
        ('items', (
            ('TEST50', {'copy_number': 50}),
            ('TEST51', {'copy_number': 51}),
            ('TEST52', {'copy_number': 52}),
            ('TEST53', {'copy_number': 53}),
            ('TEST54', {'copy_number': 54}),
            ('TEST55', {'copy_number': 55}),
         ), 'copyNumber[range]=[52,52]', ['TEST52']),
    }, { 'range int (items/copy_number) | non-matching range: no matches':
        ('items', (
            ('TEST50', {'copy_number': 50}),
            ('TEST51', {'copy_number': 51}),
            ('TEST52', {'copy_number': 52}),
            ('TEST53', {'copy_number': 53}),
            ('TEST54', {'copy_number': 54}),
            ('TEST55', {'copy_number': 55}),
         ), 'copyNumber[range]=[90,100]', None),
    }, { 'range int (items/copy_number) | negated':
        ('items', (
            ('TEST50', {'copy_number': 50}),
            ('TEST51', {'copy_number': 51}),
            ('TEST52', {'copy_number': 52}),
            ('TEST53', {'copy_number': 53}),
            ('TEST54', {'copy_number': 54}),
            ('TEST55', {'copy_number': 55}),
         ), 'copyNumber[-range]=[52,54]', ['TEST50', 'TEST51', 'TEST55']),
    }, { 'range date (items/due_date) | multi-value range':
        ('items', (
            ('TEST1', {'due_date': datetime(2018, 11, 30, 10, 0, 0,
                                            tzinfo=utc)}),
            ('TEST2', {'due_date': datetime(2018, 11, 30, 16, 0, 0,
                                            tzinfo=utc)}),
            ('TEST3', {'due_date': datetime(2018, 12, 01, 10, 0, 0,
                                            tzinfo=utc)}),
            ('TEST4', {'due_date': datetime(2018, 12, 02, 12, 0, 0,
                                            tzinfo=utc)}),
         ), 'dueDate[range]=[2018-11-30T16:00:00Z,2018-12-02T12:00:00Z]',
         ['TEST2', 'TEST3', 'TEST4']),
    }, { 'range string (locations/label) | multi-value range':
        ('locations', (
            ('TEST1', {'label': 'A 1'}),
            ('TEST10', {'label': 'A 10'}),
            ('TEST2', {'label': 'A 2'}),
            ('TEST20', {'label': 'A 20'}),
         ), 'label[range]=[A 1,A 2]', ['TEST1', 'TEST10', 'TEST2']),
    },

    # IN (`in`) takes an array of values and tries to find records
    # where the queried field value exactly matches one of the values
    # in the array. Equivalent to an SQL IN query. It works with all
    # field types, although it shares the `exact` operator's issues
    # with text fields, and querying boolean fields with IN doesn't
    # make any sense.
    { 'in text (bibs/creator) | one match':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A'}),
            ('TEST2', {'creator': 'Person, Test B'}),
            ('TEST3', {'creator': 'Person, Test C'}),
         ), 'creator[in]=["Person, Test A","Person, Test D"]', ['TEST1'])
    }, { 'in text (bibs/creator) | multiple matches':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A'}),
            ('TEST2', {'creator': 'Person, Test B'}),
            ('TEST3', {'creator': 'Person, Test C'}),
         ), 'creator[in]=["Person, Test A","Person, Test C"]',
         ['TEST1', 'TEST3'])
    }, { 'in string (locations/label) | one match':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1'}),
            ('TEST2', {'label': 'TEST LABEL 2'}),
         ), 'label[in]=[TEST LABEL 1,TEST LABEL 3]', ['TEST1']),
    }, { 'in string (locations/label) | multiple matches':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1'}),
            ('TEST2', {'label': 'TEST LABEL 2'}),
         ), 'label[in]=[TEST LABEL 1,TEST LABEL 2]', ['TEST1', 'TEST2']),
    }, { 'in string (locations/label) | escape quotation marks and commas':
        ('locations', (
            ('TEST1', {'label': 'TEST "LABEL" 1'}),
            ('TEST2', {'label': 'TEST "LABEL" 2'}),
            ('TEST3', {'label': 'TEST, 3'}),
         ), 'label[in]=[TEST \\"LABEL\\" 1,"TEST \\"LABEL\\" 2",TEST\\, 3]',
         ['TEST1', 'TEST2', 'TEST3']),
    }, { 'in string (locations/label) | negated':
        ('locations', (
            ('TEST1', {'label': 'TEST LABEL 1'}),
            ('TEST2', {'label': 'TEST LABEL 2'}),
         ), 'label[-in]=[TEST LABEL 1,TEST LABEL 3]', ['TEST2']),
    }, { 'in int (items/copy_number) | one match':
        ('items', (
            ('TEST1', {'copy_number': 54}),
            ('TEST2', {'copy_number': 12}),
         ), 'copyNumber[in]=[12,34,91]', ['TEST2']),
    }, { 'in int (items/copy_number) | mutiple matches':
        ('items', (
            ('TEST1', {'copy_number': 54}),
            ('TEST2', {'copy_number': 12}),
         ), 'copyNumber[in]=[12,34,54]', ['TEST1', 'TEST2']),
    }, { 'in date (items/due_date) | one match':
        ('items', (
            ('TEST1', {'due_date': datetime(2018, 11, 30, 5, 0, 0,
                                            tzinfo=utc)}),
            ('TEST2', {'due_date': datetime(2018, 12, 13, 9, 0, 0,
                                            tzinfo=utc)}),
         ), 'dueDate[in]=[2018-11-30T05:00:00Z,2019-01-30T05:00:00Z]',
         ['TEST1']),
    }, { 'in date (items/due_date) | multiple matches':
        ('items', (
            ('TEST1', {'due_date': datetime(2018, 11, 30, 5, 0, 0,
                                            tzinfo=utc)}),
            ('TEST2', {'due_date': datetime(2018, 12, 13, 9, 0, 0,
                                            tzinfo=utc)}),
         ), 'dueDate[in]=[2018-11-30T05:00:00Z,2018-12-13T09:00:00Z]',
         ['TEST1', 'TEST2']),
    },

    # IS NULL (`isnull`) always takes a boolean value as the query
    # argument. If false, returns records where the queried field
    # exists; if true, returns records where the queried field does not
    # exist. Note: behavior doesn't change based on field type, so just
    # testing one type of field is sufficient.
    { 'isnull text (bibs/creator) | true: one match':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A'}),
            ('TEST2', {'creator': 'Person, Test B'}),
            ('TEST3', {'creator': None}),
         ), 'creator[isnull]=true', ['TEST3'])
    }, { 'isnull text (bibs/creator) | false: multiple matches':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A'}),
            ('TEST2', {'creator': 'Person, Test B'}),
            ('TEST3', {'creator': None}),
         ), 'creator[isnull]=false', ['TEST1', 'TEST2'])
    }, { 'isnull text (bibs/creator) | true: no matches':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A'}),
            ('TEST2', {'creator': 'Person, Test B'}),
            ('TEST3', {'creator': 'Person, Test C'}),
         ), 'creator[isnull]=true', None)
    }, { 'isnull text (bibs/creator) | false: no matches':
        ('bibs', (
            ('TEST1', {'creator': None}),
            ('TEST2', {'creator': None}),
            ('TEST3', {'creator': None}),
         ), 'creator[isnull]=false', None)
    }, { 'isnull text (bibs/creator) | negated':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A'}),
            ('TEST2', {'creator': 'Person, Test B'}),
            ('TEST3', {'creator': None}),
         ), 'creator[-isnull]=true', ['TEST1', 'TEST2'])
    },

    # SEARCH / SEARCHTYPE: The `search` argument combined with a valid
    # `searchtype` conducts a full-text-style search of the targeted
    # resource. It's similar to the `keywords` operator in that it
    # passes your search query to Solr as a keyword query, but it
    # searches multiple fields at once (rather than just one field).
    # The `searchtype` argument corresponds with a set of fields,
    # weights, etc. defined in api.filters.HaystackFilter that are
    # passed to Solr along with the search query, for relevance
    # ranking.
    #
    # At the moment, 'journals' and 'databases' are the two valid
    # searchtypes. These were made specifically for the Bento Box
    # search. The tests below use query strings like the Bento Box API
    # uses.
    { 'searchtype journals | full_title match':
        ('bibs', (
            ('TEST1', {'full_title': 'Online Journal of Medicine',
                       'alternate_titles': ['MedJournal'],
                       'creator': 'Person, Test A.',
                       'full_subjects': ['Hearts', 'Eyeballs', 'Brains'],
                       'material_type': 'EJOURNALS',
                       'suppressed': False}),
            ('TEST2', {'full_title': 'Journal of Medicine in Print',
                       'alternate_titles': ['MedJournal'],
                       'creator': 'Person, Test A.',
                       'full_subjects': ['Hearts', 'Eyeballs', 'Brains'],
                       'material_type': 'JOURNALS',
                       'suppressed': False}),
            ('TEST3', {'full_title': 'Puppies Today',
                       'alternate_titles': ['Puppies'],
                       'creator': 'Person, Test B.',
                       'full_subjects': ['Puppers', 'Doge'],
                       'material_type': 'JOURNALS',
                       'suppressed': False}),
            ('TEST4', {'full_title': 'Texas Journal of Open Heart Surgery',
                       'alternate_titles': ['TJOHS'],
                       'creator': 'Person, Test C.',
                       'full_subjects': ['Hearts', 'Medicine'],
                       'material_type': 'EJOURNALS',
                       'suppressed': False}),
            ('TEST5', {'full_title': 'Book about Medicine',
                       'creator': 'Person, Test D.',
                       'full_subjects': ['Medicine', 'Medical stuff'],
                       'material_type': 'BOOKS',
                       'suppressed': False}),
            ('TEST6', {'full_title': 'Out-of-Print Journal of Medicine',
                       'creator': 'Person, Test A.',
                       'full_subjects': ['Medicine', 'Medical stuff'],
                       'material_type': 'JOURNALS',
                       'suppressed': True}),
         ), ('search="journal of medicine"'
             '&searchtype=journals&suppressed=false&materialType[in]=[JOURNAL,'
             'JOURNALS,EJOURNAL,EJOURNALS]'),
         ['TEST1', 'TEST2'])
    }, { 'searchtype journals | full_subjects match':
        ('bibs', (
            ('TEST1', {'full_title': 'Online Journal of Medicine',
                       'alternate_titles': ['MedJournal'],
                       'creator': 'Person, Test A.',
                       'full_subjects': ['Hearts', 'Eyeballs', 'Brains'],
                       'material_type': 'EJOURNALS',
                       'suppressed': False}),
            ('TEST2', {'full_title': 'Journal of Medicine in Print',
                       'alternate_titles': ['MedJournal'],
                       'creator': 'Person, Test A.',
                       'full_subjects': ['Hearts', 'Eyeballs', 'Brains'],
                       'material_type': 'JOURNALS',
                       'suppressed': False}),
            ('TEST3', {'full_title': 'Puppies Today',
                       'alternate_titles': ['Puppies'],
                       'creator': 'Person, Test B.',
                       'full_subjects': ['Puppers', 'Doge'],
                       'material_type': 'JOURNALS',
                       'suppressed': False}),
            ('TEST4', {'full_title': 'Texas Journal of Open Heart Surgery',
                       'alternate_titles': ['TJOHS'],
                       'creator': 'Person, Test C.',
                       'full_subjects': ['Hearts', 'Medicine'],
                       'material_type': 'EJOURNALS',
                       'suppressed': False}),
            ('TEST5', {'full_title': 'Book about Medicine',
                       'creator': 'Person, Test D.',
                       'full_subjects': ['Medicine', 'Medical stuff'],
                       'material_type': 'BOOKS',
                       'suppressed': False}),
            ('TEST6', {'full_title': 'Out-of-Print Journal of Medicine',
                       'creator': 'Person, Test A.',
                       'full_subjects': ['Medicine', 'Medical stuff'],
                       'material_type': 'JOURNALS',
                       'suppressed': True}),
         ), ('search=puppers'
             '&searchtype=journals&suppressed=false&materialType[in]=[JOURNAL,'
             'JOURNALS,EJOURNAL,EJOURNALS]'),
         ['TEST3'])
    }, { 'searchtype journals | title and subjects match':
        ('bibs', (
            ('TEST1', {'full_title': 'Online Journal of Medicine',
                       'alternate_titles': ['MedJournal'],
                       'creator': 'Person, Test A.',
                       'full_subjects': ['Hearts', 'Eyeballs', 'Brains'],
                       'material_type': 'EJOURNALS',
                       'suppressed': False}),
            ('TEST2', {'full_title': 'Journal of Medicine in Print',
                       'alternate_titles': ['MedJournal'],
                       'creator': 'Person, Test A.',
                       'full_subjects': ['Hearts', 'Eyeballs', 'Brains'],
                       'material_type': 'JOURNALS',
                       'suppressed': False}),
            ('TEST3', {'full_title': 'Puppies Today',
                       'alternate_titles': ['Puppies'],
                       'creator': 'Person, Test B.',
                       'full_subjects': ['Puppers', 'Doge'],
                       'material_type': 'JOURNALS',
                       'suppressed': False}),
            ('TEST4', {'full_title': 'Texas Journal of Open Heart Surgery',
                       'alternate_titles': ['TJOHS'],
                       'creator': 'Person, Test C.',
                       'full_subjects': ['Hearts', 'Medicine'],
                       'material_type': 'EJOURNALS',
                       'suppressed': False}),
            ('TEST5', {'full_title': 'Book about Medicine',
                       'creator': 'Person, Test D.',
                       'full_subjects': ['Medicine', 'Medical stuff'],
                       'material_type': 'BOOKS',
                       'suppressed': False}),
            ('TEST6', {'full_title': 'Out-of-Print Journal of Medicine',
                       'creator': 'Person, Test A.',
                       'full_subjects': ['Medicine', 'Medical stuff'],
                       'material_type': 'JOURNALS',
                       'suppressed': True}),
         ), ('search=medicine'
             '&searchtype=journals&suppressed=false&materialType[in]=[JOURNAL,'
             'JOURNALS,EJOURNAL,EJOURNALS]'),
         ['TEST1', 'TEST2', 'TEST4'])
    }, { 'searchtype journals | alternate_titles match':
        ('bibs', (
            ('TEST1', {'full_title': 'Online Journal of Medicine',
                       'alternate_titles': ['MedJournal'],
                       'creator': 'Person, Test A.',
                       'full_subjects': ['Hearts', 'Eyeballs', 'Brains'],
                       'material_type': 'EJOURNALS',
                       'suppressed': False}),
            ('TEST2', {'full_title': 'Journal of Medicine in Print',
                       'alternate_titles': ['MedJournal'],
                       'creator': 'Person, Test A.',
                       'full_subjects': ['Hearts', 'Eyeballs', 'Brains'],
                       'material_type': 'JOURNALS',
                       'suppressed': False}),
            ('TEST3', {'full_title': 'Puppies Today',
                       'alternate_titles': ['Puppies'],
                       'creator': 'Person, Test B.',
                       'full_subjects': ['Puppers', 'Doge'],
                       'material_type': 'JOURNALS',
                       'suppressed': False}),
            ('TEST4', {'full_title': 'Texas Journal of Open Heart Surgery',
                       'alternate_titles': ['TJOHS'],
                       'creator': 'Person, Test C.',
                       'full_subjects': ['Hearts', 'Medicine'],
                       'material_type': 'EJOURNALS',
                       'suppressed': False}),
            ('TEST5', {'full_title': 'Book about Medicine',
                       'creator': 'Person, Test D.',
                       'full_subjects': ['Medicine', 'Medical stuff'],
                       'material_type': 'BOOKS',
                       'suppressed': False}),
            ('TEST6', {'full_title': 'Out-of-Print Journal of Medicine',
                       'creator': 'Person, Test A.',
                       'full_subjects': ['Medicine', 'Medical stuff'],
                       'material_type': 'JOURNALS',
                       'suppressed': True}),
         ), ('search=medjournal'
             '&searchtype=journals&suppressed=false&materialType[in]=[JOURNAL,'
             'JOURNALS,EJOURNAL,EJOURNALS]'),
         ['TEST1', 'TEST2'])
    }, { 'searchtype journals | wrong suppression or mat type => no match':
        ('bibs', (
            ('TEST1', {'full_title': 'Online Journal of Medicine',
                       'alternate_titles': ['MedJournal'],
                       'creator': 'Person, Test A.',
                       'full_subjects': ['Hearts', 'Eyeballs', 'Brains'],
                       'material_type': 'EJOURNALS',
                       'suppressed': False}),
            ('TEST2', {'full_title': 'Journal of Medicine in Print',
                       'alternate_titles': ['MedJournal'],
                       'creator': 'Person, Test A.',
                       'full_subjects': ['Hearts', 'Eyeballs', 'Brains'],
                       'material_type': 'JOURNALS',
                       'suppressed': False}),
            ('TEST3', {'full_title': 'Puppies Today',
                       'alternate_titles': ['Puppies'],
                       'creator': 'Person, Test B.',
                       'full_subjects': ['Puppers', 'Doge'],
                       'material_type': 'JOURNALS',
                       'suppressed': False}),
            ('TEST4', {'full_title': 'Texas Journal of Open Heart Surgery',
                       'alternate_titles': ['TJOHS'],
                       'creator': 'Person, Test C.',
                       'full_subjects': ['Hearts', 'Medicine'],
                       'material_type': 'EJOURNALS',
                       'suppressed': False}),
            ('TEST5', {'full_title': 'Book about Medicine',
                       'creator': 'Person, Test D.',
                       'full_subjects': ['Medicine', 'Medical stuff'],
                       'material_type': 'BOOKS',
                       'suppressed': False}),
            ('TEST6', {'full_title': 'Out-of-Print Journal of Medicine',
                       'creator': 'Person, Test A.',
                       'full_subjects': ['Medicine', 'Medical stuff'],
                       'material_type': 'JOURNALS',
                       'suppressed': True}),
         ), ('search="medical stuff"'
             '&searchtype=journals&suppressed=false&materialType[in]=[JOURNAL,'
             'JOURNALS,EJOURNAL,EJOURNALS]'),
         None)
    }, { 'searchtype databases | title match':
        ('eresources', (
            ('TEST1', {'title': 'Medical Database',
                       'alternate_titles': ['MedDB'],
                       'subjects': ['Hearts', 'Brains', 'Medicine'],
                       'summary': 'This is a database about medical stuff.',
                       'holdings': ['Online Journal of Medicine',
                                    'Texas Journal of Open Heart Surgery'],
                       'suppressed': False}),
            ('TEST2', {'title': 'General Database',
                       'alternate_titles': ['EBSCO'],
                       'subjects': ['Nerds', 'Sweater vests', 'Studying'],
                       'summary': 'Resources for all your academic needs.',
                       'holdings': ['English Literature Today',
                                    'Math Today',
                                    'History Yesterday',
                                    'Neuroscience Today',
                                    'Psychology Today',
                                    'Ascots Today'],
                       'suppressed': False}),
            ('TEST3', {'title': 'English Database',
                       'alternate_titles': ['Tallyho', 'Bobs your uncle'],
                       'subjects': ['Tea', 'Football', 'The Queen'],
                       'summary': 'Resources for Englishmen.',
                       'holdings': ['English Literature Today',
                                    'Shakespeare'],
                       'suppressed': False}),
         ), ('search="medical database"'
             '&searchtype=databases&suppressed=false'),
         ['TEST1'])
    }, { 'searchtype databases | alternate_titles match':
        ('eresources', (
            ('TEST1', {'title': 'Medical Database',
                       'alternate_titles': ['MedDB'],
                       'subjects': ['Hearts', 'Brains', 'Medicine'],
                       'summary': 'This is a database about medical stuff.',
                       'holdings': ['Online Journal of Medicine',
                                    'Texas Journal of Open Heart Surgery'],
                       'suppressed': False}),
            ('TEST2', {'title': 'General Database',
                       'alternate_titles': ['EBSCO'],
                       'subjects': ['Nerds', 'Sweater vests', 'Studying'],
                       'summary': 'Resources for all your academic needs.',
                       'holdings': ['English Literature Today',
                                    'Math Today',
                                    'History Yesterday',
                                    'Neuroscience Today',
                                    'Psychology Today',
                                    'Ascots Today'],
                       'suppressed': False}),
            ('TEST3', {'title': 'English Database',
                       'alternate_titles': ['Tallyho', 'Bobs your uncle'],
                       'subjects': ['Tea', 'Football', 'The Queen'],
                       'summary': 'Resources for Englishmen.',
                       'holdings': ['English Literature Today',
                                    'Shakespeare'],
                       'suppressed': False}),
         ), ('search=EBSCO'
             '&searchtype=databases&suppressed=false'),
         ['TEST2'])
    }, { 'searchtype databases | holdings match':
        ('eresources', (
            ('TEST1', {'title': 'Medical Database',
                       'alternate_titles': ['MedDB'],
                       'subjects': ['Hearts', 'Brains', 'Medicine'],
                       'summary': 'This is a database about medical stuff.',
                       'holdings': ['Online Journal of Medicine',
                                    'Texas Journal of Open Heart Surgery'],
                       'suppressed': False}),
            ('TEST2', {'title': 'General Database',
                       'alternate_titles': ['EBSCO'],
                       'subjects': ['Nerds', 'Sweater vests', 'Studying'],
                       'summary': 'Resources for all your academic needs.',
                       'holdings': ['English Literature Today',
                                    'Math Today',
                                    'History Yesterday',
                                    'Neuroscience Today',
                                    'Psychology Today',
                                    'Ascots Today'],
                       'suppressed': False}),
            ('TEST3', {'title': 'English Database',
                       'alternate_titles': ['Tallyho', 'Bobs your uncle'],
                       'subjects': ['Tea', 'Football', 'The Queen'],
                       'summary': 'Resources for Englishmen.',
                       'holdings': ['English Literature Today',
                                    'Shakespeare'],
                       'suppressed': False}),
         ), ('search=English'
             '&searchtype=databases&suppressed=false'),
         ['TEST2', 'TEST3'])
    }, { 'searchtype databases | subjects match':
        ('eresources', (
            ('TEST1', {'title': 'Medical Database',
                       'alternate_titles': ['MedDB'],
                       'subjects': ['Hearts', 'Brains', 'Medicine'],
                       'summary': 'This is a database about medical stuff.',
                       'holdings': ['Online Journal of Medicine',
                                    'Texas Journal of Open Heart Surgery'],
                       'suppressed': False}),
            ('TEST2', {'title': 'General Database',
                       'alternate_titles': ['EBSCO'],
                       'subjects': ['Nerds', 'Sweater vests', 'Studying'],
                       'summary': 'Resources for all your academic needs.',
                       'holdings': ['English Literature Today',
                                    'Math Today',
                                    'History Yesterday',
                                    'Neuroscience Today',
                                    'Psychology Today',
                                    'Ascots Today'],
                       'suppressed': False}),
            ('TEST3', {'title': 'English Database',
                       'alternate_titles': ['Tallyho', 'Bobs your uncle'],
                       'subjects': ['Tea', 'Football', 'The Queen'],
                       'summary': 'Resources for Englishmen.',
                       'holdings': ['English Literature Today',
                                    'Shakespeare'],
                       'suppressed': False}),
         ), ('search=tea'
             '&searchtype=databases&suppressed=false'),
         ['TEST3'])
    }, { 'searchtype databases | summary match':
        ('eresources', (
            ('TEST1', {'title': 'Medical Database',
                       'alternate_titles': ['MedDB'],
                       'subjects': ['Hearts', 'Brains', 'Medicine'],
                       'summary': 'This is a database about medical stuff.',
                       'holdings': ['Online Journal of Medicine',
                                    'Texas Journal of Open Heart Surgery'],
                       'suppressed': False}),
            ('TEST2', {'title': 'General Database',
                       'alternate_titles': ['EBSCO'],
                       'subjects': ['Nerds', 'Sweater vests', 'Studying'],
                       'summary': 'Resources for all your academic needs.',
                       'holdings': ['English Literature Today',
                                    'Math Today',
                                    'History Yesterday',
                                    'Neuroscience Today',
                                    'Psychology Today',
                                    'Ascots Today'],
                       'suppressed': False}),
            ('TEST3', {'title': 'English Database',
                       'alternate_titles': ['Tallyho', 'Bobs your uncle'],
                       'subjects': ['Tea', 'Football', 'The Queen'],
                       'summary': 'Resources for Englishmen.',
                       'holdings': ['English Literature Today',
                                    'Shakespeare'],
                       'suppressed': False}),
         ), ('search=academic'
             '&searchtype=databases&suppressed=false'),
         ['TEST2'])
    },

    # MULTIPLE ARGUMENTS: Queries that use multiple arguments should
    # effectively "AND" them together, returning a set of records where
    # all queried fields match all query parameters.
    { 'multi-arg | multiple criteria against the same field':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test 1900-1950'}),
            ('TEST2', {'creator': 'Person, Test 1940-2010'}),
            ('TEST3', {'creator': 'Person, Test 1970-'}),
         ), 'creator[contains]=Person&creator[contains]=1970', ['TEST3'])
    }, { 'multi-arg | multiple criteria against a multi-valued field':
        ('bibs', (
            ('TEST1', {'sudoc_numbers': ['A 1', 'A 2', 'A 3']}),
            ('TEST2', {'sudoc_numbers': ['B 1', 'B 2']}),
            ('TEST3', {'sudoc_numbers': ['A 4', 'B 3']}),
         ), 'sudocNumbers[startswith]=A&sudocNumbers[startswith]=B', ['TEST3'])
    }, { 'multi-arg | multiple criteria against different fields':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test', 'suppressed': True}),
            ('TEST2', {'creator': 'Person, Test', 'suppressed': False}),
            ('TEST3', {'creator': 'Person, Test', 'suppressed': False}),
         ), 'creator=Person, Test&suppressed=false', ['TEST2', 'TEST3'])
    }, { 'multi-arg | kw query with multiple criteria':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test', 'suppressed': True}),
            ('TEST2', {'creator': 'Person, Joe', 'suppressed': False}),
            ('TEST3', {'creator': 'Person, Test', 'suppressed': False}),
         ), 'creator[keywords]=person OR test&suppressed=false',
         ['TEST2', 'TEST3'])
    }, { 'multi-arg | multiple criteria with negation':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test 1900-1950'}),
            ('TEST2', {'creator': 'Person, Test 1940-2010'}),
            ('TEST3', {'creator': 'Person, Test 1970-'}),
         ), 'creator[contains]=Person&creator[-contains]=1970',
         ['TEST1', 'TEST2'])
    }, { 'multi-arg | kw query with multiple criteria and negation':
        ('bibs', (
            ('TEST1', {'creator': 'Smith, Test', 'suppressed': True}),
            ('TEST2', {'creator': 'Smith, Joe', 'suppressed': False}),
            ('TEST3', {'creator': 'Person, Sally', 'suppressed': False}),
         ), 'creator[-keywords]=person OR test&suppressed=false', ['TEST2'])
    }, 
)

# PARAMETERS__FILTER_TESTS__STRANGE: Parameters for testing API filter
# behavior that either is unintentional, is counter to what you'd
# expect, or is ambiguous or undefined in some way (such as using
# operators with fields they weren't designed to be used with). This
# set of test parameters documents the known strange behavior. Some of
# it is legitimately buggy and we should go back and fix it later; some
# of it we may need to add validation for so we can alert the client.
# Or, it may be sufficient just to document it here.
PARAMETERS__FILTER_TESTS__STRANGE = (
    'resource, test_data, search, expected',
    { 'TO_FIX: exact text (bibs/creator) | matches keywords or phrases':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[exact]=Test A.', ['TEST1'])
    }, { 'TO_FIX: exact text (bibs/creator) | case does not have to match':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[exact]=person, test a. 1900-', ['TEST1'])
    }, { 'TO_FIX: exact text (bibs/creator) | punct. does not have to match':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[exact]=person test a 1900', ['TEST1'])
    }, { 'FYI: exact string (items/call_number) | CN normalization':
        # Call numbers are strings, but, unlike other strings, they are
        # normalized before matching, since, e.g., MT 100 == mt 100 ==
        # mt100 == MT-100.
        ('items', (
            ('TEST1', {'call_number': 'MT 100.1 .C35 1995'}),
            ('TEST2', {'call_number': 'MT 100.1 .G322 2001'}),
         ), 'callNumber[exact]=mt100.1 c35 1995', ['TEST1']),
    }, { 'TO_FIX: contains text (bibs/creator) | multiple words: no match':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[contains]=Test A. 1900-', None),
    }, { 'TO_FIX: contains text (bibs/creator) | punctuation: no match':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[contains]=A.', None),
    }, { 'FYI: contains string (items/call_number) | CN normalization':
        # Call numbers are strings, but, unlike other strings, they are
        # normalized before matching, since, e.g., MT 100 == mt 100 ==
        # mt100 == MT-100.
        ('items', (
            ('TEST1', {'call_number': 'MT 100.1 .C35 1995'}),
            ('TEST2', {'call_number': 'MT 100.1 .G322 2001'}),
         ), 'callNumber[contains]=100.1 c35', ['TEST1']),
    }, { 'UNSURE: contains int (items/copy_number) | no match, ever':
        ('items', (
            ('TEST32', {'copy_number': 32}),
            ('TEST320', {'copy_number': 320}),
            ('TEST3', {'copy_number': 3}),
            ('TEST2', {'copy_number': 2}),
            ('TEST321', {'copy_number': 321}),
            ('TEST392', {'copy_number': 392}),
            ('TEST932', {'copy_number': 932}),
            ('TEST3092', {'copy_number': 3092}),
         ), 'copyNumber[contains]=32', None),
    }, { 'TO_FIX: startswith text (bibs/creator) | matches start of any word':
        ('bibs', (
            ('TEST1', {'creator': 'Per Test A. 1900-'}),
            ('TEST2', {'creator': 'Person Test B. 1900-'}),
         ), 'creator[startswith]=Tes', ['TEST1', 'TEST2']),
    }, { 'TO_FIX: startswith text (bibs/creator) | multiple words: no match':
        ('bibs', (
            ('TEST1', {'creator': 'Per Test A. 1900-'}),
            ('TEST2', {'creator': 'Person Test B. 1900-'}),
         ), 'creator[startswith]=Person Test', None),
    }, { 'TO_FIX: startswith text (bibs/creator) | punctuation: no match':
        ('bibs', (
            ('TEST1', {'creator': 'Per, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[startswith]=Person,', None),
    }, { 'FYI: startswith string (items/call_number) | CN normalization':
        # Call numbers are strings, but, unlike other strings, they are
        # normalized before matching, since, e.g., MT 100 == mt 100 ==
        # mt100 == MT-100.
        ('items', (
            ('TEST1', {'call_number': 'MT 100.1 .C35 1995'}),
            ('TEST2', {'call_number': 'MT 100.1 .G322 2001'}),
         ), 'callNumber[startswith]=MT100', ['TEST1', 'TEST2']),
    }, { 'UNSURE: startswith int (items/copy_number) | no match, ever':
        ('items', (
            ('TEST32', {'copy_number': 32}),
            ('TEST320', {'copy_number': 320}),
            ('TEST3', {'copy_number': 3}),
            ('TEST2', {'copy_number': 2}),
            ('TEST321', {'copy_number': 321}),
            ('TEST392', {'copy_number': 392}),
            ('TEST932', {'copy_number': 932}),
            ('TEST3092', {'copy_number': 3092}),
         ), 'copyNumber[startswith]=3', None),
    }, { 'TO_FIX: endswith text (bibs/creator) | matches end of any word':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
         ), 'creator[endswith]=est', ['TEST1', 'TEST2']),
    }, { 'TO_FIX: endswith text (bibs/creator) | multiple words: no match':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test Alpha'}),
            ('TEST2', {'creator': 'Person, Test Beta'}),
         ), 'creator[endswith]=Test Alpha', None),
    }, { 'TO_FIX: endswith text (bibs/creator) | punctuation: no match':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[endswith]=1900-', None),
    }, { 'FYI: endswith string (items/call_number) | CN normalization':
        # Call numbers are strings, but, unlike other strings, they are
        # normalized before matching, since, e.g., MT 100 == mt 100 ==
        # mt100 == MT-100.
        ('items', (
            ('TEST1', {'call_number': 'MT 100.1 .C35 1995'}),
            ('TEST2', {'call_number': 'MT 100.1 .G322 2001'}),
         ), 'callNumber[endswith]=100.1 c35 1995', ['TEST1']),
    }, { 'UNSURE: endswith int (items/copy_number) | no match, ever':
        ('items', (
            ('TEST32', {'copy_number': 32}),
            ('TEST320', {'copy_number': 320}),
            ('TEST3', {'copy_number': 3}),
            ('TEST2', {'copy_number': 2}),
            ('TEST321', {'copy_number': 321}),
            ('TEST392', {'copy_number': 392}),
            ('TEST932', {'copy_number': 932}),
            ('TEST3092', {'copy_number': 3092}),
         ), 'copyNumber[endswith]=2', None),
    }, { 'TO_FIX: matches text (bibs/creator) | ^ matches start of word':
        ('bibs', (
            ('TEST1', {'creator': 'Smith, Sonia'}),
            ('TEST2', {'creator': 'Person, Test'}),
         ), 'creator[matches]=^[Ss]on', ['TEST1']),
    }, { 'TO_FIX: matches text (bibs/creator) | $ matches end of word':
        ('bibs', (
            ('TEST1', {'creator': 'Smith, Sonia'}),
            ('TEST2', {'creator': 'Person, Test'}),
         ), 'creator[matches]=[Ss]on$', ['TEST2']),
    }, { 'TO_FIX: matches text (bibs/creator) | cannot match across >1 words':
        ('bibs', (
            ('TEST1', {'creator': 'Test A Person'}),
            ('TEST2', {'creator': 'Test B Person'}),
         ), 'creator[matches]=Test [AB] Person', None),
    }, { 'TO_FIX: matches text (bibs/creator) | punctuation: no match':
        ('bibs', (
            ('TEST1', {'creator': 'Person, Test A. 1900-'}),
            ('TEST2', {'creator': 'Person, Test B. 1900-'}),
         ), 'creator[matches]=Person,', None),
    }, { 'FYI: matches string (items/call_number) | CN normalization':
        # Call numbers are strings, but, unlike other strings, they are
        # normalized before matching, since, e.g., MT 100 == mt 100 ==
        # mt100 == MT-100.
        ('items', (
            ('TEST1', {'call_number': 'MT 100.1 .C35 1995'}),
            ('TEST2', {'call_number': 'MT 100.1 .G322 2001'}),
         ), 'callNumber[matches]=^mt100', ['TEST1', 'TEST2']),
    }, { 'UNSURE: matches int (items/copy_number) | no match, ever':
        ('items', (
            ('TEST1', {'copy_number': 32}),
            ('TEST2', {'copy_number': 320}),
         ), 'copyNumber[matches]=^3', None),
    }, { 'TO_FIX: gt/gte/lt/lte string (items/call_number) | CN normalization':
        # Call number normalization for searching is useless for
        # gt/gte/lt/lte/range comparisons, but that's currently what's
        # used. Currently doing a call_number[gt]=mt100 filter will
        # match both "MT 20" and "MT 1 .B82" -- because the search
        # normalization removes spaces and punctuation. (MT 20 ==> MT20
        # and MT 1 .B82 ==> MT1B82.) We SHOULD use call number sort
        # normalization for these operators.
        ('items', (
            ('TEST1', {'call_number': 'MT 100.1 .C35 1995'}),
            ('TEST2', {'call_number': 'MT 20'}),
            ('TEST3', {'call_number': 'MT 1 .B82'}),
         ), 'callNumber[gt]=mt100', ['TEST1', 'TEST2', 'TEST3']),
    }, 
)


# PARAMETERS__ORDERBY_TESTS__INTENDED: Parameters for testing API
# filters that use an orderBy parameter (to define what order to return
# results in). These are similar to the
# PARAMETERS__FILTER_TESTS__INTENDED parameters, but they include an
# orderBy parameter in the search string.
PARAMETERS__ORDERBY_TESTS__INTENDED = (
    'resource, test_data, search, expected',
    { 'order by int (items/copy_number) | ascending':
        ('items', (
            ('TEST11', {'volume': 'TEST', 'copy_number': 11}),
            ('TEST2', {'volume': 'TEST', 'copy_number': 2}),
            ('TEST1', {'volume': 'TEST', 'copy_number': 1}),
            ('TEST200', {'volume': 'TEST', 'copy_number': 200}),
            ('TEST10', {'volume': 'TEST', 'copy_number': 10}),
            ('TEST3', {'volume': 'TEST', 'copy_number': 3}),
         ), 'volume=TEST&orderBy=copyNumber',
         ['TEST1', 'TEST2', 'TEST3', 'TEST10', 'TEST11', 'TEST200']),
    }, { 'order by int (items/copy_number) | descending':
        ('items', (
            ('TEST11', {'volume': 'TEST', 'copy_number': 11}),
            ('TEST2', {'volume': 'TEST', 'copy_number': 2}),
            ('TEST1', {'volume': 'TEST', 'copy_number': 1}),
            ('TEST200', {'volume': 'TEST', 'copy_number': 200}),
            ('TEST10', {'volume': 'TEST', 'copy_number': 10}),
            ('TEST3', {'volume': 'TEST', 'copy_number': 3}),
         ), 'volume=TEST&orderBy=-copyNumber',
         ['TEST200', 'TEST11', 'TEST10', 'TEST3', 'TEST2', 'TEST1']),
    }, { 'order by string (items/barcode) | ascending':
        ('items', (
            ('TEST11', {'volume': 'TEST', 'barcode': 'A11'}),
            ('TEST2', {'volume': 'TEST', 'barcode': 'A2'}),
            ('TEST1', {'volume': 'TEST', 'barcode': 'A1'}),
            ('TEST200', {'volume': 'TEST', 'barcode': 'A200'}),
            ('TEST10', {'volume': 'TEST', 'barcode': 'A10'}),
            ('TEST3', {'volume': 'TEST', 'barcode': 'A3'}),
         ), 'volume=TEST&orderBy=barcode',
         ['TEST1', 'TEST10', 'TEST11', 'TEST2', 'TEST200', 'TEST3']),
    }, { 'order by string (items/barcode) | descending':
        ('items', (
            ('TEST11', {'volume': 'TEST', 'barcode': 'A11'}),
            ('TEST2', {'volume': 'TEST', 'barcode': 'A2'}),
            ('TEST1', {'volume': 'TEST', 'barcode': 'A1'}),
            ('TEST200', {'volume': 'TEST', 'barcode': 'A200'}),
            ('TEST10', {'volume': 'TEST', 'barcode': 'A10'}),
            ('TEST3', {'volume': 'TEST', 'barcode': 'A3'}),
         ), 'volume=TEST&orderBy=-barcode',
         ['TEST3', 'TEST200', 'TEST2', 'TEST11', 'TEST10', 'TEST1']),
    }, { 'order by date (items/checkout_date) | ascending':
        ('items', (
            ('TEST4', {'volume': 'TEST',
                       'checkout_date': datetime(2018, 10, 11, 2, 0, 0,
                                                  tzinfo=utc)}),
            ('TEST1', {'volume': 'TEST',
                       'checkout_date': datetime(2018, 2, 20, 0, 0, 0,
                                                  tzinfo=utc)}),
            ('TEST6', {'volume': 'TEST',
                       'checkout_date': datetime(2019, 1, 1, 12, 0, 0,
                                                  tzinfo=utc)}),
            ('TEST3', {'volume': 'TEST',
                       'checkout_date': datetime(2018, 10, 2, 2, 0, 0,
                                                  tzinfo=utc)}),
            ('TEST5', {'volume': 'TEST', 
                       'checkout_date': datetime(2018, 10, 11, 11, 0, 0,
                                                  tzinfo=utc)}),
            ('TEST2', {'volume': 'TEST',
                       'checkout_date': datetime(2018, 2, 20, 0, 0, 1,
                                                  tzinfo=utc)}),
         ), 'volume=TEST&orderBy=checkoutDate',
         ['TEST1', 'TEST2', 'TEST3', 'TEST4', 'TEST5', 'TEST6']),
    }, { 'order by date (items/checkout_date) | descending':
        ('items', (
            ('TEST4', {'volume': 'TEST',
                       'checkout_date': datetime(2018, 10, 11, 2, 0, 0,
                                                  tzinfo=utc)}),
            ('TEST1', {'volume': 'TEST',
                       'checkout_date': datetime(2018, 2, 20, 0, 0, 0,
                                                  tzinfo=utc)}),
            ('TEST6', {'volume': 'TEST',
                       'checkout_date': datetime(2019, 1, 1, 12, 0, 0,
                                                  tzinfo=utc)}),
            ('TEST3', {'volume': 'TEST',
                       'checkout_date': datetime(2018, 10, 2, 2, 0, 0,
                                                  tzinfo=utc)}),
            ('TEST5', {'volume': 'TEST', 
                       'checkout_date': datetime(2018, 10, 11, 11, 0, 0,
                                                  tzinfo=utc)}),
            ('TEST2', {'volume': 'TEST',
                       'checkout_date': datetime(2018, 2, 20, 0, 0, 1,
                                                  tzinfo=utc)}),
         ), 'volume=TEST&orderBy=-checkoutDate',
         ['TEST6', 'TEST5', 'TEST4', 'TEST3', 'TEST2', 'TEST1']),
    }, { 'order by multiple | string asc, int asc':
        ('items', (
            ('TEST5', {'volume': 'TEST', 'copy_number': 1, 'barcode': 'B'}),
            ('TEST6', {'volume': 'TEST', 'copy_number': 2, 'barcode': 'B'}),
            ('TEST2', {'volume': 'TEST', 'copy_number': 2, 'barcode': 'A'}),
            ('TEST1', {'volume': 'TEST', 'copy_number': 1, 'barcode': 'A'}),            
            ('TEST3', {'volume': 'TEST', 'copy_number': 10, 'barcode': 'A'}),
            ('TEST4', {'volume': 'TEST', 'copy_number': 1, 'barcode': 'AA'}),
         ), 'volume=TEST&orderBy=barcode,copyNumber',
         ['TEST1', 'TEST2', 'TEST3', 'TEST4', 'TEST5', 'TEST6']),
    }, { 'order by multiple | string desc, int desc':
        ('items', (
            ('TEST5', {'volume': 'TEST', 'copy_number': 1, 'barcode': 'B'}),
            ('TEST6', {'volume': 'TEST', 'copy_number': 2, 'barcode': 'B'}),
            ('TEST2', {'volume': 'TEST', 'copy_number': 2, 'barcode': 'A'}),
            ('TEST1', {'volume': 'TEST', 'copy_number': 1, 'barcode': 'A'}),            
            ('TEST3', {'volume': 'TEST', 'copy_number': 10, 'barcode': 'A'}),
            ('TEST4', {'volume': 'TEST', 'copy_number': 1, 'barcode': 'AA'}),
         ), 'volume=TEST&orderBy=-barcode,-copyNumber',
         ['TEST6', 'TEST5', 'TEST4', 'TEST3', 'TEST2', 'TEST1']),
    }, { 'order by multiple | int asc, string asc':
        ('items', (
            ('TEST3', {'volume': 'TEST', 'copy_number': 1, 'barcode': 'B'}),
            ('TEST5', {'volume': 'TEST', 'copy_number': 2, 'barcode': 'B'}),
            ('TEST4', {'volume': 'TEST', 'copy_number': 2, 'barcode': 'A'}),
            ('TEST1', {'volume': 'TEST', 'copy_number': 1, 'barcode': 'A'}),            
            ('TEST6', {'volume': 'TEST', 'copy_number': 10, 'barcode': 'A'}),
            ('TEST2', {'volume': 'TEST', 'copy_number': 1, 'barcode': 'AA'}),
         ), 'volume=TEST&orderBy=copyNumber,barcode',
         ['TEST1', 'TEST2', 'TEST3', 'TEST4', 'TEST5', 'TEST6']),
    }, { 'order by multiple | int desc, string desc':
        ('items', (
            ('TEST3', {'volume': 'TEST', 'copy_number': 1, 'barcode': 'B'}),
            ('TEST5', {'volume': 'TEST', 'copy_number': 2, 'barcode': 'B'}),
            ('TEST4', {'volume': 'TEST', 'copy_number': 2, 'barcode': 'A'}),
            ('TEST1', {'volume': 'TEST', 'copy_number': 1, 'barcode': 'A'}),            
            ('TEST6', {'volume': 'TEST', 'copy_number': 10, 'barcode': 'A'}),
            ('TEST2', {'volume': 'TEST', 'copy_number': 1, 'barcode': 'AA'}),
         ), 'volume=TEST&orderBy=-copyNumber,-barcode',
         ['TEST6', 'TEST5', 'TEST4', 'TEST3', 'TEST2', 'TEST1']),
    }, { 'order by multiple | int asc, string desc':
        ('items', (
            ('TEST1', {'volume': 'TEST', 'copy_number': 1, 'barcode': 'B'}),
            ('TEST4', {'volume': 'TEST', 'copy_number': 2, 'barcode': 'B'}),
            ('TEST5', {'volume': 'TEST', 'copy_number': 2, 'barcode': 'A'}),
            ('TEST3', {'volume': 'TEST', 'copy_number': 1, 'barcode': 'A'}),            
            ('TEST6', {'volume': 'TEST', 'copy_number': 10, 'barcode': 'A'}),
            ('TEST2', {'volume': 'TEST', 'copy_number': 1, 'barcode': 'AA'}),
         ), 'volume=TEST&orderBy=copyNumber,-barcode',
         ['TEST1', 'TEST2', 'TEST3', 'TEST4', 'TEST5', 'TEST6']),
    }, { 'order by multiple | int desc, string asc':
        ('items', (
            ('TEST1', {'volume': 'TEST', 'copy_number': 1, 'barcode': 'B'}),
            ('TEST4', {'volume': 'TEST', 'copy_number': 2, 'barcode': 'B'}),
            ('TEST5', {'volume': 'TEST', 'copy_number': 2, 'barcode': 'A'}),
            ('TEST3', {'volume': 'TEST', 'copy_number': 1, 'barcode': 'A'}),            
            ('TEST6', {'volume': 'TEST', 'copy_number': 10, 'barcode': 'A'}),
            ('TEST2', {'volume': 'TEST', 'copy_number': 1, 'barcode': 'AA'}),
         ), 'volume=TEST&orderBy=-copyNumber,barcode',
         ['TEST6', 'TEST5', 'TEST4', 'TEST3', 'TEST2', 'TEST1']),
    },
)


# PARAMETERS__ORDERBY_TESTS__STRANGE: Parameters for testing API
# filters that use an orderBy parameter and don't quite behave as you
# might expect. These are similar to the
# PARAMETERS__FILTER_TESTS__STRANGE parameters, but they include an
# orderBy parameter in the search string.
PARAMETERS__ORDERBY_TESTS__STRANGE = (
    'resource, test_data, search, expected',
    # Order by TEXT fields: Currently we don't actually allow ordering
    # by any text fields (hasn't been needed). If we ever enable that,
    # we should add `strange` tests here to capture the odd ordering
    # behavior, and then work to fix it, if it's still broken at that
    # point.

    # Order by CALL NUMBERS: Sorting items by call number is core
    # functionality. So why am I putting it in STRANGE? Most fields
    # actually sort on the field in the `orderBy` parameter. But for
    # call numbers, if a request contains 'orderBy=callNumber', it
    # uses the `call_number_sort` field instead, automatically. Which
    # ... maybe that would be okay if `callNumberSort` weren't a field
    # that the API exposes (which it is)! To make things even stranger,
    # 'orderBy=callNumberSort' doesn't work, because it's not a field
    # that's enabled for orderBy. So--it's a case where the API tries
    # to be smarter than the API consumer, but the behavior isn't
    # consistent with how other fields behave, so it may be confusing.
    { 'order by call number (items/call_number) | ascending':
        ('items', (
            ('TEST3', {'volume': 'TEST', 'call_number_type': 'lc',
                       'call_number': 'MT 100 .G322 2001'}),
            ('TEST6', {'volume': 'TEST', 'call_number_type': 'lc',
                       'call_number': 'MT 120 .G322 2001'}),
            ('TEST1', {'volume': 'TEST', 'call_number_type': 'lc',
                       'call_number': 'MS 100 .C35 1995'}),
            ('TEST5', {'volume': 'TEST', 'call_number_type': 'lc',
                       'call_number': 'MT 100.1 .A2 1999'}),
            ('TEST2', {'volume': 'TEST', 'call_number_type': 'lc',
                       'call_number': 'MT 20 .B5 2016'}),
            ('TEST4', {'volume': 'TEST', 'call_number_type': 'lc',
                       'call_number': 'MT 100.1 .A12 1999'}),
         ), 'volume=TEST&orderBy=callNumber',
         ['TEST1', 'TEST2', 'TEST3', 'TEST4', 'TEST5', 'TEST6']),
    }, { 'order by call number (items/call_number) | descending':
        ('items', (
            ('TEST3', {'volume': 'TEST', 'call_number_type': 'lc',
                       'call_number': 'MT 100 .G322 2001'}),
            ('TEST6', {'volume': 'TEST', 'call_number_type': 'lc',
                       'call_number': 'MT 120 .G322 2001'}),
            ('TEST1', {'volume': 'TEST', 'call_number_type': 'lc',
                       'call_number': 'MS 100 .C35 1995'}),
            ('TEST5', {'volume': 'TEST', 'call_number_type': 'lc',
                       'call_number': 'MT 100.1 .A2 1999'}),
            ('TEST2', {'volume': 'TEST', 'call_number_type': 'lc',
                       'call_number': 'MT 20 .B5 2016'}),
            ('TEST4', {'volume': 'TEST', 'call_number_type': 'lc',
                       'call_number': 'MT 100.1 .A12 1999'}),
         ), 'volume=TEST&orderBy=-callNumber',
         ['TEST6', 'TEST5', 'TEST4', 'TEST3', 'TEST2', 'TEST1']),
    }, 
)

# TESTDATA__FIRSTITEMPERLOCATION: We use a consistent set of test data
# for testing the firstitemperlocation resource.
TESTDATA__FIRSTITEMPERLOCATION = (
    ( 'atest1',
        { 'location_code': 'atest',
          'barcode': '1',
          'call_number': 'BB 1234 C35 1990',
          'call_number_type': 'lc' } ),
    ( 'atest2',
        { 'location_code': 'atest',
          'barcode': '2',
          'call_number': 'BB 1234 A22 2000',
          'call_number_type': 'lc' } ),
    ( 'atest3',
        { 'location_code': 'atest',
          'barcode': '3',
          'call_number': 'BC 2345 F80',
          'call_number_type': 'lc' } ),
    ( 'atest4',
        { 'location_code': 'atest',
          'barcode': '4',
          'call_number': 'BB 1234',
          'call_number_type': 'sudoc' } ),
    ( 'btest1',
        { 'location_code': 'btest',
          'barcode': '3',
          'call_number': 'BB 1234 D99',
          'call_number_type': 'lc' } ),
    ( 'btest2',
        { 'location_code': 'btest',
          'barcode': '4',
          'call_number': 'BB 1234 A22',
          'call_number_type': 'sudoc' } ),
    ( 'btest3',
        { 'location_code': 'btest',
          'barcode': '5',
          'call_number': 'CC 9876 H43',
          'call_number_type': 'lc' } ),
    ( 'btest4',
        { 'location_code': 'btest',
          'barcode': '6',
          'call_number': 'BB 1234',
          'call_number_type': 'sudoc' } ),
    ( 'ctest1',
        { 'location_code': 'ctest',
          'barcode': '8',
          'call_number': 'BB 1234 D99 2016',
          'call_number_type': 'lc' } ),
    ( 'ctest2',
        { 'location_code': 'ctest',
          'barcode': '9',
          'call_number': 'CC 1234 A22',
          'call_number_type': 'other' } ),
    ( 'ctest3',
        { 'location_code': 'ctest',
          'barcode': '10',
          'call_number': '900.1 H43',
          'call_number_type': 'dewey' } ),
    ( 'ctest4',
        { 'location_code': 'ctest',
          'barcode': '11',
          'call_number': 'AB 1234',
          'call_number_type': 'other' } ),
)

PARAMETERS__FIRSTITEMPERLOCATION = (
    ('test_data, search, expected'),
    { 'LC call number type | A match at each location':
        (TESTDATA__FIRSTITEMPERLOCATION,
         'callNumber[startswith]=BB 12&callNumberType=lc',
         ['atest2', 'btest1', 'ctest1']),
    }, { 'LC call number type | A match at one location':
        (TESTDATA__FIRSTITEMPERLOCATION,
         'callNumber[startswith]=BC&callNumberType=lc',
         ['atest3']),
    }, { 'LC call number type | No matches':
        (TESTDATA__FIRSTITEMPERLOCATION,
         'callNumber[startswith]=D&callNumberType=lc',
         None),
    }, { 'SUDOC call number type | A match at two locations':
        (TESTDATA__FIRSTITEMPERLOCATION,
         'callNumber[startswith]=BB&callNumberType=sudoc',
         ['atest4', 'btest4']),
    }, { 'DEWEY call number type | A match at one location':
        (TESTDATA__FIRSTITEMPERLOCATION,
         'callNumber[startswith]=900&callNumberType=dewey',
         ['ctest3']),
    }, { 'OTHER call number type | A match at one location':
        (TESTDATA__FIRSTITEMPERLOCATION,
         'callNumber[startswith]=C&callNumberType=other',
         ['ctest2']),
    }, { 'BARCODE | A match at two locations':
        (TESTDATA__FIRSTITEMPERLOCATION,
         'barcode=3',
         ['atest3', 'btest1']),
    }, 
)

# TESTDATA__CALLNUMBERMATCHES: We use a consistent set of test data for
# testing the callnumbermatches resource.
TESTDATA__CALLNUMBERMATCHES = (
    ( 'atest1',
        { 'location_code': 'atest',
          'call_number': 'ZZZ 1005',
          'call_number_type': 'lc' } ),
    ( 'atest2',
        { 'location_code': 'atest',
          'call_number': 'ZZZ 1000',
          'call_number_type': 'lc' } ),
    ( 'atest3',
        { 'location_code': 'atest',
          'call_number': 'ZZZ 1001',
          'call_number_type': 'lc' } ),
    ( 'btest1',
        { 'location_code': 'btest',
          'call_number': 'ZZZ 1003',
          'call_number_type': 'lc' } ),
    ( 'btest2',
        { 'location_code': 'btest',
          'call_number': 'ZZZ 1002',
          'call_number_type': 'lc' } ),
    ( 'btest3',
        { 'location_code': 'btest',
          'call_number': 'ZZZ 1004',
          'call_number_type': 'lc' } ),
    ( 'ctest1',
        { 'location_code': 'ctest',
          'call_number': 'ZZZ 1.3',
          'call_number_type': 'sudoc' } ),
    ( 'ctest2',
        { 'location_code': 'ctest',
          'call_number': 'ZZZ 1.2',
          'call_number_type': 'sudoc' } ),
    ( 'ctest3',
        { 'location_code': 'ctest',
          'call_number': 'ZZZ 1.1',
          'call_number_type': 'sudoc' } ),
)

PARAMETERS__CALLNUMBERMATCHES = (
    ('test_data, search, expected'),
    { 'Match all locations, all CN types':
        (TESTDATA__CALLNUMBERMATCHES,
         'callNumber[startswith]=ZZZ',
         ['ZZZ 1.1', 'ZZZ 1.2', 'ZZZ 1.3', 'ZZZ 1000', 'ZZZ 1001', 'ZZZ 1002',
          'ZZZ 1003', 'ZZZ 1004', 'ZZZ 1005']),
    }, { 'Match all locations, all CN types, with limit':
        (TESTDATA__CALLNUMBERMATCHES,
         'callNumber[startswith]=ZZZ&limit=4',
         ['ZZZ 1.1', 'ZZZ 1.2', 'ZZZ 1.3', 'ZZZ 1000']),
    }, { 'Match all locations, LC type':
        (TESTDATA__CALLNUMBERMATCHES,
         'callNumber[startswith]=ZZZ&callNumberType=lc',
         ['ZZZ 1000', 'ZZZ 1001', 'ZZZ 1002', 'ZZZ 1003', 'ZZZ 1004',
          'ZZZ 1005']),
    }, { 'Match all locations, SUDOC type':
        (TESTDATA__CALLNUMBERMATCHES,
         'callNumber[startswith]=ZZZ&callNumberType=sudoc',
         ['ZZZ 1.1', 'ZZZ 1.2', 'ZZZ 1.3']),
    }, { 'Match location atest, all CN types':
        (TESTDATA__CALLNUMBERMATCHES,
         'callNumber[startswith]=ZZZ&locationCode=atest',
         ['ZZZ 1000', 'ZZZ 1001', 'ZZZ 1005']),
    }, { 'Match location btest, all CN types':
        (TESTDATA__CALLNUMBERMATCHES,
         'callNumber[startswith]=ZZZ&locationCode=btest',
         ['ZZZ 1002', 'ZZZ 1003', 'ZZZ 1004']),
    }, { 'Match location ctest, all CN types':
        (TESTDATA__CALLNUMBERMATCHES,
         'callNumber[startswith]=ZZZ&locationCode=ctest',
         ['ZZZ 1.1', 'ZZZ 1.2', 'ZZZ 1.3']),
    }, { 'Match location atest, LC type':
        (TESTDATA__CALLNUMBERMATCHES,
         'callNumber[startswith]=ZZZ&callNumberType=lc&locationCode=atest',
         ['ZZZ 1000', 'ZZZ 1001', 'ZZZ 1005']),
    }, { 'Match location ctest, LC type':
        (TESTDATA__CALLNUMBERMATCHES,
         'callNumber[startswith]=ZZZ&callNumberType=lc&locationCode=ctest',
         []),
    }, { 'Match one call number':
        (TESTDATA__CALLNUMBERMATCHES,
         'callNumber[startswith]=ZZZ1001',
         ['ZZZ 1001']),
    }, 
)

# HELPER FUNCTIONS for compiling test data into pytest parameters

def compile_resource_links(resources):
    """
    Return a (resource, links) tuple for RESOURCE_METADATA (or similar)
    entries that have a `links` element, for test parametrization.
    """
    return [(k, v['links']) for k, v in resources.items()
            if v.get('links', None)]


def compile_params(parameters):
    """
    Compile a tuple of test parameters for pytest.parametrize, from one
    of the above PARAMETERS__* constants.
    """
    return tuple(p.values()[0] for p in parameters[1:])


def compile_ids(parameters):
    """
    Compile a tuple of test IDs for pytest.parametrize, from one of the
    above PARAMETERS__* constants.
    """
    return tuple(p.keys()[0] for p in parameters[1:])


# PYTEST FIXTURES

@pytest.fixture
def api_settings(settings):
    """
    Pytest fixture that sets a few default Django settings for the API
    tests in this module. Returns the `settings` object. Doing setup
    like this here via a fixture seems slightly better than putting
    this in the `test` settings module--the relevant settings are
    closer to the tests that use them. Just have to make sure to
    include this fixture in all of the tests that need them.
    """
    settings.REST_FRAMEWORK['PAGINATE_BY_PARAM'] = 'limit'
    settings.REST_FRAMEWORK['PAGINATE_PARAM'] = 'offset'
    settings.REST_FRAMEWORK['SEARCH_PARAM'] = 'search'
    settings.REST_FRAMEWORK['SEARCHTYPE_PARAM'] = 'searchtype'
    settings.REST_FRAMEWORK['MAX_PAGINATE_BY'] = 500
    settings.REST_FRAMEWORK['PAGINATE_BY'] = 500
    return settings


# TESTS
# ---------------------------------------------------------------------

@pytest.mark.django_db
def test_apiusers_authenticated_requests(api_client,
                                         simple_sig_auth_credentials,
                                         assert_obj_fields_match_serializer):
    """
    The apiusers resource requires authentication to access; users that
    can authenticate can view the apiusers list and details of a single
    apiuser. Authentication must be renewed after each request.
    """
    api_user = APIUser.objects.create_user('test', 'secret', password='pw',
                                           email='test@test.com',
                                           first_name='F', last_name='Last')
    api_client.credentials(**simple_sig_auth_credentials(api_user))
    list_resp = api_client.get('{}apiusers/'.format(API_ROOT))
    assert list_resp.status_code == 200

    api_client.credentials(**simple_sig_auth_credentials(api_user))
    detail_resp = api_client.get('{}apiusers/{}'.format(API_ROOT, 'test'))
    serializer = detail_resp.renderer_context['view'].get_serializer()
    assert_obj_fields_match_serializer(detail_resp.data, serializer)


@pytest.mark.django_db
def test_apiusers_not_django_users(model_instance, api_client,
                                   simple_sig_auth_credentials):
    """
    Django Users that don't have associated APIUsers records should
    not appear in the list of apiusers.
    """
    api_user = APIUser.objects.create_user('test', 'secret', password='pw',
                                           email='test@test.com',
                                           first_name='F', last_name='Last')
    user = model_instance(User, 'bob', 'bob@bob.com', 'bobpassword')
    api_client.credentials(**simple_sig_auth_credentials(api_user))
    response = api_client.get('{}apiusers/'.format(API_ROOT))
    usernames = [r['username'] for r in response.data['_embedded']['apiusers']]
    assert 'test' in usernames
    assert 'bob' not in usernames


@pytest.mark.django_db
def test_apiusers_unauthenticated_requests_fail(api_client):
    """
    Requesting an apiuser list or detail view without providing any
    authentication credentials should result in a 403 error.
    """
    api_user = APIUser.objects.create_user('test', 'secret', password='pw',
                                           email='test@test.com',
                                           first_name='F', last_name='Last')
    list_resp = api_client.get('{}apiusers/'.format(API_ROOT))
    detail_resp = api_client.get('{}apiusers/test'.format(API_ROOT))
    assert list_resp.status_code == 403
    assert detail_resp.status_code == 403


@pytest.mark.django_db
def test_apiusers_wrong_username_requests_fail(api_client,
                                               simple_sig_auth_credentials):
    """
    Providing an incorrect username/password pair in authentication
    headers results in a 403 error.
    """
    api_user1 = APIUser.objects.create_user('test', 'secret', password='pw',
                                            email='test@test.com',
                                            first_name='F', last_name='Last')
    api_user2 = APIUser.objects.create_user('test2', 'secret', password='pw2',
                                            email='test2@test.com',
                                            first_name='G', last_name='Last')
    credentials = simple_sig_auth_credentials(api_user1)
    credentials['HTTP_X_USERNAME'] = 'test2'
    api_client.credentials(**credentials)
    list_resp = api_client.get('{}apiusers/'.format(API_ROOT))
    assert list_resp.status_code == 403


@pytest.mark.django_db
def test_apiusers_repeated_requests_fail(api_client,
                                         simple_sig_auth_credentials):
    """
    Attempting to beat apiusers authentication by submitting multiple
    requests without renewing credentials should result in a 403 error
    on the second request.
    """
    api_user = APIUser.objects.create_user('test', 'secret', password='pw',
                                           email='test@test.com',
                                           first_name='F', last_name='Last')
    api_client.credentials(**simple_sig_auth_credentials(api_user))
    resp_one = api_client.get('{}apiusers/'.format(API_ROOT))
    resp_two = api_client.get('{}apiusers/'.format(API_ROOT))
    assert resp_one.status_code == 200
    assert resp_two.status_code == 403


@pytest.mark.parametrize('resource', RESOURCE_METADATA.keys())
def test_standard_resource(resource, api_settings, api_solr_env, api_client,
                           pick_reference_object_having_link,
                           assert_obj_fields_match_serializer):
    """
    Standard resources (each with a "list" and "detail" view) should
    have objects available in an "_embedded" object in the list view,
    and accessing an object's "_links / self" URL should give you the
    same data object. Data objects should have fields matching the
    associated view serializer's `fields` attribute.
    """
    list_resp = api_client.get('{}{}/'.format(API_ROOT, resource))
    objects = list_resp.data['_embedded'][resource]
    ref_obj = pick_reference_object_having_link(objects, 'self')
    detail_resp = api_client.get(ref_obj['_links']['self']['href'])
    detail_obj = detail_resp.data
    assert ref_obj == detail_obj

    serializer = detail_resp.renderer_context['view'].get_serializer()
    assert_obj_fields_match_serializer(detail_obj, serializer)


@pytest.mark.parametrize('resource, links',
                         compile_resource_links(RESOURCE_METADATA))
def test_standard_resource_links(resource, links, api_settings, api_solr_env,
                                 api_client,
                                 pick_reference_object_having_link,
                                 assert_obj_fields_match_serializer,
                                 get_linked_view_and_objects):
    """
    Accessing linked resources from standard resources (via _links)
    should return the expected resource(s).
    """
    resp = api_client.get('{}{}/'.format(API_ROOT, resource))
    objects = resp.data['_embedded'][resource]
    for linked_resource, field in links.items():
        ref_obj = pick_reference_object_having_link(objects, field)
        lview, lobjs = get_linked_view_and_objects(api_client, ref_obj, field)
        assert lview.resource_name == linked_resource
        assert_obj_fields_match_serializer(lobjs[0], lview.get_serializer())

        revfield = RESOURCE_METADATA[linked_resource]['links'][resource]
        _, rev_objs = get_linked_view_and_objects(api_client, lobjs[0],
                                                  revfield)
        assert ref_obj in rev_objs


@pytest.mark.parametrize('url, err_text', [
    ('items/?dueDate[gt]=2018', 'datetime was formatted incorrectly'),
    ('items/?recordNumber[invalid]=i10000100', 'not a valid operator'),
    ('items/?recordNumber[in]=i10000100', 'require an array'),
    ('items/?recordNumber[range]=i10000100', 'require an array'),
    ('items/?recordNumber=[i1,i2]', 'Arrays of values are only used'),
    ('items/?nonExistent=0', 'not a valid field for filtering'),
    ('items/?orderBy=nonExistent', 'not a valid field for ordering'),
    ('bibs/?searchtype=nonExistent', 'searchtype parameter must be'),
    ('bibs/?search=none:none', 'undefined field'),
    ('bibs/?suppressed=not', 'expected a boolean'),
    ('bibs/?recordNumber[isnull]=not', 'expected a boolean'),
    ('items/?copyNumber[range]=[1, 2]', 'input string: " 2"'),
])
def test_request_error_badquery(url, err_text, api_solr_env, api_client,
                                api_settings):
    """
    Requesting from the given URL should result in a 400 error response
    (due to a bad query), which contains the given error text.
    """
    response = api_client.get('{}{}'.format(API_ROOT, url))
    assert response.status_code == 400
    assert err_text in response.data['detail']


@pytest.mark.parametrize('resource, default_limit, max_limit, limit, offset, '
                         'exp_results, exp_start, exp_end, exp_prev_offset, '
                         'exp_next_offset', [
    ('items', 20, 50, None, None, 20, 0, 19, None, 20),
    ('items', 20, 50, 20, None, 20, 0, 19, None, 20),
    ('items', 20, 50, None, 0, 20, 0, 19, None, 20),
    ('items', 20, 50, 20, 0, 20, 0, 19, None, 20),
    ('items', 20, 50, 20, 1, 20, 1, 20, 0, 21),
    ('items', 20, 50, 20, 20, 20, 20, 39, 0, 40),
    ('items', 20, 50, 20, 40, 20, 40, 59, 20, 60),
    ('items', 20, 50, 25, 20, 25, 20, 44, 0, 45),
    ('items', 20, 50, 20, 180, 20, 180, 199, 160, None),
    ('items', 20, 50, 20, 190, 10, 190, 199, 170, None),
    ('items', 20, 50, 0, None, 0, 0, -1, None, 0),
    ('items', 20, 50, 50, None, 50, 0, 49, None, 50),
    ('items', 20, 50, 51, None, 50, 0, 49, None, 50),
    ('items', 20, 300, 300, None, 200, 0, 199, None, None),
    ('items', 20, 50, 20, 300, 0, 300, 199, 280, None),
], ids=[
    'no limit or offset given => use defaults',
    'limit=default, no offset given => use defaults',
    'no limit given, offset=0 => use defaults',
    'limit=default and offset=0 => use defaults',
    'limit=20, offset=1 => 20 results, page offset by 1',
    'limit=20, offset=20 => 20 results, page offset by 20',
    'limit=20, offset=40 => 20 results, page offset by 40',
    'limit=25, offset=20 => 25 results, page offset by 20',
    'limit=20, offset=180 (total recs is 200) => 20 results, no next page',
    'limit=20, offset=190 (total recs is 200) => 10 results, no next page',
    'limit=0 => 0 results (STRANGE: endRow, next page)',
    'limit=max => max results',
    'limit > max => max results',
    'limit > total => total results, no next page',
    'offset > total => 0 results, no next page (STRANGE: startRow, prev page)'
])
def test_list_view_pagination(resource, default_limit, max_limit, limit,
                              offset, exp_results, exp_start, exp_end,
                              exp_prev_offset, exp_next_offset, api_settings,
                              api_solr_env, api_client):
    """
    Requesting the given resource using the provided limit and offset
    parameters should result in a data structure that we can paginate
    through in predictable ways.
    """
    api_settings.REST_FRAMEWORK['PAGINATE_BY'] = default_limit
    api_settings.REST_FRAMEWORK['MAX_PAGINATE_BY'] = max_limit
    profile = RESOURCE_METADATA[resource]['profile']
    exp_total = len(api_solr_env.records[profile])

    base_url = '{}{}/'.format(API_ROOT, resource)
    limitq = 'limit={}'.format(limit) if limit is not None else ''
    offsetq = 'offset={}'.format(offset) if offset is not None else ''
    qstring = '&'.join([part for part in (limitq, offsetq) if part])
    url = '?'.join([part for part in (base_url, qstring) if part])
    response = api_client.get(url)
    data = response.data
    self_link = data['_links']['self']['href']
    next_link = data['_links'].get('next', {'href': None})['href']
    prev_link = data['_links'].get('previous', {'href': None})['href']
    records = data.get('_embedded', {resource: []})[resource]

    assert response.status_code == 200
    assert len(records) == exp_results
    assert data['totalCount'] == exp_total
    assert data['startRow'] == exp_start
    assert data['endRow'] == exp_end

    assert self_link.endswith(url)

    if exp_next_offset is None:
        assert next_link is None
    else:
        assert limitq in next_link
        assert 'offset={}'.format(exp_next_offset) in next_link

    if exp_prev_offset is None:
        assert prev_link is None
    else:
        assert limitq in prev_link
        assert 'offset={}'.format(exp_prev_offset) in prev_link


@pytest.mark.parametrize('resource, test_data, search, expected',
                         compile_params(PARAMETERS__FILTER_TESTS__INTENDED) +
                         compile_params(PARAMETERS__FILTER_TESTS__STRANGE),
                         ids=compile_ids(PARAMETERS__FILTER_TESTS__INTENDED) +
                             compile_ids(PARAMETERS__FILTER_TESTS__STRANGE))
def test_list_view_filters(resource, test_data, search, expected, api_settings,
                           assemble_test_records, api_client, get_found_ids,
                           do_filter_search):
    """
    Given the provided `test_data` records: requesting the given
    `resource` using the provided search filter parameters (`search`)
    should return each of the records in `expected` and NONE of the
    records NOT in `expected`.
    """
    test_ids = set([r[0] for r in test_data])
    expected_ids = set(expected) if expected is not None else set()
    not_expected_ids = test_ids - expected_ids

    profile = RESOURCE_METADATA[resource]['profile']
    id_field = RESOURCE_METADATA[resource]['id_field']
    erecs, trecs = assemble_test_records(profile, id_field, test_data)

    # First let's do a quick sanity check to make sure the resource
    # returns the correct num of records before the filter is applied.
    resource_url = '{}{}/'.format(API_ROOT, resource)
    check_response = api_client.get(resource_url)
    assert check_response.data['totalCount'] == len(erecs) + len(trecs)

    response = do_filter_search(resource_url, search, api_client)
    found_ids = set(get_found_ids(id_field, response))
    assert all([i in found_ids for i in expected_ids])
    assert all([i not in found_ids for i in not_expected_ids])


@pytest.mark.parametrize('resource, test_data, search, expected',
                         compile_params(PARAMETERS__ORDERBY_TESTS__INTENDED) +
                         compile_params(PARAMETERS__ORDERBY_TESTS__STRANGE),
                         ids=compile_ids(PARAMETERS__ORDERBY_TESTS__INTENDED) +
                             compile_ids(PARAMETERS__ORDERBY_TESTS__STRANGE))
def test_list_view_orderby(resource, test_data, search, expected, api_settings,
                           assemble_test_records, api_client, get_found_ids,
                           do_filter_search):
    """
    Given the provided `test_data` records: requesting the given
    `resource` using the provided search filter parameters (`search`)
    (which include an `orderBy` parameter), should return records in
    the `expected` order.
    """
    profile = RESOURCE_METADATA[resource]['profile']
    id_field = RESOURCE_METADATA[resource]['id_field']
    erecs, trecs = assemble_test_records(profile, id_field, test_data)
    print [r.get('call_number_sort', None) for r in trecs]
    resource_url = '{}{}/'.format(API_ROOT, resource)
    response = do_filter_search(resource_url, search, api_client)
    found_ids = get_found_ids(id_field, response)
    assert found_ids == expected


@pytest.mark.parametrize('test_data, search, expected',
                         compile_params(PARAMETERS__FIRSTITEMPERLOCATION),
                         ids=compile_ids(PARAMETERS__FIRSTITEMPERLOCATION))
def test_firstitemperlocation_list(test_data, search, expected, api_settings,
                                   assemble_test_records, api_client,
                                   get_found_ids, do_filter_search):
    """
    The `firstitemperlocation` resource is basically a custom filter
    for `items` that submits a facet-query to Solr asking for the first
    item at each location code that matches the provided call number
    (plus cn type) or barcode. (Used by the Inventory App when doing a
    call number or barcode lookup without providing a location.)
    """
    lcodes = set([r['location_code'] for _, r in test_data])
    data = {
        'locations': tuple((code, {'label': code}) for code in lcodes),
        'items': test_data
    }

    test_ids = set([r[0] for r in test_data])
    expected_ids = set(expected) if expected is not None else set()
    not_expected_ids = test_ids - expected_ids

    for resource in data.keys():
        profile = RESOURCE_METADATA[resource]['profile']
        id_field = RESOURCE_METADATA[resource]['id_field']
        assemble_test_records(profile, id_field, data[resource])

    resource_url = '{}firstitemperlocation/'.format(API_ROOT)
    rsp = do_filter_search(resource_url, search, api_client)
    found_ids = set(get_found_ids(RESOURCE_METADATA['items']['id_field'], rsp))
    assert all([i in found_ids for i in expected_ids])
    assert all([i not in found_ids for i in not_expected_ids])


@pytest.mark.parametrize('test_data, search, expected',
                         compile_params(PARAMETERS__CALLNUMBERMATCHES),
                         ids=compile_ids(PARAMETERS__CALLNUMBERMATCHES))
def test_callnumbermatches_list(test_data, search, expected, api_settings,
                                assemble_test_records, api_client,
                                do_filter_search):
    """
    The `callnumbermatches` resource simply returns an array of
    callnumber strings, in order, matching the critera that's given.
    It's used to power the callnumber autocomplete in the Inventory
    App.
    """
    lcodes = set([r['location_code'] for _, r in test_data])
    data = {
        'locations': tuple((code, {'label': code}) for code in lcodes),
        'items': test_data
    }

    for resource in data.keys():
        profile = RESOURCE_METADATA[resource]['profile']
        id_field = RESOURCE_METADATA[resource]['id_field']
        assemble_test_records(profile, id_field, data[resource])

    resource_url = '{}callnumbermatches/'.format(API_ROOT)
    response = do_filter_search(resource_url, search, api_client)
    assert response.data == expected

