"""
Contains integration tests for the `api` app.
"""

import urllib
from datetime import datetime
from pytz import utc

import pytest

from utils.test_helpers import solr_test_profiles as tp

# FIXTURES AND TEST DATA
# External fixtures used below can be found in
# django/sierra/conftest.py:
#
# django/sierra/api/tests/conftest.py:
#     api_solr_env
#     api_data_assembler
#     api_client


# API_ROOT: Base URL for the API we're testing.
API_ROOT = '/api/v1/'

# RESOURCE_METADATA: Lookup dict for mapping API resources to various
# test parameters.
RESOURCE_METADATA = {
    'bibs': { 'profile': 'bib', 'id_field': 'record_number' },
    'items': { 'profile': 'item', 'id_field': 'record_number' },
    'eresources': { 'profile': 'eresource', 'id_field': 'record_number' },
    'itemstatuses': { 'profile': 'itemstatus', 'id_field': 'code' },
    'itemtypes': { 'profile': 'itype', 'id_field': 'code' },
    'locations': { 'profile': 'location', 'id_field': 'code' }
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

    # STARTS WITH (`starswith`) returns records where the beginning of
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

    # RANGE (`range`) takes an array of two values -- [start, end] --
    # and returns results where the value in the queried field is in
    # the provided range. The range filter is INCLUSIVE: [1, 3] matches
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
    return settings


# TESTS

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
                           api_solr_env, api_data_assembler, api_client):
    """
    Given the provided `test_data` records: requesting the given
    `resource` using the provided search filter parameters (`search`)
    should return each of the records in `expected` and NONE of the
    records NOT in `expected`.
    """
    assembler = api_data_assembler
    gens = assembler.gen_factory
    profile = RESOURCE_METADATA[resource]['profile']
    solr_id_field = RESOURCE_METADATA[resource]['id_field']
    env_recs = api_solr_env.records[profile]
    test_recs = assembler.load_static_test_data(profile, test_data,
                                                solr_id_field, env_recs)
    test_ids = set([r[0] for r in test_data])
    expected_ids = set(expected) if expected is not None else set()
    not_expected_ids = test_ids - expected_ids

    api_settings.REST_FRAMEWORK['MAX_PAGINATE_BY'] = 500
    api_settings.REST_FRAMEWORK['PAGINATE_BY'] = 500

    # First let's do a quick sanity check to make sure the resource
    # returns the correct num of records before the filter is applied.
    check_response = api_client.get('{}{}/'.format(API_ROOT, resource))
    assert check_response.data['totalCount'] == len(env_recs) + len(test_recs)

    # Now the actual filter test.
    qs = '&'.join(['='.join([urllib.quote_plus(v) for v in pair.split('=')])
                  for pair in search.split('&')])
    response = api_client.get('{}{}/?{}'.format(API_ROOT, resource, qs))
    serializer = response.renderer_context['view'].get_serializer()
    api_id_field = serializer.render_field_name(solr_id_field)
    total_found = response.data['totalCount']
    data = response.data.get('_embedded', {resource: []})[resource]
    found_ids = set([r[api_id_field] for r in data])

    # FAIL if we've returned any data not on this page of results.
    assert len(data) == total_found    
    assert all([i in found_ids for i in expected_ids])
    assert all([i not in found_ids for i in not_expected_ids])
