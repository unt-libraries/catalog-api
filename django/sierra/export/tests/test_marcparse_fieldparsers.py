# -*- coding: utf-8 -*-

"""
Tests the export.marcparse.fieldparsers classes/functions.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import pytest
from six.moves import range, zip

from export import sierramarc as sm
from export.marcparse import fieldparsers as fp


# FIXTURES AND TEST DATA
pytestmark = pytest.mark.django_db(databases=['sierra'])


# TESTS

def test_explodesubfields_returns_expected_results():
    """
    `explode_subfields` should return lists of subfield values for a
    pymarc Field object based on the provided sftags string.
    """
    field = sm.SierraMarcField(
        '260', subfields=[
            'a', 'Place :', 'b', 'Publisher,', 'c', '1960;',
            'a', 'Another place :', 'b', 'Another Publisher,', 'c', '1992.'
        ]
    )
    places, pubs, dates = fp.explode_subfields(field, 'abc')
    assert places == ['Place :', 'Another place :']
    assert pubs == ['Publisher,', 'Another Publisher,']
    assert dates == ['1960;', '1992.']


@pytest.mark.parametrize('fparams, inc, exc, unq, start, end, lmt, expected', [
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     'abc', '', 'abc', '', '', None,
     ('a1 b1 c1', 'a2 b2 c2')),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     'ac', '', 'ac', '', '', None,
     ('a1 c1', 'a2 c2')),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     'acd', '', 'acd', '', '', None,
     ('a1 c1', 'a2 c2')),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     'cba', '', 'cba', '', '', None,
     ('a1 b1 c1', 'a2 b2 c2')),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1']),
     'abc', '', 'abc', '', '', None,
     ('a1 b1 c1',)),
    (('260', ['a', 'a1', 'b', 'b1',
              'a', 'a2', 'c', 'c2']),
     'abc', '', 'abc', '', '', None,
     ('a1 b1', 'a2 c2')),
    (('260', ['b', 'b1',
              'b', 'b2', 'a', 'a1', 'c', 'c1']),
     'abc', '', 'abc', '', '', None,
     ('b1', 'b2 a1 c1')),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', 'abc', 'abc', '', '', None,
     ('')),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', 'ac', 'abc', '', '', None,
     ('b1', 'b2')),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     'abc', '', 'abc', '', '', 1,
     ('a1 b1 c1 a2 b2 c2',)),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     'abc', '', 'abc', '', '', 2,
     ('a1 b1 c1', 'a2 b2 c2',)),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     'abc', '', 'abc', '', '', 3,
     ('a1 b1 c1', 'a2 b2 c2',)),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', '', '', '', '', None,
     ('a1 b1 c1 a2 b2 c2',)),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', '', '', 'ac', '', None,
     ('', 'a1 b1', 'c1', 'a2 b2', 'c2')),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', '', '', 'ac', '', 1,
     ('a1 b1 c1 a2 b2 c2',)),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', '', '', 'ac', '', 2,
     ('', 'a1 b1 c1 a2 b2 c2',)),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', '', '', 'ac', '', 3,
     ('', 'a1 b1', 'c1 a2 b2 c2',)),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', '', '', '', 'ac', None,
     ('a1', 'b1 c1', 'a2', 'b2 c2')),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', '', '', '', 'ac', 1,
     ('a1 b1 c1 a2 b2 c2',)),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', '', '', '', 'ac', 2,
     ('a1', 'b1 c1 a2 b2 c2',)),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     '', '', '', '', 'ac', 3,
     ('a1', 'b1 c1', 'a2 b2 c2',)),
    (('260', ['a', 'a1.1', 'a', 'a1.2', 'b', 'b1.1',
              'a', 'a2.1', 'b', 'b2.1',
              'b', 'b3.1']),
     'ab', '', '', '', 'b', None,
     ('a1.1 a1.2 b1.1', 'a2.1 b2.1', 'b3.1')),
    (('700', ['a', 'Name', 'd', 'Dates', 't', 'Title', 'p', 'Part']),
     '', '', '', 'tp', '', 2,
     ('Name Dates', 'Title Part')),
])
def test_groupsubfields_groups_correctly(fparams, inc, exc, unq, start, end,
                                         lmt, expected, params_to_fields):
    """
    `group_subfields` should put subfields from a pymarc Field object
    into groupings based on the provided parameters.
    """
    field = params_to_fields([fparams])[0]
    result = fp.group_subfields(field, inc, exc, unq, start, end, lmt)
    assert len(result) == len(expected)
    for group, exp in zip(result, expected):
        assert group.value() == exp


@pytest.mark.parametrize('fparams, sftags, expected', [
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     'a',
     (['a1', 'a2'])),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     'abc',
     (['a1', 'b1', 'c1', 'a2', 'b2', 'c2'])),
    (('260', ['a', 'a1', 'b', 'b1', 'c', 'c1',
              'a', 'a2', 'b', 'b2', 'c', 'c2']),
     None,
     (['a1', 'b1', 'c1', 'a2', 'b2', 'c2'])),
])
def test_pullfromsubfields_and_no_pullfunc(fparams, sftags, expected,
                                           params_to_fields):
    """
    Calling `pull_from_subfields` with no `pull_func` specified should
    return values from the given pymarc Field object and the specified
    sftags, as a list.
    """
    field = params_to_fields([fparams])[0]
    for val, exp in zip(fp.pull_from_subfields(field, sftags), expected):
        assert val == exp


def test_pullfromsubfields_with_pullfunc(params_to_fields):
    """
    Calling `pull_from_subfields` with a custom `pull_func` specified
    should return values from the given pymarc Field object and the
    specified sftags, run through pull_func, as a flat list.
    """
    subfields = ['a', 'a1.1 a1.2', 'b', 'b1.1 b1.2', 'c', 'c1',
                 'a', 'a2', 'b', 'b2', 'c', 'c2.1 c2.2']
    field = params_to_fields([('260', subfields)])[0]

    def pf(val):
        return val.split(' ')

    expected = ['a1.1', 'a1.2', 'b1.1', 'b1.2', 'c1', 'a2', 'b2', 'c2.1',
                'c2.2']
    pulled = fp.pull_from_subfields(field, sftags='abc', pull_func=pf)
    for val, exp in zip(pulled, expected):
        assert val == exp


@pytest.mark.parametrize('tag, subfields, expected', [
    # Start with edge cases: missing data, non-ISBD punctuation, etc.

    ('245', [],
     {'nonfiling_chars': 0,
      'transcribed': []}),

    ('245', ['a', ''],
     {'nonfiling_chars': 0,
      'transcribed': []}),

    ('245', ['a', '', 'b', 'oops mistake /'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['oops mistake']}]}),

    ('246', ['a', '   ', 'i', 'Some blank chars at start:', 'a', 'Oops'],
     {'display_text': 'Some blank chars at start',
      'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Oops']}]}),

    ('245 12', ['a', 'A title', 'b', 'no punctuation', 'c', 'by Joe'],
     {'nonfiling_chars': 2,
      'transcribed': [
          {'parts': ['A title no punctuation'],
           'responsibility': 'by Joe'}]}),

    ('245 12', ['a', 'A title', 'b', 'no punctuation', 'n', 'Part 1',
                'p', 'the quickening', 'c', 'by Joe'],
     {'nonfiling_chars': 2,
      'transcribed': [
          {'parts': ['A title no punctuation', 'Part 1, the quickening'],
           'responsibility': 'by Joe'}]}),

    ('245 12', ['a', 'A title', 'b', 'no punctuation', 'p', 'The quickening',
                'p', 'Subpart A', 'c', 'by Joe'],
     {'nonfiling_chars': 2,
      'transcribed': [
          {'parts': ['A title no punctuation', 'The quickening',
                     'Subpart A'],
           'responsibility': 'by Joe'}]}),

    ('245 12', ['a', 'A title,', 'b', 'non-ISBD punctuation;', 'n', 'Part 1,',
                'p', 'the quickening', 'c', 'by Joe'],
     {'nonfiling_chars': 2,
      'transcribed': [
          {'parts': ['A title, non-ISBD punctuation', 'Part 1, the quickening'],
           'responsibility': 'by Joe'}]}),

    ('245', ['a', 'A title!', 'b', 'Non-ISBD punctuation;',
             'p', 'The quickening', 'c', 'by Joe'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['A title! Non-ISBD punctuation', 'The quickening'],
           'responsibility': 'by Joe'}]}),

    ('245 12', ['a', 'A title : with punctuation, all in $a. Part 1 / by Joe'],
     {'nonfiling_chars': 2,
      'transcribed': [
          {'parts': ['A title: with punctuation, all in $a. Part 1 / by Joe']}]}),

    ('245', ['b', ' = A parallel title missing a main title'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['A parallel title missing a main title']}]}),

    ('245', ['a', '1. One thing, 2. Another, 3. A third :',
             'b', 'This is like some Early English Books Online titles / '
                  'by Joe = 1. One thing, 2. Another, 3. A third : Plus long '
                  'subtitle etc. /'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['1. One thing, 2. Another, 3. A third: This is like some '
                     'Early English Books Online titles / by Joe'],
           'parallel': [
              {'parts': ['1. One thing, 2. Another, 3. A third: Plus long subtitle '
                         'etc.']}
          ]}],
      }),

    ('245', ['a', '1. This is like another Early English Books Online title :',
             'b', 'something: 2. Something else: 3. About the 22th. of June, '
                  '1678. by Richard Greene of Dilwin, etc.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['1. This is like another Early English Books Online title: '
                     'something: 2. Something else: 3. About the 22th. of June, '
                     '1678. by Richard Greene of Dilwin, etc.']}]}),

    ('245', ['a', 'A forward slash somewhere in the title / before sf c /',
             'c', 'by Joe.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['A forward slash somewhere in the title / before sf c'],
           'responsibility': 'by Joe'}]}),

    ('245', ['a', 'Quotation marks /', 'c', 'by "Joe."'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Quotation marks'],
           'responsibility': 'by "Joe"'}]}),

    ('245', ['a', 'Multiple ISBD marks / :', 'b', 'subtitle', 'c', 'by Joe.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Multiple ISBD marks /: subtitle'],
           'responsibility': 'by Joe'}]}),

    # Now test cases on more standard data.

    ('245', ['a', 'Title :', 'b', 'with subtitle.'],
     {'nonfiling_chars': 0,
      'transcribed': [{'parts': ['Title: with subtitle']}]}),

    ('245', ['a', 'First title ;', 'b', 'Second title.'],
     {'nonfiling_chars': 0,
      'transcribed': [{'parts': ['First title']},
                      {'parts': ['Second title']}]}),

    ('245', ['a', 'First title ;', 'b', 'Second title ; Third title'],
     {'nonfiling_chars': 0,
      'transcribed': [{'parts': ['First title']}, {'parts': ['Second title']},
                      {'parts': ['Third title']}]}),

    ('245', ['a', 'First title ;', 'b', 'and Second title'],
     {'nonfiling_chars': 0,
      'transcribed': [{'parts': ['First title']},
                      {'parts': ['Second title']}]}),

    ('245', ['a', 'First title,', 'b', 'and Second title'],
     {'nonfiling_chars': 0,
      'transcribed': [{'parts': ['First title']},
                      {'parts': ['Second title']}]}),

    ('245', ['a', 'Title /', 'c', 'by Author.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title'],
           'responsibility': 'by Author'}]}),

    ('245', ['a', 'Title /', 'c', 'Author 1 ; Author 2 ; Author 3.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title'],
           'responsibility': 'Author 1; Author 2; Author 3'}]}),

    ('245', ['a', 'Title!', 'b', 'What ending punctuation should we keep?'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title! What ending punctuation should we keep?']}]}),

    # Titles that include parts ($n and $p).

    ('245', ['a', 'Title.', 'n', 'Part 1.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title', 'Part 1']}]}),

    ('245', ['a', 'Title.', 'p', 'Name of a part.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title', 'Name of a part']}]}),

    ('245', ['a', 'Title.', 'n', 'Part 1,', 'p', 'Name of a part.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title', 'Part 1, Name of a part']}]}),

    ('245', ['a', 'Title.', 'n', 'Part 1', 'p', 'Name of a part.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title', 'Part 1, Name of a part']}]}),

    ('245', ['a', 'Title.', 'n', 'Part 1.', 'p', 'Name of a part.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title', 'Part 1', 'Name of a part']}]}),

    ('245', ['a', 'Title.', 'n', '1. Part', 'p', 'Name of a part.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title', '1. Part, Name of a part']}]}),

    ('245', ['a', 'Title.', 'n', '1. Part A', 'n', '2. Part B'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title', '1. Part A', '2. Part B']}]}),

    ('245', ['a', 'Title :', 'b', 'subtitle.', 'n', '1. Part A',
             'n', '2. Part B'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title: subtitle', '1. Part A', '2. Part B']}]}),

    ('245', ['a', 'Title one.', 'n', 'Book 2.', 'n', 'Chapter V /',
             'c', 'Author One. Title two. Book 3. Chapter VI / Author Two.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title one', 'Book 2', 'Chapter V'],
           'responsibility': 'Author One'},
          {'parts': ['Title two', 'Book 3. Chapter VI'],
              'responsibility': 'Author Two'}]}),

    # Fun with parallel titles!

    ('245', ['a', 'Title in French =', 'b', 'Title in English /',
             'c', 'by Author.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title in French'],
           'responsibility': 'by Author',
           'parallel': [
              {'parts': ['Title in English']}]
           }],
      }),

    ('245', ['a', 'Title in French /',
             'c', 'by Author in French = Title in English / by Author in '
                  'English.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title in French'],
           'responsibility': 'by Author in French',
           'parallel': [
              {'parts': ['Title in English'],
               'responsibility': 'by Author in English'}]
           }],
      }),

    ('245', ['a', 'Title in French =',
             'b', 'Title in English = Title in German /', 'c', 'by Author.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title in French'],
           'responsibility': 'by Author',
           'parallel': [
              {'parts': ['Title in English']},
              {'parts': ['Title in German']}],
           }],
      }),

    ('245', ['a', 'First title in French =',
             'b', 'First title in English ; Second title in French = Second '
                  'title in English.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['First title in French'],
           'parallel': [
              {'parts': ['First title in English']}]
           },
          {'parts': ['Second title in French'],
              'parallel': [
              {'parts': ['Second title in English']}]
           }],
      }),

    ('245', ['a', 'Title in French.', 'p', 'Part One =',
             'b', 'Title in English.', 'p', 'Part One.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title in French', 'Part One'],
           'parallel': [
              {'parts': ['Title in English', 'Part One']}]
           }],
      }),

    ('245', ['a', 'Title in French.', 'p', 'Part One :',
             'b', 'subtitle = Title in English.', 'p', 'Part One : subtitle.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title in French', 'Part One: subtitle'],
           'parallel': [
              {'parts': ['Title in English', 'Part One: subtitle']}]
           }],
      }),

    ('245', ['a', 'Title in French /',
             'c', 'by Author in French = by Author in English'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title in French'],
           'responsibility': 'by Author in French',
           'parallel': [
              {'responsibility': 'by Author in English'}]
           }],
      }),

    ('245', ['a', 'Title in French.', 'p', 'Part One :',
             'b', 'subtitle = Title in English.', 'p', 'Part One : subtitle.',
             'c', 'by Author in French = by Author in English'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title in French', 'Part One: subtitle'],
           'responsibility': 'by Author in French',
           'parallel': [
              {'parts': ['Title in English', 'Part One: subtitle']},
              {'responsibility': 'by Author in English'}],
           }],
      }),

    # $h (medium) is ignored, except for ISBD punctuation

    ('245', ['a', 'First title', 'h', '[sound recording] ;',
             'b', 'Second title.'],
     {'nonfiling_chars': 0,
      'transcribed': [{'parts': ['First title']},
                      {'parts': ['Second title']}]}),

    ('245', ['a', 'Title in French.', 'p', 'Part One',
             'h', '[sound recording] =', 'b', 'Title in English.',
             'p', 'Part One.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title in French', 'Part One'],
           'parallel': [
              {'parts': ['Title in English', 'Part One']}]
           }],
      }),

    # Subfields for archives and archival collections (fgks)

    ('245', ['a', 'Smith family papers,', 'f', '1800-1920.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Smith family papers, 1800-1920']}]}),

    ('245', ['a', 'Smith family papers', 'f', '1800-1920.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Smith family papers, 1800-1920']}]}),

    ('245', ['a', 'Smith family papers', 'f', '1800-1920', 'g', '1850-1860.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Smith family papers, 1800-1920 (bulk 1850-1860)']}]}),

    ('245', ['a', 'Smith family papers', 'g', '1850-1860.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Smith family papers, 1850-1860']}]}),

    ('245', ['a', 'Smith family papers', 'f', '1800-1920,', 'g', '1850-1860.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Smith family papers, 1800-1920 (bulk 1850-1860)']}]}),

    ('245', ['a', 'Smith family papers', 'f', '1800-1920,',
             'g', '(1850-1860).'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Smith family papers, 1800-1920, (1850-1860)']}]}),

    ('245', ['a', 'Smith family papers', 'f', '1800-1920', 'g', '(1850-1860).'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Smith family papers, 1800-1920 (1850-1860)']}]}),

    ('245', ['a', 'Some title :', 'k', 'typescript', 'f', '1800.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Some title: typescript, 1800']}]}),

    ('245', ['a', 'Hearing Files', 'k', 'Case Files', 'f', '1800',
             'p', 'District 6.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Hearing Files, Case Files, 1800', 'District 6']}]}),

    ('245', ['a', 'Hearing Files.', 'k', 'Case Files', 'f', '1800',
             'p', 'District 6.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Hearing Files', 'Case Files, 1800', 'District 6']}]}),

    ('245', ['a', 'Report.', 's', 'Executive summary.'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Report', 'Executive summary']}]}),

    ('245', ['a', 'Title', 'k', 'Form', 's', 'Version', 'f', '1990'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Title, Form, Version, 1990']}]}),

    ('245', ['k', 'Form', 's', 'Version', 'f', '1990'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Form, Version, 1990']}]}),

    # 242s (Translated titles)

    ('242 14', ['a', 'The Annals of chemistry', 'n', 'Series C,',
                'p', 'Organic chemistry and biochemistry.', 'y', 'eng'],
     {'display_text': 'Title translation, English',
      'nonfiling_chars': 4,
      'transcribed': [
          {'parts': ['The Annals of chemistry',
                     'Series C, Organic chemistry and biochemistry']}]}),

    # 246s (Variant titles)

    ('246', ['a', 'Archives for meteorology, geophysics, and bioclimatology.',
             'n', 'Serie A,', 'p', 'Meteorology and geophysics'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Archives for meteorology, geophysics, and bioclimatology',
                     'Serie A, Meteorology and geophysics']}]}),

    ('246 12', ['a', 'Creating jobs', 'f', '1980'],
     {'display_text': 'Issue title',
      'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Creating jobs, 1980']}]}),

    ('246 12', ['a', 'Creating jobs', 'g', '(varies slightly)', 'f', '1980'],
     {'display_text': 'Issue title',
      'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Creating jobs (varies slightly) 1980']}]}),

    ('246 1 ', ['i', 'At head of title:', 'a', 'Science and public affairs',
                'f', 'Jan. 1970-Apr. 1974'],
     {'display_text': 'At head of title',
      'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Science and public affairs, Jan. 1970-Apr. 1974']}]}),

    ('247', ['a', 'Industrial medicine and surgery', 'x', '0019-8536'],
     {'issn': '0019-8536',
      'display_text': 'Former title',
      'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Industrial medicine and surgery']}]}),

    # Testing 490s: similar to 245s but less (differently?) structured

    ('490', ['a', 'Series statement / responsibility'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Series statement'],
           'responsibility': 'responsibility'}]}),

    ('490', ['a', 'Series statement =', 'a', 'Series statement in English'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Series statement'],
           'parallel': [
              {'parts': ['Series statement in English']}]
           }],
      }),

    ('490', ['a', 'Series statement ;', 'v', 'v. 1 =',
             'a', 'Series statement in English ;', 'v', 'v. 1'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Series statement; v. 1'],
           'parallel': [
              {'parts': ['Series statement in English; v. 1']}]
           }],
      }),

    ('490', ['3', 'Vol. 1:', 'a', 'Series statement'],
     {'nonfiling_chars': 0,
      'materials_specified': ['Vol. 1'],
      'transcribed': [
          {'parts': ['Series statement']}]}),

    ('490', ['a', 'Series statement,', 'x', '1234-5678 ;', 'v', 'v. 1'],
     {'nonfiling_chars': 0,
      'issn': '1234-5678',
      'transcribed': [
          {'parts': ['Series statement; v. 1']}]}),

    ('490', ['a', 'Series statement ;', 'v', '1.',
             'a', 'Sub-series / Responsibility ;', 'v', 'v. 36'],
     {'nonfiling_chars': 0,
      'transcribed': [
          {'parts': ['Series statement; [volume] 1', 'Sub-series; v. 36'],
           'responsibility': 'Responsibility'}]}),

    ('490', ['a', 'Series statement ;', 'v', 'v. 1.', 'l', '(LC12345)'],
     {'nonfiling_chars': 0,
      'lccn': 'LC12345',
      'transcribed': [
          {'parts': ['Series statement; v. 1']}]}),

])
def test_transcribedtitleparser_parse(tag, subfields, expected,
                                      params_to_fields):
    """
    TranscribedTitleParser `parse` method should return a dict with the
    expected structure, given the provided MARC field. Can handle 242s,
    245s, 246s, and 247s, but is mainly geared toward 245s (for obvious
    reasons).
    """
    if ' ' in tag:
        tag, indicators = tag.split(' ', 1)
    else:
        indicators = '  '
    field = params_to_fields([(tag, subfields, indicators)])[0]
    parsed = fp.TranscribedTitleParser(field).parse()
    print(parsed)
    assert parsed == expected


@pytest.mark.parametrize('tag, subfields, expected', [
    # Start with edge cases: missing data, non-ISBD punctuation, etc.

    ('130', [],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': [],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    ('130', ['a', ''],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': [],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    ('130', ['a', '', 'k', 'Selections.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Selections'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    ('130', ['a', 'A title,', 'm', 'instruments,', 'n', ',', 'r', 'D major.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['A title, instruments, D major'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    ('130 2 ', ['a', 'A Basic title no punctuation', 'n', 'Part 1'],
     {'nonfiling_chars': 2,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['A Basic title no punctuation', 'Part 1'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    ('130', ['a', 'Basic title no punctuation', 'p', 'Named part'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Basic title no punctuation', 'Named part'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    ('130', ['a', 'Basic title no punctuation', 'n', 'Part 1',
             'p', 'named part'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Basic title no punctuation', 'Part 1, named part'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    ('130', ['a', 'Basic title no punctuation', 'n', 'Part 1', 'n', 'Part 2'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Basic title no punctuation', 'Part 1', 'Part 2'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    ('130', ['a', 'Basic title no punctuation', 'p', 'Named part',
             'n', 'Part 2'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Basic title no punctuation', 'Named part', 'Part 2'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    ('130', ['a', 'Basic title no punctuation', 'n', 'Part 1', 'l', 'English'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Basic title no punctuation', 'Part 1'],
      'expression_parts': ['English'],
      'languages': ['English'],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    # Once the first expression-level subfield appears, the rest are
    # interpreted as expression parts, whatever they are.
    ('130', ['a', 'Basic title no punctuation', 'n', 'Part 1',
             's', 'Version A', 'p', 'Subpart C'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Basic title no punctuation', 'Part 1'],
      'expression_parts': ['Version A', 'Subpart C'],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    ('130', ['a', 'Basic title no punctuation', 'n', 'Part 1',
             's', 'Version A', 'p', 'Subpart C'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Basic title no punctuation', 'Part 1'],
      'expression_parts': ['Version A', 'Subpart C'],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    # Test cases on more standard data.

    # For music collective titles, the first $n or $p ('op. 10' in this
    # case) becomes a new part even if the preceding comma indicates
    # otherwise.
    ('130', ['a', 'Duets,', 'm', 'violin, viola,', 'n', 'op. 10.',
             'n', 'No. 3.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Duets, violin, viola', 'Op. 10', 'No. 3'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': True,
      'type': 'main',
      'relations': None,
      }),

    # For other titles, the first subpart becomes part of the main
    # title if there's a preceding comma.
    ('130', ['a', 'Some title,', 'n', 'the first part.', 'n', 'Volume 1.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Some title, the first part', 'Volume 1'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    # The first $n or $p starts a new part if there's a preceding period.
    ('130', ['a', 'Some title.', 'n', 'The first part.', 'n', 'Volume 1.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Some title', 'The first part', 'Volume 1'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    # A $p after $n is combined with the $n if there's a comma (or
    # nothing) preceding $p.
    ('130', ['a', 'Some title.', 'n', 'The first part,', 'p', 'part name.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Some title', 'The first part, part name'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    # A $p after $n becomes a new part if there's a period preceding
    # $p.
    ('130', ['a', 'Some title.', 'n', 'The first part.', 'p', 'Part name.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Some title', 'The first part', 'Part name'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    # For $n's and $p's (after the first), part hierarchy is based on
    # punctuation. Commas denote same part, periods denote new parts.
    ('130', ['a', 'Some title.', 'n', 'Part 1,', 'n', 'Part 2.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Some title', 'Part 1, Part 2'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    # $k is treated as a new part.
    ('130', ['a', 'Works.', 'k', 'Selections.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Works', 'Selections'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    # $k following a collective title is always a new part.
    ('130', ['a', 'Works,', 'k', 'Selections.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Works', 'Selections'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    # Languages are parsed out if multiple are found.
    ('130', ['a', 'Something.', 'l', 'English and French.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Something'],
      'expression_parts': ['English and French'],
      'languages': ['English', 'French'],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    ('130', ['a', 'Something.', 'l', 'English & French.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Something'],
      'expression_parts': ['English & French'],
      'languages': ['English', 'French'],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    ('130', ['a', 'Something.', 'l', 'English, French, and German.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Something'],
      'expression_parts': ['English, French, and German'],
      'languages': ['English', 'French', 'German'],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    # If a generic collective title, like "Works", is followed by a
    # subfield m, it's interpreted as a music form title.
    ('130', ['a', 'Works,', 'm', 'violin.', 'k', 'Selections.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Works, violin', 'Selections'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': True,
      'type': 'main',
      'relations': None,
      }),

    # Anything following a $k results in a new hierarchical part.
    ('130', ['a', 'Works,', 'm', 'violin.', 'k', 'Selections,', 'n', 'op. 8.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Works, violin', 'Selections', 'Op. 8'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': True,
      'type': 'main',
      'relations': None,
      }),

    # "[Instrument] music" is treated as a collective title but not a
    # music form title.
    ('130', ['a', 'Piano music (4 hands)', 'k', 'Selections.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Piano music (4 hands)', 'Selections'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    # $d interacts with collective titles like other subpart sf types.
    ('240', ['a', 'Treaties, etc.', 'd', '1948.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Treaties, etc.', '1948'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    ('240 14', ['a', 'The Treaty of whatever', 'd', '(1948)'],
     {'nonfiling_chars': 4,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['The Treaty of whatever (1948)'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    # ... and $d is treated like other subpart types when it occurs
    # elsewhere.
    ('240', ['a', 'Treaties, etc.', 'g', 'Poland,', 'd', '1948 Mar. 2.',
             'k', 'Protocols, etc.,', 'd', '1951 Mar. 6'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Treaties, etc.', 'Poland, 1948 Mar. 2',
                      'Protocols, etc., 1951 Mar. 6'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': False,
      'type': 'main',
      'relations': None,
      }),

    # 6XX$e and $4 are parsed as relators.
    ('630', ['a', 'Domesday book', 'z', 'United States.', 'e', 'depicted.',
             '4', 'dpc'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Domesday book'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'subject',
      'relations': ['depicted'],
      }),

    # 700, 710, and 711 fields skip past the "author" subfields but
    # handle the $i, if present.

    ('700', ['a', 'Fauré, Gabriel,', 'd', '1845-1924.', 't', 'Nocturnes,',
             'm', 'piano,', 'n', 'no. 11, op. 104, no. 1,', 'r', 'F♯ minor'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Nocturnes, piano', 'No. 11, op. 104, no. 1, F♯ minor'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': True,
      'type': 'related',
      'relations': None,
      }),

    # 7XX ind2 == 2 indicates an 'analytic' type title.
    ('700  2', ['a', 'Fauré, Gabriel,', 'd', '1845-1924.', 't', 'Nocturnes,',
                'm', 'piano,', 'n', 'no. 11, op. 104, no. 1,', 'r', 'F♯ minor'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Nocturnes, piano', 'No. 11, op. 104, no. 1, F♯ minor'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': True,
      'type': 'analytic',
      'relations': None,
      }),

    # 7XX fields with "Container of" in $i indicate an 'analytic' type
    # title, even if ind2 is not 2. In these cases, because the label
    # "Container of" is redundant with the 'analytic' type, the display
    # constant is not generated.
    ('700   ', ['i', 'Container of (work):', 'a', 'Fauré, Gabriel,',
                'd', '1845-1924.', 't', 'Nocturnes,', 'm', 'piano,',
                'n', 'no. 11, op. 104, no. 1,', 'r', 'F♯ minor'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Nocturnes, piano', 'No. 11, op. 104, no. 1, F♯ minor'],
      'expression_parts': [],
      'languages': [],
      'is_collective': True,
      'is_music_form': True,
      'type': 'analytic',
      'relations': None,
      }),

    ('710', ['i', 'Summary of (work):', 'a', 'United States.',
             'b', 'Adjutant-General\'s Office.',
             't', 'Correspondence relating to the war with Spain'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': ['Summary of'],
      'title_parts': ['Correspondence relating to the war with Spain'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'related',
      'relations': None,
      }),

    ('711', ['a', 'International Conference on Gnosticism', 'd', '(1978 :',
             'c', 'New Haven, Conn.).', 't', 'Rediscovery of Gnosticism.',
             'p', 'Modern writers.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Rediscovery of Gnosticism', 'Modern writers'],
      'expression_parts': [],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'related',
      'relations': None,
      }),

    ('730 4 ', ['i', 'Container of (expression):', 'a', 'The Bible.',
                'p', 'Epistles.', 'k', 'Selections.', 'l', 'Tabaru.',
                's', 'Common Language.', 'f', '2001'],
     {'nonfiling_chars': 4,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['The Bible', 'Epistles', 'Selections'],
      'expression_parts': ['Tabaru', 'Common Language', '2001'],
      'languages': ['Tabaru'],
      'is_collective': False,
      'is_music_form': False,
      'type': 'analytic',
      'relations': None,
      }),

    # If $o is present and begins with 'arr', the statement 'arranged'
    # is added to `expression_parts`.
    ('730', ['a', 'God save the king;', 'o', 'arr.', 'f', '1982.'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['God save the king'],
      'expression_parts': ['arranged', '1982'],
      'languages': [],
      'is_collective': False,
      'is_music_form': False,
      'type': 'related',
      'relations': None,
      }),

    # 800, 810, 811, and 830 fields are series and may have $v (volume)
    # and/or $x (ISSN)

    ('800', ['a', 'Berenholtz, Jim,', 'd', '1957-',
             't', 'Teachings of the feathered serpent ;', 'v', 'bk. 1'],
     {'nonfiling_chars': 0,
      'materials_specified': [],
      'display_constants': [],
      'title_parts': ['Teachings of the feathered serpent'],
      'expression_parts': [],
      'languages': [],
      'volume': 'bk. 1',
      'issn': '',
      'is_collective': False,
      'is_music_form': False,
      'type': 'series',
      'relations': None,
      }),

    # $3 becomes `materials_specified` if present
    ('830  2', ['3', 'v. 1-8', 'a', 'A Collection Byzantine.', 'x', '0223-3738'],
     {'nonfiling_chars': 2,
      'materials_specified': ['v. 1-8'],
      'display_constants': [],
      'title_parts': ['A Collection Byzantine'],
      'expression_parts': [],
      'languages': [],
      'volume': '',
      'issn': '0223-3738',
      'is_collective': False,
      'is_music_form': False,
      'type': 'series',
      'relations': None,
      }),
])
def test_preferredtitleparser_parse(
        tag, subfields, expected, params_to_fields):
    """
    PreferredTitleParser `parse` method should return a dict with the
    expected structure, given the provided MARC field.
    """
    if ' ' in tag:
        tag, indicators = tag.split(' ', 1)
    else:
        indicators = '  '
    fields = params_to_fields([(tag, subfields, indicators)])[0]
    assert fp.PreferredTitleParser(fields).parse() == expected


@pytest.mark.parametrize('raw_marcfield, expected', [
    # 250 Edition Statements
    # Simple edition by itself
    ('250 ## $a1st ed.', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{'value': '1st ed.'}]
        },
        'materials_specified': None,
    }),

    # Edition and materials specified
    ('250 ## $31998-2005:$a1st ed.', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{'value': '1st ed.'}]
        },
        'materials_specified': ['1998-2005'],
    }),

    # Edition with bracketed portion
    ('250 ## $a1st [ed.]', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{'value': '1st [ed.]'}]
        },
        'materials_specified': None,
    }),

    # Edition all in brackets
    ('250 ## $a[1st ed.]', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{'value': '[1st ed.]'}]
        },
        'materials_specified': None,
    }),

    # Simple edition plus responsibility
    ('250 ## $a1st ed. /$bedited by J. Smith', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith'
            }]
        },
        'materials_specified': None,
    }),

    # Edition plus responsibility and revision
    ('250 ## $a1st ed. /$bedited by J. Smith, 2nd rev.', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith, 2nd rev.'
            }]
        },
        'materials_specified': None,
    }),

    # Edition plus responsibility and revision plus responsibility
    ('250 ## $a1st ed. /$bedited by J. Smith, 2nd rev. / by N. Smith.', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith, 2nd rev., by N. Smith'
            }]
        },
        'materials_specified': None,
    }),

    # Edition and parallel edition
    ('250 ## $a1st ed. =$b1a ed.', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{'value': '1st ed.'}],
            'parallel': [{'value': '1a ed.'}]
        },
        'materials_specified': None,
    }),

    # Edition/parallel, with one SOR at end
    ('250 ## $a1st ed. =$b1a ed. / edited by J. Smith.', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith'
            }],
            'parallel': [{'value': '1a ed.'}]
        },
        'materials_specified': None,
    }),

    # Edition, with SOR and parallel SOR
    ('250 ## $a1st ed. /$bedited by J. Smith = editado por J. Smith.', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith'
            }],
            'parallel': [{'responsibility': 'editado por J. Smith'}]
        },
        'materials_specified': None,
    }),

    # Edition/SOR plus parallel edition/SOR
    ('250 ## $a1st ed. /$bedited by J. Smith = 1a ed. / editado por J. Smith.',
     {
         'edition_type': 'edition_statement',
         'edition_info': {
             'editions': [{
                 'value': '1st ed.',
                 'responsibility': 'edited by J. Smith'
             }],
             'parallel': [{
                 'value': '1a ed.',
                 'responsibility': 'editado por J. Smith'
             }]
         },
         'materials_specified': None,
     }),

    # Edition/revision plus parallel (including SORs)
    ('250 ## $a1st ed. /$bedited by J. Smith = 1a ed. / editado por J. Smith, '
     '2nd rev. / by B. Roberts = 2a rev. / por B. Roberts.',
     {
         'edition_type': 'edition_statement',
         'edition_info': {
             'editions': [{
                 'value': '1st ed.',
                 'responsibility': 'edited by J. Smith'
             }, {
                 'value': '2nd rev.',
                 'responsibility': 'by B. Roberts'
             }],
             'parallel': [{
                 'value': '1a ed.',
                 'responsibility': 'editado por J. Smith'
             }, {
                 'value': '2a rev.',
                 'responsibility': 'por B. Roberts'
             }]
         },
         'materials_specified': None,
     }),

    # 250s, edges of "revision" detection
    # AACR2 allows for a "named revision" following the main edition,
    # which denotes a specific version of an edition, and appears like
    # an additonal edition (following a ", "). It's very similar to a
    # multi-title 245 but follows ", " instead of ". " and is therefore
    # much harder to detect reliably. The AACR2 examples all show ", "
    # plus a number or capitalized word, but naively looking for that
    # pattern gives rise to many, many false positives -- lists of
    # names, for example (1st ed. / edited by J. Smith, B. Roberts).
    #
    # In reality, failing to detect a named revision may or may not be
    # a problem, depending on the situation.
    #    - `1st edition, New revision` -- In this case it's all treated
    #      as one contiguous edition string, which is fine.
    #    - `1st edition / edited by J. Smith, New revision` -- In this
    #      case, "New revision" is treated as part of the SOR; this
    #      will display relatively clearly, with the downside that the
    #      named revision becomes searchable as part of the SOR search
    #      field. Materially this shouldn't have a big impact -- the
    #      text is still searchable, just arguably with a small effect
    #      on relevance for terms that match.
    #    - `1st ed. = 1a ed., New rev. = Nueva rev.` -- In this case,
    #      not detecting the named revision makes it part of the
    #      parallel text, "1a ed." This is blatantly incorrect and
    #      ends up displaying as, `1st ed. [translated: 1a ed., New
    #      rev.]`.
    #
    # The third scenario listed above is the most problematic but also
    # the rarest. Through testing against our catalog data it does
    # appear possible to find reliably with minimal false positives.

    # Otherwise valid pattern not following " = " is not recognized
    ('250 ## $a1st ed. /$bedited by J. Smith, New revision.',
     {
         'edition_type': 'edition_statement',
         'edition_info': {
             'editions': [{
                 'value': '1st ed.',
                 'responsibility': 'edited by J. Smith, New revision'
             }]
         },
         'materials_specified': None,
     }),

    # Obvious names are not recognized
    ('250 ## $a1st ed. =$b1a ed. / edited by J. Smith, Bob Roberts.',
     {
         'edition_type': 'edition_statement',
         'edition_info': {
             'editions': [{
                 'value': '1st ed.',
                 'responsibility': 'edited by J. Smith, Bob Roberts'
             }],
             'parallel': [{
                 'value': '1a ed.'
             }]
         },
         'materials_specified': None,
     }),

    # Valid numeric pattern is recognized
    ('250 ## $a1st ed. =$b1a ed., 2nd rev.',
     {
         'edition_type': 'edition_statement',
         'edition_info': {
             'editions': [{
                 'value': '1st ed.',
             }, {
                 'value': '2nd rev.'
             }],
             'parallel': [{
                 'value': '1a ed.'
             }]
         },
         'materials_specified': None,
     }),

    # Valid one-word pattern is recognized
    ('250 ## $a1st ed. =$b1a ed., Klavierauszug = Piano reduction',
     {
         'edition_type': 'edition_statement',
         'edition_info': {
             'editions': [{
                 'value': '1st ed.',
             }, {
                 'value': 'Klavierauszug'
             }],
             'parallel': [{
                 'value': '1a ed.'
             }, {
                 'value': 'Piano reduction'
             }]
         },
         'materials_specified': None,
     }),

    # Valid multi-word pattern is recognized
    ('250 ## $a1st ed. =$b1a ed., New Blah rev.',
     {
         'edition_type': 'edition_statement',
         'edition_info': {
             'editions': [{
                 'value': '1st ed.',
             }, {
                 'value': 'New Blah rev.'
             }],
             'parallel': [{
                 'value': '1a ed.'
             }]
         },
         'materials_specified': None,
     }),

    # 250s, other edge cases
    # Missing `/` before $b
    ('250 ## $a1st ed.,$bedited by J. Smith', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith'
            }]
        },
        'materials_specified': None,
    }),

    # Full edition statement is in $a (no $b)
    ('250 ## $a1st ed. / edited by J. Smith = 1a ed. / editado por J. Smith, '
     '2nd rev. / by B. Roberts = 2a rev. / por B. Roberts.',
     {
         'edition_type': 'edition_statement',
         'edition_info': {
             'editions': [{
                 'value': '1st ed.',
                 'responsibility': 'edited by J. Smith'
             }, {
                 'value': '2nd rev.',
                 'responsibility': 'by B. Roberts'
             }],
             'parallel': [{
                 'value': '1a ed.',
                 'responsibility': 'editado por J. Smith'
             }, {
                 'value': '2a rev.',
                 'responsibility': 'por B. Roberts'
             }]
         },
         'materials_specified': None,
     }),

    # $b follows 2nd (or 3rd, etc.) ISBD punctuation mark
    ('250 ## $a1st ed. / edited by J. Smith =$b1a ed. / editado por J. Smith, '
     '2nd rev. / by B. Roberts = 2a rev. / por B. Roberts.',
     {
         'edition_type': 'edition_statement',
         'edition_info': {
             'editions': [{
                 'value': '1st ed.',
                 'responsibility': 'edited by J. Smith'
             }, {
                 'value': '2nd rev.',
                 'responsibility': 'by B. Roberts'
             }],
             'parallel': [{
                 'value': '1a ed.',
                 'responsibility': 'editado por J. Smith'
             }, {
                 'value': '2a rev.',
                 'responsibility': 'por B. Roberts'
             }]
         },
         'materials_specified': None,
     }),

    # Multiple $bs
    ('250 ## $a1st ed. /$bedited by J. Smith =$b1a ed. / editado por J. Smith, '
     '2nd rev. / by B. Roberts = 2a rev. / por B. Roberts.',
     {
         'edition_type': 'edition_statement',
         'edition_info': {
             'editions': [{
                 'value': '1st ed.',
                 'responsibility': 'edited by J. Smith'
             }, {
                 'value': '2nd rev.',
                 'responsibility': 'by B. Roberts'
             }],
             'parallel': [{
                 'value': '1a ed.',
                 'responsibility': 'editado por J. Smith'
             }, {
                 'value': '2a rev.',
                 'responsibility': 'por B. Roberts'
             }]
         },
         'materials_specified': None,
     }),

    # Extra spacing between `/` and $b
    ('250 ## $a1st ed. / $bedited by J. Smith', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{
                'value': '1st ed.',
                'responsibility': 'edited by J. Smith'
            }]
        },
        'materials_specified': None,
    }),

    # Extra spacing between `=` and $b
    ('250 ## $a1st ed. = $b1a ed.', {
        'edition_type': 'edition_statement',
        'edition_info': {
            'editions': [{'value': '1st ed.'}],
            'parallel': [{'value': '1a ed.'}]
        },
        'materials_specified': None,
    }),

    # 251 Versions
    # Single version
    ('251 ## $aFirst draft.',
     {
         'edition_type': 'version',
         'edition_info': {
             'editions': [{
                 'value': 'First draft',
             }]
         },
         'materials_specified': None,
     }),

    # Version plus materials specified
    ('251 ## $31988-1989:$aFirst draft.',
     {
         'edition_type': 'version',
         'edition_info': {
             'editions': [{
                 'value': 'First draft',
             }]
         },
         'materials_specified': ['1988-1989'],
     }),

    # Multiple versions, in multiple $as
    ('251 ## $aFirst draft$aSecond version',
     {
         'edition_type': 'version',
         'edition_info': {
             'editions': [{
                 'value': 'First draft; Second version',
             }]
         },
         'materials_specified': None,
     }),


    # 254 Music Presentation Statements
    # Single statement
    ('254 ## $aFull score.',
     {
         'edition_type': 'musical_presentation_statement',
         'edition_info': {
             'editions': [{
                 'value': 'Full score',
             }]
         },
         'materials_specified': None,
     }),

    # Multiple statements in multiple $as
    ('254 ## $aFull score$aPartitur.',
     {
         'edition_type': 'musical_presentation_statement',
         'edition_info': {
             'editions': [{
                 'value': 'Full score; Partitur',
             }]
         },
         'materials_specified': None,
     }),

], ids=[
    # 250 Edition Statements
    'Simple edition by itself',
    'Edition and materials specified',
    'Edition with bracketed portion',
    'Edition all in brackets',
    'Simple edition plus responsibility',
    'Edition plus responsibility and revision',
    'Edition plus responsibility and revision plus responsibility',
    'Edition and parallel edition',
    'Edition/parallel, with one SOR at end',
    'Edition, with SOR and parallel SOR',
    'Edition/SOR plus parallel edition/SOR',
    'Edition/revision plus parallel (including SORs)',

    # 250s, edges of "revision" detection
    'Otherwise valid pattern not following " = " not recognized',
    'Obvious names are not recognized',
    'Valid numeric pattern is recognized',
    'Valid one-word pattern is recognized',
    'Valid multi-word pattern is recognized',

    # 250s, other edge cases
    'Missing `/` before $b',
    'Full edition statement is in $a (no $b)',
    '$b follows 2nd (or 3rd, etc.) ISBD punctuation mark',
    'Multiple $bs',
    'Extra spacing between `/` and $b',
    'Extra spacing between `=` and $b',

    # 251 Versions
    'Single version',
    'Version plus materials specified;',
    'Multiple versions, in multiple $as',

    # 254 Music Presentation Statements
    'Single statement',
    'Multiple statements in multiple $as',
])
def test_editionparser_parse(raw_marcfield, expected, fieldstrings_to_fields):
    """
    When passed the given MARC field, the `EditionParser.parse` method
    should return the expected results.
    """
    field = fieldstrings_to_fields([raw_marcfield])[0]
    result = fp.EditionParser(field).parse()
    print(result)
    assert result == expected


@pytest.mark.parametrize('marc_tags_ind, sf_i, equals, test_value', [
    # General behavior
    # Most fields: use $i when ind2 is 8 and $i exists
    (['765 #8', '767 #8', '770 #8', '772 #8', '773 #8', '774 #8', '775 #8',
      '776 #8', '777 #8', '786 #8', '787 #8'],
     'Custom label:', True, 'Custom label'),

    # Most fields: no display label when ind2 is 8 and no $i exists
    (['765 #8', '767 #8', '770 #8', '772 #8', '773 #8', '774 #8', '775 #8',
      '776 #8', '777 #8', '786 #8', '787 #8'],
     None, True, None),

    # All fields: ignore $i when ind2 is not 8
    (['760 ##', '762 ##', '765 ##', '767 ##', '770 ##', '772 ##', '773 ##',
      '774 ##', '775 ##', '776 ##', '777 ##', '780 ##', '785 ##', '786 ##',
      '787 ##'],
     'Custom label:', False, 'Custom label'),

    # All fields: no display label when ind2 is outside the valid range
    (['760 #0', '762 #0', '765 #0', '767 #0', '770 #0', '772 #1', '773 #0',
      '774 #0', '775 #0', '776 #0', '777 #0', '780 #9', '785 #9', '786 #0',
      '787 #0'],
     None, True, None),

    # Exceptions to general behavior
    # Certain fields: no display label when ind2 is blank
    (['760 ##', '762 ##', '774 ##', '787 ##'],
     None, True, None),

    # Certain fields: ignore $i even when ind2 is 8 and $i exists
    (['760 #8', '762 #8', '780 #8', '785 #8'],
     'Custom label:', False, 'Custom label'),

    # Field-specific labels
    (['765 ##'], None, True, 'Translation of'),
    (['767 ##'], None, True, 'Translated as'),
    (['770 ##'], None, True, 'Supplement'),
    (['772 ##'], None, True, 'Supplement to'),
    (['772 #0'], None, True, 'Parent'),
    (['774 #8'], 'Container of:', True, None),
    (['780 ##'], None, True, None),
    (['780 #0'], None, True, 'Continues'),
    (['780 #1'], None, True, 'Continues in part'),
    (['780 #2'], None, True, 'Supersedes'),
    (['780 #3'], None, True, 'Supersedes in part'),
    (['780 #4'], None, True, 'Merger of'),
    (['780 #5'], None, True, 'Absorbed'),
    (['780 #6'], None, True, 'Absorbed in part'),
    (['780 #7'], None, True, 'Separated from'),
    (['785 ##'], None, True, None),
    (['785 #0'], None, True, 'Continued by'),
    (['785 #1'], None, True, 'Continued in part by'),
    (['785 #2'], None, True, 'Superseded by'),
    (['785 #3'], None, True, 'Superseded in part by'),
    (['785 #4'], None, True, 'Absorbed by'),
    (['785 #5'], None, True, 'Absorbed in part by'),
    (['785 #6'], None, True, 'Split into'),
    (['785 #7'], None, True, 'Merged with'),
    (['785 #8'], None, True, 'Changed back to'),
], ids=[
    # General behavior
    'Most fields: use $i when ind2 is 8 and $i exists',
    'Most fields: no display label when ind2 is 8 and no $i exists',
    'All fields: ignore $i when ind2 is not 8',
    'All fields: no display label when ind2 is outside the valid range',

    # Exceptions to general behavior
    'Certain fields: no display label when ind2 is blank',
    'Certain fields: no display label even when ind2 is 8 and $i exists',

    # Field-specific labels
    '765 ind2 blank => `Translation of`',
    '767 ind2 blank => `Translated as`',
    '770 ind2 blank => `Supplement`',
    '772 ind2 blank => `Supplement to`',
    '772 ind2 0 => `Parent`',
    '774 ind2 8 and $i is "Container of" => No display label',
    '780 ind2 blank => No display label',
    '780 ind2 0: => `Continues`',
    '780 ind2 1: => `Continues in part`',
    '780 ind2 2: => `Supersedes`',
    '780 ind2 3: => `Supersedes in part`',
    '780 ind2 4: => `Merger of`',
    '780 ind2 5: => `Absorbed`',
    '780 ind2 6: => `Absorbed in part`',
    '780 ind2 7: => `Separated from`',
    '785 ind2 blank => No display label',
    '785 ind2 0: => `Continued by`',
    '785 ind2 1: => `Continued in part by`',
    '785 ind2 2: => `Superseded by`',
    '785 ind2 3: => `Superseded in part by`',
    '785 ind2 4: => `Absorbed by`',
    '785 ind2 5: => `Absorbed in part by`',
    '785 ind2 6: => `Split into`',
    '785 ind2 7: => `Merged with`',
    '785 ind2 8: => `Changed back to`',
])
def test_linkingfieldparser_display_labels(marc_tags_ind, sf_i, equals,
                                           test_value, fieldstrings_to_fields):
    """
    For a field constructed using each in the given list of MARC tags
    plus indicators (`marc_tags_ind`), using the given $i value
    (`sf_i`), the `LinkingFieldParser.parse` method will return a dict
    where the display_label entry either `equals` (or does not equal)
    the `test_value`.
    """
    for tag_ind in marc_tags_ind:
        rawfield = tag_ind
        if sf_i:
            rawfield = '{}$i{}'.format(rawfield, sf_i)
        rawfield = '{}$tSample title'.format(rawfield)
        field = fieldstrings_to_fields([rawfield])[0]
        result = fp.LinkingFieldParser(field).parse()
        assert (result['display_label'] == test_value) == equals


@pytest.mark.parametrize('raw_marcfield, expected', [
    # Edge cases
    # Empty field
    ('787 ##$a', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # No $s or $t title
    ('787 ##$aSome author.$dPub date.$w(OCoLC)646108719', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': 'Some author',
        'short_author': 'Some author',
        'author_type': 'organization',
        'display_metadata': ['Pub date'],
        'identifiers_map': {
            'oclc': {'number': '646108719', 'numtype': 'control'}
        },
        'identifiers_list': [{
            'code': 'oclc',
            'numtype': 'control',
            'label': 'OCLC Number',
            'number': '646108719'}],
        'materials_specified': None,
    }),

    # Title, author, short author, display metadata
    # $s and $t title, use $t as title
    ('787 ##$sUniform title.$tTranscribed title.', {
        'display_label': None,
        'title_parts': ['Transcribed title'],
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # $t title only, use $t as title
    ('787 ##$tTranscribed title.', {
        'display_label': None,
        'title_parts': ['Transcribed title'],
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # $s title only, use $s as title
    ('787 ##$sUniform title.', {
        'display_label': None,
        'title_parts': ['Uniform title'],
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # title with multiple parts
    ('787 ##$sRiigi teataja (1990). English. Selections.', {
        'display_label': None,
        'title_parts': ['Riigi teataja (1990)', 'English', 'Selections'],
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # $s, non-music collective title
    ('787 ##$aBeethoven, Ludwig van.$sWorks. Selections', {
        'display_label': None,
        'title_parts': ['Works', 'Selections'],
        'title_is_collective': True,
        'title_is_music_form': False,
        'volume': None,
        'author': 'Beethoven, Ludwig van',
        'short_author': 'Beethoven, L.v.',
        'author_type': 'person',
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # $s, music form collective title
    ('787 ##$aBeethoven, Ludwig van.$sSonatas.', {
        'display_label': None,
        'title_parts': ['Sonatas'],
        'title_is_collective': True,
        'title_is_music_form': True,
        'volume': None,
        'author': 'Beethoven, Ludwig van',
        'short_author': 'Beethoven, L.v.',
        'author_type': 'person',
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # $s, music form plus instrument collective title
    ('787 ##$aBeethoven, Ludwig van.$sSonatas, piano.', {
        'display_label': None,
        'title_parts': ['Sonatas, piano'],
        'title_is_collective': True,
        'title_is_music_form': True,
        'volume': None,
        'author': 'Beethoven, Ludwig van',
        'short_author': 'Beethoven, L.v.',
        'author_type': 'person',
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # $s, "N music" collective title
    ('787 ##$aBeethoven, Ludwig van.$sPiano music.', {
        'display_label': None,
        'title_parts': ['Piano music'],
        'title_is_collective': True,
        'title_is_music_form': False,
        'volume': None,
        'author': 'Beethoven, Ludwig van',
        'short_author': 'Beethoven, L.v.',
        'author_type': 'person',
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # author, personal name
    ('787 ##$aBeethoven, Ludwig van, 1770-1827.', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': 'Beethoven, Ludwig van, 1770-1827',
        'short_author': 'Beethoven, L.v.',
        'author_type': 'person',
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # author, organizational name
    ('787 ##$aUnited States. Congress.', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': 'United States Congress',
        'short_author': 'United States, Congress',
        'author_type': 'organization',
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # author, meeting name
    ('787 ##$aFestival of Britain (1951 : London, England)', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': 'Festival of Britain',
        'short_author': 'Festival of Britain',
        'author_type': 'organization',
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # multiple metadata subfields -- should stay in order
    # also: $e, $f, $q, and $v are ignored.
    ('787 ##$b[English edition]$c(London, 1958)$dChennai : Westland, 2011'
     '$eeng$fdcu$gJan. 1992$hmicrofilm$j20100101'
     '$kAsia Pacific legal culture and globalization$mScale 1:760,320.'
     '$n"July 2011"$oN 84-11142$q15:5<30$vBase map data', {
         'display_label': None,
         'title_parts': None,
         'title_is_collective': False,
         'title_is_music_form': False,
         'volume': None,
         'author': None,
         'short_author': None,
         'author_type': None,
         'display_metadata': [
             '[English edition]', 'London, 1958', 'Chennai : Westland, 2011',
             'Jan. 1992', 'microfilm',
             'Asia Pacific legal culture and globalization', 'Scale 1:760,320',
             '"July 2011"', 'N 84-11142', 'Base map data'
         ],
         'identifiers_map': None,
         'identifiers_list': None,
         'materials_specified': None,
     }),

    # 760: $g following the title is treated as volume
    ('760 ##$tSeries title.$gVol. 1.', {
        'display_label': None,
        'title_parts': ['Series title'],
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': 'vol. 1',
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # 762: $g following the title is treated as volume
    ('762 ##$tSeries title.$gNO. 23.', {
        'display_label': None,
        'title_parts': ['Series title'],
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': 'NO. 23',
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': None,
        'identifiers_list': None,
        'materials_specified': None,
    }),

    # Identifiers
    # $r => Report Number
    ('787 ##$rEPA 430-H-02-001', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'r': {'number': 'EPA 430-H-02-001', 'numtype': 'standard'}
        },
        'identifiers_list': [{
            'code': 'r',
            'numtype': 'standard',
            'label': 'Report Number',
            'number': 'EPA 430-H-02-001'}],
        'materials_specified': None,
    }),

    # $u => STRN
    ('787 ##$uFHWA/NC/95-002', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'u': {'number': 'FHWA/NC/95-002', 'numtype': 'standard'}
        },
        'identifiers_list': [{
            'code': 'u',
            'numtype': 'standard',
            'label': 'STRN',
            'number': 'FHWA/NC/95-002'}],
        'materials_specified': None,
    }),

    # $w (OCoLC) => OCLC Number
    ('787 ##$w(OCoLC)12700508', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'oclc': {'number': '12700508', 'numtype': 'control'}
        },
        'identifiers_list': [{
            'code': 'oclc',
            'numtype': 'control',
            'label': 'OCLC Number',
            'number': '12700508'}],
        'materials_specified': None,
    }),

    # $w (DLC) => LCCN
    ('787 ##$w(DLC)   92643478', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'lccn': {'number': '92643478', 'numtype': 'control'}
        },
        'identifiers_list': [{
            'code': 'lccn',
            'numtype': 'control',
            'label': 'LCCN',
            'number': '92643478'}],
        'materials_specified': None,
    }),

    # $w (CaOONL) => CaOONL Number
    ('787 ##$w(CaOONL)890390894', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'w': {'number': '890390894', 'numtype': 'control'}
        },
        'identifiers_list': [{
            'code': 'w',
            'numtype': 'control',
            'label': 'CaOONL Number',
            'number': '890390894'}],
        'materials_specified': None,
    }),

    # $w with no qualifier => Control Number
    ('787 ##$w890390894', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'w': {'number': '890390894', 'numtype': 'control'}
        },
        'identifiers_list': [{
            'code': 'w',
            'numtype': 'control',
            'label': 'Control Number',
            'number': '890390894'}],
        'materials_specified': None,
    }),

    # $x => ISSN
    ('787 ##$x1544-7227', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'issn': {'number': '1544-7227', 'numtype': 'standard'}
        },
        'identifiers_list': [{
            'code': 'issn',
            'numtype': 'standard',
            'label': 'ISSN',
            'number': '1544-7227'}],
        'materials_specified': None,
    }),

    # $y => CODEN
    ('787 ##$yFBKRAT', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'coden': {'number': 'FBKRAT', 'numtype': 'standard'}
        },
        'identifiers_list': [{
            'code': 'coden',
            'numtype': 'standard',
            'label': 'CODEN',
            'number': 'FBKRAT'}],
        'materials_specified': None,
    }),

    # $z => ISBN
    ('787 ##$z477440490X', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'isbn': {'number': '477440490X', 'numtype': 'standard'}
        },
        'identifiers_list': [{
            'code': 'isbn',
            'numtype': 'standard',
            'label': 'ISBN',
            'number': '477440490X'}],
        'materials_specified': None,
    }),

    # Multiple different identifiers
    ('787 ##$z9781598847611$w(DLC)   2012034673$w(OCoLC)768800369', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'isbn': {'number': '9781598847611', 'numtype': 'standard'},
            'lccn': {'number': '2012034673', 'numtype': 'control'},
            'oclc': {'number': '768800369', 'numtype': 'control'},
        },
        'identifiers_list': [{
            'code': 'isbn',
            'numtype': 'standard',
            'label': 'ISBN',
            'number': '9781598847611'
        }, {
            'code': 'lccn',
            'numtype': 'control',
            'label': 'LCCN',
            'number': '2012034673'
        }, {
            'code': 'oclc',
            'numtype': 'control',
            'label': 'OCLC Number',
            'number': '768800369'
        }],
        'materials_specified': None,
    }),

    # Multiple identifiers of the same type
    # Only the first is used in `identifiers_map`.
    ('787 ##$z477440490X$z9784774404905$w(OCoLC)883612986', {
        'display_label': None,
        'title_parts': None,
        'title_is_collective': False,
        'title_is_music_form': False,
        'volume': None,
        'author': None,
        'short_author': None,
        'author_type': None,
        'display_metadata': None,
        'identifiers_map': {
            'isbn': {'number': '477440490X', 'numtype': 'standard'},
            'oclc': {'number': '883612986', 'numtype': 'control'},
        },
        'identifiers_list': [{
            'code': 'isbn',
            'numtype': 'standard',
            'label': 'ISBN',
            'number': '477440490X'
        }, {
            'code': 'isbn',
            'numtype': 'standard',
            'label': 'ISBN',
            'number': '9784774404905'
        }, {
            'code': 'oclc',
            'numtype': 'control',
            'label': 'OCLC Number',
            'number': '883612986'
        }],
        'materials_specified': None,
    }),
], ids=[
    # Edge cases
    'Empty field',
    'No $s or $t title',

    # Title, author, short author, display metadata
    '$s and $t title, use $t as title',
    '$t title only, use $t as title',
    '$s title only, use $s as title',
    '$s, non-music collective title',
    '$s, music form collective title',
    '$s, music form plus instrument collective title',
    '$s, "N music" collective title',
    'title with multiple parts',
    'author, personal name',
    'author, organizational name',
    'author, meeting name',
    'multiple metadata subfields -- should stay in order',
    '760: $g following the title is treated as volume',
    '762: $g following the title is treated as volume',

    # Identifiers
    '$r => Report Number',
    '$u => STRN',
    '$w (OCoLC) => OCLC Number',
    '$w (DLC) => LCCN',
    '$w (CaOONL) => CaOONL Number',
    '$w with no qualifier => Control Number',
    '$x => ISSN',
    '$y => CODEN',
    '$z => ISBN',
    'Multiple different identifiers',
    'Multiple identifiers of the same type',
])
def test_linkingfieldparser_parse(raw_marcfield, expected,
                                  fieldstrings_to_fields):
    """
    When passed the given MARC field, the `LinkingFieldParser.parse`
    method should return the expected results.
    """
    field = fieldstrings_to_fields([raw_marcfield])[0]
    result = fp.LinkingFieldParser(field).parse()
    print(result)
    assert result == expected


@pytest.mark.parametrize('subfields, expected', [
    (['a', 'soprano voice', 'n', '2', 'a', 'mezzo-soprano voice', 'n', '1',
      'a', 'tenor saxophone', 'n', '1', 'd', 'bass clarinet', 'n', '1',
      'a', 'trumpet', 'n', '1', 'a', 'piano', 'n', '1', 'a', 'violin',
      'n', '1', 'd', 'viola', 'n', '1', 'a', 'double bass', 'n', '1', 's', '8',
      '2', 'lcmpt'],
     {'materials_specified': [],
      'total_performers': '8',
      'total_ensembles': None,
      'parts': [
         [{'primary': [('soprano voice', '2')]}],
         [{'primary': [('mezzo-soprano voice', '1')]}],
         [{'primary': [('tenor saxophone', '1')]},
             {'doubling': [('bass clarinet', '1')]}],
         [{'primary': [('trumpet', '1')]}],
         [{'primary': [('piano', '1')]}],
         [{'primary': [('violin', '1')]}, {'doubling': [('viola', '1')]}],
         [{'primary': [('double bass', '1')]}],
     ]}),
    (['b', 'flute', 'n', '1', 'a', 'orchestra', 'e', '1', 'r', '1', 't', '1',
      '2', 'lcmpt'],
     {'materials_specified': [],
      'total_performers': '1',
      'total_ensembles': '1',
      'parts': [
         [{'solo': [('flute', '1')]}],
         [{'primary': [('orchestra', '1')]}],
     ]}),
    (['a', 'flute', 'n', '1', 'd', 'piccolo', 'n', '1', 'd', 'alto flute',
      'n', '1', 'd', 'bass flute', 'n', '1', 's', '1', '2', 'lcmpt'],
     {'materials_specified': [],
      'total_performers': '1',
      'total_ensembles': None,
      'parts': [
         [{'primary': [('flute', '1')]},
          {'doubling': [('piccolo', '1'), ('alto flute', '1'),
                        ('bass flute', '1')]}],
     ]}),
    (['a', 'violin', 'n', '1', 'd', 'flute', 'n', '1', 'p', 'piccolo', 'n', '1',
      'a', 'cello', 'n', '1', 'a', 'piano', 'n', '1', 's', '3', '2', 'lcmpt'],
     {'materials_specified': [],
      'total_performers': '3',
      'total_ensembles': None,
      'parts': [
         [{'primary': [('violin', '1')]}, {'doubling': [('flute', '1')]},
          {'alt': [('piccolo', '1')]}],
         [{'primary': [('cello', '1')]}],
         [{'primary': [('piano', '1')]}],
     ]}),
    (['b', 'soprano voice', 'n', '3', 'b', 'alto voice', 'n', '2',
      'b', 'tenor voice', 'n', '1', 'b', 'baritone voice', 'n', '1',
      'b', 'bass voice', 'n', '1', 'a', 'mixed chorus', 'e', '2',
      'v', 'SATB, SATB', 'a', 'children\'s chorus', 'e', '1', 'a',
      'orchestra', 'e', '1', 'r', '8', 't', '4', '2', 'lcmpt'],
     {'materials_specified': [],
      'total_performers': '8',
      'total_ensembles': '4',
      'parts': [
         [{'solo': [('soprano voice', '3')]}],
         [{'solo': [('alto voice', '2')]}],
         [{'solo': [('tenor voice', '1')]}],
         [{'solo': [('baritone voice', '1')]}],
         [{'solo': [('bass voice', '1')]}],
         [{'primary': [('mixed chorus', '2', ['SATB, SATB'])]}],
         [{'primary': [('children\'s chorus', '1')]}],
         [{'primary': [('orchestra', '1')]}],
     ]}),
    (['a', 'violin', 'p', 'flute', 'd', 'viola', 'p', 'alto flute',
      'd', 'cello', 'p', 'saxophone', 'd', 'double bass'],
     {'materials_specified': [],
      'total_performers': None,
      'total_ensembles': None,
      'parts': [
         [{'primary': [('violin', '1')]}, {'alt': [('flute', '1')]},
          {'doubling': [('viola', '1')]}, {'alt': [('alto flute', '1')]},
          {'doubling': [('cello', '1')]}, {'alt': [('saxophone', '1')]},
          {'doubling': [('double bass', '1')]}],
     ]}),
    (['a', 'violin', 'd', 'viola', 'd', 'cello', 'd', 'double bass',
      'p', 'flute', 'd', 'alto flute', 'd', 'saxophone'],
     {'materials_specified': [],
      'total_performers': None,
      'total_ensembles': None,
      'parts': [
         [{'primary': [('violin', '1')]},
          {'doubling': [('viola', '1'), ('cello', '1'), ('double bass', '1')]},
          {'alt': [('flute', '1')]},
          {'doubling': [('alto flute', '1'), ('saxophone', '1')]}],
     ]}),
    (['a', 'violin', 'v', 'Note1', 'v', 'Note2', 'd', 'viola', 'v', 'Note3',
      'd', 'cello', 'n', '2', 'v', 'Note4', 'v', 'Note5'],
     {'materials_specified': [],
      'total_performers': None,
      'total_ensembles': None,
      'parts': [
         [{'primary': [('violin', '1', ['Note1', 'Note2'])]},
          {'doubling': [('viola', '1', ['Note3']),
                        ('cello', '2', ['Note4', 'Note5'])]}]
     ]}),
    (['a', 'violin', 'd', 'viola', 'd', 'cello', 'd', 'double bass',
      'p', 'flute', 'p', 'clarinet', 'd', 'alto flute', 'd', 'saxophone'],
     {'materials_specified': [],
      'total_performers': None,
      'total_ensembles': None,
      'parts': [
         [{'primary': [('violin', '1')]},
          {'doubling': [('viola', '1'), ('cello', '1'), ('double bass', '1')]},
          {'alt': [('flute', '1'), ('clarinet', '1')]},
          {'doubling': [('alto flute', '1'), ('saxophone', '1')]}],
     ]}),
    (['a', 'violin', 'p', 'flute', 'p', 'trumpet', 'p', 'clarinet',
      'd', 'viola', 'p', 'alto flute', 'd', 'cello', 'p', 'saxophone',
      'd', 'double bass'],
     {'materials_specified': [],
      'total_performers': None,
      'total_ensembles': None,
      'parts': [
         [{'primary': [('violin', '1')]},
          {'alt': [('flute', '1'), ('trumpet', '1'), ('clarinet', '1')]},
          {'doubling': [('viola', '1')]}, {'alt': [('alto flute', '1')]},
          {'doubling': [('cello', '1')]}, {'alt': [('saxophone', '1')]},
          {'doubling': [('double bass', '1')]}],
     ]}),
    (['3', 'Piece One', 'b', 'flute', 'n', '1', 'a', 'orchestra', 'e', '1',
      'r', '1', 't', '1', '2', 'lcmpt'],
     {'materials_specified': ['Piece One'],
      'total_performers': '1',
      'total_ensembles': '1',
      'parts': [
         [{'solo': [('flute', '1')]}],
         [{'primary': [('orchestra', '1')]}],
     ]}),
])
def test_performancemedparser_parse(subfields, expected, params_to_fields):
    """
    PerformanceMedParser `parse` method should return a dict with the
    expected structure, given the provided MARC 382 field.
    """
    field = params_to_fields([('382', subfields)])[0]
    assert fp.PerformanceMedParser(field).parse() == expected


@pytest.mark.parametrize('subfields, expected', [
    (['b', 'Ph.D', 'c', 'University of Louisville', 'd', '1997.'],
     {'degree': 'Ph.D',
      'institution': 'University of Louisville',
      'date': '1997',
      'note_parts': ['Ph.D ― University of Louisville, 1997']
      }),
    (['b', 'Ph.D', 'c', 'University of Louisville.'],
     {'degree': 'Ph.D',
      'institution': 'University of Louisville',
      'date': None,
      'note_parts': ['Ph.D ― University of Louisville']
      }),
    (['b', 'Ph.D', 'd', '1997.'],
     {'degree': 'Ph.D',
      'institution': None,
      'date': '1997',
      'note_parts': ['Ph.D ― 1997']
      }),
    (['b', 'Ph.D'],
     {'degree': 'Ph.D',
      'institution': None,
      'date': None,
      'note_parts': ['Ph.D']
      }),
    (['g', 'Some thesis', 'b', 'Ph.D', 'c', 'University of Louisville',
      'd', '1997.'],
     {'degree': 'Ph.D',
      'institution': 'University of Louisville',
      'date': '1997',
      'note_parts': ['Some thesis', 'Ph.D ― University of Louisville, 1997']
      }),
    (['g', 'Some thesis', 'b', 'Ph.D', 'c', 'University of Louisville',
      'd', '1997.', 'g', 'Other info', 'o', 'identifier'],
     {'degree': 'Ph.D',
      'institution': 'University of Louisville',
      'date': '1997',
      'note_parts': ['Some thesis', 'Ph.D ― University of Louisville, 1997',
                     'Other info', 'identifier']
      }),
])
def test_dissertationnotesfieldparser_parse(subfields, expected,
                                            params_to_fields):
    """
    DissertationNotesFieldParser `parse` method should return a dict
    with the expected structure, given the provided MARC 502 subfields.
    """
    field = params_to_fields([('502', subfields)])[0]
    assert fp.DissertationNotesFieldParser(field).parse() == expected


@pytest.mark.parametrize('subfields, label, sep, sff, expected', [
    (['3', 'case files', 'a', 'aperture cards', 'b', '9 x 19 cm.',
      'd', 'microfilm', 'f', '48x'], None, '; ', None,
     '(case files) aperture cards; 9 x 19 cm.; microfilm; 48x'),
    (['3', 'case files', 'a', 'aperture cards', 'b', '9 x 19 cm.',
      'd', 'microfilm', 'f', '48x'], 'Specs', '; ', {'exclude': '3'},
     'Specs: aperture cards; 9 x 19 cm.; microfilm; 48x'),
    (['3', 'case files', 'a', 'aperture cards', 'b', '9 x 19 cm.',
      '3', 'microfilm', 'f', '48x'], 'Specs', '; ', None,
     '(case files) Specs: aperture cards; 9 x 19 cm.; 48x'),
    (['a', 'aperture cards', 'b', '9 x 19 cm.', 'd', 'microfilm',
      'f', '48x', '3', 'case files'], None, '; ', None,
     'aperture cards; 9 x 19 cm.; microfilm; 48x'),
    (['3', 'case files', '3', 'aperture cards', 'b', '9 x 19 cm.',
      'd', 'microfilm', 'f', '48x'], None, '; ', None,
     '(case files, aperture cards) 9 x 19 cm.; microfilm; 48x'),
    (['3', 'case files', 'a', 'aperture cards', 'b', '9 x 19 cm.',
      'd', 'microfilm', 'f', '48x'], None, '. ', None,
     '(case files) aperture cards. 9 x 19 cm. microfilm. 48x'),
    (['a', 'Register at https://libproxy.library.unt.edu/login?url=https://what'
           'ever.com'], None, ' ', None,
     'Register at https://libproxy.library.unt.edu/login?url=https://whatever.'
     'com'),
])
def test_genericdisplayfieldparser_parse(subfields, label, sep, sff, expected,
                                         params_to_fields):
    """
    The GenericDisplayFieldParser `parse` method should return the
    expected result when parsing a MARC field with the given
    `subfields`, given the provided `sep` (separator) and `sff`
    (subfield filter).
    """
    field = params_to_fields([('300', subfields)])[0]
    result = fp.GenericDisplayFieldParser(field, sep, sff, label).parse()
    assert result == expected



