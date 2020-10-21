# -*- coding: utf-8 -*-

"""
Tests the sierra.settings.marcdata.subjectmaps functionality.
"""

from __future__ import unicode_literals
import pytest

from sierra.settings.marcdata import subjectmaps

# FIXTURES AND TEST DATA

# SAMPLE_PATTERN_MAP, like subjectmaps.LCSH_SUBDIVISION_PATTERNS
war_words = '(?:war|revolution)'
SAMPLE_PATTERN_MAP = [
    [r'annexation to (.+)', 
        [('topic', 'Annexation (International law)'), ('region', '{}')],
        'Annexation to the United States'],
    [r'art and(?: the)? {}'.format(war_words),
        [('topic','Art and war')],
        'Art and the war'],
    [r'dependency on (?!foreign countries)(.+)',
        [('topic', 'Dependency'), ('region', '{}')],
        'Dependency on the United States'],
    [r'(elections, .+)',
        [('topic', 'Elections'), ('topic', '{}')],
        'Elections, 2016'],
    [r'transliteration into (.+)',
        [('topic', 'Transliteration'), ('topic', '{} language')],
        'Translisteration into English'],
]

# SAMPLE_TERM_MAP, like subjectmaps.LCSH_SUBDIVISION_TERM_MAP
SAMPLE_TERM_MAP = {
    'abandonment': {
        'parents': {
            'nests': [
                'Abandonment of nests',
            ],
        },
    },
    'absorption and adsorption': {
        'headings': [
            'Absorption',
            'Adsorption',
        ],
    },
    'certification': {
        'headings': [
            'Certification (Occupations)',
        ],
        'parents': {
            'seeds': [
                'Certification (Seeds)',
            ],
        },
    },
}


@pytest.mark.parametrize('pmap, tmap, dtype, sd_parents, sd, expected', [
    (SAMPLE_PATTERN_MAP, SAMPLE_TERM_MAP, 'topic', [],
        'Corn',
        [('topic', 'Corn')]),
    (SAMPLE_PATTERN_MAP, SAMPLE_TERM_MAP, 'topic', [],
        'Annexation to the United States',
        [('topic', 'Annexation (International law)'),
         ('region', 'United States')]),
    (SAMPLE_PATTERN_MAP, SAMPLE_TERM_MAP, 'topic', [],
        'Art and the war',
        [('topic', 'Art and war')]),
    (SAMPLE_PATTERN_MAP, SAMPLE_TERM_MAP, 'topic', [],
        'Dependency on Great Britain',
        [('topic', 'Dependency'),
         ('region', 'Great Britain')]),
    (SAMPLE_PATTERN_MAP, SAMPLE_TERM_MAP, 'topic', [],
        'Elections, 2016',
        [('topic', 'Elections'),
         ('topic', 'Elections, 2016')]),
    (SAMPLE_PATTERN_MAP, SAMPLE_TERM_MAP, 'topic', [],
        'Transliteration into English',
        [('topic', 'Transliteration'),
         ('topic', 'English language')]),
    (SAMPLE_PATTERN_MAP, SAMPLE_TERM_MAP, 'topic', [],
        'Abandonment',
        [('topic', 'Abandonment')]),
    (SAMPLE_PATTERN_MAP, SAMPLE_TERM_MAP, 'topic', ['Children'],
        'Abandonment',
        [('topic', 'Abandonment')]),
    (SAMPLE_PATTERN_MAP, SAMPLE_TERM_MAP, 'topic', ['Nests'],
        'Abandonment',
        [('topic', 'Abandonment of nests')]),
    (SAMPLE_PATTERN_MAP, SAMPLE_TERM_MAP, 'topic', ['Crows', 'Nests'],
        'Abandonment',
        [('topic', 'Abandonment of nests')]),
    (SAMPLE_PATTERN_MAP, SAMPLE_TERM_MAP, 'topic', [],
        'Absorption and adsorption',
        [('topic', 'Absorption'),
         ('topic', 'Adsorption')]),
    (SAMPLE_PATTERN_MAP, SAMPLE_TERM_MAP, 'topic', [],
        'Certification',
        [('topic', 'Certification (Occupations)')]),
    (SAMPLE_PATTERN_MAP, SAMPLE_TERM_MAP, 'topic', ['People'],
        'Certification',
        [('topic', 'Certification (Occupations)')]),
    (SAMPLE_PATTERN_MAP, SAMPLE_TERM_MAP, 'topic', ['Seeds'],
        'Certification',
        [('topic', 'Certification (Seeds)')]),
    (SAMPLE_PATTERN_MAP, SAMPLE_TERM_MAP, 'form', [],
        'Early works',
        [('form', 'Early works')]),

])
def test_lcshsdtofacetvalues_output(pmap, tmap, dtype, sd_parents, sd,
                                    expected):
    """
    Function `lcsh_sd_to_facet_values` should return the `expected`
    values given the parametrized inputs.
    """
    result = subjectmaps.lcsh_sd_to_facet_values(sd, sd_parents, dtype,
                                                 pmap, tmap)
    assert result == expected


def test_lcsh_subdivision_patterns():
    """
    This is a sanity check to make sure the actual live
    LCSH_SUBDIVISION_PATTERNS structure works and doesn't return errors
    when used with `lcsh_sd_to_facet_values`.
    """
    for pattern, headings, example in subjectmaps.LCSH_SUBDIVISION_PATTERNS:
        assert subjectmaps.lcsh_sd_to_facet_values(example) != [example]


def test_lcsh_subdivision_term_map():
    """
    This is a little-more-than-sanity check to make sure the actual
    live LCSH_SUBDIVISION_TERM_MAP structure works as expected when
    used with `lcsh_sd_to_facet_values`.
    """
    for sd, data in subjectmaps.LCSH_SUBDIVISION_TERM_MAP.items():
        if 'headings' in data:
            exp = [('topic', v) for v in data['headings']]
            assert subjectmaps.lcsh_sd_to_facet_values(sd) == exp
        if 'parents' in data:
            for parent, vals in data['parents'].items():
                exp = [('topic', v) for v in vals]
                assert subjectmaps.lcsh_sd_to_facet_values(sd, [parent]) == exp
