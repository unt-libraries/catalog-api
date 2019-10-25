"""
Contains Sierra field data parsing functions.
"""

from __future__ import unicode_literals
import logging
import re
import collections

from django.conf import settings

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


# CONDITIONALS
# The parsers in this section perform some kind of test and return a
# boolean True/False.

def has_comma_in_middle(data):
    """
    True if the given string has a comma in the middle of two words.
    Splits the data by comma and tests to make sure at least two parts
    have non-space data.
    """
    parts = data.split(',')
    return len([p for p in parts if p.strip()]) > 1


# DATA UTILITIES
# In this section are parsers used for cleaning up data.

def normalize_whitespace(data):
    """
    Normalize whitespace in the input data string.
    Removes multiple consecutive spaces. Strips whitespace from the
    beginning and end of the provided string.
    """
    consolidated = re.sub(r'\s+', ' ', data)
    return consolidated.strip()


def normalize_punctuation(data, left_space_re=settings.MARCDATA.NO_LEFT_WHITESPACE_PUNCTUATION_REGEX,
                          punctuation_re=settings.MARCDATA.ENDING_PUNCTUATION_REGEX):
    """
    Normalize punctuation in the input data string.
    Normalization entails removing whitespace to the immediate left of
    certain punctuation marks and removing instances of consecutive
    punctuation marks, which can sometimes result from parsing.
    Because normalization may remove certain punctuation marks, it's
    important this is the last step you take when you're done parsing
    field data. This parsers is simply to make the data look nicer.
    """
    extra_whitespace_removed = re.sub(r'\s+({})'.format(left_space_re), r'\1', data)
    multiples_removed = re.sub(r'(\s*{0})+(\s*{0})'.format(punctuation_re), r'\2', extra_whitespace_removed)
    return multiples_removed


def strip_brackets(data, keep_inner=True, to_keep_re=None,
                   to_remove_re=settings.MARCDATA.BRACKET_DATA_REMOVE_REGEX,
                   protect_re=settings.MARCDATA.BRACKET_DATA_PROTECT_REGEX):
    """
    Strip square brackets from the input data string.
    If `keep_inner` is True (default), then the brackets themselves
    are removed but the data inside is retained. If False, then the
    brackets and inner data are both removed.
    `to_keep_re` will explicitly keep data matching the given
    regex string, but strip brackets. (Only used if `keep_inner` is
    False.)
    `to_remove_re` will explicitly remove data matching the given
    regex string, found in brackets, along with the brackets. (Only
    used if `keep_inner` is True.)
    `protect_re` is a regex matching any bracketed data where the data
    *and* the surrounding brackets hould be kept.
    """
    to_protect = r'\[({})\]'.format(protect_re) if protect_re else '^$'
    to_remove = r'(^|\s*)\[({})\]'.format(to_remove_re) if to_remove_re and keep_inner else '^$'
    to_keep = r'\[({})\]'.format(to_keep_re) if to_keep_re and not keep_inner else '^$'
    brackets = r'[\[\]]' if keep_inner else r'(^|\s*)\[[^\]]*\]'

    brackets_protected = re.sub(to_protect, r'{\1}', data)
    certain_data_removed = re.sub(to_remove, '', brackets_protected)
    certain_data_kept = re.sub(to_keep, r'\1', certain_data_removed)
    brackets_removed = re.sub(brackets, '', certain_data_kept)
    protected_brackets_restored = re.sub(r'\{([^\}]*)\}', r'[\1]', brackets_removed)

    return protected_brackets_restored.lstrip()


def protect_periods_and_do(data, do, repl_char='~',
                           abbreviations_re=settings.MARCDATA.ABBREVIATIONS_REGEX):
    """
    Do something to an input data string, but protect certain periods.
    Periods in MARC data are often used to indicate structure, such as
    the end of the Name portion of a Name/Title heading. But sometimes
    periods are non-structural--used for abbreviations, initials, and
    certain kinds of numbering, e.g.: 1. First thing, 2. Second thing.
    Often we want to use structural periods while parsing something and
    then strip them, while ignoring and retaining non-structural
    periods.
    This parser will do the `do` function on the `data` string, but it
    will protect non-structural periods beforehand and restore them
    afterward. Returns the results.
    `repl_char` is the character you want to temporarily replace
    non-structural periods with. It should be something that will not
    occur otherwise in your data. ~ is the default.
    `abbreviations_re` is a regular-expression to use for recognizing
    abbreviations in your data. Typically it's a large regex group of
    abbreviations separated by r'|'. Any periods following these
    matches will be converted to the `repl_char`.
    """
    initials = r'([A-Z])'
    ordinal_period_numeric = r'(\d{1,3})'
    ordinal_period_alphabetic = r'(\d+[A-Za-z]+)'
    roman_numerals = r'({})'.format(settings.MARCDATA.ROMAN_NUMERAL_REGEX)
    protect_all = r'\b(({}|{}|{})(?=\.\W)|({}|{}))\.'.format(ordinal_period_numeric, ordinal_period_alphabetic,
                                                             roman_numerals, initials, abbreviations_re)
    periods_in_words_protected = re.sub(r'\.(\w)', r'{}\1'.format(repl_char), data)
    all_protected = re.sub(protect_all, r'\1{}'.format(repl_char), periods_in_words_protected)
    processed_data = do(all_protected)
    periods_restored = re.sub(repl_char, r'.', processed_data)
    return periods_restored


def strip_ends(data, end_punctuation_re=settings.MARCDATA.ENDING_PUNCTUATION_REGEX):
    """
    Strip unnecessary punctuation from both ends of the input string.
    Strips whitespace. Strips parentheses, if the full string is
    enclosed. Otherwise, it does not. Strips ending punctuation,
    including [.,;:/\]. Retains periods if they belong to an
    abbreviation.
    """
    def strip_punctuation(data):
        parens_stripped = re.sub(r'^\((.*)\)\s*{}*$'.format(end_punctuation_re), r'\1', data)
        return re.sub(r'\s*{}*$'.format(end_punctuation_re), '', parens_stripped).strip()
    return protect_periods_and_do(data.strip(), strip_punctuation)


def strip_ellipses(data):
    """
    Strip ellipses (...) from the input string.
    """
    return re.sub(r'(^\.{3}|\s*(?<=[^\.])\.{3})\s*(\.?)', r'\2 ', data).strip()


def clean(data):
    """
    Perform common clean-up operations on a string of MARC field data.
    Strips ending punctuation, brackets, and ellipses and normalizes
    whitespace and punctuation.
    """
    whitespace_normalized = normalize_whitespace(data)
    ends_stripped = strip_ends(whitespace_normalized)
    brackets_stripped = strip_brackets(ends_stripped)
    ellipses_stripped = strip_ellipses(brackets_stripped)
    cleaned = normalize_punctuation(ellipses_stripped)
    return cleaned


def person_name(data, indicators):
    """
    Parse a personal name into forename, surname, and family name.
    """
    (forename, surname, family_name) = ('', '', '')
    if has_comma_in_middle(data):
        (surname, forename) = re.split(r',\s*', data, 1)
    else:
        is_forename = True if indicators[0] == '0' else False
        is_surname = True if indicators[0] == '1' else False
        is_family_name = True if indicators[0] == '3' else False
        if is_forename:
            forename = data
        else:
            surname = data
            if is_family_name or re.search(r'\s*family\b', surname, flags=re.I):
                family_name = surname
                surname = re.sub(r'\s*family\b', '', surname, flags=re.I)
    return {'forename': strip_ends(forename),
            'surname': strip_ends(surname),
            'family_name': strip_ends(family_name)}
