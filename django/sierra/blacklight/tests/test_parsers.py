"""
Tests the blacklight.parsers functions.
"""

import re

import pytest

from blacklight import parsers


# FIXTURES AND TEST DATA

@pytest.fixture
def pp_do():
    """
    Pytest fixture that returns a sample `do` function.
    """
    def _do(data):
        return re.sub(r'\.', '', data)
    return _do



# TESTS

@pytest.mark.parametrize('data, expected', [
    ('First, Last', True),
    ('First,Last', True),
    ('First,Last,', True),
    ('First, Last, Something Else', True),
    (', Last, First', True),
    ('First,', False),
    ('First', False),
    ('First Last', False),
    ('First, ', False),
    (', Last', False),
    (',Last', False),
])
def test_has_comma_in_middle(data, expected):
    """
    `has_comma_in_middle` should return True if the given string has a
    comma separating two or more words.
    """
    assert parsers.has_comma_in_middle(data) == expected


@pytest.mark.parametrize('data, expected', [
    (' test data', 'test data'),
    ('test data ', 'test data'),
    ('test  data', 'test data'),
    (' test  data ', 'test data'),
    (' test  data test', 'test data test'),
    (' test  data test  ', 'test data test'),
    (' test data  test  ', 'test data test'),
])
def test_normalize_whitespace(data, expected):
    """
    `normalize_whitespace` should strip whitespace from the beginning
    and end of a string AND consolidate internal spacing.
    """
    assert parsers.normalize_whitespace(data) == expected


@pytest.mark.parametrize('data, expected', [
    ('test : data', 'test: data'),
    ('test / data', 'test / data'),
    ('test : data / data', 'test: data / data'),
    ('test . ; : data / data', 'test: data / data'),
    ('test .;: data / data', 'test: data / data'),
    ('test . ; : / data / data', 'test / data / data'),
    ('.:;test / data', ';test / data'),
    ('test / data.:;', 'test / data;'),
])
def test_normalize_punctuation(data, expected):
    """
    `normalize_punctuation` should remove whitespace to the immediate
    left of certain punctuation marks and instances of consecutive
    punctuation marks.
    """
    assert parsers.normalize_punctuation(data) == expected


@pytest.mark.parametrize('data, keep_inner, to_keep_re, to_remove_re, to_protect_re, expected', [
    ('Test data', True, None, None, None, 'Test data'),
    ('Test data [inner]', True, None, None, None, 'Test data inner'),
    ('Test data-[inner]', True, None, None, None, 'Test data-inner'),
    ('Test data[inner]', True, None, None, None, 'Test datainner'),
    ('Test [inner] data', True, None, None, None, 'Test inner data'),
    ('[Inner] test data', True, None, None, None, 'Inner test data'),
    ('[First] test [Middle] data [Last]', True, None, None, None, 'First test Middle data Last'),
    ('Test data', True, None, r'inner', None, 'Test data'),
    ('Test data [inner]', True, None, r'inner', None, 'Test data'),
    ('Test data-[inner]', True, None, r'inner', None, 'Test data-'),
    ('Test data[inner]', True, None, r'inner', None, 'Test data'),
    ('Test [inner] data', True, None, r'inner', None, 'Test data'),
    ('[Inner] test data', True, None, r'Inner', None, 'test data'),
    ('[First] test [Middle] data [Last]', True, None, r'(Middle|Last)', None, 'First test data'),
    ('[First] test [Middle] [Middle] data [Last]', True, None, r'(Middle|Last)', None, 'First test data'),
    ('Test data', False, None, None, None, 'Test data'),
    ('Test data [inner]', False, None, None, None, 'Test data'),
    ('Test data-[inner]', False, None, None, None, 'Test data-'),
    ('Test data[inner]', False, None, None, None, 'Test data'),
    ('Test [inner] data', False, None, None, None, 'Test data'),
    ('[Inner] test data', False, None, None, None, 'test data'),
    ('[First] test [Middle] data [Last]', False, None, None, None, 'test data'),
    ('[First] test [Middle] [Middle] data [Last]', False, None, None, None, 'test data'),
    ('Test data', False, r'inner', None, None, 'Test data'),
    ('Test data [inner]', False, r'inner', None, None, 'Test data inner'),
    ('Test data-[inner]', False, r'inner', None, None, 'Test data-inner'),
    ('Test data[inner]', False, r'inner', None, None, 'Test datainner'),
    ('Test [inner] data', False, r'inner', None, None, 'Test inner data'),
    ('[Inner] test data', False, r'Inner', None, None, 'Inner test data'),
    ('[First] test [Middle] data [Last]', False, r'(Middle|Last)', None, None, 'test Middle data Last'),
    ('[First] test [Middle] [Middle] data [Last]', False, r'(Middle|Last)', None, None, 'test Middle Middle data Last'),
    ('Test data', True, None, None, r'inner', 'Test data'),
    ('Test data [inner]', True, None, None, r'inner', 'Test data [inner]'),
    ('Test data-[inner]', True, None, None, r'inner', 'Test data-[inner]'),
    ('Test data[inner]', True, None, None, r'inner', 'Test data[inner]'),
    ('Test [inner] data', True, None, None, r'inner', 'Test [inner] data'),
    ('[Inner] test data', True, None, None, r'Inner', '[Inner] test data'),
    ('[First] test [Middle] data [Last]', True, None, None, r'(Middle|Last)', 'First test [Middle] data [Last]'),
    ('[First] test [Middle] [Middle] data [Last]', True, None, None, r'(Middle|Last)', 'First test [Middle] [Middle] data [Last]'),
])
def test_strip_brackets(data, keep_inner, to_keep_re, to_remove_re, to_protect_re, expected):
    """
    `strip_brackets` should correctly strip square brackets based on
    the provided keep/remove/protect arguments.
    """
    assert parsers.strip_brackets(data, keep_inner, to_keep_re, to_remove_re, to_protect_re) == expected


@pytest.mark.parametrize('data, expected', [
    ('No periods, no changes', 'No periods, no changes'),
    ('Remove ending period.', 'Remove ending period'),
    ('Remove ending period from numeric ordinal 1.', 'Remove ending period from numeric ordinal 1'),
    ('Remove ending period from alphabetic ordinal 21st.', 'Remove ending period from alphabetic ordinal 21st'),
    ('Remove ending period from Roman Numeral XII.', 'Remove ending period from Roman Numeral XII'),
    ('Protect ending period from abbreviation eds.', 'Protect ending period from abbreviation eds.'),
    ('Protect ending period from initial J.', 'Protect ending period from initial J.'),
    ('Lowercase initials do not count, j.', 'Lowercase initials do not count, j'),
    ('Remove inner period. Dude', 'Remove inner period Dude'),
    ('Protect period inside a word, like 1.1', 'Protect period inside a word, like 1.1'),
    ('Protect inner period from numeric ordinal 1. Dude', 'Protect inner period from numeric ordinal 1. Dude'),
    ('Protect inner period from alphabetic ordinal 21st. Dude', 'Protect inner period from alphabetic ordinal 21st. Dude'),
    ('Protect inner period from Roman Numeral XII. Dude', 'Protect inner period from Roman Numeral XII. Dude'),
    ('Protect inner period from abbreviation eds. Dude', 'Protect inner period from abbreviation eds. Dude'),
    ('Protect inner period from inital J. Dude', 'Protect inner period from inital J. Dude'),
    ('J.R.R. Tolkien', 'J.R.R. Tolkien'),
    ('Tolkien, J.R.R.', 'Tolkien, J.R.R.'),
    ('Tolkien, J.R.R..', 'Tolkien, J.R.R.'),
])
def test_protect_periods_and_do(data, expected, pp_do):
    """
    `protect_periods_and_do` should perform the supplied `do` function
    but protect "structural" periods first. The idea is that the
    supplied function parses the supplied data based on structural
    periods, so non-structural periods are converted to a different
    character before the `do` function runs.
    In this case, the pp_do fixture strips all periods. So protected
    (i.e. non-structural) periods should not be stripped.
    """
    assert parsers.protect_periods_and_do(data, pp_do) == expected


@pytest.mark.parametrize('data, expected', [
    ('do not strip inner whitespace', 'do not strip inner whitespace'),
    ('do not strip, inner punctuation', 'do not strip, inner punctuation'),
    (' strip whitespace ', 'strip whitespace'),
    ('strip one punctuation mark at end.', 'strip one punctuation mark at end'),
    ('strip repeated punctuation marks at end...', 'strip repeated punctuation marks at end'),
    ('strip multiple different punctuation marks at end./', 'strip multiple different punctuation marks at end'),
    ('strip whitespace then punctuation :', 'strip whitespace then punctuation'),
    ('strip punctuation then whitespace. ', 'strip punctuation then whitespace'),
    ('strip w then p then w : ', 'strip w then p then w'),
    ('(strip full parens)', 'strip full parens'),
    ('(strip full parens with punctuation after).', 'strip full parens with punctuation after'),
    ('(strip full parens with punctuation before.)', 'strip full parens with punctuation before'),
    ('(strip full parens with punctuation before and after.) :', 'strip full parens with punctuation before and after'),
    ('do not strip (partial parens)', 'do not strip (partial parens)'),
    ('do not strip (partial parens).', 'do not strip (partial parens)'),
    ('do not strip (partial parens) :', 'do not strip (partial parens)'),
])
def test_strip_ends(data, expected):
    """
    `strip_ends` should correctly strip whitespace and punctuation from
    both ends of the input data string.
    """
    assert parsers.strip_ends(data) == expected


@pytest.mark.parametrize('data, expected', [
    ('...', ''),
    ('something', 'something'),
    ('something.', 'something.'),
    ('something..', 'something..'),
    ('A big ... something', 'A big something'),
    ('A big... something', 'A big something'),
    ('A big...something', 'A big something'),
    ('A big ...something', 'A big something'),
    ('A big something. ...', 'A big something.'),
    ('A big something ... .', 'A big something.'),
    ('A big something ....', 'A big something.'),
    (' ... something', 'something'),
    ('... something', 'something'),
    ('...something', 'something'),
    ('A big ... something...', 'A big something'),
])
def test_strip_ellipses(data, expected):
    """
    `strip ellipses` should correctly strip ellipses (...) from the
    input data string.
    """
    assert parsers.strip_ellipses(data) == expected


@pytest.mark.parametrize('data, expected', [
    ('This is an example of a title : subtitle / ed. by John Doe.', 'This is an example of a title: subtitle / ed. by John Doe'),
    ('Some test data ... that we have (whatever [whatever]).', 'Some test data that we have (whatever whatever)'),
])
def test_clean(data, expected):
    """
    `clean` should strip ending punctuation, brackets, and ellipses and
    normalize whitespace and punctuation.
    """
    assert parsers.clean(data) == expected


@pytest.mark.parametrize('data, first_indicator, exp_forename, exp_surname, exp_family_name', [
    ('Thomale, Jason,', '0', 'Jason', 'Thomale', ''),
    ('Thomale, Jason,', '1', 'Jason', 'Thomale', ''),
    ('Thomale, Jason,', '3', 'Jason', 'Thomale', ''),
    ('John,', '0', 'John', '', ''),
    ('John II Comnenus,', '0', 'John II Comnenus', '', ''),
    ('Byron, George Gordon Byron,', '1', 'George Gordon Byron', 'Byron', ''),
    ('Joannes Aegidius, Zamorensis,', '1', 'Zamorensis', 'Joannes Aegidius', ''),
    ('Morton family.', '3', '', 'Morton', 'Morton family'),
    ('Morton family.', '2', '', 'Morton', 'Morton family'),
    ('Morton family.', '2', '', 'Morton', 'Morton family'),
])
def test_person_name(data, first_indicator, exp_forename, exp_surname, exp_family_name):
    """
    `person_name` should parse a personal name into the expected
    forename, surname, and family name.
    """
    name = parsers.person_name(data, first_indicator)
    assert name['forename'] == exp_forename
    assert name['surname'] == exp_surname
    assert name['family_name'] == exp_family_name
