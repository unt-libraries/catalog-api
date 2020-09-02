# -*- coding: utf-8 -*- 

"""
Tests the blacklight.parsers functions.
"""
from __future__ import unicode_literals
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
    ('test data', 'test data'),
    ('test  data', 'test data'),
    ('test  data ', 'test data '),
    ('  test  data test', ' test data test'),
    (' test  data test  ', ' test data test '),
    (' test data  test  ', ' test data test '),
])
def test_normalize_whitespace(data, expected):
    """
    `normalize_whitespace` should consolidate whitespace throughout the
    input data string.
    """
    assert parsers.normalize_whitespace(data) == expected


@pytest.mark.parametrize('data, expected', [
    ('test : data', 'test: data'),
    ('test / data', 'test / data'),
    ('test : data / data', 'test: data / data'),
])
def test_compress_punctuation(data, expected):
    """
    `compress_punctuation` should remove whitespace to the immediate
    left of certain punctuation marks.
    """
    assert parsers.compress_punctuation(data) == expected


@pytest.mark.parametrize('data, expected', [
    ('test . ; : data / data', 'test : data / data'),
    ('test .;: data / data', 'test : data / data'),
    ('.:;test / data', 'test / data'),
    ('test / data.:;', 'test / data;'),
    ('.:;test / data. : ;', 'test / data;'),
    ('[ : test', '[test'),
    ('( : test', '(test'),
    ('{ : test', '{test'),
    ('test : [],', 'test,'),
    ('test : [.],', 'test,'),
    ('test ; [Test :]', 'test ; [Test]'),
    ('ed. : test', 'ed. : test'),
    ('ed. . test', 'ed. test'),
    ('ed.. test', 'ed. test'),
    ('ed. ... test', 'ed. ... test'),
    ('https://example.com', 'https://example.com')
])
def test_normalize_punctuation_periods_need_protection(data, expected):
    """
    `normalize_punctuation` should normalize internal and ending
    punctuation, resulting in the expected value. This set of tests
    assumes that periods need protection, i.e. `periods_protected` is
    False, which is the default.
    """
    assert parsers.normalize_punctuation(data, False) == expected


@pytest.mark.parametrize('data, expected', [
    ('ed. : test', 'ed: test'),
    ('ed. . test', 'ed. test'),
    ('ed.. test', 'ed. test'),
    ('ed. ... test', 'ed. test')
])
def test_normalize_punctuation_periods_do_not_need_protection(data, expected):
    """
    `normalize_punctuation` should normalize internal and ending
    punctuation, resulting in the expected value. This set of tests
    assumes that periods do not need protection, i.e.
    `periods_protected` is True.
    """
    assert parsers.normalize_punctuation(data, True) == expected


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
    ('Protect ... ellipses', 'Protect ... ellipses'),
    ('Protect .... ellipses', 'Protect ... ellipses'),
    ('Protect. ... Ellipses.', 'Protect ... Ellipses'),
    ('Protect ending period from initial J.', 'Protect ending period from initial J.'),
    ('Lowercase initials do not count, j.', 'Lowercase initials do not count, j'),
    ('Remove inner period. Dude', 'Remove inner period Dude'),
    ('Protect period inside a word, like 1.1', 'Protect period inside a word, like 1.1'),
    ('Protect inner period from numeric ordinal 1. Dude', 'Protect inner period from numeric ordinal 1. Dude'),
    ('Protect inner period from longer number, 1684. followed by lowercase letter', 'Protect inner period from longer number, 1684. followed by lowercase letter'),
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


@pytest.mark.parametrize('data, gr_on_mm, brackets, expected', [
    ('st', True, '()', ['s', 't']),
    ('(st)', True, '()', [['s', 't']]),
    ('((st))', True, '()', [[['s', 't']]]),
    ('st (st)', True, '()', ['s', 't', ' ', ['s', 't']]),
    ('(st) st', True, '()', [['s', 't'], ' ', 's', 't']),
    ('(st)(st)', True, '()', [['s', 't'], ['s', 't']]),
    ('(st) (st)', True, '()', [['s', 't'], ' ', ['s', 't']]),
    ('((st) (st))', True, '()', [[['s', 't'], ' ', ['s', 't']]]),
    (' (st) ', True, '()', [' ', ['s', 't'], ' ']),
    (' (st)) ', True, '()', [[' ', ['s', 't']], ' ']),
    (' (st)) ', False, '()', [' ', ['s', 't'], ')', ' ']),
    (' ((st) ', True, '()', [' ', [['s', 't'], ' ']]),
    (' ((st) ', False, '()', [' ', '(', ['s', 't'], ' ']),
    (')()(st)', True, '()', [[], [], ['s', 't']]),
    (')()(st)', False, '()', [')', [], ['s', 't']]),
    ('(st)(', True, '()', [['s', 't'], []]),
    ('(st)(', False, '()', [['s', 't'], '(']),
    ('[st]', True, '[]', [['s', 't']]),
])
def test_deconstruct_bracketed(data, gr_on_mm, brackets, expected):
    """
    `deconstruct_bracketed` should return the expected data structure,
    when passed the given `data`, `group_on_mismatch` value, and
    `openchar` and `closechar` values.
    """
    oc, cc = brackets
    assert parsers.deconstruct_bracketed(data, gr_on_mm, oc, cc) == expected


@pytest.mark.parametrize('data, brackets, stripchars, expected', [
    (['s', 't'], '()', '', 'st'),
    ([['s', 't']], '()', '', '(st)'),
    ([[['s', 't']]], '()', '', '((st))'),
    (['s', 't', ' ', ['s', 't']], '()', '', 'st (st)'),
    ([['s', 't'], ' ', 's', 't'], '()', '', '(st) st'),
    ([['s', 't'], ['s', 't']], '()', '', '(st)(st)'),
    ([['s', 't'], ' ', ['s', 't']], '()', '', '(st) (st)'),
    ([[['s', 't'], ' ', ['s', 't']]], '()', '', '((st) (st))'),
    ([' ', ['s', 't'], ' '], '()', '', ' (st) '),
    ([' ', ['s', 't'], ')', ' '], '()', '', ' (st)) '),
    ([' ', ['s', 't'], ')', ' '], '()', '()', ' (st) '),
    (['(', ['s', 't'], ' '], '()', '', '((st) '),
    (['(', ['s', 't'], ' '], '()', '()', '(st) '),
    ([['s', 't']], '[]', '', '[st]'),
    ([' ', ['s', 't'], ')', ' '], '[]', '()', ' [st] '),
])
def test_reconstruct_bracketed(data, brackets, stripchars, expected):
    """
    `reconstruct_bracketed` should return the expected string, when
    passed the given `data` structure, `openchar` and `closechar`
    values, and `stripchars` values.
    """
    oc, cc = brackets
    assert parsers.reconstruct_bracketed(data, oc, cc, stripchars) == expected


@pytest.mark.parametrize('data, expected', [
    ('do not strip inner whitespace', 'do not strip inner whitespace'),
    ('do not strip, inner punctuation', 'do not strip, inner punctuation'),
    (' strip whitespace at ends ', 'strip whitespace at ends'),
    ('strip one punctuation mark at end.', 'strip one punctuation mark at end'),
    ('strip repeated punctuation marks at end,,', 'strip repeated punctuation marks at end'),
    ('strip multiple different punctuation marks at end./', 'strip multiple different punctuation marks at end'),
    ('strip punctuation marks and whitespace at end . ;. / ', 'strip punctuation marks and whitespace at end'),
    (';strip one punctuation mark at beginning', 'strip one punctuation mark at beginning'),
    (';;;strip repeated punctuation marks at beginning', 'strip repeated punctuation marks at beginning'),
    ('./strip multiple different punctuation marks at beginning', 'strip multiple different punctuation marks at beginning'),
    ('. ;./ strip punctuation marks and whitespace at beginning', 'strip punctuation marks and whitespace at beginning'),
    (' . . . strip punctuation and whitespace from both ends . /; ', 'strip punctuation and whitespace from both ends'),
    ('(do not strip parentheses or punct inside parentheses...);', '(do not strip parentheses or punct inside parentheses...)'),
    ('do not strip ellipses ...', 'do not strip ellipses ...'),
    ('weirdness with,                             whitespace.', 'weirdness with,                             whitespace'),
    ('abbreviations not stripped A.A.', 'abbreviations not stripped A.A.'),
    ('abbreviations not stripped A.A. .;', 'abbreviations not stripped A.A.'),
])
def test_strip_ends_periods_need_protection(data, expected):
    """
    `strip_ends` should correctly strip whitespace and punctuation from
    both ends of the input data string. This set of tests assumes
    periods need to be protected, i.e. `periods_protected` is False,
    which is the default.
    """
    assert parsers.strip_ends(data, False) == expected


@pytest.mark.parametrize('data, expected', [
    ('abbreviations stripped A.A.', 'abbreviations stripped A.A'),
    ('abbreviations stripped A.A. .;', 'abbreviations stripped A.A'),
])
def test_strip_ends_periods_do_not_need_protection(data, expected):
    """
    `strip_ends` should correctly strip whitespace and punctuation from
    both ends of the input data string. This set of tests assumes
    periods do not need to be protected, i.e. `periods_protected` is
    True
    """
    assert parsers.strip_ends(data, True) == expected


@pytest.mark.parametrize('data, end, expected', [
    (' . . . test . /; ', 'left', 'test . /; '),
    (' . . . test . /; ', 'right', ' . . . test'),
])
def test_strip_ends_left_or_right(data, end, expected):
    """
    `strip_ends` should correctly strip whitespace and puncutation from
    one end or the other based on the value of `end`.
    """
    assert parsers.strip_ends(data, end=end) == expected


@pytest.mark.parametrize('data, strip_mismatched, expected', [
    ('st', True, 'st'),
    ('(st)', True, 'st'),
    ('((st))', True, 'st'),
    ('(st (st))', True, 'st (st)'),
    ('((st) (st))', True, '(st) (st)'),
    ('(st) (st)', True, '(st) (st)'),
    ('st (st) st', True, 'st (st) st'),
    ('st (st)', True, 'st (st)'),
    # Behavior for mismatched parentheses:
    ('(st', True, 'st'),
    ('((st', True, 'st'),
    ('((st)', True, 'st'),
    ('st)', True, 'st'),
    ('((st) (st)', True, '(st) (st)'),
    ('st (st st', True, 'st (st st'),
    ('(st', False, '(st'),
    ('((st', False, '((st'),
    ('((st)', False, '(st'),
    ('st)', False, 'st)'),
    ('(st))', False, 'st)'),
    ('((st) (st)', False, '((st) (st)'),
    ('st (st st', False, 'st (st st'),
    # A few weird cases that hopefully won't appear in the wild:
    (')()st', True, '()st'),
    (')()st', False, ')()st'),
    (')st', True, 'st'),
    (')st', False, ')st'),
    ('st()(', True, 'st()'),
    ('st()(', False, 'st()('),
    ('st(', True, 'st'),
    ('st(', False, 'st('),
    ('(st(', True, 'st'),
    ('(st(', False, '(st('),
    (')st(', True, 'st'),
    (')st(', False, ')st('),
])
def test_strip_outer_parentheses(data, strip_mismatched, expected):
    """
    `strip_outer_parentheses` should correctly remove the parentheses
    on the left/right of the given `data` string, matching the
    `expected` value.
    """
    assert parsers.strip_outer_parentheses(data, strip_mismatched) == expected


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
    ('Container of (work):', 'Container of:'),
    ('Container of (Work):', 'Container of:'),
    ('Container of (item):', 'Container of:'),
    ('Container of (expression):', 'Container of:'),
    ('Container of (manifestation):', 'Container of:'),
    ('Container of (salad dressing):', 'Container of (salad dressing):'),
    ('Container of:', 'Container of:'),
    ('composer (expression)', 'composer'),
    ('(Work) blah', ' blah'),
    ('Contains work:', 'Contains work:'),
])
def test_strip_wemi(data, expected):
    """
    `strip_wemi` should correctly strip any WEMI entities contained in
    parentheses in the given data string.
    """
    assert parsers.strip_wemi(data) == expected


@pytest.mark.parametrize('data, expected', [
    ('This is an example of a title : subtitle / ed. by John Doe.', 'This is an example of a title : subtitle / ed. by John Doe'),
    ('Some test data ... that we have (whatever [whatever]).', 'Some test data that we have (whatever whatever)'),
])
def test_clean(data, expected):
    """
    `clean` should strip ending punctuation, brackets, and ellipses and
    normalize whitespace and punctuation.
    """
    assert parsers.clean(data) == expected


@pytest.mark.parametrize('data, expected', [
    ('1980.', ('1980',)),
    ('c1980.', ('1980',)),
    ('[198-]', ('198u',)),
    ('1979 printing, c1975.', ('1979', '1975')),
    ('1968 [i.e. 1971] 1973 printing.', ('1968', '1971', '1973')),
    ('1898-1945', ('1898', '1945')),
    ('April 15, 1977.', ('1977',)),
    ('1878-[1927?]', ('1878', '1927')),
    ('1878-[1927?]', ('1878', '1927')),
    ('18--?-1890', ('18uu', '1890')),
    ('[197-]-1987', ('197u', '1987')),
    ('19th and early 20th century', ('18uu', '19uu')),
    ('[1980s?]', ('198u',)),
    ('1975, [1980s?], 1996, 21st century', ('1975', '198u', '1996', '20uu')),
    ('1 in 1975, 15 in the 18th Century, and 23 in the 1810s',
     ('1975', '17uu', '181u')),
    ('300 A.D.', ('0300',)),
    ('201[4]', ('2014',)),
    ('1st semester 1976.', ('1976',)),
    ('2nd semester 1976.', ('1976',)),
    ('3rd semester 1976.', ('1976',)),
    ('4th semester 1976.', ('1976',)),
])
def test_extract_years(data, expected):
    """
    `extract_years` should extract year-strings from the given data.
    """
    assert sorted(parsers.extract_years(data)) == sorted(expected)


@pytest.mark.parametrize('data, expected', [
    ('[S.l.] :', '[] :'),
    ('[s.n.]', '[]'),
    ('s.n.]', ']'),
    ('[s.n.', '['),
    ('s.n.', ''),
    ('[Place of publication not identified]', '[]'),
    ('[S.l. : s.n., 15--?]', '[: , 15--?]')
])
def test_strip_unknown_pub(data, expected):
    """
    `strip_unknown_pub` should strip, e.g., [S.l], [s.n.], and [X not
    identified] from the given publisher-related data.
    """
    assert parsers.strip_unknown_pub(data) == expected


@pytest.mark.parametrize('data, expected', [
    ('1984', ('1984', '')),
    ('1984, 2003', ('1984, 2003', '')),
    ('1984, c2003', ('1984,', 'c2003')),
    ('c2003', ('', 'c2003')),
    ('copyright 2003', ('', 'copyright 2003')),
    ('cop. 2003 by XYZ', ('', 'cop. 2003 by XYZ')),
    ('©2003', ('', '©2003')),
    ('© 2003', ('', '© 2003')),
    ('[2003]', ('[2003]', '')),
    ('1984 printing copyright 2003', ('1984 printing', 'copyright 2003')),
    ('1984, P 2003', ('1984,', 'P 2003')),
    ('℗2003', ('', '℗2003')),
])
def test_split_pdate_and_cdate(data, expected):
    """
    `split_pdate_and_cdate` should split a string (e.g. from a 26X$c)
    into the publication date and copyright date.
    """
    assert parsers.split_pdate_and_cdate(data) == expected


@pytest.mark.parametrize('data, expected', [
    ('c1984', '©1984'),
    ('copyright 1984', '©1984'),
    ('c 1984', '©1984'),
    ('(c) 1984', '©1984'),
    ('©1984', '©1984'),
    ('c1984 by XYZ', '©1984 by XYZ'),
    ('p1984', '℗1984'),
    ('phonogram 1984', '℗1984'),
    ('p 1984', '℗1984'),
    ('(p) 1984', '℗1984'),
    ('P 1984', '℗1984'),
    ('℗1984', '℗1984'),
    ('p1984, c1985', '℗1984, ©1985'),
    ('1984', '1984'),
    ('something else 1984', 'something else 1984'),
])
def test_normalize_cr_symbol(data, expected):
    """
    `normalize_cr_symbol` should replace the appropriate patterns in
    the given string with the appropriate copyright symbol.
    """
    assert parsers.normalize_cr_symbol(data) == expected


@pytest.mark.parametrize('data, first_indicator, exp_forename, exp_surname, exp_family_name', [
    ('Thomale, Jason,', '0', 'Jason', 'Thomale', None),
    ('Thomale, Jason,', '1', 'Jason', 'Thomale', None),
    ('Thomale, Jason,', '3', 'Jason', 'Thomale', None),
    ('John,', '0', 'John', None, None),
    ('John II Comnenus,', '0', 'John II Comnenus', None, None),
    ('Byron, George Gordon Byron,', '1', 'George Gordon Byron', 'Byron', None),
    ('Joannes Aegidius, Zamorensis,', '1', 'Zamorensis', 'Joannes Aegidius',
        None),
    ('Morton family.', '3', None, 'Morton', 'Morton family'),
    ('Morton family.', '2', None, 'Morton', 'Morton family'),
    ('Morton family.', '2', None, 'Morton', 'Morton family'),
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


@pytest.mark.parametrize('data, mn, mx, trunc_patterns, trunc_to_punct, exp', [
    ('abcd', 5, 5, None, True, 'abcd'),
    ('abcd', 1, 5, None, True, 'a'),
    ('', 1, 5, None, True, ''),
    ('', 0, 0, None, True, ''),
    ('word word: word. word. Word word.', 5, 25, None, True,
     'word word: word. word'),
    ('word word: word word. Word word.', 5, 25, None, False,
     'word word: word word.'),
    ('word word: word word. Word word.', 5, 20, None, True,
     'word word'),
    ('word word: word word. Word word.', 5, 20, None, False,
     'word word: word'),
    ('word word: word word. Word word.', 5, 30, (r':\s',), True,
     'word word'),
    ('word word word word. Word word.', 5, 30, (r':\s',), True,
     'word word word word'),
    ('word word word word; Word word.', 5, 30, (r':\s',), True,
     'word word word word'),
    ('word word word word. Word word.', 5, 30, (r':\s',), False,
     'word word word word. Word'),
    ('word word: word word. Word word.', 10, 30, (r':\s',), True,
     'word word'),
    ('word word: word word. Word word.', 11, 21, None, True,
     'word word: word'),
    ('word wo: rd: word word. Word word.', 5, 30, (r':\s',), True,
     'word wo: rd'),
])
def test_truncator_truncate(data, mn, mx, trunc_patterns, trunc_to_punct, exp):
    """
    `Truncator.truncate`, when the Truncate obj is instantiated with
    the given `trunc_patterns` and `trunc_to_punct` args, should return
    the `expected` result, when passed the given `mn` and `mx` args. 
    """
    truncator = parsers.Truncator(trunc_patterns, trunc_to_punct)
    assert truncator.truncate(data, mn, mx) == exp


@pytest.mark.parametrize('data, expected', [
    ('Isidore of Seville ; translated by Helen Dill Goode and Gertrude C. '
     'Drake.',
     [['Isidore', 'Seville'], ['Helen', 'Dill', 'Goode'],
      ['Gertrude', 'C', 'Drake']]),
    ('Shakespeare ; traductions de Yves Bonnefoy, Armand Robin, et Pierre '
     'Jean Jouve',
     [['Shakespeare'], ['Yves', 'Bonnefoy'], ['Armand', 'Robin'],
      ['Pierre', 'Jean', 'Jouve']]),
    ('[music by] Rodgers & [words by] Hammerstein',
     [['Rodgers'], ['Hammerstein']]),
    ('Béla Bartók ; arranged for junior string orchestra by Gábor Darvas',
     [['Béla', 'Bartók'], ['Gábor', 'Darvas']]),
    ('J.S. Bach', [['J', 'S', 'Bach']]),
    ('JS Bach', [['J', 'S', 'Bach']]),
    ('U.S. Navy\'s Military Sealift Command',
     [['U', 'S', 'Navy\'s', 'Military', 'Sealift', 'Command']])
])
def test_findnamesinstring(data, expected):
    """
    The `find_names_in_string` function should return the expected
    names.
    """
    assert parsers.find_names_in_string(data) == expected


@pytest.mark.parametrize('sor, heading, only_first, expected', [
    ('Isidore of Seville ; translated by Helen Dill Goode and Gertrude C. '
     'Drake.', 'Isidore of Seville', True, True),
    ('Isidore of Seville ; translated by Helen Dill Goode and Gertrude C. '
     'Drake.', 'Goode, Helen Dill', False, True),
    ('Isidore of Seville ; translated by Helen Dill Goode and Gertrude C. '
     'Drake.', 'Goode, Helen Dill', True, False),
])
def test_sormatchesnameheading(sor, heading, only_first, expected):
    """
    The `sor_matches_name_heading` function should return the expected
    value given the `sor`, `heading`, and `only_first` input values.
    """
    result = parsers.sor_matches_name_heading(sor, heading, only_first)
    assert result == expected


@pytest.mark.parametrize('callnum, expected', [
    ('MT100', ['MT', 'MT100']),
    ('MT 100', ['MT', 'MT 100']),
    ('MT.100', ['MT', 'MT.100']),
    ('MT100.1 .A55 1999', ['MT', 'MT100', 'MT100.1', 'MT100.1 .A',
                           'MT100.1 .A55', 'MT100.1 .A55 1999']),
    ('MT100.A5.O32', ['MT', 'MT100', 'MT100.A', 'MT100.A5', 'MT100.A5.O',
                      'MT100.A5.O32']),
])
def test_shinglecallnum(callnum, expected):
    """
    The `shingle_callnum` function should return the expected list of
    shingles given the `callnum` value.
    """
    assert parsers.shingle_callnum(callnum) == expected
