# -*- coding: utf-8 -*- 

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
    Consolidates multiple consecutive spaces into one throughout the
    input string (and at both ends). DOES NOT strip ending whitespace
    (just use the `strip` string method for that).
    """
    return re.sub(r'\s+', ' ', data)


def compress_punctuation(data, left_space_re=settings.MARCDATA.NO_LEFT_WHITESPACE_PUNCTUATION_REGEX,
                         punctuation_re=settings.MARCDATA.ENDING_PUNCTUATION_REGEX):
    """
    Compress punctuation in the input data string.
    Compression entails removing whitespace to the immediate left of
    ending punctuation marks. (Purely cosmetic.)
    """
    return re.sub(r'\s+({})'.format(left_space_re), r'\1', data)


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


def deconstruct_bracketed(data, group_on_mismatch=True, openchar='(',
                          closechar=')'):
    """
    Deconstruct a string, grouping bracketed data into nested lists.

    Use this to parse parenthesized or bracketed data within the given
    `data` string. It converts the string to nested lists based on
    brackets defined by the given `openchar` and `closechar` -- which
    are open/closed parentheses by default but can be any two different
    characters.

    E.g., the string 'st (st) (st (st)) ' returns:
    ['s', 't', ' ' ['s', 't'], ' ' ['s', 't', ' ', ['s', 't']], ' ']

    This function always returns a list. You can use the function
    `reconstruct_bracketed` to reconstruct the string from that
    list.

    MISMATCHED BRACKETS
    -------------------
    In any given string you may have brackets that are mismatched,
    where you have too many opening or too many closing brackets. In
    such cases you may want to handle these differently depending on
    your ultimate goal. You may control this via the kwarg
    `group_on_mismatch`. If True, then it uses a reasonable grouping to
    resolve the mismatch. If False, it puts any mismatched bracket
    characters into the list at the appropiate points as literal
    characters.

    E.g., for the string '((st)', if `group_on_mismatch` is True:
    [[['s', 't']]]
    Otherwise:
    ['(', ['s', 't']]

    When `group_on_mismatch` is True, it will always ADD a group (as
    though the input data were missing a bracket) rather than removing
    a group (as though the input data had an extra bracket). Groups
    are always added at the beginning or end of the data, depending on
    which bracket is mismatched. E.g., 'st (st st)) st' is assumed to
    group as '(st (st st)) st' and not 'st ((st st)) st'. Likewise,
    'st ((st st) st' is assumed to group as '((st st) st)' and not
    '((st st)) st'.

    When `group_on_mismatch` is False, the outermost brackets are the
    ones that convert to literal bracket characters. E.g., '(st))'
    becomes [['s', 't'], ')'] and not [['s', 't', ')']].
    """
    stack, working = [], []
    for char in data:
        if char == openchar:
            stack.append(working)
            working = []
        elif char == closechar:
            if len(stack):
                working = stack.pop() + [working]
            else:
                # We only get here if there are extra closing brackets
                working = [working] if group_on_mismatch else working + [char]
        else:
            working.append(char)
    while(stack):
        # We only get here if there are extra opening brackets
        fixed = [working] if group_on_mismatch else [openchar] + working
        working = stack.pop() + fixed
    return working


def reconstruct_bracketed(data, openchar='(', closechar=')', stripchars=''):
    """
    Reconstruct a string from the output of `deconstruct_bracketed`.

    Once you've deconstructed a string that may contain brackets, you
    can use this to rebuild the string, with bracket characters in the
    correct positions.

    Optionally, provide `stripchars`, a string of characters you want
    stripped from the final output BEFORE the correct bracket
    characters get added. The main use is to help you strip any
    mismatched brackets where you had set `group_on_mismatch` to False
    when you ran `deconstruct_bracketed`. For instance, if you want to
    strip the mismatched parentheses from '(st) st))', you can do:
    reconstruct_bracketed(deconstruct_bracketed('(st) st))', False),
                          stripchars='()')
    and you'd get: '(st) st'
    """
    strings = []
    for group in data:
        if isinstance(group, list):
            st = reconstruct_bracketed(group, openchar, closechar, stripchars)
            strings.append('{}{}{}'.format(openchar, st, closechar))
        else:
            if not group in stripchars:
                strings.append(group)
    return ''.join(strings)


def protect_periods(data, repl_char='~',
                    abbreviations_re=settings.MARCDATA.ABBREVIATIONS_REGEX):
    """
    Periods in MARC data are often used to indicate structure, such as
    the end of the Name portion of a Name/Title heading. But sometimes
    periods are non-structural--used for abbreviations, initials, and
    certain kinds of numbering, e.g.: 1. First thing, 2. Second thing.
    Often we want to use structural periods while parsing something,
    while ignoring and retaining non-structural periods.

    This parser protects non-structural periods for you in the provided
    `data` string.

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
    ordinal_period_long_number = r'(\d+)(?=\.\W*[a-z])'
    roman_numerals = r'({})'.format(settings.MARCDATA.ROMAN_NUMERAL_REGEX)
    protect_all = r'\b(({}|{}|{})(?=\.\W)|{}|({}|{}))\.'.format(ordinal_period_numeric, ordinal_period_alphabetic,
                                                                roman_numerals, ordinal_period_long_number, initials,
                                                                abbreviations_re)
    periods_in_words_protected = re.sub(r'\.(\w)', r'{}\1'.format(repl_char), data)
    ellipses_protected = re.sub(r'\.{3}', r'{0}{0}{0}'.format(repl_char), periods_in_words_protected)
    return re.sub(protect_all, r'\1{}'.format(repl_char), ellipses_protected)


def restore_periods(data, repl_char='~'):
    """
    Restore periods in data processed with `protect_periods`. Be sure
    `repl_char` is the same as what was used when calling
    `protect_periods`.
    """
    return re.sub(repl_char, r'.', data)


def protect_periods_and_do(data, do, repl_char='~',
                           abbreviations_re=settings.MARCDATA.ABBREVIATIONS_REGEX):
    """
    Do something to an input data string, but protect certain periods
    first via `protect_periods`.
    
    This parser will do the `do` function on the `data` string, but it
    will protect non-structural periods beforehand and restore them for
    you afterward. (The `do` function must return a string. If you need
    to do anything more complex, call `protect_periods` first yourself
    and `restore_periods` afterward.)
    """
    protected = protect_periods(data, repl_char, abbreviations_re)
    processed_data = do(protected)
    return restore_periods(processed_data, repl_char)


def normalize_punctuation(data, periods_protected=False, repl_char='~',
                          punctuation_re=settings.MARCDATA.ENDING_PUNCTUATION_REGEX):
    """
    Normalize punctuation in the input `data` string.
    Normalization entails removing multiple ending punctuation marks
    in a row (perhaps separated by whitespace) and stripping
    punctuation from the beginning of the string or the beginning of an
    opening bracket (parenthetical, square, or curly).

    You'll want periods to be protected (via `protect_periods_and_do`)
    when the normalization runs; to prevent unnecessary overhead from
    protecting periods multiple times on the same data, you can include
    a call to this function within a `do` function, in which case it
    will run after periods have already been protected. Set
    `periods_protected` to True when used in this context.
    """
    def _normalize(data):
        bracket_front_punct_removed = re.sub(r'([\[\{{\(])(\s*{}\s*)+'.format(punctuation_re), r'\1', data)
        bracket_end_punct_removed = re.sub(r'(\s*{}\s*)+([\]\}}\)])'.format(punctuation_re), r'\2', bracket_front_punct_removed)
        empty_brackets_removed = re.sub(r'(\[\s*\]|\(\s*\)|\{\s*\})', r'', bracket_end_punct_removed)
        multiples_removed = re.sub(r'(\s?)(\s*{0})+\s*({0})(\s|$)'.format(punctuation_re), r'\1\3\4', empty_brackets_removed)
        periods_after_abbrevs_removed = re.sub(r'{}(\s*\.)(\s*[^.]|$)'.format(repl_char), r'{}\2'.format(repl_char), multiples_removed)
        front_punct_removed = re.sub(r'^(\s*{}\s*)+'.format(punctuation_re), r'', periods_after_abbrevs_removed)
        return front_punct_removed

    if periods_protected:
        normalized = _normalize(data.strip())
    else:
        normalized = protect_periods_and_do(data.strip(), _normalize, repl_char)
    return compress_punctuation(normalized, left_space_re=r'\.(?!\.\.)|,')


def strip_ends(data, periods_protected=False, end='both',
               end_punctuation_re=settings.MARCDATA.ENDING_PUNCTUATION_REGEX):
    """
    Strip unnecessary punctuation/whitespace from either or both ends
    of the input string (`data`). Retains periods if they belong to an
    abbreviation.

    You'll want periods to be protected (via `protect_periods_and_do`)
    when the strip function runs; to prevent unnecessary overhead from
    protecting periods multiple times on the same data, you can include
    a call to this function within a `do` function, in which case it
    it will run after periods have already been protected. Set
    `periods_protected` to True when used in this context.

    Use kwarg `end` to specify if you only want to strip from the left
    or right side. Default is `both`.
    """
    def strip_punctuation(data):
        if end in ('both', 'left'):
            data = re.sub(r'^({0}|\s)*(.+?)'.format(end_punctuation_re), r'\2', data)
        if end in ('both', 'right'):
            data = re.sub(r'(.+?)({0}|\s)*$'.format(end_punctuation_re), r'\1', data)
        return data

    if periods_protected:
        return strip_punctuation(data)
    return protect_periods_and_do(data, strip_punctuation)


def strip_outer_parentheses(data, strip_mismatched=True):
    """
    Remove sets of parentheses that enclose the given data string:
    (string) => string
    ((string)) => string
    (string (string)) => string (string)
    BUT:
    (string) (string) => (string) (string)
    string (string) string => string (string) string
    string (string) => string (string)

    In cases where you have mismatched parentheses at the start or end
    of a string, you may or may not want to try to strip them, so use
    `strip_mismatched` to control this. Note that mismatched
    parentheses are often ambiguous, so output may not be exactly what
    you'd expect.

    If strip_mismatched is True:
    (string => string
    ((string => string
    ((string) => string
    string) => string
    ((string) (string) => (string) (string)
    BUT:
    string (string string => string (string string

    If strip_mismatched is False:
    (string => (string
    ((string => ((string
    ((string) => (string
    string) => string)
    (string)) => string)
    ((string) (string) => ((string) (string)
    """
    def _collect_mismatched(struct, is_left_side):
        stack = []
        i = 0 if is_left_side else -1
        while len(struct) and (struct[i] == '(' or struct[i] == ')'):
            stack.append(struct[i])
            struct = struct[1:] if is_left_side else struct[:-1]
        return stack, struct

    def _unnest(struct):
        while len(struct) == 1 and isinstance(struct[0], list):
            struct = struct[0]
        return struct

    def _reattach_mismatched(struct, stack, is_left_side):
        while len(stack):
            if is_left_side:
                struct.insert(0, stack.pop())
            else:
                struct.append(stack.pop())
        return struct

    dc = deconstruct_bracketed(data, group_on_mismatch=False)
    left_stack, dc = _collect_mismatched(dc, True)
    right_stack, dc = _collect_mismatched(dc, False)
    dc = _unnest(dc)
    if not strip_mismatched:
        dc = _reattach_mismatched(dc, left_stack, True)
        dc = _reattach_mismatched(dc, right_stack, False)
    return reconstruct_bracketed(dc)


def strip_ellipses(data):
    """
    Strip ellipses (...) from the input string.
    """
    return re.sub(r'(^\.{3}|\s*(?<=[^\.])\.{3})\s*(\.?)', r'\2 ', data).strip()


def strip_wemi(data):
    """
    Strip any FRBR WEMI entity terms (work, expression, manifestation,
    or item) in the given string data.
    """
    return re.sub(r'\s*\((work|expression|manifestation|item)\)', r'', data,
                  flags=re.IGNORECASE)


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


def extract_years(data):
    """
    Extract individual years from a string, such as a publication
    string, and return them as a tuple.

    Returned years are 4 digits and formatted like years in the 008:
    0930 => the year 930
    193u => 1930s
    19uu => 20th century
    """
    dates = []
    century_re = r'(\d{1,2}st|\d{1,2}nd|\d{1,2}rd|\d{1,2}th)(?=.+centur)'
    decade_re = r'\d{2,3}0s'
    year_re = r'\d---|\d\d--|\d\d\d-|\d{3,4}(?![\ds])'
    combined_re = r'(?<!\d)({}|{}|{})'.format(century_re, decade_re, year_re)

    data = data.replace('[', '').replace(']', '')
    
    for date, _ in re.findall(combined_re, data, flags=re.IGNORECASE):
        if date.lower().endswith('0s'):
            new_date = '{}u'.format(date[:-2])
        elif date[-2:].lower() in ('st', 'nd', 'rd', 'th'):
            century = int(date[:-2]) - 1
            new_date = '{}uu'.format(century)
        else:
            new_date = date.replace('-', 'u')
        dates.append(new_date.zfill(4))
    return tuple(dates)


def strip_unknown_pub(data):
    """
    Strip, e.g., "S.l", "s.n.", and "X not identified" from the given
    publisher-related data.

    Caveat: This does NOT strip or otherwise normalize punctuation that
    may be left over after stripping this text. `normalize_punctuation`
    should help.
    """
    unknown_re = r'([A-Za-z\s]+ not identified|[Ss]\.?\s*[LlNn]\.?\s*)'
    return re.sub(unknown_re, r'', data)


def split_pdate_and_cdate(data):
    """
    Split a value from a 260 or 264 $c into two: publication date and
    copyright date (return a tuple: (pdate, cdate)). A copyright date
    must be set off by a label such as: 'c1964', 'copyright 1964', or
    '©1964'; otherwise it is not interpreted as a copyright date. The
    copyright date, label, and any text following it are included in
    the return value.

    IMPORTANT: After the split, extraneous punctuation from the
    publication date portion is NOT stripped; the caller is responsible
    for taking care of that.

    Examples:

    '1964, copyright 1963' returns the tuple ('1964,', 'copyright 1963')
    '1964' returns the tuple ('1964', '')
    'c1964' returns the tuple ('', 'c1964')

    Hint: To normalize display of the copyright symbol, use the
    `normalize_cr_symbol` parser function on the returned `cdate` value.
    """
    copyright_labels = r'\(c\)|c|C|©|copyright|cop\.?|\(p\)|p|P|℗|phonogram'
    copyright_re = r'(^|\s+)(({})\s*\d{{4}}.*)$'.format(copyright_labels)
    cdate_match = re.search(copyright_re, data)
    if cdate_match:
        cdate = cdate_match.groups()[1]
        pub_date = data.replace(cdate_match.group(), '')
        return pub_date, cdate
    return data, ''


def normalize_cr_symbol(cr_statement):
    """
    Normalize copyright symbols in the provided copyright statement.

    In the given `cr_statement` string, any strings standing in for the
    copyright or phonogram symbols (© or ℗) are converted to the proper
    symbol. If none are found, nothing is changed. The statement--with
    replacements--is returned.
    """
    labels_map = (
        (r'\(c\)|c|C|©|copyright|cop\.?', '©'),
        (r'\(p\)|p|P|℗|phonogram', '℗')
    )
    re_template = r'(^|\s+)({})\s*(\d{{4}})'
    for label_re, symbol in labels_map:
        cr_statement = re.sub(re_template.format(label_re),
                              r'\1{}\3'.format(symbol), cr_statement)
    return cr_statement


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
    return {'forename': strip_ends(forename) or None,
            'surname': strip_ends(surname) or None,
            'family_name': strip_ends(family_name) or None}


class Truncator(object):
    punct_trunc_pattern = r'[^\w\s]\s+'
    space_trunc_pattern = r'\s+'

    def __init__(self, trunc_patterns=None, truncate_to_punctuation=True):
        super(Truncator, self).__init__()
        trunc_patterns = list(trunc_patterns or [])
        if truncate_to_punctuation:
            trunc_patterns.append(self.punct_trunc_pattern)
        trunc_patterns.append(self.space_trunc_pattern)

        self.trunc_patterns = []
        for p in trunc_patterns:
            self.trunc_patterns.append(r'{0}(.(?!{0}))*$'.format(p))

    def truncate(self, text, min_len, max_len):
        slice_range = (min_len-1, max_len)
        text_slice = text[slice(*slice_range)]
        for pattern in self.trunc_patterns:
            match = re.search(pattern, text_slice)
            if match:
                trunc_index = match.start() + slice_range[0]
                return text[:trunc_index]
        return text[:min_len]


def find_names_in_string(string):
    """
    Return a list of word-lists, where each word-list represents a
    proper name found in the input `string`. This is designed to pull
    name candidates out of a statement-of-responsibility string for
    matching against name headings. Names found via this function do
    not include any lowercase words, e.g., ['Ludwig', 'Beethoven'].
    """
    def push_word_to_name(word, name, names):
        word = ''.join(word)
        if word.islower():
            if name and word == 'and':
                names.append(name)
                name = []
        else:
            name.append(word)
        return names, name

    names, name, word = [], [], []
    for ch in string:
        if ch.isupper():
            if word:
                names, name = push_word_to_name(word, name, names)
            word = [ch]
        elif ch.isalpha() or ch == '\'' or ord(ch) > 127:
            word.append(ch)
        else:
            if word:
                names, name = push_word_to_name(word, name, names)
                word = []
            if ch not in (' ', '.') and name:
                names.append(name)
                name = []
    if word:
        names, name = push_word_to_name(word, name, names)
    if name:
        names.append(name)
    return names


def sor_matches_name_heading(sor, heading, only_first=True):
    """
    Determine whether the provided statement-of-responsibility string
    `sor` contains a name that matches the provided name `heading`
    string. Pass True for `only_first` if you want just the first name
    from the SOR matched (i.e. if you're looking for the main author).
    """
    for name in find_names_in_string(sor):
        found = all((npart in heading for npart in name))
        if found or only_first:
            return found
    return False
