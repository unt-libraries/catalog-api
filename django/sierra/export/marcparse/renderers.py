# -*- coding: utf-8 -*-

"""
Contains functions/classes for rendering parsed MARC data.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import re

try:
    # Python 3
    from re import ASCII
except ImportError:
    # Python 2
    ASCII = 0
from collections import OrderedDict

from django.conf import settings

from utils import toascii
from . import stringparsers as sp


class PersonalNamePermutator(object):
    """
    This is a one-off class designed to help create variations of
    personal names, particularly for searching.

    To use: first, generate a parsed personal name (e.g. from an X00
    field) via the PersonalNameParser class. Pass that to `__init__`.
    The original name structure will be accessible via the attribute
    `original_name`. Access standardized/tokenized versions of the
    forename, surname, and forename initials via the `authorized_name`
    dictionary attribute. If there is a "fuller_form_of_name" (e.g.
    from X00$q), you can access the expanded version of the full name
    via the `fullest_name` attribute.

    Several methods are included for working with name parts as sets of
    tokens. At this time the primary purpose is to generate search
    permutations, which you can get via the `get_search_permutations`
    method.
    """

    def __init__(self, name):
        """
        `name` must be a name structure output from using the
        PersonalNameParser class.
        """
        self.original_name = name
        self._authorized_name = None
        self._fullest_name = None
        self._nicknames = []

    def tokenize_name_part(self, np):
        """
        Split part of a name (`np`), such as a forename or surname,
        into individual tokens. Handles initials given in various
        forms: 'JJ', 'J J', 'J.J.', and 'J. J.' all return ['J', 'J'].
        """
        if np:
            return [t for t in re.split(r'([A-Z](?=[A-Z]))|[.\s]+', np) if t]
        return []

    def _try_name_expansion(self, fuller_tokens, cmp_tokens):
        """
        Name expansion (from X00$q) works by generating a regex pattern
        from the authorized form of the heading (`cmp_tokens`), which
        tries to find anchor points for the tokens from the fuller form
        of name in the set of comparison tokens--basically looking to
        find a point where each `fuller_token` consecutively starts
        with a corresponding token from `cmp_tokens`.

        Returns the list of tokens representing the fully expanded
        name, or None if no match is found.

        Examples:

        # "H. D." => "Hilda Doolittle" -- returns ['Hilda', 'Doolittle']
        cmp_tokens = ['H', 'D']
        fuller_tokens = ['Hilda', 'Doolittle']

        # "Elizabeth" => "Ann Elizabeth" -- returns ['Ann', 'Elizabeth']
        cmp_tokens = ['Elizabeth']
        fuller_tokens = ['Ann', 'Elizabeth']
        """
        parts_pattern = r'\s'.join([r'({}\S*)'.format(t) for t in cmp_tokens])
        cmp_pattern = r'(?:^|(.+)\s){}(?:\s(.+)|$)'.format(parts_pattern)
        matches = re.match(cmp_pattern, ' '.join(fuller_tokens))
        if matches:
            return [m for m in matches.groups() if m]

    def _expand_name(self):
        expanded = {}
        name = self.original_name
        nicknames = []
        ftokens = self.tokenize_name_part(name['fuller_form_of_name'])
        expansion_done = False
        for key in ('forename', 'surname'):
            name_tokens = self.authorized_name[key]
            if name_tokens and not expansion_done:
                expanded[key] = self._try_name_expansion(ftokens, name_tokens)
            if expanded.get(key):
                expansion_done = True
            else:
                expanded[key] = name_tokens
        if ftokens and not expansion_done:
            if expanded['forename']:
                if expanded['forename'] == self.authorized_name['forename']:
                    nicknames = ftokens
            else:
                expanded['forename'] = ftokens
        return nicknames, expanded['forename'], expanded['surname']

    def split_nickname(self, forename):
        match = re.search(r'^(.+?)\s+[(\'"](\S+?)[)\'"]$', forename)
        if match:
            return match.groups()
        return forename, ''

    @property
    def authorized_name(self):
        if self._authorized_name is None:
            name, auth_name = self.original_name, {}
            fn, nn = self.split_nickname(name['forename'] or '')
            parts = {'forename': fn, 'surname': name['surname']}
            for partkey, part in parts.items():
                norm = sp.strip_all_punctuation(part) if part else ''
                tokens = self.tokenize_name_part(norm)
                initials = [t[0] for t in tokens]
                auth_name[partkey] = tokens
                auth_name['_'.join((partkey, 'initials'))] = initials
            self._authorized_name = auth_name
            if nn:
                self._nicknames.extend(self.tokenize_name_part(nn))
        return self._authorized_name

    @property
    def fullest_name(self):
        if self._fullest_name is None:
            name = self.original_name
            nn, exp_forename_tokens, exp_surname_tokens = self._expand_name()
            fullest_name = {
                'forename': exp_forename_tokens,
                'forename_initials': [f[0] for f in exp_forename_tokens],
                'surname': exp_surname_tokens,
            }
            self._fullest_name = fullest_name
            if nn:
                self._nicknames.extend(nn)
        return self._fullest_name

    @property
    def nicknames(self):
        self.authorized_name
        self.fullest_name
        return self._nicknames

    def split_n_prefix_titles(self, n):
        """
        Use `parsers.person_titles` to help subdivide a list of
        personal titles (X00$c) into `n` prefix titles and the rest
        suffix titles. This is to identify titles (e.g. honorifics)
        that should be placed as a prefix, such as in "Sir Ian
        McKellan" or "Saint Thomas, Aquinas." Returns a tuple: a list
        of prefixes and a list of suffixes.

        In some cases a full suffix implies a shorter prefix title:
        "King of England" implies the prefix "King." When these are
        found, assuming `n` hasn't been reached, the prefix "King"
        is added as a prefix and the full suffix "King of England" is
        added as a suffix. (Terms are deduplicated, as well.)
        """
        prefixes, suffixes = OrderedDict(), OrderedDict()
        for i, t in enumerate(self.original_name['person_titles'] or []):
            t = sp.strip_all_punctuation(t)
            needs_suffix = True
            if len(prefixes) < n:
                parsed = sp.person_title(t)
                if parsed:
                    prefix = parsed['prefix']
                    particle = parsed['particle']
                    if t == particle:
                        prefixes[t] = None
                        needs_suffix = False
                    else:
                        if particle and t.endswith(' {}'.format(particle)):
                            fn = self.authorized_name['forename']
                            sn = self.authorized_name['surname']
                            complete_with = sn or fn
                            if complete_with:
                                t = ' '.join([t, complete_with[0]])
                        if prefix:
                            prefixes[prefix] = None
                            if t == parsed['prefix']:
                                needs_suffix = False
            if needs_suffix:
                suffixes[t] = None
        return list(prefixes.keys()), list(suffixes.keys())

    def is_initial(self, token):
        """
        Is the given token an initial?
        """
        return token == token[0]

    def render_name_part(self, tokens, for_search=True):
        """
        Join a list of name-part `tokens` back into a string--basically
        the converse of `tokenize_name_part`.

        Tokens that aren't initials are joined using a space character.
        Strings of initials are rendered differently depending on
        whether the string is meant for searching (`for_search` is
        True) or not. If `for_search` is True, e.g.
        ['J', 'T', 'Thomas'] returns 'J.T Thomas'. If False, it returns
        'J. T. Thomas' (i.e. to use as a display value).

        The formatting for searching is intended to work well with the
        WordDelimiterFilterFactory settings in Solr we're using for
        searchable text, to ensure flexible matching of initials.
        """
        render_stack, prev_token = [], ''
        for token in (tokens or []):
            if prev_token:
                if self.is_initial(prev_token):
                    if for_search and self.is_initial(token):
                        render_stack.append('.')
                    elif not for_search:
                        render_stack.extend(['.', ' '])
                    else:
                        render_stack.append(' ')
                else:
                    render_stack.append(' ')
            render_stack.append(token)
            prev_token = token
        return ''.join(render_stack)

    def render_name(self, fore_tokens, sur_tokens, inverted=False,
                    for_search=True):
        """
        Render a name (forename and surname only). Set `inverted` to
        True if you want an inverted name (Last, First). `for_search`
        should be True if you're rendering the name to use as a search
        value (see `render_name_part` for more information).

        Returns the name rendered as a string. If either `foretokens`
        or `sur_tokens` is empty, then it just returns the rendered
        forename or surname.
        """
        fore = self.render_name_part(fore_tokens, for_search)
        sur = self.render_name_part(sur_tokens, for_search)
        if inverted:
            return ', '.join([p for p in (sur, fore) if p])
        return self.render_name_part([p for p in (fore, sur) if p])

    def get_standard_permutations(self, name):
        """
        This generates standard permutations of names (forenames and/or
        surnames) used primarily when generating search permutations.
        It generates each applicable of:
            inverted name,
            forward name,
            inverted name (using forename initials),
            forward name (using forename initials)

        Initials for forenames without surnames are not included, nor
        are duplicative permutations.
        """
        inv = self.render_name(name['forename'], name['surname'], True)
        fwd = self.render_name(name['forename'], name['surname'])
        permutations = [inv] if inv == fwd else [inv, fwd]
        if (name['forename_initials'] != name['forename']) and name['surname']:
            inv_initials = self.render_name(name['forename_initials'],
                                            name['surname'], True)
            fwd_initials = self.render_name(name['forename_initials'],
                                            name['surname'])
            for initials in (inv_initials, fwd_initials):
                if initials not in permutations:
                    permutations.append(initials)
        return permutations

    def dedupe_search_permutations(self, perms1, perms2):
        """
        This method deduplicates sets of search permutations (generated
        via `get_standard_permutations`) against each other. Generally,
        `perms1` should be a set of permutations generated from the
        `authorized_name` and `perms2` is generated from
        `fullest_name`. A permutation from `perms1` where a permutation
        in `perms2` ends with that string is considered a duplicate,
        because the phrase is repeated in the second set of perms.

        Example:
        perms1 = ['Elizabeth Smith', 'Smith, Elizabeth']
        perms2 = ['Ann Elizabeth Smith', 'Smith, Ann Elizabeth']

        perms1[0] is eliminated because it duplicates a phrase found in
        perms2[0]. All others are unique.
        """
        result = []
        for p1 in perms1:
            if all([not p2.endswith(p1) for p2 in perms2]):
                result.append(p1)
        return result

    def _find_perm_overlap(self, perm, cumulative):
        test_str = ''
        for nextchunk in re.split(r'([, .])', perm):
            if nextchunk:
                test_str = ''.join([test_str, nextchunk])
                match = re.search(
                    r'(?:^|.*\s){}$'.format(test_str), cumulative)
                if match:
                    return test_str, perm[len(test_str):]
        return None, None

    def compress_search_permutations(self, permutations):
        """
        For searching, sets of (standard) permutations are compressed
        into a form that enables good phrase matching while
        de-emphasizing parts of the name (first, middle) that are less
        important. Compression involves finding overlapping
        permutations and creating one long overlapping string.

        Example:
        permutations = ['Smith, Ann Elizabeth', 'Ann Elizabeth Smith']
        compressed => 'Smith, Ann Elizabeth Smith'

        The compressed form matches searches on both the inverted and
        forward forms of the name; the first and middle name are only
        included once (tf=1) and the last name twice (tf=2). Combined
        with other techniques, this is effective at disambiguating e.g.
        names used as first, middle, or last names (William, John,
        Henry, etc.).
        """
        compressed, cumulative = [], permutations[0]
        for perm in permutations[1:]:
            match, remainder = self._find_perm_overlap(perm, cumulative)
            if match is None or remainder is None:
                compressed.append(cumulative)
                cumulative = perm
            else:
                cumulative = ''.join([cumulative, remainder])
        compressed.append(cumulative)
        return compressed

    def get_search_permutations(self):
        """
        Generate/return all permutations of a name for searching. This
        includes:

        - Compressed standard permutations for the authorized name.
            E.g.:
            ['Smith, Elizabeth Smith, E Smith']

        - Compressed standard permutations for the fullest form of the
        name. E.g.:
            ['Smith, Ann Elizabeth Smith, A.E Smith']

        - Note the above two examples would be deduplicated and again
        compressed, so the actual values would be:
            ['Smith, Elizabeth', 'Smith, E',
             'Smith, Ann Elizabeth Smith, A.E Smith']

        - The fullest first name (only) and last name, including
          titles and numeration, if present. Commas are stripped from
          the titles-suffixes listing. E.g.:
            ['Ann Smith'] or
            ['Emperor John Comnenus II Emperor of the East']

        - The "best" form of the authorized name, in forward form, also
        included titles and numeration, if present. For names with
        titles as suffixes, commas are included.
            ['Mrs Elizabeth Smith'] or
            ['Emperor John Comnenus II, Emperor of the East']
        """
        fullest_fl, best_fwd, all_titles = '', '', ''
        nicknames = self.nicknames
        prefix_titles, suffix_titles = self.split_n_prefix_titles(1)
        prefix_title = (prefix_titles or [None])[0]
        if self.original_name['person_titles']:
            all_titles = ', '.join(self.original_name['person_titles'])

        std_perm = self.get_standard_permutations(self.authorized_name)
        if std_perm:
            forename = (self.authorized_name['forename'] or []) + nicknames
            auth_name = self.render_name(forename,
                                         self.authorized_name['surname'])
            best_fwd_parts = [prefix_title, auth_name,
                              self.original_name['numeration']]
            best_fwd = ' '.join([p for p in best_fwd_parts if p])
            best_fwd = ', '.join([best_fwd] + suffix_titles)
        else:
            best_fwd = all_titles

        if self.fullest_name != self.authorized_name:
            full_std_perm = self.get_standard_permutations(self.fullest_name)
            std_perm = self.dedupe_search_permutations(std_perm, full_std_perm)
            std_perm.extend(full_std_perm)

        fullest_first, fullest_last = None, None
        if self.fullest_name['surname']:
            ffn = self.fullest_name['forename'] or [None]
            fullest_first = (ffn)[0]

            rev_prepositions = []
            for name in reversed(ffn[1:]):
                if name[0].islower():
                    rev_prepositions.append(name)
                else:
                    break
            if rev_prepositions:
                prepositions = [n for n in reversed(rev_prepositions)]
                fullest_first = ' '.join([fullest_first] + prepositions)

            fullest_last = self.render_name_part(self.fullest_name['surname'])
        elif self.fullest_name['forename']:
            fullest_first = self.render_name_part(
                self.fullest_name['forename'])
        else:
            fullest_fl = all_titles

        if not fullest_fl and (fullest_first or fullest_last):
            parts = [prefix_title, fullest_first, fullest_last,
                     self.original_name['numeration']] + suffix_titles
            fullest_fl = ' '.join([p for p in parts if p])

        if nicknames and self.fullest_name['surname']:
            temp_name = {
                'forename': nicknames,
                'forename_initials': [n[0] for n in nicknames],
                'surname': self.fullest_name['surname']
            }
            nn_std_perm = self.get_standard_permutations(temp_name)
            std_perm = self.dedupe_search_permutations(std_perm, nn_std_perm)
            std_perm.extend(nn_std_perm)

        permutations = self.compress_search_permutations(std_perm)
        permutations.extend([fullest_fl, best_fwd])
        return permutations


def shorten_name(parsed):
    """
    Convert the given `parsed` name structure to a short name string.

    `parsed` can be the result of parsing using `PersonalNameParser` or
    `OrgEventNameParser`.

    Use this to generate the shortened-name that goes into the short-
    name slug for titles, e.g. `Beethoven, L.v.`. 
    """
    if parsed['type'] == 'person':
        forename, surname = parsed['forename'], parsed['surname']
        titles = parsed['person_titles']
        numeration = parsed['numeration']
        first, second = (surname or forename or ''), ''
        if numeration:
            first = '{} {}'.format(first, numeration)

        if surname and forename:
            initials_split_re = r'[\.\-,;\'"\s]'
            parts = [n[0] for n in re.split(initials_split_re, forename) if n]
            initials = '.'.join(parts)
            if initials:
                second = '{}.'.format(initials)
        elif titles:
            second = ', '.join(titles)
            second = re.sub(r', (\W)', r' \1', second)
        to_render = ([first] if first else []) + ([second] if second else [])
        return ', '.join(to_render)

    parts = [part['name'] for part in parsed['heading_parts']]
    if parts:
        if len(parts) == 1:
            return parts[0]
        if len(parts) == 2:
            return ', '.join(parts)
        return ' ... '.join([parts[0], parts[-1]])
    return ''


def make_relator_search_variations(base_name, relators):
    """
    Given a name (string) and list of relators (list of strings),
    generate a list of combinations of the name plus each relator.
    E.g.: 
       `base_name` = 'Joe Smith'
       `relators` = ['author', 'illustrator']
       => ['Joe Smith author', 'Joe Smith illustrator']

    This is like generated responsibility data, for searching.
    """
    relators = relators or []
    return ['{} {}'.format(base_name, r) for r in relators]


def make_searchable_callnumber(cn_string):
    """
    Pass in a complete `cn_string` and generate a normalized version
    for searching. Note that the normalization operations we want here
    depend heavily on what kinds of fields/analyzers we have set up in
    Solr.
    """
    norm = cn_string.strip()
    norm = re.sub(r'(\d),(\d)', r'\1\2', norm)
    norm = re.sub(r'(\D)\s+(\d)', r'\1\2', norm)
    norm = re.sub(r'(\d)\s+(\D)', r'\1\2', norm)
    return sp.shingle_callnum(norm)


def format_materials_specified(materials_specified):
    """
    Render a list of `materials_specified` values.
    """
    return '({})'.format(', '.join(materials_specified))


def format_display_constants(display_constants):
    """
    Render a list of display constants (i.e. field labels).
    """
    return '{}:'.format(', '.join(display_constants))


def format_title_short_author(title, preposition, short_author):
    """
    Render the short-author slug for the given `title`.
    """
    prep_author = ([preposition] if preposition else []) + [short_author]
    return '{} [{}]'.format(title, ' '.join(prep_author))


def format_translation(translated_text):
    """
    Render parallel text (i.e. a translation).
    """
    return '[translated: {}]'.format(translated_text)


def format_volume(volume):
    """
    Render a volume string, such as what may appear in a serial title.

    '[volume]' is used as a label if this is a volume number with no
    label.
    """
    volume_separator = '; '
    if volume.isdigit():
        volume = '[volume] {}'.format(volume)
    return volume_separator, volume


def format_degree_statement(institution, date, degree):
    """
    Render a str statement from a given institution, date, and degree.
    
        `institution` = 'University of North Texas'
        `date` = 'May 2021'
        `degree` = 'Ph.D.'
        => University of North Texas, May 2021 — Ph.D. 
    """
    result = ', '.join([v for v in (institution, date) if v])
    return ' ― '.join([v for v in (degree, result) if v])


def generate_facet_key(value, nonfiling_chars=0, space_char=r'-'):
    """
    Render a normalized facet/sort key from the given `value`.
        - Convert to lowercase
        - Map diacritics to the best ASCII equivalent
        - Convert spaces to `space_char` (default is -)
        - Remove N nonfiling characters from the beginning, if it
          won't cause a break in the middle of a word.
    """
    key = value.lower()
    nonfiling_chars = int(nonfiling_chars)
    if nonfiling_chars and len(key) > nonfiling_chars:
        last_nfchar_is_nonword = not key[nonfiling_chars - 1].isalnum()
        if last_nfchar_is_nonword and len(value) > nonfiling_chars:
            key = key[nonfiling_chars:]
    key = toascii.map_from_unicode(key)
    key = re.sub(r'\W+', space_char, key, flags=ASCII).strip(space_char)
    return key or '~'


def format_key_facet_value(heading, nonfiling_chars=0, keysep='!'):
    """
    Render a facet value (`heading`) into a key+facet.

    The given `keysep` (default !) is used to join the key and facet.
    E.g.:
        `heading` = 'The Lord of the Rings'
        `key_separator` = '!'
        `nonfiling_chars` = 4
        => 'lord-of-the-rings!The Lord of the Rings'
    """
    key = generate_facet_key(heading, nonfiling_chars)
    return keysep.join((key, heading))


def format_number_search_val(numtype, number):
    """
    Render a standard or control number for searching.

    This is to enable lookups by type, e.g. 'isbn:123456789'.
    """
    exclude = ('unknown',)
    numtype = '' if numtype in exclude else numtype
    return ':'.join([v for v in (numtype, number) if v])


def format_number_display_val(parsed):
    """
    Render a display string for a parsed standard/control number.
    """
    normstr, qualstr, sourcestr = '', '', ''
    if parsed.get('normalized'):
        normstr = 'i.e., {}'.format(parsed['normalized'])
    if parsed.get('qualifiers'):
        qualstr = ', '.join(parsed['qualifiers'])
    is_other = parsed['type'] not in ('isbn', 'issn', 'lccn', 'oclc')
    if is_other and not parsed.get('type_label'):
        sourcestr = 'source: {}'.format(parsed['type'])
    qualifiers = [s for s in (normstr, qualstr, sourcestr) if s]

    render_stack = []
    if parsed.get('type_label'):
        if is_other:
            render_stack = ['{}:'.format(parsed['type_label'])]
    render_stack.append(parsed['number'])
    if qualifiers:
        render_stack.append('({})'.format('; '.join(qualifiers)))
    if not parsed['is_valid']:
        render_stack.append('[{}]'.format(parsed['invalid_label']))
    return ' '.join(render_stack)

