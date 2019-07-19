"""
Contains components for deriving `bl-suggest` auto suggest records from
the alpha-solrmarc index.
"""

from __future__ import unicode_literals
import re
import base64
import hashlib
from utils.helpers import NormalizedCallNumber

# Start with a few constants and utility functions for parsing data

BRACKETS = r'()\[\]{}<>'
END_PUNCT = r'.,;:\\/'
ALL_PUNCT = r'~!@#$%^&*\-+=|\"\'\?{}{}'.format(BRACKETS, END_PUNCT)


def clean_end(string):
    """
    Strip the left side of a string of spaces and some end punctuation
    """
    return re.sub(r'[{} ]+$'.format(END_PUNCT), r'', string)


def convert_punctuation(string, char=' '):
    """
    Convert punctuation within the given `string` to one character
    (`char`). Defaults to space.
    """
    ret = re.sub(r'\s+', char, re.sub(r'[{}]'.format(ALL_PUNCT), ' ', string))
    return clean_end(ret)


def parse_name(name):
    """
    Simple method for parsing an inverted `name` heading string.
    """
    match = re.match(r'^(.*?)([,;:]\s)(.*?)((\s*[\d,;:]\s*.*)?$)', name)
    try:
        sur, given, remainder = match.group(1), match.group(3), match.group(4)
    except AttributeError:
        return None
    initials = re.split(r'([A-Z])', given)[1::2]
    return {
        'given': given,
        'initials': initials,
        'sur': sur,
        'remainder': remainder
    }


def make_name_variations(name):
    """
    Generate a set of variations for the given `name` string.
    """
    variations = set()
    parts = parse_name(name)
    if parts:
        in1 = ' '.join(parts['initials'])
        in2 = ''.join(parts['initials'])
        for given_var in (in1, in2, parts['given']):
            variations.add('{} {}'.format(given_var, parts['sur']))
            if given_var != parts['given']:
                variations.add('{} {}'.format(parts['sur'], given_var))
    return variations


def truncate(string, limit):
    """
    Truncate the given string to the given number of characters, adding
    " ..." at the end. If truncated, the length of the returned string,
    including the elipses, will equal the given limit.
    """
    if len(string) > limit:
        return '{} ...'.format(string[0:(limit-4)])
    return string


def make_normalized_heading_string(heading):
    """
    Create a basic normalized form of a heading string.

    Punctuation is converted to space, and then spaces between digits
    and non digits are removed. Queries against this field must be
    normalized the same way.

    "Bach, Johann Sebastian, 1685-1750"
    becomes
    "Bach Johann Sebastian1685 1750"

    "MT 130 .C35 1996"
    becomes
    "MT130C35 1996"

    The idea is that spacing/punctuation around ambiguous word
    delimiters is normalized out both at index and query time to
    provide the best chance of matching.
    """
    heading = convert_punctuation(heading, ' ')
    parts = re.split(r'\s*([\d\s]+)\s*', heading)
    return ''.join([p.strip() or p for p in parts])


class SuggestBuilder(object):
    """
    Abstract base class for creating objects to coordinate building
    indexes of records to power auto-suggest features for a discovery
    system, such as blacklight.

    Implement this class by subclassing it and defining a series of
    HeadingExtractor-based classes, each of which defines how to
    extract a particular type of heading from a source content record.
    Then set the `extractor_classes` attribute to be a tuple of all the
    HeadingExtractor classes that apply to this builder.

    You may also populate `source_facet_fields` with a list of field
    names from content records that contain facets that you want
    recorded as '_fs' facets in the resulting suggest records. These
    are used for narrowing suggestions based on what content facets are
    selected.

    See the `SuggestBuilder.HeadingExtractor` docstring for more info
    about how to implement that class. See `BlSuggestBuilder` for a
    sample implementation of the `SuggestBuilder` class.
    """
    class HeadingExtractor(object):
        """
        Define how to build one type of suggest record for a certain
        SuggestBuilder implementation.

        Implement this class by subclassing it and defining the
        following.

        `htype`: a string representing the heading type (such as
        "title" or "author"); this gets stored in the suggest record's
        `heading_type` field, so that suggestions can be limited by
        type.

        `heading_source_fields`: a tuple containing the list of field
        names from content records containing the headings that this
        HeadingExtractor will extract.

        `other_source_fields`: optional tuple containing a list of
        field names from content records containing any other data that
        this HeadingExtractor needs for `extract_info`--e.g., fields
        containing peripheral information. (This ensures the needed
        fields are grabbed in the Solr query.)

        `extract_info`: defines how to compile a suggest record for the
        given content record and heading string.

        `compose_id`: (optional) -- you shouldn't need to change this,
        but it defines how to create a unique ID field for a given
        suggest record.
        """

        htype = None
        heading_source_fields = tuple()
        other_source_fields = tuple()

        def __init__(self, source_facet_fields, suppression_fields):
            self.source_facet_fields = source_facet_fields
            self.other_source_fields += tuple(suppression_fields)

        @property
        def source_fields(self):
            """
            Return a complete list of fields needed from source content
            records to compile a suggest record.
            """
            return set(self.heading_source_fields + self.other_source_fields
                       + self.source_facet_fields)

        def compose_id(self, srec):
            """
            Compose a unique ID for the given suggest record (srec).
            Your subclass may override this, but it shouldn't need to.
            """
            id_ = '{}|{}'.format(srec['heading_type'], srec['heading'])
            id_ = base64.b64encode(hashlib.md5(id_.encode('utf-8')).digest())
            return id_

        def normalize_heading(self, heading):
            """
            Produce a normalized (string) form of the given `heading`.
            Default behavior is to convert punctuation to space and
            remove spaces between digits and non-digits.
            """
            return make_normalized_heading_string(heading)

        def extract_info(self, bib, source_field, heading):
            """
            Given a `bib` content record and a `heading` string from a
            particular `source_field` on the content record, compile
            and return a suggest record (dict). This is a basic default
            implementation; override in your subclass if needed.
            """
            normalized_heading = self.normalize_heading(heading)
            srec = {
                'heading': heading,
                'heading_display': heading,
                'heading_type': self.htype,
                'heading_keyphrases': set([normalized_heading]),
                'heading_sort': normalized_heading
            }
            srec['id'] = self.compose_id(srec)
            return srec

    extractor_classes = tuple()
    source_facet_fields = tuple()
    suppression_fields = ('suppressed',)

    def __init__(self):
        self.srecs = {}
        self.extractors = tuple(
            cl(self.source_facet_fields, self.suppression_fields)
                for cl in self.extractor_classes
        )

    @property
    def fs_field_by_source_facet(self):
        """
        Return a dict mapping all `source_facet_fields` from content
        records to the appropriate '_fs' field. Keys are source facet
        fields and values are fs fields.
        """
        try:
            return self._fs_field_by_source_facet
        except AttributeError:
            self._fs_field_by_source_facet = {
                f: '{}_fs'.format(f) for f in self.source_facet_fields
            }
            return self._fs_field_by_source_facet

    @property
    def multi_fields(self):
        """
        Return a set of suggest record field names that are multi-
        valued.
        """
        fs_fields = tuple(self.fs_field_by_source_facet.values())
        return set(fs_fields + ('heading_variations', 'more_context',
                                'heading_keyphrases', 'this_facet_values'))

    @property
    def all_source_fields(self):
        """
        Return a set of ALL fields from the source content records
        needed to build all of the headings for this builder. This is
        suitable for building the `fl` argument in a Solr query to get
        content records.
        """
        try:
            return self._all_source_fields
        except AttributeError:
            self._all_source_fields = set(
                [f for e in self.extractors for f in e.source_fields]
            )
            return self._all_source_fields

    @property
    def all_heading_source_fields(self):
        """
        Return a set of the fields from source content records that
        contain the heading strings we're using as a basis for creating
        suggest records.
        """
        try:
            return self._all_heading_source_fields
        except AttributeError:
            self._all_heading_source_fields = set(
                [f for e in self.extractors for f in e.heading_source_fields]
            )
            return self._all_heading_source_fields

    @property
    def extractors_by_heading_source(self):
        """
        Return a dict that maps each heading source field (from content
        records) to the extractor object that should process it. Keys
        are field names and values are extractor objects.
        """
        try:
            return self._extractors_by_heading_source
        except AttributeError:
            self._extractors_by_heading_source = {
                v: obj for obj in self.extractors
                    for v in obj.heading_source_fields
            }
            return self._extractors_by_heading_source

    def is_bib_suppressed(self, bib):
        """
        Simple utility method for determining whether a bib record is
        suppressed. By default it looks for a `suppressed` bool value
        on the Solr content record. Override this in subclasses as
        needed. Be sure that any bib fields you need to access are
        included in the `suppression_fields` class attribute.
        """
        return bib['suppressed']

    def _extract_info(self, bib, source_field, heading):
        """
        Generate a base suggest record given a `bib` content record, a
        `heading` string, and the exact `source_field` from the content
        record that the heading came from. Returns a suggest record
        dict.
        """
        extractor = self.extractors_by_heading_source[source_field]
        srec = extractor.extract_info(bib, source_field, heading)
        if srec['heading']:
            new_facet_value = '{}:{}'.format(source_field, heading)
            srec['this_facet_values'] = srec.get('this_facet_values', set())
            srec['this_facet_values'].add(new_facet_value)
            srec['record_count'] = 0 if self.is_bib_suppressed(bib) else 1
            return srec

    def _absorb_srec(self, srec):
        """
        Add the provided suggest record (`srec`) to the cumulative set
        of suggest records (self.srecs).
        """
        key = srec['id']
        self.srecs[key]['record_count'] += srec['record_count']
        for fname in ('more_context',):
            new_val = srec.get(fname, None)
            if new_val:
                joined = self.srecs[key].get(fname, set()) | new_val
                self.srecs[key][fname] = joined

    def _add_facets_from_bib(self, key, bib):
        """
        Add '_fs' facet values from the provided `bib` content record
        to the suggest record with the given `key` (i.e. id).
        """
        for source_field, fs_field in self.fs_field_by_source_facet.items():
            self.srecs[key][fs_field] = self.srecs[key].get(fs_field, set())
            for fval in bib.get(source_field, []):
                self.srecs[key][fs_field].add(fval)

    def _build_suggest_rec_for_heading(self, bib, source_field, heading):
        """
        Build the suggest record for the given `heading` from the
        specific `source_field` of the provided `bib` content record.
        The suggest record is added to the cumulative set of suggest
        records (self.srecs).
        """
        srec = self._extract_info(bib, source_field, heading)
        if srec is not None:
            key = srec['id']
            need_to_absorb = True
            if self.srecs.get(key, None) is None:
                self.srecs[key] = srec
                need_to_absorb = False
            if not self.is_bib_suppressed(bib):
                if need_to_absorb:
                    self._absorb_srec(srec)
                self._add_facets_from_bib(key, bib)

    def _pop_srecs(self):
        """
        Returns the cumulative set of suggest records (self.srecs) as a
        list and clears self.srecs--essentially a "pop" operation. Any
        multi_fields that are stored internally as sets are converted
        to lists.
        """
        srec_list = []
        for srec in self.srecs.values():
            srec = self._finalize_suggest_rec(srec)
            srec_list.append(srec)
        self.srecs = {}
        return srec_list

    def _finalize_suggest_rec(self, srec):
        """
        Do anything that needs to be done to finalize a suggest record
        before returning it.
        """
        for fname in self.multi_fields:
            srec[fname] = list(srec.get(fname, [])) or None
        return srec

    def extract_suggest_recs(self, bibs, only_fields=None, only_headings=None):
        """
        Public method for extracting suggest records from a list of bib
        content records (`bibs`).

        Optional kwargs include:

        `only_fields`: a list or tuple of fields in the source content
        records you want to extract headings from. Each field in the
        list MUST be one listed in a `heading_source_fields` attribute
        in one of the `extractor` objects. By default, headings from
        all `heading_source_fields` are extracted.

        `only_headings`: a list or tuple of heading strings you want to
        generate suggest records for. If provided, a heading that is in
        one of the content records is skipped if not in the list. By
        default, all headings are extracted.

        Returns a list of suggest records, suitable for loading into
        Solr.
        """
        for bib in bibs:
            for field in (only_fields or self.all_heading_source_fields):
                headings = bib.get(field, [])
                for heading in headings:
                    if only_headings is None or heading in only_headings:
                        self._build_suggest_rec_for_heading(bib, field,
                                                            heading)
        return self._pop_srecs()


class BlSuggestBuilder(SuggestBuilder):
    """
    Extract headings from Solr record for a bib object, to populate the
    bl-suggest auto-suggest index.
    """
    class TitleExtractor(SuggestBuilder.HeadingExtractor):
        """
        Build auto-suggest records for title headings, including
        any applicable author information, if such can be determined.

        The heading comes from the `public_title_facet` in the bib Solr
        record. First the heading is compared against the
        `author_title_search` entries to find a match. If one is found,
        the author name is extracted from the appropriate entry.

        Otherwise, the author name is pulled from the `creator` field,
        if present.

        Next, the title is compared against the `main_title` and
        `uniform_title` fields, e.g., to determine if the title string
        in question is from a 24X field. If so, then the complete list
        of authors/contributors (from `public_author_facet`) as well as
        publication dates are added to the 'more_context' suggest-
        record field.

        The display title is truncated to 75 characters.

        Finally, if an author name was found to be associated with this
        title, the display heading is built by extracting the author's
        last name and attaching it, in parentheses, to the end of the
        title. The author heading is added as a 'this_facet_values'
        entry, so that selecting this title will limit based on both
        author AND title.
        """
        htype = 'title'
        heading_source_fields = tuple(['public_title_facet'])
        other_source_fields = tuple(['main_title', 'uniform_title',
                                     'author_title_search', 'creator',
                                     'subtitle'])

        def _add_author_to_display_title(self, author, title):
            parts = parse_name(author)
            if parts:
                sur = parts['sur']
                ini = ' '.join(['{}.'.format(p[0]) for p in parts['initials']])
                short = '{} {}'.format(ini, sur) if ini else sur
            else:
                short = re.split(r'[.,;]\s', author)[0]
            return '{} ({})'.format(title, short)

        def _pull_author_from_author_title(self, title, bib):
            norm_title = clean_end(title)
            for author_title in bib.get('author_title_search', []):
                norm_at = clean_end(author_title)
                if norm_at.endswith(norm_title):
                    author = norm_at[0:author_title.rindex(norm_title)]
                    return clean_end(author)

        def extract_info(self, bib, source_field, heading):
            mtitle_key = convert_punctuation(bib.get('main_title', ''))
            utitle_key = convert_punctuation(bib.get('uniform_title', ''))

            title_key = convert_punctuation(heading)
            display = truncate(heading, 75)
            context, this_facet_values = set(), set()

            author = self._pull_author_from_author_title(heading, bib)
            if title_key in (utitle_key, mtitle_key):
                context = set(bib.get('public_author_facet', []))
                context |= set(bib.get('publication_dates_facet', []))
                author = author or bib.get('creator', None)
                if title_key == mtitle_key:
                    context.add(bib.get('subtitle'))

            if author:
                heading = '{} {}'.format(heading, author)
                display = self._add_author_to_display_title(author, display)
                this_facet_values.add('public_author_facet:{}'.format(author))

            normalized_heading = self.normalize_heading(heading)

            srec = {
                'heading': heading,
                'heading_display': display,
                'heading_keyphrases': set([normalized_heading]),
                'heading_sort': normalized_heading,
                'more_context': context,
                'this_facet_values': this_facet_values,
                'heading_type': self.htype
            }
            srec['id'] = self.compose_id(srec)
            return srec

    class AuthorExtractor(SuggestBuilder.HeadingExtractor):
        """
        Build auto-suggest records for author headings, from the
        `public_author_facet` field in the bib Solr record.

        Names are indexed as-is, except that heading_variations are
        created for different forms of the name.
        """
        htype = 'author'
        heading_source_fields = tuple(['public_author_facet'])

        def extract_info(self, bib, source_field, heading):
            parent = BlSuggestBuilder.AuthorExtractor
            srec = super(parent, self).extract_info(bib, source_field, heading)
            srec['heading_variations'] = make_name_variations(heading)
            return srec

    class SubjectExtractor(SuggestBuilder.HeadingExtractor):
        """
        Build auto-suggest records for subject headings, from the
        `public_subject_facet` field in the bib Solr record.

        Subjects are indexed as-is.
        """
        htype = 'subject'
        heading_source_fields = tuple(['public_subject_facet'])

    class GenreExtractor(SuggestBuilder.HeadingExtractor):
        """
        Build auto-suggest records for genre headings, from the
        `public_genre_facet` field in the bib Solr record.

        Genres are indexed as-is.
        """
        htype = 'genre'
        heading_source_fields = tuple(['public_genre_facet'])

    class CallNumberExtractor(SuggestBuilder.HeadingExtractor):
        """
        Build auto-suggest records for call numbers, from the
        `*_call_numbers` fields in the bib Solr record.

        Call numbers are indexed as-is, except that a specialized
        sortable version is created and indexed in the `heading_sort`
        field, and the normalized version is added to
        `heading_variations`.
        """
        htype = 'call_number'
        source_to_type_map = {
            'loc_call_numbers': 'lc',
            'dewey_call_numbers': 'dewey',
            'other_call_numbers': 'other'
        }
        heading_source_fields = tuple(source_to_type_map.keys())

        def extract_info(self, bib, source_field, heading):
            parent = BlSuggestBuilder.CallNumberExtractor
            srec = super(parent, self).extract_info(bib, source_field, heading)
            srec['heading_variations'] = srec['heading_keyphrases']
            cn_type = self.source_to_type_map[source_field]
            sort_normalizer = NormalizedCallNumber(heading, cn_type)
            srec['heading_sort'] = sort_normalizer.normalize()
            return srec

    class SudocExtractor(CallNumberExtractor):
        """
        Build auto-suggest records for SuDoc numbers, from the
        `sudoc_numbers` field in the bib Solr record.

        Sudocs are indexed like call numbers.
        """
        htype = 'sudoc'
        source_to_type_map = { 'sudoc_numbers': 'sudoc' }
        heading_source_fields = tuple(source_to_type_map.keys())

    extractor_classes = tuple(
        [TitleExtractor, AuthorExtractor, SubjectExtractor, GenreExtractor,
         CallNumberExtractor, SudocExtractor]
    )
    source_facet_fields = (
        'bib_location_codes', 'item_location_codes', 'material_type',
        'languages', 'publication_dates_facet', 'public_author_facet',
        'public_title_facet', 'public_series_facet', 'meetings_facet',
        'public_genre_facet', 'public_subject_facet', 'geographic_terms_facet',
        'era_terms_facet', 'game_facet'
    )
    title_thing_type_defs = {
        'archive': set(['p']),
        'book': set(['a', 'i', 'n']),
        'computer_file': set(['m']),
        'database': set(['b']),
        'graphic': set(['k']),
        'journal': set(['q', 'y']),
        'manuscript': set(['t']),
        'map': set(['e']),
        'music': set(['j', 'c']),
        'object': set(['o', 'r']),
        'thesis': set(['z']),
        'video': set(['g']),
    }

    def determine_thing_type_for_title(self, srec):
        for label, mtype_set in self.title_thing_type_defs.items():
            if srec['material_type_fs'] - mtype_set == set([]):
                return label
        return 'general_work'

    def _finalize_suggest_rec(self, srec):
        if srec['heading_type'] == 'title':
            srec['thing_type'] = self.determine_thing_type_for_title(srec)
        else:
            srec['thing_type'] = srec['heading_type']
        return super(BlSuggestBuilder, self)._finalize_suggest_rec(srec)
