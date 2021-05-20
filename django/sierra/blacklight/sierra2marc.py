# -*- coding: utf-8 -*-

"""
Sierra2Marc module for catalog-api `blacklight` app.
"""

from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
import pymarc
import logging
import re
import itertools
import ujson
from datetime import datetime
from collections import OrderedDict

from django.conf import settings
from base import models, local_rulesets
from export.sierra2marc import S2MarcBatch, S2MarcError
from blacklight import parsers as p
from utils import helpers, toascii
import six
from six.moves import range


# These are MARC fields that we are currently not including in public
# catalog records, listed by III field group tag.
IGNORED_MARC_FIELDS_BY_GROUP_TAG = {
    'n': ('539', '901', '959'),
    'r': ('306', '307', '335', '336', '337', '338', '341', '355', '357', '381',
          '387', '389'),
}


FACET_KEY_SEPARATOR = '!'


class SierraMarcField(pymarc.field.Field):
    """
    Subclass of pymarc field.Field; adds `group_tag` (III field group tag)
    to the Field object.
    """
    def __init__(self, tag, indicators=None, subfields=None, data=None,
                 group_tag=None):
        kwargs = {'tag': tag}
        if data is None:
            kwargs['indicators'] = indicators or [' ', ' ']
            kwargs['subfields'] = subfields or []
        else:
            kwargs['data'] = data
        super(SierraMarcField, self).__init__(**kwargs)
        self.group_tag = group_tag or ' '
        self.full_tag = ''.join((self.group_tag, tag))

    def matches_tag(self, tag):
        """
        Does this field match the provided `tag`? The `tag` may be the
        MARC field tag ('100'), the group_field tag ('a'), or both
        ('a100').
        """
        return tag in (self.tag, self.group_tag, self.full_tag)

    def filter_subfields(self, include=None, exclude=None):
        """
        Filter subfields on this field based on the provided subfields
        to `include` or `exclude`. Both, either, or neither args may
        be provided, and they may be strings ('abcde'), lists, or
        tuples. Conceptually, the set of SFs to exclude is substracted
        from the set of SFs to include; if `include` is None, then the
        "include" set includes all SFs. A subfield listed in `exclude`
        will always be excluded. Include='abcde', exclude='a' will only
        include subfields b, c, d, and e. Include=None and exclude='a'
        will include all subfields except a.

        Produces a generator that yields a sftag, val tuple for each
        matching subfield, in the order they appear on the field.
        """
        incl, excl = include or '', exclude or ''
        for tag, val in self:
            if ((incl and tag in incl) or not incl) and tag not in excl:
                yield (tag, val)


class SierraMarcRecord(pymarc.record.Record):
    """
    Subclass of pymarc record.Record. Changes `get_fields` method to
    enable getting fields by field group tag, for SierraMarcField
    instances.
    """

    def _field_matches_tag(self, field, tag):
        try:
            return field.matches_tag(tag)
        except AttributeError:
            return field.tag == tag

    def get_fields_gen(self, *args):
        """
        Same as `get_fields`, but it's a generator.
        """
        no_args = len(args) == 0
        for f in self.fields:
            if no_args:
                yield f
            else:
                for arg in args:
                    if self._field_matches_tag(f, arg):
                        yield f
                        break

    def get_fields(self, *args):
        """
        Return a list of fields (in the order they appear on the
        record) that match the given list of args. Args may include
        MARC tags ('100'), III field group tags ('a'), or both
        ('a100'). 'a100' would get all a-tagged 100 fields; separate
        'a' and '100' args would get all a-tagged fields and all 100
        fields.

        This returns a list to maintain compatibility with the parent
        class `get_fields` method. Use `get_fields_gen` if you want a
        generator instead.
        """
        return list(self.get_fields_gen(*args))

    def filter_fields(self, include=None, exclude=None):
        """
        Like `get_fields_gen` but lets you provide a list of tags to
        include and a list to exclude. All tags should be ones such as
        what's defined in the `get_fields` docstring.
        """
        include, exclude = include or tuple(), exclude or tuple()
        for f in self.get_fields_gen(*include):
            if all([not self._field_matches_tag(f, ex) for ex in exclude]):
                yield f


def make_mfield(tag, data=None, indicators=None, subfields=None,
                group_tag=None):
    """
    Create a new SierraMarcField object with the given parameters.

    `tag` is required. Creates a control field if `data` is not None,
    otherwise creates a variable-length field. `group_tag` is the
    III variable field group tag character. `subfields`, `indicators`,
    and `group_tag` default to blank values.
    """
    return SierraMarcField(tag, indicators=indicators, subfields=subfields,
                           data=data, group_tag=group_tag)


def explode_subfields(pmfield, sftags):
    """
    Get subfields (`sftags`) if on the given pymarc Field object
    (`pmfield`) and split them into a tuple, where each tuple value
    contains the list of values for the corresponding subfield tag.
    E.g., subfields 'abc' would return a tuple of 3 lists, the first
    corresponding with all subfield 'a' values from the MARC field, the
    second with subfield 'b' values, and the third with subfield 'c'
    values. Any subfields not present become an empty list.

    Use like this:
        title, subtitle, responsibility = explode_subfields(f245, 'abc')
    """
    return (pmfield.get_subfields(tag) for tag in sftags)


def group_subfields(pmfield, include='', exclude='', unique='', start='',
                    end='', limit=None):
    """
    Put subfields from the given `pmfield` pymarc Field object into
    groupings based on the given args. (Subfields in each group remain
    in the order in which they appear in the field.)

    Define which subfield tags to group using `include` or `exclude`.
    These are mutually exclusive; if both are specified, the first
    overrides the second. If neither is specified, all tags are
    included.

    `unique` lists tags that must be unique in each group. A new group
    starts upon encountering a second instance of a unique tag.

    `start` lists tags that immediately signal the start of a new
    group. As soon as one is encountered, it becomes the first tag of
    the next group.

    `end` lists tags that signal the end of a grouping. When one is
    encountered, it becomes the end of that group, and a new group is
    started.
    """
    def _include_tag(tag, include, exclude):
        return (not include and not exclude) or (include and tag in include) or\
               (exclude and tag not in exclude)

    def _finish_group(pmfield, grouped, group, limit=None):
        if not limit or (len(grouped) < limit - 1):
            grouped.append(make_mfield(pmfield.tag, subfields=group,
                                        indicators=pmfield.indicators))
            group = []
        return grouped, group

    def _is_repeated_unique(tag, unique, group):
        return tag in unique and tag in [gi[0] for gi in group]

    grouped, group = [], []
    for tag, value in pmfield:
        if _include_tag(tag, include, exclude):
            if tag in start or _is_repeated_unique(tag, unique, group):
                grouped, group = _finish_group(pmfield, grouped, group, limit)
            group.extend([tag, value])
            if tag in end:
                grouped, group = _finish_group(pmfield, grouped, group, limit)
    if group:
        grouped, group = _finish_group(pmfield, grouped, group)
    return grouped


def pull_from_subfields(pmfield, sftags=None, pull_func=None):
    """
    Extract a list of values from the given pymarc Field object
    (`pmfield`). Optionally specify which `sftags` to pull data from
    and/or a `pull_func` function. The function should take a string
    value (i.e. from one subfield) and return a LIST of values.
    A single flattened list of collective values is returned.
    """
    sftags = tuple(sftags) if sftags else [sf[0] for sf in pmfield]
    vals = pmfield.get_subfields(*sftags)
    if pull_func is None:
        return vals
    return [v2 for v1 in vals for v2 in pull_func(v1)]


class MarcUtils(object):
    marc_relatorcode_map = settings.MARCDATA.RELATOR_CODES
    marc_sourcecode_map = settings.MARCDATA.STANDARD_ID_SOURCE_CODES
    subject_sd_pattern_map = settings.MARCDATA.LCSH_SUBDIVISION_PATTERNS
    subject_sd_term_map = settings.MARCDATA.LCSH_SUBDIVISION_TERM_MAP
    control_sftags = 'w01256789'
    title_sftags_7xx = 'fhklmoprstvx'

    def compile_relator_terms(self, tag, val):
        if tag == '4':
            term = self.marc_relatorcode_map.get(val, None)
            return [term] if term else []
        return [p.strip_wemi(v) for v in p.strip_ends(val).split(', ')]

    def fieldstring_to_field(self, fstring):
        """
        Parse `fstring`, a formatted MARC field string, and generate a
        SierraMarcField object.

        `fstring` may follow several patterns.
            - An LC-docs style string.
                  100   1#$aBullett, Gerald William,$d1894-1958.
            - An OCLC-docs style string.
                  100  1  Bullett ǂd 1894-1958.
            - A MarcEdit-style string.
                  =100  1\$aBullett, Gerald William,$d1894-1958.
            - A III Sierra-style string (field group tag is optional).
                  a100 1  Bullett, Gerald William,|d1894-1958.
            - A combination of the above.
                  a100 1# $aBullett, Gerald William,$d1894-1958.
            - A control field.
                  001 ocn012345678
            - A control field that specifies a char-position range.
                  008/18-21 b###

        Take care with spacing following the field tag.
            - If this is a control field, then any spaces between the
              field tag and first non-space character are removed; the
              first legitimate data value should not be space; use '#'
              or '\' for blank. Spaces may be used throughout the rest
              of the field data.
            - Otherwise, it attempts to determine two indicator values
              which each may be 0-9, space, #, or \. If spaces are used
              for indicator values in combination with spaces used for
              separation, then every attempt is made to interpret them
              correctly but it may be ambiguous. The first non-space
              character will be considered the first indicator value
              unless positioned against the subfield data.
                  `100  1 $a` -- Indicators are 1 and blank.
                  `100  1$a` -- Indicators are blank and 1.
        """
        fstring = re.sub(r'\n\s*', ' ', fstring)
        tag_match = re.match(r'[\s=]*([a-z])?(\d{3})(\/[\d\-]+)?(.*)$', fstring)
        group_tag, tag, cps, remainder = tag_match.groups()
        data, ind, sfs = None, None, None

        if int(tag) < 10:
            data = remainder.lstrip().replace('#', ' ').replace('\\', ' ')
            if cps:
                start = int(cps[1:].split('-')[0])
                data = ''.join([' ' * start, data])
        else:
            rem_match = re.match(r'\s*([\s\d#\\])\s*([\s\d#\\])\s*(?=\S)(.*)$',
                                 remainder)
            ind = ''.join(rem_match.groups()[0:2])
            ind = ind.replace('#', ' ').replace('\\', ' ')
            sf_str = re.sub(r'\s?ǂ(.)\s', r'$\1', rem_match.group(3))
            if sf_str[0] != '$':
                sf_str = '$a{}'.format(sf_str)
            sfs = re.split(r'[\$\|](.)', sf_str)[1:]
        return make_mfield(tag, data=data, subfields=sfs, indicators=ind,
                           group_tag=group_tag)

    def map_subject_subdivision(self, subdivision, sd_parents=[],
                                default_type='topic'):
        """
        Wrapper for:
        django.settings.MARCDATA.subjectmaps.lcsh_sd_to_facet_values.
        See the docstring for that function for a description of the
        parameters and return value.

        You can use class attributes `subject_sd_pattern_map` and
        `subject_sd_term_map` to change what mappings are used here.
        (Or just override this method.)
        """
        return settings.MARCDATA.lcsh_sd_to_facet_values(
            subdivision, sd_parents, default_type, self.subject_sd_pattern_map,
            self.subject_sd_term_map)


class MarcFieldGrouper(object):
    """
    Use this to parse a SierraMarcRecord object into pre-defined groups
    based on (e.g.) MARC tag. This way, if you are doing a lot of
    operations on a MARC record, where you are issuing a lot of
    separate `get_fields` requests, you can pre-partition fields into
    the needed groupings. Effectively, this means you loop over all
    MARC fields in the record ONCE instead of with each call to
    `get_fields`.

    Note that the order of MARC fields from the record is retained, and
    individual fields may appear in multiple groups.

    To use, initialize an object by passing a dict of
    `group_definitions`, where keys are group names and values are
    lists or sets of MARC tags. Then call `make_groups`, passing a
    SierraMarcRecord object whose fields you want to group. It will
    return a dict where keys are group names and values are lists of
    field objects.
    """
    def __init__(self, group_definitions):
        self.group_definitions = group_definitions
        self.inverse_definitions = self.invert_dict(group_definitions)

    @classmethod
    def invert_dict(cls, d):
        """
        Return an inverted dict (values are keys and vice-versa).
        This handles original values that are strings or
        list/tuple/sets. Each value in the return dict is a list, i.e.
        the list of keys from the original dict that included a certain
        value.

        Use this to create a reverse lookup table.
        """
        inverse = {}
        for key, val in d.items():
            if isinstance(val, (list, tuple, set)):
                for item in val:
                    inverse[item] = inverse.get(item, [])
                    inverse[item].append(key)
            else:
                inverse[val] = inverse.get(val, [])
                inverse[val].append(key)
        return inverse

    def make_groups(self, marc_record):
        """
        Return a dict of groups => MARC fields, based on this object's
        group_definitions, for the given `marc_record`.
        """
        registry, groups = set(), {}
        for f in marc_record.fields:
            for tag in (f.tag, f.group_tag, f.full_tag):
                for groupname in self.inverse_definitions.get(tag, []):
                    if (f, groupname) not in registry:
                        groups[groupname] = groups.get(groupname, [])
                        groups[groupname].append(f)
                        registry.add((f, groupname))
        return groups


class SequentialMarcFieldParser(object):
    """
    Parse a pymarc Field obj by parsing subfields sequentially.

    This is a skeletal base class; subclass to create parser classes
    for looping through all subfields in a MARC field (in the order
    they appear) and returning a result.

    The `parse` method is the active method; it calls `parse_subfield`
    on each subfield tag/val pair in the field, in order. When done, it
    calls `do_post_parse`, and then returns results via
    `compile_results`. Define these methods in your subclass in order
    to parse a field. (See PersonalNameParser and OrgEventParser for
    sample implementations.)
    """
    def __init__(self, field):
        self.field = field
        self.utils = MarcUtils()
        self.materials_specified = []

    def __call__(self):
        return self.parse()

    def parse_subfield(self, tag, val):
        """
        This method is called once for each subfield on a field, in the
        order it occurs. No explicit return value is needed, but if one
        is provided that evaluates to a True value, then it signals the
        end of parsing (even if it is not the last subfield) and breaks
        out of the loop.
        """
        pass

    def parse_materials_specified(self, val):
        """
        Default method for handling materials specified ($3).
        """
        return p.strip_ends(val)

    def do_post_parse(self):
        """
        This is called after the repeated loop that calls
        `parse_subfield` ends, so you can take care of any cleanup
        before results are compiled.
        """
        pass

    def compile_results(self):
        """
        After a field has been completely parsed, this is called last
        to compile and return results.
        """
        return None

    def parse(self):
        """
        This is the "main" method for objects of this type. This is
        what should be called in order to parse a field.
        """
        look_for_materials_specified = True
        for tag, val in self.field:
            if look_for_materials_specified:
                if tag == '3':
                    ms_val = self.parse_materials_specified(val)
                    self.materials_specified.append(ms_val)
                else:
                    look_for_materials_specified = False
            if self.parse_subfield(tag, val):
                break
        self.do_post_parse()
        return self.compile_results()


class PersonalNameParser(SequentialMarcFieldParser):
    relator_sftags = 'e4'
    done_sftags = 'fhklmoprstvxz'
    ignore_sftags = 'iw012356789'

    def __init__(self, field):
        super(PersonalNameParser, self).__init__(field)
        self.heading_parts = []
        self.relator_terms = OrderedDict()
        self.parsed_name = {}
        self.main_name = ''
        self.titles = []
        self.numeration = ''
        self.fuller_form_of_name = ''

    def do_relators(self, tag, val):
        for relator_term in self.utils.compile_relator_terms(tag, val):
            self.relator_terms[relator_term] = None

    def do_titles(self, tag, val):
        self.titles.extend([v for v in p.strip_ends(val).split(', ')])

    def do_numeration(self, tag, val):
        self.numeration = p.strip_ends(val)

    def do_fuller_form_of_name(self, tag, val):
        ffn = re.sub(r'^(?:.*\()?([^\(\)]+)(?:\).*)?$', r'\1', val)
        self.fuller_form_of_name = p.strip_ends(ffn)

    def parse_subfield(self, tag, val):
        if tag in self.done_sftags:
            return True
        elif tag in self.relator_sftags:
            self.do_relators(tag, val)
        elif tag not in self.ignore_sftags:
            self.heading_parts.append(val)
            if tag == 'a':
                self.main_name = val
            elif tag == 'b':
                self.do_numeration(tag, val)
            elif tag == 'c':
                self.do_titles(tag, val)
            elif tag == 'q':
                self.do_fuller_form_of_name(tag, val)

    def compile_results(self):
        heading = p.normalize_punctuation(' '.join(self.heading_parts))
        parsed_name = p.person_name(self.main_name, self.field.indicators)
        return {
            'heading': p.strip_ends(heading) or None,
            'relations': list(self.relator_terms.keys()) or None,
            'forename': parsed_name.get('forename', None),
            'surname': parsed_name.get('surname', None),
            'numeration': self.numeration or None,
            'person_titles': self.titles,
            'type': 'person',
            'fuller_form_of_name': self.fuller_form_of_name or None,
            'materials_specified': self.materials_specified or None
        }


class OrgEventNameParser(SequentialMarcFieldParser):
    event_info_sftags = 'cdgn'
    done_sftags = 'fhklmoprstvxz'
    ignore_sftags = '3i'

    def __init__(self, field):
        super(OrgEventNameParser, self).__init__(field)
        self.relator_terms = OrderedDict()
        if field.tag.endswith('10'):
            self.subunit_sftag = 'b'
            self.relator_sftags = 'e4'
            self.field_type = 'X10'
        else:
            self.subunit_sftag = 'e'
            self.relator_sftags = 'j4'
            self.field_type = 'X11'
        self.parts = {'org': [], 'event': [], 'combined': []}
        self._stacks = {'org': [], 'event': [], 'combined': []}
        self._event_info, self._prev_part_name, self._prev_tag = [], '', ''
        self.is_jurisdiction = self.field.indicator1 == '1'

    def do_relators(self, tag, val):
        for relator_term in self.utils.compile_relator_terms(tag, val):
            self.relator_terms[relator_term] = None

    def sf_is_first_subunit_of_jd_field(self, tag):
        is_jurisdiction = tag == self.subunit_sftag and self.is_jurisdiction
        return (tag == 'q' or is_jurisdiction) and self._prev_tag == 'a'

    def _build_unit_name(self, part_type):
        unit_name = self._prev_part_name
        other_part_type = 'org' if part_type == 'event' else 'event'
        if self._stacks[part_type]:
            context = ' '.join(self._stacks[part_type])
            unit_name = '{}, {}'.format(context, unit_name)
            self._stacks['org'], self._stacks['event'] = [], []
        self._stacks[other_part_type].append(self._prev_part_name)
        return unit_name

    def _build_event_info(self):
        return p.strip_ends(' '.join(self._event_info))

    def do_unit(self):
        org_parts, event_parts = [], []
        event_info = None
        if self._event_info:
            event_info = self._build_event_info()
            event_parts.append({'name': self._build_unit_name('event'),
                                'qualifier': event_info})

            # Even if this is clearly an event, if it's the first thing
            # in an X10 field, record it as an org as well.
            if self.field_type == 'X10' and not self.parts['org']:
                org_parts.append({'name': self._prev_part_name})
                if self._stacks['org']:
                    self._stacks['org'].pop()
        elif self.field_type == 'X11' and not self.parts['event']:
            part = {'name': self._build_unit_name('event')}
            event_parts.append(part)
        else:
            part = {'name': self._build_unit_name('org')}
            org_parts.append(part)

        combined_part = {'name': self._prev_part_name}
        if event_info:
            combined_part['qualifier'] = event_info
        return org_parts, event_parts, [combined_part]

    def parse_subfield(self, tag, val):
        if tag in self.done_sftags:
            return True
        elif tag in (self.relator_sftags):
            for relator_term in self.utils.compile_relator_terms(tag, val):
                self.relator_terms[relator_term] = None
        elif tag in self.event_info_sftags:
            self._event_info.append(val)
        elif tag == 'a':
            self._prev_part_name = p.strip_ends(val)
        elif self.sf_is_first_subunit_of_jd_field(tag):
            part = '{} {}'.format(self._prev_part_name, p.strip_ends(val))
            self._prev_part_name = part
        elif tag == self.subunit_sftag:
            new_org_parts, new_event_parts, new_combined_parts = self.do_unit()
            self.parts['org'].extend(new_org_parts)
            self.parts['event'].extend(new_event_parts)
            self.parts['combined'].extend(new_combined_parts)
            self._event_info = []
            self._prev_part_name = p.strip_ends(val)
        self._prev_tag = tag

    def do_post_parse(self):
        new_org_parts, new_event_parts, new_combined_parts = self.do_unit()
        self.parts['org'].extend(new_org_parts)
        self.parts['event'].extend(new_event_parts)
        self.parts['combined'].extend(new_combined_parts)

    def compile_results(self):
        ret_val = []
        relators = list(self.relator_terms.keys()) or None
        needs_combined = self.parts['org'] and self.parts['event']
        for part_type in ('org', 'event', 'combined'):
            do_it = part_type in ('org', 'event') or needs_combined
            if do_it and self.parts[part_type]:
                ret_val.append({
                    'relations': relators,
                    'heading_parts': self.parts[part_type],
                    'type': 'organization' if part_type == 'org' else part_type,
                    'is_jurisdiction': self.is_jurisdiction,
                    'materials_specified': self.materials_specified or None
                })
                relators = None
        return ret_val


class HierarchicalTitlePartAnalyzer(object):
    def __init__(self, isbd_punct_mapping):
        super(HierarchicalTitlePartAnalyzer, self).__init__()
        self.isbd_punct_mapping = isbd_punct_mapping

    def pop_isbd_punct_from_title_part(self, part):
        end_punct = part.strip()[-1]
        if end_punct in self.isbd_punct_mapping:
            chars_to_strip = '{} '.format(end_punct)
            return part.strip(chars_to_strip), end_punct
        return part, ''

    def what_type_is_this_part(self, prev_punct, flags):
        ptype = self.isbd_punct_mapping.get(prev_punct, '')
        lock_same_title = flags.get('lock_same_title', False)
        default_to_new_part = flags.get('default_to_new_part', False)
        if not ptype or (ptype == 'new_title' and lock_same_title):
            return 'new_part' if default_to_new_part else 'same_part'
        return ptype


class TranscribedTitleParser(SequentialMarcFieldParser):
    variant_types = {
            '0': '',
            '1': 'Title translation',
            '2': 'Issue title',
            '3': 'Other title',
            '4': 'Cover title',
            '5': 'Added title page title',
            '6': 'Caption title',
            '7': 'Running title',
            '8': 'Spine title'
        }
    fields_with_nonfiling_chars = ('242', '245')
    f247_display_text = 'Former title'

    def __init__(self, field):
        super(TranscribedTitleParser, self).__init__(field)
        self.prev_punct = ''
        self.prev_tag = ''
        self.part_type = ''
        self.flags = {}
        self.lock_parallel = False
        self.lock_subfield_c = False
        self.title_parts = []
        self.responsibility = ''
        self.display_text = ''
        self.issn = ''
        self.lccn = ''
        self.language_code = ''
        self.nonfiling_chars = 0
        self.titles = []
        self.parallel_titles = []
        self.analyzer = HierarchicalTitlePartAnalyzer({
            ';': 'new_title',
            ':': 'same_part',
            '/': 'responsibility',
            '=': 'parallel_title',
            ',': 'same_part',
            '.': 'new_part'
        })

        if field.tag in self.fields_with_nonfiling_chars:
            if field.indicator2 in [str(i) for i in range(0, 10)]:
                self.nonfiling_chars = int(field.indicator2)

    def start_next_title(self, is_last=False):
        title, is_parallel = {}, self.lock_parallel
        if self.title_parts:
            parts = [p.compress_punctuation(tp) for tp in self.title_parts]
            title['parts'] = parts
            self.title_parts = []
        if self.responsibility:
            no_prev_r = self.titles and 'responsibility' not in self.titles[-1]
            responsibility = p.compress_punctuation(self.responsibility)
            if is_parallel and no_prev_r:
                self.titles[-1]['responsibility'] = responsibility
            else:
                title['responsibility'] = responsibility
            self.responsibility = ''
        if title:
            if is_last and is_parallel and self.lock_subfield_c:
                if title.get('parts') and not title.get('responsibility'):
                    title['responsibility'] = '; '.join(title['parts'])
                    del(title['parts'])
            if is_parallel and self.titles:
                last_title = self.titles[-1]
                last_title['parallel'] = last_title.get('parallel', [])
                last_title['parallel'].append(title)
                self.titles[-1] = last_title
            else:
                self.titles.append(title)
        self.lock_parallel = False

    def join_parts(self, last_part, part, prev_punct):
        if self.flags['subpart_may_need_formatting']:
            if self.flags['is_bulk_following_incl_dates']:
                if re.match(r'\d', part[0]):
                    prev_punct = ''
                    part = '(bulk {})'.format(part)
            if re.match(r'\w', last_part[-1]) and re.match(r'\w', part[0]):
                prev_punct = prev_punct or ','
        return '{}{} {}'.format(last_part, prev_punct, part)

    def push_title_part(self, part, prev_punct, part_type=None):
        if part_type is None:
            part_type = self.analyzer.what_type_is_this_part(prev_punct,
                                                             self.flags)
        part = p.restore_periods(part)
        if self.flags['is_245b'] and re.match('and [A-Z]', part):
            part = part.lstrip('and ')
            part_type = 'new_title'
        if part_type == 'same_part' and len(self.title_parts):
            part = self.join_parts(self.title_parts[-1], part, prev_punct)
            self.title_parts[-1] = part
        elif part_type in ('new_title', 'parallel_title'):
            self.start_next_title()
            self.title_parts = [part]
            if part_type == 'parallel_title':
              self.lock_parallel = True
        elif part_type == 'responsibility':
            self.responsibility = part
            if self.field.tag != '490':
                self.start_next_title()
        else:
            # i.e.:
            # if part_type == 'new_part', or
            # if part_type =='same_part' but len(self.title_parts) == 0
            self.title_parts.append(part)

    def append_volume(self, part):
        vol_sep, volume = format_volume(p.restore_periods(part))
        if len(self.title_parts):
            part = vol_sep.join((self.title_parts.pop(), volume))
        self.title_parts.append(part)

    def split_compound_title(self, tstring, handle_internal_periods):
        prev_punct = self.prev_punct
        int_punct_re = r'\s+[;=]\s+'
        int_periods_re = r'|\.\s?' if handle_internal_periods else ''
        split_re = r'({}{})'.format(int_punct_re, int_periods_re)
        for i, val in enumerate(re.split(split_re, tstring)):
            if i % 2 == 0:
                yield prev_punct, val
            else:
                prev_punct = val.strip()

    def do_compound_title_part(self, part, handle_internal_periods):
        comp_tparts = self.split_compound_title(part, handle_internal_periods)
        for prev_punct, subpart in comp_tparts:
            if subpart:
                self.push_title_part(subpart, prev_punct)

    def split_sor_from_tstring(self, tstring):
        sor_parts = re.split(r'(\s+=\s+|\.\s?)', tstring, 1)
        if len(sor_parts) == 3:
            sor, end_punct, rem_tstring = sor_parts
            return sor, end_punct.strip(), rem_tstring
        sor = sor_parts[0] if len(sor_parts) else ''
        return sor, '', ''

    def split_title_and_sor(self, tstring):
        title_and_remaining = tstring.split(' / ', 1)
        title = title_and_remaining[0]
        rem = title_and_remaining[1] if len(title_and_remaining) > 1 else ''
        return title, rem

    def do_titles_and_sors(self, tstring, is_subfield_c):
        tstring = p.normalize_punctuation(tstring, periods_protected=True,
                                          punctuation_re=r'[\.\/;:,=]')
        do_sor_next = is_subfield_c
        handle_internal_periods = is_subfield_c or self.field.tag == '490'
        while tstring:
            if do_sor_next:
                sor, end_punct, tstring = self.split_sor_from_tstring(tstring)
                self.push_title_part(sor, self.prev_punct, 'responsibility')
                self.prev_punct = end_punct
            if tstring:
                title_part, tstring = self.split_title_and_sor(tstring)
                self.do_compound_title_part(title_part, handle_internal_periods)
                do_sor_next = True

    def get_flags(self, tag, val):
        if self.field.tag == '490':
            def_to_newpart = tag == 'a'
        else:
            def_to_newpart = tag == 'n' or (tag == 'p' and self.prev_tag != 'n')
        is_bdates = tag == 'g' and self.field.tag == '245'
        valid_tags = 'alxv3' if self.field.tag == '490' else 'abcfghiknpsxy'
        is_valid = val.strip() and tag in valid_tags
        return {
            'default_to_new_part': def_to_newpart,
            'lock_same_title': tag in 'fgknps',
            'is_valid': is_valid,
            'is_display_text': tag == 'i',
            'is_main_part': tag in 'ab',
            'is_245b': tag == 'b' and self.field.tag == '245',
            'is_subpart': tag in 'fgknps',
            'is_subfield_c': tag == 'c',
            'is_lccn': tag == 'l' and self.field.tag == '490',
            'is_issn': tag == 'x',
            'is_volume': tag == 'v' and self.field.tag == '490',
            'is_language_code': tag == 'y',
            'is_bulk_following_incl_dates': is_bdates and self.prev_tag == 'f',
            'subpart_may_need_formatting': tag in 'fgkps'
        }

    def parse_subfield(self, tag, val):
        self.flags = self.get_flags(tag, val)
        if self.flags['is_valid']:
            prot = p.protect_periods(val)

            isbd = r''.join(list(self.analyzer.isbd_punct_mapping.keys()))
            switchp = r'"\'~\.,\)\]\}}'
            is_245bc = self.flags['is_245b'] or self.flags['is_subfield_c']
            if is_245bc or self.field.tag == '490':
                p_switch_re = r'([{}])(\s*[{}]+)(\s|$)'.format(isbd, switchp)
            else:
                p_switch_re = r'([{}])(\s*[{}]+)($)'.format(isbd, switchp)
            prot = re.sub(p_switch_re, r'\2\1\3', prot)

            part, end_punct = self.analyzer.pop_isbd_punct_from_title_part(prot)
            if part:
                if self.flags['is_display_text']:
                    self.display_text = p.restore_periods(part)
                elif self.flags['is_main_part']:
                    if self.flags['is_245b']:
                        self.do_compound_title_part(part, False)
                    elif self.field.tag == '490':
                        self.do_titles_and_sors(part, False)
                    else:
                        self.push_title_part(part, self.prev_punct)
                elif self.flags['is_subfield_c']:
                    self.lock_subfield_c = True
                    self.do_titles_and_sors(part, True)
                    return True
                elif self.flags['is_subpart'] and part:
                    self.push_title_part(part, self.prev_punct)
                elif self.flags['is_issn']:
                    self.issn = part
                elif self.flags['is_language_code']:
                    self.language_code = part
                elif self.flags['is_volume']:
                    self.append_volume(part)
                elif self.flags['is_lccn']:
                    lccn = p.strip_outer_parentheses(part)
                    self.lccn = p.restore_periods(lccn)

            self.prev_punct = end_punct
            self.prev_tag = tag

    def do_post_parse(self):
        self.start_next_title(is_last=True)

    def compile_results(self):
        display_text = ''
        if self.field.tag == '242':
            display_text = self.variant_types['1']
            if self.language_code:
                lang = settings.MARCDATA.LANGUAGE_CODES.get(self.language_code,
                                                            None)
                display_text = '{}, {}'.format(display_text, lang)
        if self.field.tag == '246':
            ind2 = self.field.indicators[1]
            display_text = self.display_text or self.variant_types.get(ind2, '')

        if self.field.tag == '247':
            display_text = self.f247_display_text

        ret_val = {
            'transcribed': self.titles,
            'nonfiling_chars': self.nonfiling_chars
        }
        if self.materials_specified:
            ret_val['materials_specified'] = self.materials_specified
        if display_text:
            ret_val['display_text'] = display_text
        if self.issn:
            ret_val['issn'] = self.issn
        if self.lccn:
            ret_val['lccn'] = self.lccn
        return ret_val


class PreferredTitleParser(SequentialMarcFieldParser):
    title_only_fields = ('130', '240', '243', '630', '730', '740', '830')
    name_title_fields = ('600', '610', '611', '700', '710', '711', '800',
                         '810', '811')
    main_title_fields = ('130', '240', '243')
    subject_fields = ('600', '610', '611', '630')
    nonfiling_char_ind1_fields = ('130', '630', '730', '740')
    nonfiling_char_ind2_fields = ('240', '243', '830')
    nt_title_tags = 'tfklmoprs'
    subpart_tags = 'dgknpr'
    expression_tags = 'flos'
    subject_sd_tags = 'vxyz'

    def __init__(self, field, utils=None):
        super(PreferredTitleParser, self).__init__(field)
        self.utils = utils or MarcUtils()
        self.relator_terms = OrderedDict()
        self.prev_punct = ''
        self.prev_tag = ''
        self.flags = {}
        self.lock_title = False
        self.lock_expression_info = False
        self.seen_subpart = False
        self.primary_title_tag = 't'
        self.display_constants = []
        self.title_parts = []
        self.expression_parts = []
        self.languages = []
        self.volume = ''
        self.issn = ''
        self.nonfiling_chars = 0
        self.title_is_collective = field.tag == '243'
        self.title_is_music_form = False
        self.title_type = ''
        self.analyzer = HierarchicalTitlePartAnalyzer({
            ';': 'start_version_info',
            ',': 'same_part',
            '.': 'new_part'
        })

        if field.tag.endswith('11'):
            self.relator_sftags = 'j4'
        else:
            self.relator_sftags = 'e4'

        if field.tag in self.title_only_fields:
            self.lock_title = True
            self.primary_title_tag = 'a'

        ind_val = None
        if field.tag in self.nonfiling_char_ind1_fields:
            ind_val = field.indicator1
        elif field.tag in self.nonfiling_char_ind2_fields:
            ind_val = field.indicator2
        if ind_val is not None and ind_val in [str(i) for i in range(0, 10)]:
            self.nonfiling_chars = int(ind_val)

        if field.tag.startswith('8'):
            self.title_type = 'series'
            self.nt_title_tags = '{}vx'.format(self.nt_title_tags)
        elif field.tag.startswith('7'):
            if field.indicator2 == '2':
                self.title_type = 'analytic'
            else:
                self.title_type = 'related'
        elif field.tag in self.subject_fields:
            self.title_type = 'subject'
        elif field.tag in self.main_title_fields:
            self.title_type = 'main'

    def do_relators(self, tag, val):
        for relator_term in self.utils.compile_relator_terms(tag, val):
            self.relator_terms[relator_term] = None

    def join_parts(self, last_part, part, prev_punct):
        if re.match(r'\w', last_part[-1]) and re.match(r'\w', part[0]):
            prev_punct = prev_punct or ','
        return '{}{} {}'.format(last_part, prev_punct, part)

    def force_new_part(self):
        if len(self.title_parts):
            if self.prev_tag == 'k' and self.title_parts[-1] == 'Selections':
                return True
            if self.flags['first_subpart'] and self.title_is_collective:
                return True
        return False

    def push_title_part(self, part, prev_punct):
        part_type = self.analyzer.what_type_is_this_part(prev_punct, self.flags)
        part = p.restore_periods(part)
        force_new = self.force_new_part()
        if not force_new and part_type == 'same_part' and len(self.title_parts):
            part = self.join_parts(self.title_parts[-1], part, prev_punct)
            self.title_parts[-1] = part
        else:
            if force_new and part:
                part = '{}{}'.format(part[0].upper(), part[1:])
            self.title_parts.append(part)

    @classmethod
    def describe_collective_title(cls, title_part, is_243=False,
                                  might_include_instrument=False):
        is_collective, is_music_form = is_243, False
        norm_part = title_part.lower()
        if not is_collective:
            is_expl_ct = norm_part in settings.MARCDATA.COLLECTIVE_TITLE_TERMS
            is_legal_ct = re.search(r's, etc\W?$', norm_part)
            is_music_ct = re.search(r'\smusic(\s+\(.+\)\s*)?$', norm_part)
            if is_expl_ct or is_music_ct or is_legal_ct:
                is_collective = True
        if might_include_instrument:
            norm_part = norm_part.split(', ')[0]
        if norm_part in settings.MARCDATA.MUSIC_FORM_TERMS_ALL:
            is_music_form = True
            is_plural = norm_part in settings.MARCDATA.MUSIC_FORM_TERMS_PLURAL
            is_collective = is_plural
        return is_collective, is_music_form

    def parse_languages(self, lang_str):
        return [l for l in re.split(r', and | and | & |, ', lang_str) if l]

    def get_flags(self, tag, val):
        if not self.lock_title:
            self.lock_title = tag in self.nt_title_tags
        if self.lock_expression_info:
            is_subpart = False
        else:
            is_subpart = tag in self.subpart_tags
            self.lock_expression_info = tag in self.expression_tags
        def_to_new_part = tag == 'n' or (tag == 'p' and self.prev_tag != 'n')
        is_control = tag in self.utils.control_sftags or tag == '3'
        is_valid_title_part = self.lock_title and val.strip() and not is_control
        return {
            'is_display_const': tag == 'i',
            'is_valid_title_part': is_valid_title_part,
            'is_main_part': tag == self.primary_title_tag,
            'is_subpart': is_subpart,
            'first_subpart': not self.seen_subpart and is_subpart,
            'is_perf_medium': tag == 'm',
            'is_language': tag == 'l',
            'is_arrangement': tag == 'o',
            'is_volume': tag == 'v' and self.title_type == 'series',
            'is_issn': tag == 'x' and self.title_type == 'series',
            'default_to_new_part': def_to_new_part,
        }

    def parse_subfield(self, tag, val):
        if self.title_type == 'subject' and tag in self.subject_sd_tags:
            return None
        self.flags = self.get_flags(tag, val)
        if tag in (self.relator_sftags):
            for relator_term in self.utils.compile_relator_terms(tag, val):
                self.relator_terms[relator_term] = None
        elif self.flags['is_display_const']:
            display_val = p.strip_ends(p.strip_wemi(val))
            if display_val.lower() == 'container of':
                self.title_type = 'analytic'
            else:
                self.display_constants.append(display_val)
        elif self.flags['is_valid_title_part']:
            prot = p.protect_periods(val)
            part, end_punct = self.analyzer.pop_isbd_punct_from_title_part(prot)
            if part:
                if self.flags['is_main_part']:
                    is_243 = self.field.tag == '243'
                    coll, mf = self.describe_collective_title(part, is_243)
                    self.title_is_collective = coll
                    self.title_is_music_form = mf
                    self.push_title_part(part, self.prev_punct)
                elif self.flags['is_perf_medium']:
                    if self.title_is_collective:
                        self.title_is_music_form = True
                    self.push_title_part(part, self.prev_punct)
                elif self.flags['is_subpart']:
                    self.push_title_part(part, self.prev_punct)
                    self.seen_subpart = True
                elif self.flags['is_volume']:
                    self.volume = p.restore_periods(part)
                elif self.flags['is_issn']:
                    self.issn = p.restore_periods(part)
                else:
                    if self.flags['is_language']:
                        self.languages = self.parse_languages(part)
                    if self.flags['is_arrangement'] and part.startswith('arr'):
                        part = 'arranged'
                    self.expression_parts.append(p.restore_periods(part))
            self.prev_punct = end_punct
        self.prev_tag = tag

    def compile_results(self):
        ret_val = {
            'relations': list(self.relator_terms.keys()) or None,
            'nonfiling_chars': self.nonfiling_chars,
            'materials_specified': self.materials_specified,
            'display_constants': self.display_constants,
            'title_parts': self.title_parts,
            'expression_parts': self.expression_parts,
            'languages': self.languages,
            'is_collective': self.title_is_collective,
            'is_music_form': self.title_is_music_form,
            'type': self.title_type
        }
        if self.title_type == 'series':
            ret_val['volume'] = self.volume
            ret_val['issn'] = self.issn
        return ret_val


class EditionParser(SequentialMarcFieldParser):
    edition_types = {
        '250': 'edition_statement',
        '251': 'version',
        '254': 'musical_presentation_statement'
    }
    def __init__(self, field):
        super(EditionParser, self).__init__(field)
        self.edition_type = self.edition_types.get(field.tag)
        self._edition_stack = []

    def extract_info_from_edition_statement(self, stmt):
        edition_info = {}

        # The idea with the below regex and `re.split` call is to split
        # off any secondary (or tertiary, etc.) statements, usually
        # identifying a revision of a specific edition. These are set
        # off with ", " and then a number or capitalized word, like:
        #    3rd ed. / by John Doe, New revision / by J. Smith.
        #                         ---
        # However, there is a problem with false positives. So this
        # uses the following (more constrained) heuristics.
        #    - Look for ", " following parallel information (" = ").
        #      These are less likely to be false positives, and it's
        #      more important to know where the translation ends. E.g.,
        #      "3rd ed. = Tercer ed., New rev. = Nuevo rev." Not
        #      splitting on ", " here would put "New rev." with the
        #      Spanish translation for "3rd ed."
        #    - The ", " must be followed by any one of three patterns
        #      that occur before the next comma or ISBD punctuation:
        #        - A number plus any number of words.
        #        - One capitalized word ONLY.
        #        - One capitalized word plus any number of words, at
        #          least one of which must not be capitalized.
        rev_re = (r'(.*? = .*?), (?='
                  r'(?:[0-9][^,=/]+|[A-Z][^,=/\s]+|[A-Z][^,=/]+ [a-z][^,=/]+)'
                  r'(?: [/=]|$))')
        for ed in re.split(rev_re, stmt):
            values, sors = [], []
            lock_sor = False
            for p_chunk in ed.split(' = '):
                sor_chunks = [p.strip_ends(v) for v in p_chunk.split(' / ')]
                if len(sor_chunks) == 1:
                    if lock_sor:
                        sors.append(sor_chunks[0])
                    else:
                        values.append(sor_chunks[0])
                else:
                    values.append(sor_chunks[0])
                    sors.append(', '.join(sor_chunks[1:]))
                    lock_sor = True

            for i, pair in enumerate(itertools.zip_longest(values, sors)):
                value, sor = pair
                entry = {}
                if value:
                    entry['value'] = p.restore_periods(value)
                if sor:
                    entry['responsibility'] = p.restore_periods(sor)
                if entry:
                    key = 'editions' if i == 0 else 'parallel'
                    edition_info[key] = edition_info.get(key, [])
                    edition_info[key].append(entry)
        return edition_info

    def parse_subfield(self, tag, val):
        if val:
            val = p.protect_periods(val).strip()
            if tag == 'b' and self._edition_stack:
                last_val = self._edition_stack[-1]
                if len(last_val) > 1 and last_val[-2:] not in (' /', ' ='):
                    self._edition_stack[-1] = ' '.join([last_val, '/'])
            if tag in 'ab':
                self._edition_stack.append(val)

    def compile_results(self):
        ed_info = {}
        if self._edition_stack:
            if self.edition_type == 'edition_statement':
                isbd_str = ' '.join(self._edition_stack)
                ed_info = self.extract_info_from_edition_statement(isbd_str)
            else:
                ed = '; '.join([p.strip_ends(v) for v in self._edition_stack])
                ed_info = {'editions': [{'value': p.restore_periods(ed)}]}

        return {
            'edition_type': self.edition_type,
            'edition_info': ed_info or None,
            'materials_specified': self.materials_specified or None
        }


class StandardControlNumberParser(SequentialMarcFieldParser):
    standard_num_fields = ('020', '022', '024', '025', '026', '027', '028',
                           '030', '074', '088')
    control_num_fields = ('010', '016', '035')
    f024_ind1_types = {
        '0': 'isrc',
        '1': 'upc',
        '2': 'ismn',
        '3': 'ean',
        '4': 'sici',
        '8': 'unknown',
    }
    other_types = {
        '010': 'lccn',
        '016': 'lac',
        '020': 'isbn',
        '022': 'issn',
        '025': 'oan',
        '026': 'fingerprint',
        '027': 'strn',
        '030': 'coden',
        '035': 'oclc',
        '074': 'gpo',
        '088': 'report'
    }
    oclc_suffix_separator = '/'

    def __init__(self, field, utils=None):
        super(StandardControlNumberParser, self).__init__(field)
        self.utils = utils or MarcUtils()
        self.numbers = []
        self.qualifiers = []
        self.publisher_name = ''
        self.fingerprint_parts = []

        if field.tag == '024':
            self.ntype = self.f024_ind1_types.get(field.indicator1)
        elif field.tag == '028':
            self.ntype = 'dn' if field.indicator1 == '6' else 'pn'
        else:
            self.ntype = self.other_types.get(field.tag)

    @classmethod
    def split_cn_and_source(cls, data):
        try:
            source, num = data[1:].split(')', 1)
        except ValueError:
            source, num = None, data
        return source, num.strip()

    def clean_isbn(self, isbn):
        match = re.search(r'(?:^|\s)([\dX\-]+)[^\(]*(?:\((.+)\))?', isbn)
        if match:
            number, qstr = match.groups()
            if number:
                number = ''.join(number.split('-'))
                qualifiers = self.clean_qualifier(qstr) if qstr else None
                return number, qualifiers
        return None, None

    def clean_oclc_num(self, oclc_num):
        ntype, norm = self.split_cn_and_source(oclc_num)
        ntype = ntype or 'unknown'
        if ntype == 'OCoLC':
            ntype = None
            norm = re.sub('^[A-Za-z0]+', r'', norm)
        return norm, ntype

    @classmethod
    def normalize_lccn(cls, lccn):
        lccn = ''.join(lccn.split(' ')).split('/')[0]
        if '-' in lccn:
            left, right = lccn.split('-', 1)
            lccn = ''.join((left, right.zfill(6)))
        return lccn

    def clean_qualifier(self, qstr):
        cleaned = re.sub(r'[\(\)]', r'', p.strip_ends(qstr))
        return re.split(r'\s*[,;:]\s+', cleaned)

    def generate_validity_label(self, tag):
        if tag == 'z':
            return 'Canceled' if self.ntype == 'issn' else 'Invalid'
        return 'Canceled' if tag == 'm' else 'Incorrect' if tag == 'y' else None

    def generate_type_label(self, ntype):
        label = self.utils.marc_sourcecode_map.get(ntype)
        if label and self.publisher_name:
            label = '{}, {}'.format(label, self.publisher_name)
        return label

    def parse_subfield(self, tag, val):
        if self.field.tag == '026' and tag in 'abcde':
            self.fingerprint_parts.append(val)
        elif tag in 'almyz' or (self.field.tag == '010' and tag == 'b'):
            entry = {
                'is_valid': tag in 'abl',
                'invalid_label': self.generate_validity_label(tag)
            }
            val = val.strip()
            if self.ntype == 'isbn':
                val, qualifiers = self.clean_isbn(val)
                if qualifiers:
                    self.qualifiers.extend(qualifiers)
            elif self.ntype == 'issn' and tag in 'lm':
                entry['type'] = 'issnl'
            elif self.ntype == 'lccn':
                norm = self.normalize_lccn(val)
                if norm != val:
                    entry['normalized'] = norm
                if tag == 'b':
                    entry['type'] = 'nucmc'
            elif self.ntype == 'oclc':
                val, ntype = self.clean_oclc_num(val)
                if ntype:
                    entry['type'] = ntype
                else:
                    val, sep, suffix = val.partition(self.oclc_suffix_separator)
                    if sep:
                        entry['oclc_suffix'] = ''.join([sep, suffix])
            if val:
                entry['number'] = val
                self.numbers.append(entry)
        elif tag in 'qc':
            qualifiers = self.clean_qualifier(val)
            if qualifiers:
                self.qualifiers.extend(qualifiers)
        elif tag == 'b' and self.field.tag == '028':
            self.publisher_name = val
        elif tag == 'd' and self.field.tag == '024':
            if self.numbers and 'number' in self.numbers[-1]:
                num = self.numbers[-1]['number']
                self.numbers[-1]['number'] = ' '.join([num, val])
        elif tag == '2' and self.field.tag in ('016', '024'):
            self.ntype = val

    def compile_results(self):
        if self.field.tag == '026':
            self.numbers = [{'number': ' '.join(self.fingerprint_parts),
                             'is_valid': True,
                             'invalid_label': None}]
        for n in self.numbers:
            n['qualifiers'] = self.qualifiers
            n['type'] = n.get('type') or self.ntype
            n['type_label'] = self.generate_type_label(n['type'])
        return self.numbers


class LanguageParser(SequentialMarcFieldParser):
    """
    Parse 041/377 fields to extract detailed language information.
    """
    category_map = (
        ('a', 'Item content'),
        ('h', 'Translated from (original)'),
        ('k', 'Intermediate translations'),
        ('b', 'Summary or abstract'),
        ('f', 'Table of contents'),
        ('i', 'Intertitles'),
        ('j', 'Subtitles'),
        ('p', 'Captions'),
        ('q', 'Accessible audio'),
        ('r', 'Accessible visual language'),
        ('t', 'Transcripts'),
        ('e', 'Librettos'),
        ('n', 'Librettos translated from (original)'),
        ('g', 'Accompanying materials'),
        ('m', 'Accompanying materials translated from (original)'),
    )

    def __init__(self, field, utils=None):
        super(LanguageParser, self).__init__(field)
        self.utils = utils or MarcUtils()
        self.languages = OrderedDict()
        self.categorized = OrderedDict()

    @classmethod
    def generate_language_notes_display(cls, cat):
        vals = []
        for key, label in cls.category_map:
            if cat.get(key):
                languages = ', '.join(cat[key])
                vals.append(': '.join((label, languages)))
        return vals

    def parse_subfield(self, tag, val):
        if tag not in self.utils.control_sftags:
            cat = 'a' if self.field.tag == '377' or tag == 'd' else tag
            if self.field.tag == '377' and tag == 'l':
                language = p.strip_ends(val)
            else:
                language = settings.MARCDATA.LANGUAGE_CODES.get(val)

            if language:
                self.languages[language] = None
                self.categorized[cat] = self.categorized.get(cat, OrderedDict())
                self.categorized[cat][language] = None

    def compile_results(self):
        return {
            'languages': list(self.languages.keys()),
            'categorized': {k: list(v.keys()) for k, v in self.categorized.items()}
        }


class LinkingFieldParser(SequentialMarcFieldParser):
    """
    Parse linking fields (76X-78X) to extract detailed information.
    """
    display_label_map = {
        '765  ': 'Translation of',
        '767  ': 'Translated as',
        '770  ': 'Supplement',
        '772  ': 'Supplement to',
        '772 0': 'Parent',
        '773  ': 'In',
        '775  ': 'Other edition',
        '776  ': 'Other format',
        '777  ': 'Issued with',
        '780 0': 'Continues',
        '780 1': 'Continues in part',
        '780 2': 'Supersedes',
        '780 3': 'Supersedes in part',
        '780 4': 'Merger of',
        '780 5': 'Absorbed',
        '780 6': 'Absorbed in part',
        '780 7': 'Separated from',
        '785 0': 'Continued by',
        '785 1': 'Continued in part by',
        '785 2': 'Superseded by',
        '785 3': 'Superseded in part by',
        '785 4': 'Absorbed by',
        '785 5': 'Absorbed in part by',
        '785 6': 'Split into',
        '785 7': 'Merged with',
        '785 8': 'Changed back to',
        '786  ': 'Data source',
    }
    tags_to_id_types = {
        'r': ('r', 'Report Number'),
        'u': ('u', 'STRN'),
        'x': ('issn', 'ISSN'),
        'y': ('coden', 'CODEN'),
        'z': ('isbn', 'ISBN'),
    }
    display_metadata_sftags = 'bcdghkmnov'
    identifiers_sftags = 'ruwxyz'
    series_fieldtags = ('760', '762')

    def __init__(self, field):
        super(LinkingFieldParser, self).__init__(field)
        self.prev_sftag = ''
        self.display_label_from_i = ''
        self.ttitle = ''
        self.stitle = ''
        self.author = ''
        self.short_author = ''
        self.display_metadata = []
        self.identifiers_map = {}
        self.identifiers_list = []
        self.ntype = None
        self.volume = None
        self.is_series = field.tag in self.series_fieldtags

    def make_display_label(self, label_from_i):
        tag, ind2 = self.field.tag, self.field.indicator2
        if tag not in ('760', '762', '780', '785') and ind2 == '8':
            if tag != '774' or label_from_i.lower() != 'container of':
                return label_from_i or None
        return self.display_label_map.get(' '.join((tag, ind2)))

    def title_to_parts(self, title):
        protected = p.protect_periods(title)
        return [p.restore_periods(tp) for tp in protected.split('. ') if tp]

    def do_identifier(self, tag, val):
        if tag == 'w':
            id_numtype = 'control'
            source, num = StandardControlNumberParser.split_cn_and_source(val)
            if source is None:
                id_code, id_label = tag, 'Control Number'
            else:
                if source == 'DLC':
                    id_code, id_label = 'lccn', 'LCCN'
                    num = StandardControlNumberParser.normalize_lccn(num)
                elif source == 'OCoLC':
                    id_code, id_label = 'oclc', 'OCLC Number'
                else:
                    id_code, id_label = tag, ' '.join((source, 'Number'))
        else:
            num = val
            id_numtype = 'standard'
            id_code, id_label = self.tags_to_id_types[tag]
        if id_code not in self.identifiers_map:
            self.identifiers_map[id_code] = {
                'number': num,
                'numtype': id_numtype
            }
        self.identifiers_list.append({
            'code': id_code,
            'numtype': id_numtype,
            'label': id_label,
            'number': num
        })

    def do_volume(self, volume):
        if len(volume) > 2 and volume[0].isupper() and volume[1].islower():
            return ''.join([volume[0].lower(), volume[1:]])
        return volume

    def parse_subfield(self, tag, val):
        if tag == 'i':
            self.display_label_from_i = p.strip_ends(p.strip_wemi(val))
        else:
            val = p.strip_ends(val)
            if tag == 's':
                self.stitle = val
            elif tag == 't':
                self.ttitle = val
            elif tag == 'g' and self.is_series and self.prev_tag in ('t', 's'):
                self.volume = self.do_volume(val)
            elif tag == 'a':
                name_struct = parse_name_string(val)
                self.short_author = shorten_name(name_struct)
                self.ntype = name_struct['type']
                if name_struct['type'] == 'person':
                    self.author = name_struct['heading']
                else:
                    self.author = ' '.join([
                        hp['name'] for hp in name_struct['heading_parts']
                    ])
            elif tag in self.display_metadata_sftags:
                self.display_metadata.append(p.strip_outer_parentheses(val))
            elif tag in self.identifiers_sftags:
                self.do_identifier(tag, val)
        self.prev_tag = tag

    def compile_results(self):
        tp, is_coll, is_mf = None, False, False
        if self.ttitle:
            tp = self.title_to_parts(self.ttitle)
        elif self.stitle:
            tp = self.title_to_parts(self.stitle)
            if tp:
                ptp = PreferredTitleParser
                is_coll, is_mf = ptp.describe_collective_title(tp[0], False,
                                                               True)
        return {
            'display_label': self.make_display_label(self.display_label_from_i),
            'title_parts': tp or None,
            'title_is_collective': is_coll,
            'title_is_music_form': is_mf,
            'volume': self.volume or None,
            'author': self.author or None,
            'short_author': self.short_author or None,
            'author_type': self.ntype or None,
            'display_metadata': self.display_metadata or None,
            'identifiers_map': self.identifiers_map or None,
            'identifiers_list': self.identifiers_list or None,
            'materials_specified': self.materials_specified or None,
        }


def extract_name_structs_from_field(field):
    if field.tag.endswith('00'):
        return [PersonalNameParser(field).parse()]
    if field.tag.endswith('10') or field.tag.endswith('11'):
        return OrgEventNameParser(field).parse()
    return []


def extract_title_struct_from_field(field):
    return PreferredTitleParser(field).parse()


def parse_name_string(name_string):
    """
    Parse a name heading contained in a string.

    The goal of this function is to *try* parsing a name heading string
    into a data structure such as what a `PersonalNameParser` or
    `OrgEventNameParser` would produce.

    The main purpose is to parse author names from Linking Fields
    (76X-78X $a) that are just heading strings without any additional
    subfield coding and get them into a form we can use to work with.
    """
    def _forename_is_ptitle(forename):
        parsed = p.person_title(forename)
        return bool(parsed['full_title'])

    is_person = False
    protected = p.strip_ends(p.protect_periods(name_string), True, 'right')
    forename, surname, ptitles, fuller_form = None, None, None, None
    relations = None

    relations_match = re.match(r'(.+?), ([^A-Z0-9]+)$', protected)
    try:
        protected, rel_string = relations_match.groups()
    except AttributeError:
        pass
    else:
        relations = rel_string.split(', ')

    heading = p.restore_periods(protected)
    
    dates_re = r'[^,\.]*(?:\D?[\d]{4}\??-|\D\d{4})'
    pdates_match = re.match(r'(.+), ({})$'.format(dates_re), protected)
    try:
        protected, _ = pdates_match.groups()
    except AttributeError:
        pass

    pname_match = re.match(r'((?:\s?[^,\.\s]+){1,3}), ([^,\.]+)(?:, )?(.+)?$',
                           protected)
    try:
        surname, forename, ptitles = pname_match.groups()
    except AttributeError:
        if pdates_match:
            is_person = True
            forename = protected
    else:
        is_person = True
        if forename and _forename_is_ptitle(forename):
            if ptitles:
                ptitles =  ', '.join([forename, ptitles])
            else:
                ptitles = forename
            forename = None

    if is_person:
        if forename:
            fullerform_match = re.match(r'(.*?) ?\((.+)\)$', forename)
            if fullerform_match:
                forename, fuller_form = fullerform_match.groups()
        if ptitles:
            ptitles = [p.restore_periods(pt) for pt in ptitles.split(', ')]

        ret_val = {
            'heading': heading,
            'relations': relations,
            'forename': forename,
            'surname': surname,
            'numeration': None,
            'person_titles': ptitles,
            'type': 'person',
            'fuller_form_of_name': fuller_form,
            'materials_specified': None
        }
        for key in ('forename', 'surname', 'fuller_form_of_name'):
            if ret_val[key]:
                ret_val[key] = p.restore_periods(ret_val[key])
        return ret_val

    heading_parts = []
    for part_str in protected.split('. '):
        if part_str:
            qual_match = re.match(r'(.*?) ?\((.+)\)$', part_str)
            if qual_match:
                name, qualifier = qual_match.groups()
                part = {'name': p.restore_periods(name),
                        'qualifier': p.restore_periods(qualifier)}
            else:
                part = {'name': p.restore_periods(part_str)}
            heading_parts.append(part)
    return {
        'relations': None,
        'heading_parts': heading_parts,
        'type': 'organization' if heading_parts else None,
        'is_jurisdiction': False,
        'materials_specified': None,
    }


def shorten_name(name_struct):
    if name_struct['type'] == 'person':
        forename, surname = name_struct['forename'], name_struct['surname']
        titles = name_struct['person_titles']
        numeration = name_struct['numeration']
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

    parts = [part['name'] for part in name_struct['heading_parts']]
    if parts:
        if len(parts) == 1:
            return parts[0]
        if len(parts) == 2:
            return ', '.join(parts)
        return ' ... '.join([parts[0], parts[-1]])
    return ''


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
            return [t for t in re.split(r'([A-Z](?=[A-Z]))|[\.\s]+', np) if t]
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
        match = re.search(r'^(.+?)\s+[\(\'"](\S+?)[\)\'"]$', forename)
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
                norm = p.strip_all_punctuation(part) if part else ''
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
            t = p.strip_all_punctuation(t)
            needs_suffix = True
            if len(prefixes) < n:
                parsed = p.person_title(t)
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
        for nextchunk in re.split(r'([, \.])', perm):
            if nextchunk:
                test_str = ''.join([test_str, nextchunk])
                match = re.search(r'(?:^|.*\s){}$'.format(test_str), cumulative)
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
            best_fwd_parts  = [prefix_title, auth_name,
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
            fullest_first = self.render_name_part(self.fullest_name['forename'])
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


def make_relator_search_variations(base_name, relators):
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
    return p.shingle_callnum(norm)


def format_materials_specified(materials_specified):
    return '({})'.format(', '.join(materials_specified))


def format_display_constants(display_constants):
    return '{}:'.format(', '.join(display_constants))


def format_title_short_author(title, conjunction, short_author):
    conj_author = ([conjunction] if conjunction else []) + [short_author]
    return '{} [{}]'.format(title, ' '.join(conj_author))


def format_translation(translated_text):
    return '[translated: {}]'.format(translated_text)


def format_volume(volume):
    volume_separator = '; '
    if volume.isdigit():
        volume = '[volume] {}'.format(volume)
    return volume_separator, volume


def generate_facet_key(value, nonfiling_chars=0, space_char=r'-'):
    key = value.lower()
    if nonfiling_chars and len(key) > nonfiling_chars:
        last_nfchar_is_nonword = not key[nonfiling_chars - 1].isalnum()
        if last_nfchar_is_nonword and len(value) > nonfiling_chars:
            key = key[nonfiling_chars:]
    key = toascii.map_from_unicode(key)
    key = re.sub(r'\W+', space_char, key).strip(space_char)
    return key or '~'


def format_key_facet_value(heading, nonfiling_chars=0):
    key = generate_facet_key(heading, nonfiling_chars)
    return FACET_KEY_SEPARATOR.join((key, heading))


def format_number_search_val(numtype, number):
    exclude = ('unknown',)
    numtype = '' if numtype in exclude else numtype
    return ':'.join([v for v in (numtype, number) if v])


def format_number_display_val(parsed):
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


class GenericDisplayFieldParser(SequentialMarcFieldParser):
    """
    Parse/format a MARC field for display. This is conceptually similar
    to using the `pymarc.field.Field.format_field` method, with the
    following improvements: you can specify a custom `separator` (space
    is the default); you can specify an optional `sf_filter` to include
    or exclude certain subfields; subfield 3 (materials specified) is
    handled automatically, assuming it occurs at the beginning of the
    field.
    """
    def __init__(self, field, separator=' ', sf_filter=None, label=None):
        filtered = field.filter_subfields(**sf_filter) if sf_filter else field
        super(GenericDisplayFieldParser, self).__init__(filtered)
        self.separator = separator
        self.original_field = field
        self.sf_filter = sf_filter
        self.value_stack = []
        self.label = label

    def determine_separator(self, val):
        return self.separator

    def add_separator(self, val):
        sep = self.determine_separator(val)
        val = val.rstrip(sep)
        if sep != ' ':
            val = p.strip_ends(val, end='right')
        return ''.join([val, sep])

    def parse_subfield(self, tag, val):
        if tag != '3':
            self.value_stack.append(val)

    def compile_results(self):
        result_stack = []
        if self.materials_specified:
            ms_str = format_materials_specified(self.materials_specified)
            result_stack.append(ms_str)
        if self.label:
            result_stack.append(format_display_constants([self.label]))

        value_stack = []
        for i, val in enumerate(self.value_stack):
            is_last = i == len(self.value_stack) - 1
            if not is_last:
                val = self.add_separator(val)
            value_stack.append(val)
        result_stack.append(''.join(value_stack))
        return ' '.join(result_stack)


class PerformanceMedParser(SequentialMarcFieldParser):
    def __init__(self, field):
        super(PerformanceMedParser, self).__init__(field)
        self.parts = []
        self.part_stack = []
        self.instrument_stack = []
        self.last_part_type = ''
        self.total_performers = None
        self.total_ensembles = None

    def push_instrument(self, instrument, number=None, notes=None):
        number = number or '1'
        entry = (instrument, number, notes) if notes else (instrument, number)
        self.instrument_stack.append(entry)

    def push_instrument_stack(self):
        if self.instrument_stack:
            self.part_stack.append({self.last_part_type: self.instrument_stack})
            self.instrument_stack = []

    def push_part_stack(self):
        self.push_instrument_stack()
        if self.part_stack:
            self.parts.append(self.part_stack)
            self.part_stack = []

    def update_last_instrument(self, number=None, notes=None):
        try:
            entry = self.instrument_stack.pop()
        except IndexError:
            pass
        else:
            instrument, old_num = entry[:2]
            number = number or old_num
            if len(entry) == 3:
                old_notes = entry[2]
                notes = old_notes + notes if notes else old_notes
            self.push_instrument(instrument, number, notes)

    def parse_subfield(self, tag, val):
        if tag in 'abdp':
            if tag in 'ab':
                self.push_part_stack()
                part_type = 'primary' if tag == 'a' else 'solo'
            elif tag in 'dp':
                part_type = 'doubling' if tag == 'd' else 'alt'
                if part_type != self.last_part_type:
                    self.push_instrument_stack()
            self.push_instrument(val)
            self.last_part_type = part_type
        elif tag in 'en':
            if val != '1':
                self.update_last_instrument(number=val)
        elif tag == 'v':
            self.update_last_instrument(notes=[val])
        elif tag in 'rs':
            self.total_performers = val
        elif tag == 't':
            self.total_ensembles = val

    def do_post_parse(self):
        self.push_part_stack()

    def compile_results(self):
        return {
            'materials_specified': self.materials_specified,
            'parts': self.parts,
            'total_performers': self.total_performers,
            'total_ensembles': self.total_ensembles
        }


class DissertationNotesFieldParser(SequentialMarcFieldParser):
    def __init__(self, field):
        super(DissertationNotesFieldParser, self).__init__(field)
        self.degree = None
        self.institution = None
        self.date = None
        self.note_parts = []
        self.degree_statement_is_done = False

    def format_degree_statement(self):
        result = ', '.join([v for v in (self.institution, self.date) if v])
        return ' ― '.join([v for v in (self.degree, result) if v])

    def try_to_do_degree_statement(self):
        if not self.degree_statement_is_done:
            if self.degree or self.institution or self.date:
                self.note_parts.append(self.format_degree_statement())
                self.degree_statement_is_done = True

    def parse_subfield(self, tag, val):
        if tag == 'b':
            self.degree = p.strip_ends(val)
        elif tag == 'c':
            self.institution = p.strip_ends(val)
        elif tag == 'd':
            self.date = p.strip_ends(val)
        elif tag in 'go':
            self.try_to_do_degree_statement()
            self.note_parts.append(val)

    def do_post_parse(self):
        self.try_to_do_degree_statement()

    def compile_results(self):
        return {
            'degree': self.degree,
            'institution': self.institution,
            'date': self.date,
            'note_parts': self.note_parts
        }


class MultiFieldMarcRecordParser(object):
    """
    General purpose class for parsing blocks of fields on a MARC
    record. The `parse` method returns a dictionary that maps field
    names to lists of values after parsing the fields on the input
    MARC `record`.

    The `mapping` value (passed to __init__) controls how to translate
    MARC to the return value. For example:

        mapping = {
            '100': {
                'subfields': {'exclude': 'w01258'},
                'solr_fields': ('author_display', 'author_search'),
                'parse_func': lambda field: field.format_field()
            },
            'a': {
                'subfields': {'exclude': 'w01258'},
                'solr_fields': ('author_display', 'author_search'),
            },
            'exclude': set(('110', '111'))
        }

    Each key should be a MARC tag, a III field group tag, or `exclude`.
    The parse process loops through each MARC field on the record, in
    the order in which it appears. It tries to find a valid definition
    entry in `mapping`, first by MARC tag, and then by III field group
    tag. If a specific MARC tag is found, then that is used. If the
    appropriate group field tag is found, then that is used IF the MARC
    tag does not appear in the set of `exclude` tags.

    The field definition is a dict that uses the following keys. 

    `solr_fields` -- (Required.) A list or tuple containing the Solr
    fields to generate for the given MARC field.

    `subfields` -- (Optional.) The subfield filter to pass as kwargs to
    the `filter_subfields` method during processing. Defaults to the
    `default_sf_filter` passed on initialization, or a filter that
    simply excludes `utils.control_sftags`.

    `parse_func` -- (Optional.) A function or method that parses each
    individual field. It should receive the MARC field object and
    applicable subfield filter, and it should return a string.
    Defaults to the `default_parse_func` method.
    """
    def __init__(self, record, mapping, utils=None, default_sf_filter=None):
        self.record = record
        self.mapping = mapping
        self.utils = utils or MarcUtils()
        self.default_sf_filter = default_sf_filter or {'exclude':
                                                       utils.control_sftags}

    def default_parse_func(self, field, sf_filter):
        return GenericDisplayFieldParser(field, ' ', sf_filter).parse()

    def parse(self):
        ret_val = {}
        for f in self.record.fields:
            fdef = self.mapping.get(f.tag)
            if fdef is None and f.tag not in self.mapping.get('exclude', set()):
                fdef = self.mapping.get(f.group_tag)
            if fdef:
                parse_func = fdef.get('parse_func', self.default_parse_func)
                sff = fdef.get('subfields', self.default_sf_filter)
                field_val = parse_func(f, sff)
                if field_val:
                    for fname in fdef['solr_fields']:
                        ret_val[fname] = ret_val.get(fname, [])
                        if isinstance(field_val, (list, tuple)):
                            ret_val[fname].extend(field_val)
                        else:
                            ret_val[fname].append(field_val)
        return ret_val


class ToDiscoverPipeline(object):
    """
    This is a one-off class to hold functions/methods for creating the
    processed/custom fields that we're injecting into MARC records
    before passing them through SolrMarc. Since we're going to be
    moving away from SolrMarc, this helps contain all of the localized
    processing we're doing so we can more easily reimplement it.

    To use: add a method to this class that takes a Sierra DB BibRecord
    model instance (`r`) and a pymarc object (`marc_record`). Both
    objects should represent the same record. In the method, use these
    objects to compile whatever info you need, and return a dictionary,
    where each key represents the solr field that gets the
    corresponding data value. (Keys should be unique.)

    Name the method using the specified `prefix` class attr--default is
    'get_'. Then add the suffix to the `fields` list in the order you
    want processing to happen.

    Use the `do` method to run something through the pipeline and get a
    fully-populated dict.
    """
    fields = [
        'id', 'suppressed', 'date_added', 'item_info', 'urls_json',
        'thumbnail_url', 'pub_info', 'access_info', 'resource_type_info',
        'contributor_info', 'title_info', 'notes', 'call_number_info',
        'standard_number_info', 'control_number_info', 'games_facets_info',
        'subjects_info', 'language_info', 'record_boost', 'linking_fields',
        'editions', 'serial_holdings'
    ]
    marc_grouper = MarcFieldGrouper({
        '008': set(['008']),
        'control_numbers': set(['001', '010', '016', '035']),
        'standard_numbers': set(['020', '022', '024', '025', '026', '027',
                                 '028', '030', '074', '088']),
        'language_code': set(['041', '377']),
        'coded_dates': set(['046']),
        'main_author': set(['100', '110', '111']),
        'uniform_title': set(['130', '240', '243']),
        'key_title': set(['210', '222']),
        'transcribed_title': set(['245']),
        'alternate_title': set(['242', '246', '247']),
        'edition': set(['250', '251', '254']),
        'production_country': set(['257']),
        'publication': set(['260', '264']),
        'dates_of_publication': set(['362']),
        'music_number_and_key': set(['383', '384']),
        'physical_description': set(['r', '310', '321', '340', '342', '343',
                                     '344', '345', '346', '347', '348', '352',
                                     '382', '385', '386', '388']),
        'series_statement': set(['490']),
        'notes': set(['n', '502', '505', '508', '511', '520', '546', '583']),
        'local_game_note': set(['592']),
        'subject_genre': set(['380', '600', '610', '611', '630', '647', '648',
                              '650', '651', '653', '655', '656', '657', '690',
                              '691', '692']),
        'curriculum_objective': set(['658']),
        'title_added_entry': set(['700', '710', '711', '730', '740']),
        'geographic_info': set(['751', '752']),
        'system_details': set(['753']),
        'linking_760_762': set(['760', '762']),
        'linking_774': set(['774']),
        'linking_780_785': set(['780', '785']),
        'linking_other': set(['765', '767', '770', '772', '773', '775', '776',
                              '777', '786', '787']),
        'series_added_entry': set(['800', '810', '811', '830']),
        'url': set(['856']),
        'library_has': set(['866']),
        'media_link': set(['962']),
    })
    prefix = 'get_'
    access_online_label = 'Online'
    access_physical_label = 'At the Library'
    item_rules = local_rulesets.ITEM_RULES
    bib_rules = local_rulesets.BIB_RULES
    hierarchical_name_separator = ' > '
    hierarchical_subject_separator = ' > '
    ignore_fast_headings = True
    utils = MarcUtils()

    def __init__(self):
        super(ToDiscoverPipeline, self).__init__()
        self.bundle = {}
        self.name_titles = []
        self.work_title_keys = {}
        self.title_languages = []
        self.this_year = datetime.now().year
        self.year_upper_limit = self.this_year + 5
        self.year_for_boost = None
        self.r = None
        self.marc_record = None
        self.marc_fieldgroups = None

    @property
    def sierra_location_labels(self):
        if not hasattr(self, '_sierra_location_labels'):
            self._sierra_location_labels = {}
            pf = 'locationname_set'
            for loc in models.Location.objects.prefetch_related(pf).all():
                loc_name = loc.locationname_set.all()[0].name
                self._sierra_location_labels[loc.code] = loc_name
        return self._sierra_location_labels

    def set_up(self, r=None, marc_record=None, reset_params=True):
        if reset_params:
            self.bundle = {}
            self.name_titles = []
            self.work_title_keys = {}
            self.title_languages = []
            self.year_for_boost = None
        if self.marc_record != marc_record:
            self.marc_record = marc_record
            if marc_record:
                groups = self.marc_grouper.make_groups(marc_record)
                self.marc_fieldgroups = groups
        if self.r != r:
            self.r = r

    def do(self, r, marc_record, fields=None, reset_params=True):
        """
        This is the "main" method for objects of this class. Use this
        to run any data through the pipeline (or part of the pipeline).

        Provide `r`, a base.models.BibRecord instance, and
        `marc_record`, a pymarc Record object (both representing the
        same record). Runs each method identified via `fields` and
        returns a dict composed of all keys returned by the individual
        methods.

        If `fields` is not provided, it uses the `fields` class
        attribute by default, i.e. the entire pipeline.
        """
        self.set_up(r=r, marc_record=marc_record, reset_params=reset_params)
        for fname in (fields or self.fields):
            method_name = '{}{}'.format(self.prefix, fname)
            # Uncomment this block and comment out the following line
            # to force the record ID for records that are causing
            # errors to be output, at the expense of the traceback.
            # try:
            #     result = getattr(self, method_name)()
            # except Exception as e:
            #     msg = '{}: {}'.format(self.bundle['id'], e)
            #     raise Exception(msg)
            result = getattr(self, method_name)()
            for k, v in result.items():
                self.bundle[k] = self.bundle.get(k)
                if v:
                    if self.bundle[k]:
                        self.bundle[k].extend(v)
                    else:
                        self.bundle[k] = v
        return self.bundle

    def fetch_varfields(self, record, vf_code, only_first=False):
        """
        Fetch varfield content from the given `record`, limited to the
        given `vf_code` (i.e. field tag or varfield type code). If
        `only_first` is True, then it gets only the first vf, based on
        vf.occ_num.
        """
        vf_set = record.record_metadata.varfield_set
        vfields = [f for f in vf_set.all() if f.varfield_type_code == vf_code]
        if len(vfields) > 0:
            vfields = sorted(vfields, key=lambda f: f.occ_num)
            if only_first:
                return vfields[0].field_content
            return [vf.field_content for vf in vfields]
        return None

    def get_id(self):
        """
        Return the III Record Number, minus the check digit.
        """
        return { 'id': self.r.record_metadata.get_iii_recnum(False) }

    def get_suppressed(self):
        """
        Return 'true' if the record is suppressed, else 'false'.
        """
        return { 'suppressed': 'true' if self.r.is_suppressed else 'false' }

    def get_date_added(self):
        """
        Return a date that most closely approximates when the record
        was added to the catalog. E-resources (where all bib locations
        are online) use record_metadata.creation_date_gmt; all others
        use the CAT DATE (cataloged date) of the Bib record. Dates are
        converted to the string format needed by Solr.
        """
        r = self.r
        if all((l.code.endswith('www') for l in r.locations.all())):
            cdate = r.record_metadata.creation_date_gmt
        else:
            cdate = r.cataloging_date_gmt
        rval = None if cdate is None else cdate.strftime('%Y-%m-%dT%H:%M:%SZ')
        return { 'date_added': rval }

    def get_item_info(self):
        """
        Return a dict containing item table information: `items_json`,
        `has_more_items`, and `more_items_json`.
        """
        r = self.r
        items = []
        item_links = [l for l in r.bibrecorditemrecordlink_set.all()]
        for link in sorted(item_links, key=lambda l: l.items_display_order):
            item = link.item_record
            if not item.is_suppressed:
                item_id, callnum, barcode, notes, rqbility = '', '', '', [], ''
                callnum, vol = self.calculate_item_display_call_number(r, item)
                item_id = str(item.record_metadata.record_num)
                barcode = self.fetch_varfields(item, 'b', only_first=True)
                notes = self.fetch_varfields(item, 'p')
                requestability = self.calculate_item_requestability(item)

                items.append({'i': item_id, 'c': callnum, 'v': vol,
                              'b': barcode, 'n': notes, 'r': requestability})

        if len(items) == 0:
            bib_locations = r.locations.all()
            bib_callnum, _ = self.calculate_item_display_call_number(r)
            for location in bib_locations:
                items.append({'i': None, 'c': bib_callnum, 'l': location.code})
            if len(bib_locations) == 0:
                items.append({'i': None, 'c': bib_callnum, 'l': 'none'})

        items_json, has_more_items, more_items_json = [], False, []
        items_json = [ujson.dumps(i) for i in items[0:3]]
        if len(items) > 3:
            has_more_items = True
            more_items_json = [ujson.dumps(i) for i in items[3:]]
        return {
            'items_json': items_json,
            'has_more_items': 'true' if has_more_items else 'false',
            'more_items_json': more_items_json or None
        }

    def calculate_item_display_call_number(self, bib, item=None):
        """
        Sub-method used by `get_item_info` to return the display call
        number for the given `item`.
        """
        cn_string, vol = '', None
        item_cn_tuples = [] if item is None else item.get_call_numbers()

        if len(item_cn_tuples) > 0:
            cn_string = item_cn_tuples[0][0]
        else:
            bib_cn_tuples = bib.get_call_numbers()
            if len(bib_cn_tuples) > 0:
                cn_string = bib_cn_tuples[0][0]

        if item is not None:
            vol = self.fetch_varfields(item, 'v', only_first=True)
            if item.copy_num > 1:
                if vol is None:
                    cn_string = '{} c.{}'.format(cn_string, item.copy_num)
                else:
                    vol = '{} c.{}'.format(vol, item.copy_num)

        return (cn_string or None, vol)

    def calculate_item_requestability(self, item):
        """
        Sub-method used by `get_item_info` to return a requestability
        string based on established request rules.
        """
        item_rules = self.item_rules
        if item_rules['is_at_jlf'].evaluate(item):
            return 'jlf'
        if item_rules['is_requestable_through_aeon'].evaluate(item):
            return 'aeon'
        if item_rules['is_requestable_through_catalog'].evaluate(item):
            return 'catalog'
        return None

    def _sanitize_url(self, url):
        return re.sub(r'^([^"]+).*$', r'\1', url)

    def get_urls_json(self):
        """
        Return a JSON string representing URLs associated with the
        given record.
        """
        urls_data = []
        for f856 in self.marc_fieldgroups.get('url', []):
            url = f856.get_subfields('u')
            if url:
                url = self._sanitize_url(url[0])
                note = ' '.join(f856.get_subfields('3', 'z')) or None
                label = ' '.join(f856.get_subfields('y')) or None
                utype = 'fulltext' if f856.indicator2 in ('0', '1') else 'link'

                urls_data.append({'u': url, 'n': note, 'l': label,
                                  't': utype})

        for i, f962 in enumerate(self.marc_fieldgroups.get('media_link', [])):
            urls = f962.get_subfields('u')
            if urls:
                url, utype = urls[0], 'media'
            else:
                url, utype = self._make_reserve_url(i), 'fulltext'

            if not self._url_is_image(url):
                titles = f962.get_subfields('t') or [None]
                urls_data.append({'u': url, 'n': titles[0], 'l': None,
                                  't': utype})

        urls_json = []
        for ud in urls_data:
            ud['t'] = self.review_url_type(ud, len(urls_data), self.r)
            urls_json.append(ujson.dumps(ud))

        return {'urls_json': urls_json}

    def _url_is_image(self, url):
        """
        Return True if the given `url` appears to point to an image
        file.
        """
        # The below list of extensions is taken from
        # https://developer.mozilla.org/en-US/docs/Web/Media/Formats
        # /Image_types
        image_extensions = (
            'apng', 'bmp', 'gif', 'ico', 'cur', 'jpg', 'jpeg', 'jfif', 'pjpeg',
            'pjp', 'png', 'svg', 'tif', 'tiff', 'webp'
        )
        return url.split('.')[-1].lower() in image_extensions

    def _make_reserve_url(self, nth=0):
        recnum = self.bundle['id'].lstrip('.')
        return ('https://iii.library.unt.edu/search~S12?/.{0}/.{0}/1,1,1,B/l962'
                '~{0}&FF=&1,0,,{1},0'.format(recnum, nth))

    def _url_is_from_digital_library(self, url):
        """
        Return True if the given `url` is from the UNT Digital Library
        (or Portal to Texas History).
        """
        return 'digital.library.unt.edu/ark:' in url or\
               'texashistory.unt.edu/ark:' in url

    def _url_is_from_media_booking_system(self, url):
        """
        Return True if the given `url` is from the UNTL Media Booking
        system.
        """
        return 'mediabook.library.unt.edu' in url

    def _url_note_indicates_fulltext(self, note):
        """
        Return True if the given `note` (|z text from an 856 URL)
        matches a pattern indicating it's probably a full-text link.
        """
        eres_re = r'[Ee]lectronic|[Ee].?[Rr]esource'
        online_re = r'[Oo]nline.?'
        fulltext_re = r'[Ff]ull.?[Tt]ext.?'
        alternatives = r'|'.join([eres_re, online_re, fulltext_re])
        regex = r'(^|\s)({})(\s|$)'.format(alternatives)
        return bool(re.search(regex, note))

    def _bib_has_item_with_online_status(self, bib):
        """
        Return True if there's at least one unsuppressed item attached
        to this bib with an item status ONLINE (w).
        """
        for link in bib.bibrecorditemrecordlink_set.all():
            item = link.item_record
            if not item.is_suppressed and item.item_status_id == 'w':
                return True
        return False

    def review_url_type(self, url_data, num_urls, bib):
        """
        Make a second pass at determining the URL type: full_text,
        booking, media, or link. If it has a media-booking URL, then
        it's a booking URL. If it has a note indicating that it's an
        online or full-text link, then it's fulltext. If it has an
        item attached with status 'ONLINE' (w) and it's the only URL,
        then it's fulltext. Otherwise, keep whatever soft determination
        was made in the `get_urls_json` method.
        """
        if self._url_is_from_media_booking_system(url_data['u']):
            return 'booking'
        if url_data['n'] and self._url_note_indicates_fulltext(url_data['n']):
            return 'fulltext'
        if num_urls == 1 and self._bib_has_item_with_online_status(bib):
            return 'fulltext'
        return url_data['t']

    def get_thumbnail_url(self):
        """
        Try finding a (local) thumbnail URL for this bib record. If it
        exists, it will either be from a cover image scanned by the
        Media Library, or it will be from the Digital Library or
        Portal.
        """
        f856s = self.marc_fieldgroups.get('url', [])
        f962s = self.marc_fieldgroups.get('media_link', [])
        def _try_media_cover_image(f962s):
            for f962 in f962s:
                urls = f962.get_subfields('u')
                url = self._sanitize_url(urls[0]) if urls else None
                if url and self._url_is_image(url):
                    sub_pattern = r'^https?:\/\/(www\.(?=library\.unt\.edu))?'
                    return re.sub(sub_pattern, 'https://', url)

        def _try_digital_library_image(f856s):
            for f856 in f856s:
                urls = f856.get_subfields('u')
                url = self._sanitize_url(urls[0]) if urls else None
                if url and self._url_is_from_digital_library(url):
                    url = url.split('?')[0].rstrip('/')
                    url = re.sub(r'^http:', 'https:', url)
                    return '{}/small/'.format(url)

        url = _try_media_cover_image(f962s) or\
              _try_digital_library_image(f856s) or\
              None

        return {'thumbnail_url': url}

    def _extract_pub_statements_from_26x(self, f26x):
        """
        Return a list of publication statements found in the given 26X
        field (pymarc Field object).
        """
        def _clean_pub_statement(statement):
            return p.strip_outer_parentheses(p.strip_ends(statement), True)

        ind2_type_map = {'0': 'creation', '1': 'publication',
                         '2': 'distribution', '3': 'manufacture',
                         '4': 'copyright'}
        ptype = ind2_type_map.get(f26x.indicator2, 'publication')
        statements = []
        for gr in group_subfields(f26x, 'abc', end='c'):
            if f26x.tag == '260':
                d = pull_from_subfields(gr, 'c', p.split_pdate_and_cdate)
                pdate, cdate = tuple(d[0:2]) if len(d) > 1 else ('', '')
                pdate = p.normalize_punctuation(pdate)
                cdate = _clean_pub_statement(p.normalize_cr_symbol(cdate))
                if cdate:
                    statements.append(('copyright', cdate))
            else:
                pdate = (pull_from_subfields(gr, 'c') or [''])[0]
                if ptype == 'copyright':
                    pdate = p.normalize_cr_symbol(pdate)
            parts = gr.get_subfields('a', 'b') + ([pdate] if pdate else [])
            statement = _clean_pub_statement(' '.join(parts))
            if statement:
                statements.append((ptype, statement))

        for group in group_subfields(f26x, 'efg'):
            statement = _clean_pub_statement(group.format_field())
            statements.append(('manufacture', statement))
        return statements

    def _sanitize_date(self, dnum, dstr='', allow_9999=False):
        if dnum < 100:
            return None
        if allow_9999 and dnum == 9999:
            return self.this_year
        if dnum > self.year_upper_limit:
            if 'u' in dstr:
                mil, cent, dec = list(str(self.year_upper_limit)[:-1])
                valid_approximates = [
                    '{}uuu'.format(mil),
                    '{}{}uu'.format(mil, cent),
                    '{}{}{}u'.format(mil, cent, dec)
                ]
                if dstr in valid_approximates:
                    return self.year_upper_limit
            return None
        return dnum

    def _normalize_coded_date(self, date, allow_9999=False):
        if date in ('uuuu', '1uuu', '0uuu'):
            return 'uuuu', -1, -1
        if re.search(r'^[\du]+$', date):
            low = int(date.replace('u', '0'))
            low = self._sanitize_date(low, date, allow_9999)
            high = int(date.replace('u', '9'))
            high = self._sanitize_date(high, date, allow_9999)
            if low is not None and high is not None:
                return date, low, high
        return None, None, None

    def _normalize_coded_date_range(self, d1, d2):
        if d2 == '    ':
            d2 = '9999'
        dstr1, low1, high1 = self._normalize_coded_date(d1)
        dstr2, low2, high2 = self._normalize_coded_date(d2, allow_9999=True)

        if any((v is None for v in (dstr1, low1, high1))):
            dstr1, low1, high1 = None, None, None
        if any((v is None for v in (dstr2, low2, high2))):
            dstr2, low2, high2 = None, None, None

        lowest, highest = low1 or high1, high2 or low2
        dnum1 = low2 if lowest == -1 else lowest
        dnum2 = high1 if highest == -1 else highest
        if dnum1 > dnum2:
            dnum2 = dnum1
            dstr2 = None
        
        if dstr1 is None:
            if dnum1 is None:
                return dstr2, None, dnum2, dnum2
            return dstr2, None, dnum1, dnum2

        if dnum2 is None:
            return dstr1, None, dnum1, dnum1

        if dstr1 == dstr2:
            return dstr1, None, dnum1, dnum2

        return dstr1, dstr2, dnum1, dnum2

    def interpret_coded_date(self, dtype, date1, date2):
        pubtype_map_single_range = {
            'i': ('creation', 'Collection created in '),
            'k': ('creation', 'Collection created in '),
            '046kl': ('creation', ''),
            '046op': ('creation', 'Content originally created in ')
        }
        pubtype_map_atomic = {            
            'p': [('distribution', 'Released in '),
                  ('creation', 'Created or produced in ')],
            'r': [('distribution', 'Reproduced or reissued in '),
                  ('publication', 'Originally published in ')],
            't': [('publication', ''), ('copyright', '')],
            
        }
        default = ('publication', '')

        d2_type = None
        if dtype in list(pubtype_map_atomic.keys()):
            d2_type = 'atomic'
        else:
            this_is_serial = self.marc_record.leader[7] in 'is'
            d2_is_serial_range = this_is_serial and dtype in 'cdu'
            d2_is_nonserial_range = not this_is_serial and dtype in 'ikmq'
            d2_is_046_range = dtype.startswith('046')
            if d2_is_serial_range or d2_is_nonserial_range or d2_is_046_range:
                d2_type = 'range'

        if d2_type == 'range':
            ds1, ds2, dn1, dn2 = self._normalize_coded_date_range(date1, date2)
            if ds1 is not None or ds2 is not None:
                pub_field, label = pubtype_map_single_range.get(dtype, default)
                return [(ds1, ds2, dn1, dn2, pub_field, label)]

        vals = [self._normalize_coded_date(date1)]
        if d2_type == 'atomic':
            vals.append(self._normalize_coded_date(date2))
            return [
                (vals[i][0], None, vals[i][1], vals[i][2], deets[0], deets[1])
                for i, deets in enumerate(pubtype_map_atomic[dtype])
                if vals[i][0] is not None
            ]

        dstr, dnum1, dnum2 = vals[0]
        if dstr is not None:
            pub_field, label = pubtype_map_single_range.get(dtype, default)
            return [(dstr, None, dnum1, dnum2, pub_field, label)]

        return []

    def _format_years_for_display(self, year1, year2=None, the=False):
        """
        Convert a single year (`year1`) or a year range (`year1` to
        `year2`), where each year is formatted ~ MARC 008 ("196u" is
        "1960s"), to a display label. Pass True for `the` if you want
        the word `the` included, otherwise False. (E.g.: "the 20th
        century" or "the 1960s".)
        """
        def _format_year(year, the):
            the = 'the ' if the else ''
            century_suffix_map = {'1': 'st', '2': 'nd', '3': 'rd'}
            year = year.lstrip('0') if year else None
            match = re.search(r'^(\d*)(u+)$', year or '')
            if match:
                if match.groups()[1] == 'u':
                    year = year.replace('u', '0s')
                elif match.groups()[1] == 'uu':
                    century = six.text_type(int(match.groups()[0] or 0) + 1)
                    if century[-2:] in ('11', '12', '13'):
                        suffix = 'th'
                    else:
                        suffix = century_suffix_map.get(century[-1], 'th')
                    year = '{}{} century'.format(century, suffix)
                else:
                    return '?'
                return '{}{}'.format(the, year)
            return year

        disp_y1, disp_y2 = _format_year(year1, the), _format_year(year2, the)
        if disp_y1 is None:
            return ''

        if disp_y1 == '9999':
            return 'present year'

        if disp_y1 == '?' and disp_y2 in (None, '?'):
            return 'dates unknown'

        if disp_y2 is None:
            return disp_y1

        if disp_y2 == '9999':
            return '{} to present'.format(disp_y1)

        if disp_y1.endswith('century') and disp_y2.endswith('century'):
            disp_y1 = disp_y1.replace(' century', '')

        if disp_y1 != '?':
            # This is like: (19uu, 1935) => "20th century (to 1935)"
            if int(year1.replace('u', '9')) >= int(year2.replace('u', '9')):
                return '{} (to {})'.format(disp_y1, disp_y2)

        return '{} to {}'.format(disp_y1, disp_y2)

    def _expand_years(self, coded_dates, described_years):
        def do_expand(dstr1, dstr2, dnum1, dnum2):
            dstrs, years = [], []
            for dstr in [dstr1, dstr2]:
                if dstr and dstr != 'uuuu':
                    dstrs.append(dstr)
            if (dnum1, dnum2) == (-1, -1):
                return [], []
            years = list(range(dnum1, dnum2 + 1))
            return dstrs, years

        for dstr1, dstr2, dnum1, dnum2, _, _ in coded_dates:
            yield do_expand(dstr1, dstr2, dnum1, dnum2)

        for d1, d2 in set(described_years):
            fake_dtype = 's'
            if d2 is not None:
                fake_dtype = 'd' if self.marc_record.leader[7] in 'is' else 'm'
            for entry in self.interpret_coded_date(fake_dtype, d1, d2):
                dstr1, dstr2, dnum1, dnum2, _, _ = entry
                yield do_expand(dstr1, dstr2, dnum1, dnum2)

    def _get_year_for_boost(self, dstrs, latest_year):
        """
        Use this to get a good year for the recentness boost factor.
        dstrs should be the 008/date1 and date2 values, or the
        equivalent. latest_year should be the latest possible year
        value (as an int) represented by the dstrs.
        """
        use_date = dstrs[0]
        if self.marc_record.leader[7] == 's':
            use_date = dstrs[-1]
        if use_date == '9999':
            return latest_year
        if 'u' in use_date:
            lower = int(use_date.replace('u', '0'))
            upper = int(use_date.replace('u', '9')) + 1
            if upper > self.year_upper_limit:
                upper = self.year_upper_limit
            return lower + ((upper - lower) / 2)
        try:
            return int(use_date)
        except ValueError:
            return None

    def get_pub_info(self):
        """
        Get and handle all the needed publication and related info for
        the given bib and marc record.
        """
        def _strip_unknown_pub(data):
            pub_stripped = p.normalize_punctuation(p.strip_unknown_pub(data))
            if re.search(r'\w', pub_stripped):
                return [pub_stripped]
            return []

        pub_info, described_years, places, publishers = {}, [], [] , []
        publication_date_notes = []
        for f26x in self.marc_fieldgroups.get('publication', []):
            years = pull_from_subfields(
                f26x, 'cg', lambda v: p.extract_years(v, self.year_upper_limit))
            described_years.extend(years)
            for stype, stext in self._extract_pub_statements_from_26x(f26x):
                pub_info[stype] = pub_info.get(stype, [])
                pub_info[stype].append(stext)

            for place in pull_from_subfields(f26x, 'ae', _strip_unknown_pub):
                place = p.strip_ends(place)
                places.append(p.strip_outer_parentheses(place, True))

            for pub in pull_from_subfields(f26x, 'bf', _strip_unknown_pub):
                pub = p.strip_ends(pub)
                publishers.append(p.strip_outer_parentheses(pub, True))

        for f257 in self.marc_fieldgroups.get('production_country', []):
            places.extend([p.strip_ends(sf) for sf in f257.get_subfields('a')])

        for f in self.marc_fieldgroups.get('geographic_info', []):
            place = ' '.join([sf for sf in f.get_subfields(*tuple('abcdfgh'))])
            places.append(place)

        for f362 in self.marc_fieldgroups.get('dates_of_publication', []):
            formatted_date = ' '.join(f362.get_subfields('a'))
            # NOTE: Extracting years from 362s (as below) was leading
            # to falsely extracting volume numbers as years, so we
            # probably should not do that. That's why the next two
            # lines are commented out.
            # years = p.extract_years(formatted_date, self.year_upper_limit)
            # described_years.extend(years)
            if f362.indicator1 == '0':
                pub_info['publication'] = pub_info.get('publication', [])
                pub_info['publication'].append(formatted_date)
            else:
                publication_date_notes.append(f362.format_field())

        coded_dates = []
        f008 = self.marc_fieldgroups.get('008', [None])[0]
        if f008 is not None and len(f008.data) >= 15:
            data = f008.data
            entries = self.interpret_coded_date(data[6], data[7:11],
                                                data[11:15])
            coded_dates.extend(entries)

        for field in self.marc_fieldgroups.get('coded_dates', []):
            coded_group = group_subfields(field, 'abcde', unique='abcde')
            if coded_group:
                dtype = (coded_group[0].get_subfields('a') or [''])[0]
                date1 = (coded_group[0].get_subfields('c') or [''])[0]
                date2 = (coded_group[0].get_subfields('e') or [''])[0]
                entries = self.interpret_coded_date(dtype, date1, date2)
                coded_dates.extend(entries)

            other_group = group_subfields(field, 'klop', unique='klop')
            if other_group:
                _k = (other_group[0].get_subfields('k') or [''])[0]
                _l = (other_group[0].get_subfields('l') or [''])[0]
                _o = (other_group[0].get_subfields('o') or [''])[0]
                _p = (other_group[0].get_subfields('p') or [''])[0]
                coded_dates.extend(self.interpret_coded_date('046kl', _k, _l))
                coded_dates.extend(self.interpret_coded_date('046op', _o, _p))

        sort, year_display = '', ''
        for i, row in enumerate(coded_dates):
            dstr1, dstr2, dnum1, dnum2, pub_field, label = row
            if i == 0:
                sort = dstr1
                year_display = self._format_years_for_display(dstr1, dstr2)

            not_already_described = (dstr1, dstr2) not in described_years
            if not pub_info.get(pub_field, []) and not_already_described:
                display_date = self._format_years_for_display(dstr1, dstr2,
                                                              the=True)
                if display_date != 'dates unknown':
                    new_stext = '{}{}'.format(label, display_date)
                    pub_info[pub_field] = [new_stext]

        if not coded_dates and described_years:
            sort = sorted([y[0] for y in described_years])[0]
            year_display = self._format_years_for_display(sort)

        facet_dates, search_dates = [], []
        for ystrs, expanded in self._expand_years(coded_dates, described_years):
            if expanded:
                if self.year_for_boost is None:
                    boost_year = self._get_year_for_boost(ystrs, expanded[-1])
                    self.year_for_boost = boost_year
                facet_dates.extend(expanded)
            if ystrs:
                new_sdates = [self._format_years_for_display(y) for y in ystrs
                              if y != '9999']
                search_dates.extend(new_sdates)
        search_dates.extend([str(d) for d in facet_dates])

        ret_val = {'{}_display'.format(k): v for k, v in pub_info.items()}
        ret_val.update({
            'publication_sort': sort.replace('u', '-'),
            'publication_year_range_facet': list(set(facet_dates)),
            'publication_year_display': year_display,
            'publication_places_search': list(set(places)),
            'publishers_search': list(set(publishers)),
            'publication_dates_search': list(set(search_dates)),
            'publication_date_notes': publication_date_notes
        })
        return ret_val

    def get_access_info(self):
        r = self.r
        accessf, buildingf, shelff, collectionf = set(), set(), set(), set()

        # Note: We only consider bib locations if the bib record has no
        # attached items, in which case bib locations stand in for item
        # locations.

        item_rules = self.item_rules
        item_info = [{'location_id': l.item_record.location_id}
                        for l in r.bibrecorditemrecordlink_set.all()
                        if not l.item_record.is_suppressed]
        if len(item_info) == 0:
            item_info = [{'location_id': l.code} for l in r.locations.all()]

        for item in item_info:
            if item_rules['is_online'].evaluate(item):
                accessf.add(self.access_online_label)
            else:
                shelf = self.sierra_location_labels.get(item['location_id'],
                                                        None)
                building_lcode = item_rules['building_location'].evaluate(item)
                building = None
                if building_lcode is not None:
                    building = self.sierra_location_labels[building_lcode]
                    buildingf.add(building)
                    accessf.add(self.access_physical_label)
                if (shelf is not None) and (shelf != building):
                    if item_rules['is_at_public_location'].evaluate(item):
                        shelff.add(shelf)
            in_collections = item_rules['in_collections'].evaluate(item)
            if in_collections is not None:
                collectionf |= set(in_collections)

        return {
            'access_facet': list(accessf),
            'building_facet': list(buildingf),
            'shelf_facet': list(shelff),
            'collection_facet': list(collectionf),
        }

    def get_resource_type_info(self):
        rtype_info = self.bib_rules['resource_type'].evaluate(self.r)
        return {
            'resource_type': rtype_info['resource_type'],
            'resource_type_facet': rtype_info['resource_type_categories'],
            'media_type_facet': rtype_info['media_type_categories']
        }

    def compile_person(self, name_struct):
        heading, relations = name_struct['heading'], name_struct['relations']
        json = {'r': relations} if relations else {}
        fval = heading or None
        json['p'] = [{'d': heading, 'v': fval}]
        permutator = PersonalNamePermutator(name_struct)
        search_vals = permutator.get_search_permutations()
        base_name = (search_vals or [''])[-1]
        rel_search_vals = make_relator_search_variations(base_name, relations)
        return {'heading': heading, 'json': json, 'search_vals': search_vals,
                'relator_search_vals': rel_search_vals,
                'facet_vals': [fval],
                'short_author': shorten_name(name_struct)}

    def compile_org_or_event(self, name_struct):
        sep = self.hierarchical_name_separator
        heading, relations = '', name_struct['relations']
        json = {'r': relations} if relations else {}
        json['p'], facet_vals = [], []
        for i, part in enumerate(name_struct['heading_parts']):
            this_is_first_part = i == 0
            this_is_last_part = i == len(name_struct['heading_parts']) - 1
            json_entry = {'d': part['name']}
            if this_is_first_part:
                heading = part['name']
            else:
                heading = sep.join((heading, part['name']))
            fval = heading
            json_entry['v'] = fval
            json['p'].append(json_entry)
            facet_vals.append(fval)
            if 'qualifier' in part:
                qualifier = part['qualifier']
                need_punct_before_qualifier = bool(re.match(r'^\w', qualifier))
                if need_punct_before_qualifier:
                    heading = ', '.join((heading, qualifier))
                    json['p'][-1]['s'] = ', '
                else:
                    heading = ' '.join((heading, qualifier))
                ev_fval = heading
                json_entry = {'d': qualifier, 'v': ev_fval}
                json['p'].append(json_entry)
                facet_vals.append(ev_fval)
            if not this_is_last_part:
                json['p'][-1]['s'] = sep
        base_name = ' '.join([h['name'] for h in name_struct['heading_parts']])
        rel_search_vals = make_relator_search_variations(base_name, relations)
        return {'heading': heading, 'json': json, 'search_vals': [],
                'relator_search_vals': rel_search_vals,
                'facet_vals': facet_vals,
                'short_author': shorten_name(name_struct)}

    def select_best_name(self, names, org_event_default='combined'):
        if len(names) == 1:
            return names[0]

        for name in names:
            if name['parsed']['type'] == org_event_default:
                return name

    def do_facet_keys(self, struct, nf_chars=0):
        try:
            struct.get('p')
        except AttributeError:
            new_facet_vals = []
            for fval in struct:
                new_facet_vals.append(format_key_facet_value(fval, nf_chars))
            return new_facet_vals

        new_p = []
        for entry in struct.get('p', []):
            new_entry = entry
            if 'v' in new_entry:
                new_entry['v'] = format_key_facet_value(entry['v'], nf_chars)
            new_p.append(new_entry)
        struct['p'] = new_p
        return struct

    def _prep_author_summary_info(self, struct, org_event_default='combined',
                                  from_linking_field=False):
        if from_linking_field:
            return {
                'full_name': struct.get('author', ''),
                'short_name': struct.get('short_author', ''),
                'is_jd': False,
                'ntype': struct.get('author_type', ''),
            }

        name = self.select_best_name(struct, org_event_default)
        if name and name['compiled']['heading']:
            return {
                'full_name': name['compiled']['heading'],
                'short_name': name['compiled']['short_author'],
                'is_jd': name['parsed'].get('is_jurisdiction', False),
                'ntype': name['parsed']['type']
            }
        return {'full_name': '', 'short_name': '', 'is_jd': False, 'ntype': ''}

    def _prep_coll_title_parts(self, orig_title_parts, author_info, is_mform,
                               for_subject=False):
        title_parts = []
        p1 = orig_title_parts[0]
        num_parts = len(orig_title_parts)
        if author_info['short_name'] and not for_subject:
            is_org_event = author_info['ntype'] != 'person'
            conj = 'by' if is_mform else '' if is_org_event else 'of'
            p1 = format_title_short_author(p1, conj, author_info['short_name'])
        title_parts.append(p1)
        if num_parts == 1:
            if not author_info['is_jd']:
                title_parts.append('Complete')
        else:
            title_parts.extend(orig_title_parts[1:])
        return title_parts

    def prerender_authorized_title(self, title, auth_info, for_subject=False):
        sep = self.hierarchical_name_separator
        components = []

        is_coll = title['is_collective']
        is_mform = title['is_music_form']
        tparts = title['title_parts']
        eparts = title['expression_parts']
        volume = title.get('volume', '')
        issn = title.get('issn', '')
        ms = title['materials_specified']
        dc = title['display_constants']

        ms_str = format_materials_specified(ms) if ms else ''
        dc_str = format_display_constants(dc) if dc else ''
        before = ([ms_str] if ms_str else []) + ([dc_str] if dc_str else [])

        if is_coll:
            tparts = self._prep_coll_title_parts(tparts, auth_info, is_mform,
                                                 for_subject)
        for i, part in enumerate(tparts):
            this_is_first_part = i == 0
            this_is_last_part = i == len(tparts) - 1
            next_part = None if this_is_last_part else tparts[i + 1]
            d_part = part
            skip = part in ('Complete', 'Selections')
            skip_next = False

            if this_is_first_part:
                if not is_coll and auth_info['short_name'] and not for_subject:
                    conj = 'by' if auth_info['ntype'] == 'person' else ''
                    d_part = format_title_short_author(part, conj,
                                                       auth_info['short_name'])
            if not skip:
                component = {'facet': part, 'display': d_part, 'sep': sep}
                if next_part in ('Complete', 'Selections'):
                    next_part = '({})'.format(next_part)
                    d_part = ' '.join((d_part, next_part))
                    if not is_coll or is_mform or auth_info['is_jd']:
                        components.append({'facet': part, 'display': '',
                                           'sep': ' '})
                        next_facet_part = next_part
                    else:
                        next_facet_part = ' '.join((part, next_part))

                    component = {'facet': next_facet_part, 'display': d_part,
                                 'sep': sep}
                    skip_next = True
                components.append(component)

            if this_is_last_part and components:
                if volume:
                    volume_sep, volume = format_volume(volume)
                    components[-1]['sep'] = volume_sep
                    components.append({'facet': volume, 'display': volume})
                else:
                    components[-1]['sep'] = None

        id_parts = [{'label': 'ISSN', 'value': issn}] if issn else []

        return {
            'before_string': ' '.join(before),
            'title_components': components,
            'expression_components': eparts if eparts else None,
            'id_components': id_parts if id_parts else None,
        }

    def render_authorized_title(self, title, names, for_subject=False):
        best_author_type = 'combined' if for_subject else 'organization'
        author_info = self._prep_author_summary_info(names, best_author_type)
        pre_info = self.prerender_authorized_title(title, author_info,
                                                   for_subject)
        heading, json, facet_vals = '', {'p': []}, []

        if not for_subject and author_info['full_name']:
            json['a'] = format_key_facet_value(author_info['full_name'])

        if pre_info['before_string']:
            json['b'] = pre_info['before_string']

        prev_comp = {}
        for comp in pre_info['title_components']:
            prev_s = prev_comp.get('sep', '')
            heading = prev_s.join((heading, comp['facet']))
            facet_vals.append(heading)
            if comp['display']:
                json['p'].append({'d': comp['display'], 'v': heading})
                if json['p'] and comp.get('sep') and comp['sep'] != ' ':
                    json['p'][-1]['s'] = comp['sep']
            prev_comp = comp

        args = [pre_info['expression_components'], pre_info['id_components']]
        if any(args):
            kargs = {'json': json, 'facet_vals': facet_vals, 'heading': heading}
            result = self.render_title_expression_id(*args, **kargs)
            rkeys = ('json', 'facet_vals', 'heading')
            json, facet_vals, heading = (result[key] for key in rkeys)

        return {
            'json': json,
            'facet_vals': facet_vals,
            'heading': heading,
            'author_info': author_info,
            'work_heading': heading
        }

    def render_title_expression_id(self, exp_parts, id_parts, json=None, 
                                   facet_vals=None, heading=None,
                                   exp_is_part_of_heading=True):
        json = json or {'p': []}
        facet_vals = facet_vals or []
        heading = heading or ''

        internal_sep, section_sep = '; ', ' — '
        rendered_exp = ''

        if json['p']:
            json['p'][-1]['s'] = ' ('
        else:
            json['p'].append({'d': '('})
        if exp_parts:
            rendered_exp = internal_sep.join(exp_parts)
            new_p = {'d': rendered_exp}
            if exp_is_part_of_heading:
                new_p['v'] = '{} ({})'.format(heading, rendered_exp).lstrip()
                facet_vals.append(new_p['v'])
                paren = '({})'.format(rendered_exp)
                heading = ' '.join((heading, paren)) if heading else paren
            json['p'].append(new_p)
        if id_parts:
            display_ids = []
            if exp_parts:
                json['p'][-1]['s'] = section_sep
            to_render = []
            for i, id_part in enumerate(id_parts):
                is_last_id_part = i == len(id_parts) - 1
                value = id_part['value']
                label = id_part.get('label')
                link_key = id_part.get('link_key')
                display = ' '.join((label, value)) if label else value
                display_ids.append(display)
                if link_key:
                    if to_render:
                        json['p'].append({'d': internal_sep.join(to_render),
                                          's': internal_sep})
                    new_p = {'d': display, link_key: value}
                    if not is_last_id_part:
                        new_p['s'] = internal_sep
                    json['p'].append(new_p)
                else:
                    to_render.append(display)
            if to_render:
                json['p'].append({'d': internal_sep.join(to_render)})
            rendered_ids = internal_sep.join(display_ids)
        json['p'][-1]['s'] = ')'

        return {
            'json': json,
            'facet_vals': facet_vals,
            'heading': heading
        }

    def render_linking_field(self, linking, as_search=False):
        heading, disp_heading, json, facet_vals = '', '', {'p': []}, []
        sep = self.hierarchical_name_separator
        author_info = self._prep_author_summary_info(linking,
                                                     from_linking_field=True)
        label = linking['display_label']
        title_info = {
            'is_collective': linking['title_is_collective'],
            'is_music_form': linking['title_is_music_form'],
            'title_parts': linking['title_parts'],
            'expression_parts': None,
            'materials_specified': linking['materials_specified'],
            'display_constants': [label] if label else None,
            'volume': linking['volume']
        }
        pre_info = self.prerender_authorized_title(title_info, author_info)

        if pre_info['before_string']:
            json['b'] = pre_info['before_string']

        prev_comp = {}
        for comp in pre_info['title_components']:
            prev_s = prev_comp.get('sep', '')
            heading = prev_s.join((heading, comp['facet']))
            disp_heading = prev_s.join((disp_heading, comp['display']))
            facet_vals.append(heading)
            if comp['display'] and not as_search:
                json['p'].append({'d': comp['display'], 'v': heading})
                if json['p'] and comp.get('sep') and comp['sep'] != ' ':
                    json['p'][-1]['s'] = comp['sep']
            prev_comp = comp

        expression_components = linking['display_metadata'] or []
        id_components = []
        if as_search:
            id_map = linking['identifiers_map'] or {}
            new_jsonp = {'d': disp_heading}
            tkw = heading
            if tkw:
                # Limit the size of the linked `title` search to 20
                # words; strip quotation marks.
                new_jsonp['t'] = ' '.join(tkw.split(' ')[0:20]).replace('"', '')
            if linking['author']:
                new_jsonp['a'] = linking['author'].replace('"', '')
            for id_code in ('oclc', 'isbn', 'issn', 'lccn', 'w', 'coden',
                            'u', 'r'):
                if id_code in id_map:
                    numdef = id_map[id_code]
                    numtype = 'cn' if numdef['numtype'] == 'control' else 'sn'
                    new_jsonp[numtype] = numdef['number']
                    break
            json['p'].append(new_jsonp)

        for id_def in linking['identifiers_list'] or []:
            new_id_component = {
                'value': id_def['number'],
                'label': id_def['label']
            }
            if as_search:
                link_key = 'cn' if id_def['numtype'] == 'control' else 'sn'
                new_id_component['link_key'] = link_key
            id_components.append(new_id_component)

        work_heading = heading
        args = [expression_components, id_components]
        if any(args):
            kargs = {'json': json, 'facet_vals': facet_vals, 'heading': heading,
                     'exp_is_part_of_heading': False}
            result = self.render_title_expression_id(*args, **kargs)
            rkeys = ('json', 'facet_vals', 'heading',)
            json, facet_vals, heading = (result[key] for key in rkeys)

        return {
            'json': json,
            'facet_vals': facet_vals,
            'heading': heading,
            'work_heading': work_heading
        }

    def compile_added_title(self, field, title_struct, names):
        if not title_struct['title_parts']:
            return None

        rendered = self.render_authorized_title(title_struct, names)
        s_rendered = None
        if field.tag.startswith('6'):
            s_rendered = self.render_authorized_title(title_struct, names, True)

        title_key = ''
        if len(rendered['facet_vals']):
            title_key = rendered['facet_vals'][-1]


        return {
            'author_info': rendered['author_info'],
            'heading': rendered['heading'],
            'work_title_key': rendered['work_heading'],
            'title_key': title_key,
            'json': rendered['json'],
            'search_vals': [rendered['heading']],
            'facet_vals': rendered['facet_vals'],
            'as_subject': s_rendered
        }

    def parse_nametitle_field(self, f, names=None, title=None, try_title=True):
        def gather_name_info(field, name):
            ctype = 'person' if name['type'] == 'person' else 'org_or_event'
            compiled = getattr(self, 'compile_{}'.format(ctype))(name)
            if compiled and compiled['heading']:
                return {'field': field, 'parsed': name, 'compiled': compiled}

        def gather_title_info(field, title, names):
            compiled = self.compile_added_title(field, title, names)
            if compiled and compiled['heading']:
                return {'field': field, 'parsed': title, 'compiled': compiled}

        entry = {'names': names or [], 'title': title}
        if not names:
            names = extract_name_structs_from_field(f)
            name_info = [gather_name_info(f, n) for n in names]
            entry['names'] = [n for n in name_info if n is not None]

        if try_title and not title:
            title = extract_title_struct_from_field(f)
            if title:
                entry['title'] = gather_title_info(f, title, entry['names'])
                if title['type'] in ('main', 'analytic'):
                    self.title_languages.extend(title.get('languages', []))
        return entry

    def parse_nonsubject_name_titles(self):
        if self.name_titles:
            for entry in self.name_titles:
                yield entry
        else:
            entry = {'names': [], 'title': None}
            for f in self.marc_fieldgroups.get('main_author', []):
                entry = self.parse_nametitle_field(f, try_title=False)
                break

            for f in self.marc_fieldgroups.get('uniform_title', []):
                entry = self.parse_nametitle_field(f, names=entry['names'])
                break

            self.name_titles = [entry]
            yield entry

            title_added = self.marc_fieldgroups.get('title_added_entry', [])
            series_added = self.marc_fieldgroups.get('series_added_entry', [])
            for f in (title_added + series_added):
                entry = self.parse_nametitle_field(f)
                self.name_titles.append(entry)
                yield entry

    def get_contributor_info(self):
        """
        This is responsible for using the 100, 110, 111, 700, 710, 711,
        800, 810, and 811 to determine the entirety of author,
        contributor, and meeting fields.
        """
        author_json, contributors_json, meetings_json = {}, [], []
        author_search, contributors_search, meetings_search = [], [], []
        author_contributor_facet, meeting_facet = [], []
        responsibility_search = []
        a_sort = None
        headings_set = set()

        for entry in self.parse_nonsubject_name_titles():
            for name in entry['names']:
                compiled = name['compiled']
                field = name['field']
                parsed = name['parsed']
                json = self.do_facet_keys(compiled['json'])
                facet_vals = self.do_facet_keys(compiled['facet_vals'])
                this_is_event = parsed['type'] == 'event'
                this_is_1XX = field.tag.startswith('1')
                this_is_7XX = field.tag.startswith('7')
                this_is_8XX = field.tag.startswith('8')
                is_combined = parsed['type'] == 'combined'

                if compiled['heading'] not in headings_set and not is_combined:
                    if this_is_event:
                        meetings_search.append(compiled['heading'])
                        meetings_search.extend(compiled['search_vals'])
                        meeting_facet.extend(facet_vals)
                        meetings_json.append(json)
                    else:
                        have_seen_author = bool(author_contributor_facet)
                        if not have_seen_author:
                            if this_is_1XX or this_is_7XX:
                                a_sort = generate_facet_key(compiled['heading'])
                            if this_is_1XX:
                                author_json = json
                                search_vals = [compiled['heading']]
                                search_vals.extend(compiled['search_vals'])
                                author_search.extend(search_vals)
                                contributors_search.extend(search_vals)
                        if have_seen_author or this_is_7XX or this_is_8XX:
                            contributors_search.append(compiled['heading'])
                            contributors_search.extend(compiled['search_vals'])
                            contributors_json.append(json)
                        author_contributor_facet.extend(facet_vals)
                    rel_search_vals = compiled['relator_search_vals']
                    responsibility_search.extend(rel_search_vals)
                    headings_set.add(compiled['heading'])

        return {
            'author_json': ujson.dumps(author_json) if author_json else None,
            'contributors_json': [ujson.dumps(v) for v in contributors_json]
                                 or None,
            'meetings_json': [ujson.dumps(v) for v in meetings_json]
                             or None,
            'author_search': author_search or None,
            'contributors_search': contributors_search or None,
            'meetings_search': meetings_search or None,
            'author_contributor_facet': author_contributor_facet or None,
            'meeting_facet': meeting_facet or None,
            'author_sort': a_sort,
            'responsibility_search': responsibility_search or None
        }

    def analyze_name_titles(self, entries):
        parsed_130_240, incl_authors = None, set()
        main_author = None
        num_controlled_at = 0
        num_uncontrolled_at = 0
        analyzed_entries = []

        for entry in entries:
            analyzed_entry = entry

            if not main_author:
                name = self.select_best_name(entry['names'], 'organization')
                if name and name['compiled']['heading']:
                    if name['field'].tag in ('100', '110', '111'):
                        main_author = name

            title = entry['title']
            if title:
                if title['field'].tag in ('130', '240', '243'):
                    parsed_130_240 = entry

                analyzed_entry['is_740'] = title['field'].tag == '740'
                if title['parsed']['type'] in ('analytic', 'main'):
                    if title['parsed']['type'] == 'analytic':
                        analyzed_entry['title_type'] = 'included'
                    else:
                        analyzed_entry['title_type'] = 'main'
                    author_info = title['compiled']['author_info']
                    if author_info['full_name']:
                        incl_authors.add(author_info['full_name'])
                    if title['parsed']['type'] == 'analytic':
                        if analyzed_entry['is_740']:
                            num_uncontrolled_at += 1
                        else:
                            num_controlled_at += 1
                else:
                    analyzed_entry['title_type'] = title['parsed']['type']
            analyzed_entries.append(analyzed_entry)
        return {
            'main_author': main_author,
            'num_controlled_analytic_titles': num_controlled_at,
            'num_uncontrolled_analytic_titles': num_uncontrolled_at,
            'parsed_130_240': parsed_130_240,
            'num_included_works_authors': len(incl_authors),
            'analyzed_entries': analyzed_entries
        }

    def truncate_each_ttitle_part(self, ttitle, thresh=200, min_len=80,
                                  max_len=150):
        truncator = p.Truncator([r':\s'], True)
        for i, full_part in enumerate(ttitle.get('parts', [])):
            disp_part = full_part
            if i == 0 and len(full_part) > thresh:
                disp_part = truncator.truncate(full_part, min_len, max_len)
                disp_part = '{} ...'.format(disp_part)
            yield (disp_part, full_part)

    def compile_main_title(self, transcribed, nf_chars, parsed_130_240):
        display, non_trunc, search, sortable = '', '', [], ''
        primary_main_title = ''
        sep = self.hierarchical_name_separator
        has_truncation = False
        if transcribed:
            disp_titles, raw_disp_titles, full_titles = [], [], []
            for i, title in enumerate(transcribed):
                disp_parts, full_parts = [], []
                if i == 0 and title:
                    first_part = (title.get('parts') or [''])[0]
                    primary_main_title = first_part.split(':')[0]
                for disp, full in self.truncate_each_ttitle_part(title):
                    disp_parts.append(disp)
                    full_parts.append(full)
                    if not has_truncation and disp != full:
                        has_truncation = True

                raw_disp_title = sep.join(disp_parts)
                full_title = sep.join(full_parts)

                ptitles = []
                for ptitle in title.get('parallel', []):
                    rendered = sep.join(ptitle.get('parts', []))
                    if rendered:
                        search.append(rendered)
                        ptitles.append(rendered)
                if ptitles:
                    translation = format_translation('; '.join(ptitles))
                    disp_title = ' '.join([raw_disp_title, translation])
                else:
                    disp_title = raw_disp_title

                disp_titles.append(disp_title)
                raw_disp_titles.append(raw_disp_title)
                full_titles.append(full_title)

            display = '; '.join(disp_titles)
            raw_display = '; '.join(raw_disp_titles)
            non_trunc = '; '.join(full_titles) if has_truncation else None
            search = [non_trunc or raw_display or []] + search
            sortable = non_trunc or raw_display
        elif parsed_130_240:
            title = parsed_130_240['title']
            display = title['compiled']['heading']
            if display:
                primary_main_title = display.split(' > ')[0].split(':')[0]
            else:
                primary_main_title = ''
            nf_chars = title['parsed']['nonfiling_chars']
            search, sortable = [display], display

        return {
            'display': display,
            'non_truncated': non_trunc or None,
            'search': search,
            'primary_main_title': primary_main_title,
            'sort': generate_facet_key(sortable, nf_chars) if sortable else None
        }

    def needs_added_ttitle(self, f245_ind1, nth_ttitle, total_ttitles, f130_240,
                           total_analytic_titles):
        # If 245 ind1 is 0, then we explicitly don't create an added
        # entry (i.e. facet value) for it.
        if f245_ind1 == '0':
            return False

        if nth_ttitle == 0:
            # If this is the first/only title from 245 and there
            # is a 130/240, then we assume the first title from 245
            # should not create an added facet because it's likely to 
            # duplicate that 130/240.
            if f130_240:
                if total_ttitles == 1:
                    return False

            # If we're here it means there's no 130/240. At this point 
            # we add the first/only title from the 245 if it's probably
            # not duplicated in a 700-730. I.e., if it's the only title
            # in the 245, then it's probably the title for the whole
            # resource and there won't be an added analytical title for
            # it. (If there were, it would probably be the 130/240.)
            # Or, if there are multiple titles in the 245 but there are not
            # enough added analytical titles on the record to cover all
            # the individual titles in the 245, then the later titles
            # are more likely than the first to be covered, so we
            # should go ahead and add the first.
            if total_ttitles == 1:
                return 'main'

            if total_ttitles > total_analytic_titles:
                return 'included'
        return 'included' if total_analytic_titles == 0 else False

    def compile_added_ttitle(self, ttitle, nf_chars, author,
                             needs_author_in_title):
        if not ttitle.get('parts', []):
            return None

        auth_info = self._prep_author_summary_info([author])
        sep = self.hierarchical_name_separator
        search, heading, json = [], '', {'p': []}
        if auth_info['full_name']:
            json['a'] = format_key_facet_value(auth_info['full_name'])
        facet_vals = []

        for i, res in enumerate(self.truncate_each_ttitle_part(ttitle)):
            part = res[0]
            this_is_first_part = i == 0
            this_is_last_part = i == len(ttitle['parts']) - 1

            if this_is_first_part:
                heading = part
                if needs_author_in_title and auth_info['short_name']:
                    conj = 'by' if auth_info['ntype'] == 'person' else ''
                    part = format_title_short_author(part, conj,
                                                     auth_info['short_name'])
            else:
                heading = sep.join((heading, part))

            json_entry = {'d': part, 'v': heading}
            if not this_is_last_part:
                json_entry['s'] = sep

            json['p'].append(json_entry)
            facet_vals.append(heading)

        search, ptitles = [heading], []
        for ptitle in ttitle.get('parallel', []):
            ptstr = sep.join(ptitle.get('parts', []))
            if ptstr:
                search.append(ptstr)
                ptitles.append(ptstr)
                facet_vals.append(ptstr)

        if ptitles:
            translation = format_translation('; '.join(ptitles))
            if json['p']:
                json['p'][-1]['s'] = ' '
            json['p'].append({'d': translation})

        return {
            'heading': heading,
            'title_key': '' if not len(facet_vals) else facet_vals[-1],
            'work_title_key': heading,
            'json': json,
            'search_vals': search,
            'facet_vals': facet_vals
        }

    def _match_name_from_sor(self, nametitle_entries, sor):
        for entry in nametitle_entries:
            for name in entry['names']:
                heading = name['compiled']['heading']
                if heading and p.sor_matches_name_heading(sor, heading):
                    return name

    def get_title_info(self):
        """
        This is responsible for using the 130, 240, 242, 243, 245, 246,
        247, 490, 700, 710, 711, 730, 740, 800, 810, 811, and 830 to
        determine the entirety of title and series fields.
        """
        main_title_info = {}
        main_search = []
        json_fields = {'main': '', 'included': [], 'related': [], 'series': []}
        search_fields = {'included': [], 'related': [], 'series': []}
        title_keys = {'included': set(), 'related': set(), 'series': set()}
        work_title_keys = {'included': set(), 'related': set(), 'series': set()}
        variant_titles_notes, variant_titles_search = [], []
        title_series_facet = []
        title_sort = ''
        responsibility_display, responsibility_search = '', []
        hold_740s = []

        name_titles = self.parse_nonsubject_name_titles()
        analyzed_name_titles = self.analyze_name_titles(name_titles)
        num_iw_authors = analyzed_name_titles['num_included_works_authors']
        num_cont_at = analyzed_name_titles['num_controlled_analytic_titles']
        num_uncont_at = analyzed_name_titles['num_uncontrolled_analytic_titles']
        parsed_130_240 = analyzed_name_titles['parsed_130_240']
        analyzed_entries = analyzed_name_titles['analyzed_entries']
        main_author = analyzed_name_titles['main_author']

        for entry in analyzed_entries:
            if entry['title']:
                compiled = entry['title']['compiled']
                parsed = entry['title']['parsed']
                nfc = parsed['nonfiling_chars']
                json = self.do_facet_keys(compiled['json'], nfc)
                search_vals = compiled['search_vals']
                facet_vals = self.do_facet_keys(compiled['facet_vals'], nfc)
                title_key = generate_facet_key(compiled['title_key'], nfc)
                wt_key = generate_facet_key(compiled['work_title_key'], nfc)
                if entry['is_740']:
                    hold_740s.append({
                        'title_type': entry['title_type'],
                        'json': json,
                        'svals': search_vals,
                        'fvals': facet_vals,
                        'title_key': title_key,
                        'work_title_key': wt_key,
                    })
                else:
                    if entry['title_type'] == 'main':
                        json_fields['main'] = json
                        search_fields['included'].extend(search_vals)
                        title_keys['included'].add(title_key)
                        work_title_keys['included'].add(wt_key)
                    else:
                        json_fields[entry['title_type']].append(json)
                        search_fields[entry['title_type']].extend(search_vals)
                        title_keys[entry['title_type']].add(title_key)
                        work_title_keys[entry['title_type']].add(wt_key)
                    title_series_facet.extend(facet_vals)

        f245, parsed_245 = None, {}
        for f in self.marc_fieldgroups.get('transcribed_title', []):
            f245 = f
            parsed_245 = TranscribedTitleParser(f).parse()
            break
        transcribed = parsed_245.get('transcribed', [])
        nf_chars = parsed_245.get('nonfiling_chars', 0)
        main_title_info = self.compile_main_title(transcribed, nf_chars,
                                                  parsed_130_240)
        sor_display_values, author = [], main_author
        for i, ttitle in enumerate(transcribed):
            sor = ''
            is_first = i == 0
            if 'responsibility' in ttitle:
                author = '' if (sor or not is_first) else main_author
                sor = ttitle['responsibility']
                responsibility_search.append(sor)

            psor_display_values = []
            for ptitle in ttitle.get('parallel', []):
                if 'parts' in ptitle:
                    vt = self.hierarchical_name_separator.join(ptitle['parts'])
                    display_text = TranscribedTitleParser.variant_types['1']
                    note = '{}: {}'.format(display_text, vt)
                    variant_titles_notes.append(note)
                    variant_titles_search.append(vt)

                if 'responsibility' in ptitle:
                    psor = ptitle['responsibility']
                    if psor not in responsibility_search:
                        responsibility_search.append(psor)
                    psor_display_values.append(psor)

            if sor:
                if psor_display_values:
                    psor = '; '.join(psor_display_values)
                    psor_translation = format_translation(psor)
                    sor_display_values.append(' '.join([sor, psor_translation]))
                else:
                    sor_display_values.append(sor)

            # `if needs_added_ttitle()` means, "If an added entry needs
            # to be created for this transcribed title" ...
            added_tt = self.needs_added_ttitle(f245.indicator1, i,
                                               len(transcribed), parsed_130_240,
                                               num_cont_at)
            if added_tt:
                if not author and sor:
                    author = self._match_name_from_sor(analyzed_entries, sor)

                # needs_author_in_title = num_iw_authors > 1
                nfc = nf_chars if is_first else 0
                compiled = self.compile_added_ttitle(ttitle, nfc, author, True)
                if compiled is not None:
                    json = json_fields['included']
                    sv = search_fields['included']
                    fv = title_series_facet
                    njson = self.do_facet_keys(compiled['json'], nfc)
                    nsv = compiled['search_vals']
                    nfv = self.do_facet_keys(compiled['facet_vals'], nfc)

                    if added_tt == 'main':
                        json_fields['main'] = njson
                    else:
                        json_fields['included'] = json[:i] + [njson] + json[i:]

                    search_fields['included'] = sv[:i] + nsv + sv[i:]
                    title_series_facet = fv[:i] + nfv + fv[i:]
                    t_key = generate_facet_key(compiled['title_key'], nfc)
                    wt_key = generate_facet_key(compiled['work_title_key'], nfc)
                    title_keys['included'].add(t_key)
                    work_title_keys['included'].add(wt_key)

        responsibility_display = '; '.join(sor_display_values)

        for entry in hold_740s:
            if entry['title_key'] not in title_keys[entry['title_type']]:
                json_fields[entry['title_type']].append(entry['json'])
                search_fields[entry['title_type']].extend(entry['svals'])
                title_series_facet.extend(entry['fvals'])
                title_keys[entry['title_type']].add(entry['title_key'])
                wt_key = entry['work_title_key']
                work_title_keys[entry['title_type']].add(wt_key)

        for f in self.marc_fieldgroups.get('key_title', []):
            t = ' '.join([sf[1] for sf in f.filter_subfields('ab')])
            if t:
                if t not in variant_titles_search:
                    variant_titles_search.append(t)
                if f.tag == '210':
                    label = 'Abbreviated title'
                else:
                    label = 'ISSN key title'
                variant_titles_notes.append('{}: {}'.format(label, t))

        for f in self.marc_fieldgroups.get('alternate_title', []):
            parsed = TranscribedTitleParser(f).parse()
            f246_add_notes = f.tag == '246' and f.indicator1 in ('01')
            f247_add_notes = f.tag == '247' and f.indicator2 == '0'
            add_notes = f.tag == '242' or f246_add_notes or f247_add_notes
            display_text = parsed.get('display_text', '')
            for vtitle in parsed.get('transcribed', []):
                if 'parts' in vtitle:
                    t = self.hierarchical_name_separator.join(vtitle['parts'])
                    if t not in variant_titles_search:
                        variant_titles_search.append(t)
                    if add_notes:
                        if display_text:
                            note = '{}: {}'.format(display_text, t)
                        else:
                            note = t
                        if note not in variant_titles_notes:
                            variant_titles_notes.append(note)
                if 'responsibility' in vtitle:
                    if vtitle['responsibility'] not in responsibility_search:
                        responsibility_search.append(vtitle['responsibility'])

        for f in self.marc_fieldgroups.get('series_statement', []):
            if f.indicator1 == '0':
                before, id_parts = '', []
                parsed = TranscribedTitleParser(f).parse()
                if 'materials_specified' in parsed:
                    ms = parsed['materials_specified']
                    before = format_materials_specified(ms)
                if 'issn' in parsed:
                    id_parts.append({'label': 'ISSN', 'value': parsed['issn']})
                if 'lccn' in parsed:
                    id_parts.append({'label': 'LC Call Number',
                                     'value': parsed['lccn']})

                for stitle in parsed['transcribed']:
                    parts = stitle.get('parts', [])
                    sor = stitle.get('responsibility')
                    if parts and sor:
                        parts[0] = '{} [{}]'.format(parts[0], sor)
                    st_heading = self.hierarchical_name_separator.join(parts)
                    new_json = {'p': [{'d': st_heading}]}
                    wt_key = generate_facet_key(st_heading)
                    work_title_keys['series'].add(wt_key)
                    if before:
                        new_json['b'] = before
                    if id_parts:
                        args = [None, id_parts]
                        kargs = {'json': new_json, 'heading': st_heading,
                                 'exp_is_part_of_heading': False}
                        result = self.render_title_expression_id(*args, **kargs)
                        new_json = result['json']
                        st_heading = result['heading']
                    json_fields['series'].append(new_json)
                    search_fields['series'].append(st_heading)

        mwork_json = None
        if json_fields['main']:
            mwork_json = ujson.dumps(json_fields['main'])
        iworks_json = [ujson.dumps(v) for v in json_fields['included'] if v]
        rworks_json = [ujson.dumps(v) for v in json_fields['related'] if v]
        series_json = [ujson.dumps(v) for v in json_fields['series'] if v]
        if main_title_info['primary_main_title']:
            main_search.append(main_title_info['primary_main_title'])
        for title in main_title_info['search']:
            if title and title not in variant_titles_search:
                variant_titles_search.append(title)

        music_fields = self.marc_fieldgroups.get('music_number_and_key', [])
        if music_fields:
            title_test_keys = work_title_keys['included']
            if main_title_info['sort']:
                title_test_keys.add(main_title_info['sort'])
            for f in music_fields:
                val_stack = []
                for val in f.get_subfields(*tuple('abcde')):
                    val_key = generate_facet_key(val)
                    print(val_key)
                    if not any([val_key in k for k in title_test_keys]):
                        val_stack.append(val)
                if val_stack:
                    variant_titles_search.append(' '.join(val_stack))

        self.work_title_keys = work_title_keys
        return {
            'title_display': main_title_info['display'] or None,
            'non_truncated_title_display': main_title_info['non_truncated'],
            'main_work_title_json': mwork_json or None,
            'included_work_titles_json': iworks_json or None,
            'related_work_titles_json': rworks_json or None,
            'related_series_titles_json': series_json or None,
            'variant_titles_notes': variant_titles_notes or None,
            'main_title_search': main_search or None,
            'included_work_titles_search': search_fields['included'] or None,
            'related_work_titles_search': search_fields['related'] or None,
            'related_series_titles_search': search_fields['series'] or None,
            'variant_titles_search': variant_titles_search or None,
            'title_series_facet': title_series_facet or None,
            'title_sort': main_title_info['sort'] or None,
            'responsibility_search': responsibility_search or None,
            'responsibility_display': responsibility_display or None
        }

    def compile_performance_medium(self, parsed_pm):
        def _render_instrument(entry):
            instrument, number = entry[:2]
            render_stack = [instrument]
            if number != '1':
                render_stack.append('({})'.format(number))
            if len(entry) == 3:
                notes = entry[2]
                render_stack.append('[{}]'.format(' / '.join(notes)))
            return ' '.join(render_stack)

        def _render_clause(rendered_insts, conjunction, prefix):
            if prefix:
                render_stack = [' '.join((prefix, rendered_insts[0]))]
            else:
                render_stack = [rendered_insts[0]]
            num_insts = len(rendered_insts)
            item_sep = ', ' if num_insts > 2 else ' '
            if num_insts > 1:
                last_inst = ' '.join((conjunction, rendered_insts[-1]))
                render_stack.extend(rendered_insts[1:-1] + [last_inst])
            return item_sep.join(render_stack)

        def _render_totals(parsed_pm):
            render_stack, nums = [], {}
            nums['performer'] = parsed_pm['total_performers']
            nums['ensemble'] = parsed_pm['total_ensembles']
            for entity_type, num in nums.items():
                if num:
                    s = '' if num == '1' else 's'
                    render_stack.append('{} {}{}'.format(num, entity_type, s))
            return ' and '.join(render_stack)

        totals = _render_totals(parsed_pm)
        compiled_parts = []
        for parsed_part in parsed_pm['parts']:
            rendered_clauses = []
            for clause in parsed_part:
                part_type, instruments = list(clause.items())[0]
                conjunction = 'or' if part_type == 'alt' else 'and'
                prefix = part_type if part_type in ('doubling', 'solo') else ''
                rendered_insts = [_render_instrument(i) for i in instruments]
                if part_type == 'alt':
                    if len(rendered_clauses):
                        last_clause = rendered_clauses.pop()
                        rendered_insts = [last_clause] + rendered_insts
                rendered_clause = _render_clause(rendered_insts, conjunction,
                                                 prefix)
                rendered_clauses.append(rendered_clause)
            compiled_parts.append(' '.join(rendered_clauses))
        pstr = '; '.join(compiled_parts)
        final_stack = ([totals] if totals else []) + ([pstr] if pstr else [])
        if final_stack:
            final_render = ': '.join(final_stack)
            if parsed_pm['materials_specified']:
                ms_render = ', '.join(parsed_pm['materials_specified'])
                final_render = ' '.join(('({})'.format(ms_render), final_render))
            return ''.join([final_render[0].upper(), final_render[1:]])

    def get_notes(self):
        """
        This is the main method responsible for returning notes fields,
        which we're characterizing as both 3XX and 5XX fields. I.e., we
        are using most 3XX fields to generate a note.
        """
        label_maps = {
            '520': {
                '0': 'Subject',
                '1': 'Review',
                '2': 'Scope and content',
                '3': 'Abstract',
                '4': 'Content advice'
            },
            '521': {
                '0': 'Reading grade level',
                '1': 'Ages',
                '2': 'Grades',
                '3': 'Special audience characteristics',
                '4': 'Motivation/interest level'
            },
            '588': {
                '0': 'Description based on',
                '1': 'Latest issue consulted'
            }
        }
        def join_subfields_with_spaces(f, sf_filter, label=None):
            return GenericDisplayFieldParser(f, ' ', sf_filter, label).parse()

        def join_subfields_with_semicolons(f, sf_filter, label=None):
            return GenericDisplayFieldParser(f, '; ', sf_filter, label).parse()

        def get_subfields_as_list(f, sf_filter):
            return [v for sf, v in f.filter_subfields(**sf_filter)]

        def parse_performance_medium(field, sf_filter):
            parsed = PerformanceMedParser(field).parse()
            return self.compile_performance_medium(parsed)

        def parse_502_dissertation_notes(field, sf_filter):
            if field.get_subfields('a'):
                return join_subfields_with_spaces(field, {'include': 'ago'})
            parsed_dn = DissertationNotesFieldParser(field).parse()
            diss_note = '{}.'.format('. '.join(parsed_dn['note_parts']))
            return p.normalize_punctuation(diss_note)

        def parse_511_performers(field, sf_filter):
            label = 'Cast' if field.indicator1 == '1' else None
            return join_subfields_with_spaces(field, sf_filter, label)

        def parse_520_summary_notes(field, sf_filter):
            class SummaryParser(GenericDisplayFieldParser):
                def parse_subfield(self, tag, val):
                    if tag == 'c':
                        val = p.strip_brackets(val, keep_inner=True,
                                               to_remove_re=r'', protect_re=r'')
                        val = p.strip_ends(val, end='right')
                        val = '[{}]'.format(val)
                    super(SummaryParser, self).parse_subfield(tag, val)
            label = label_maps['520'].get(field.indicator1, None)
            return SummaryParser(field, ' ', sf_filter, label).parse()

        def parse_audience(field, sf_filter):
            ind1 = ' ' if field.tag == '385' else field.indicator1
            label = label_maps['521'].get(ind1)
            val = join_subfields_with_semicolons(field, sf_filter, label)
            if field.tag == '521':
                source = ', '.join(field.get_subfields('b'))
                if source:
                    val = '{} (source: {})'.format(val, p.strip_ends(source))
            return val

        def parse_creator_demographics(field, sf_filter):
            labels = [p.strip_ends(sf) for sf in field.get_subfields('i')]
            label = ', '.join(labels) if labels else None
            return join_subfields_with_semicolons(field, sf_filter, label)

        def parse_system_details(field, sf_filter):
            if field.tag == '753':
                return join_subfields_with_semicolons(field, sf_filter)

            class Field538Parser(GenericDisplayFieldParser):
                def determine_separator(self, val):
                    return '; ' if val[-1].isalnum() else ' '

            return Field538Parser(field, '', sf_filter).parse()

        def parse_all_other_notes(field, sf_filter):
            label = label_maps.get(field.tag, {}).get(field.indicator1)
            if field.tag == '583':
                if field.indicator1 == '1':
                    return join_subfields_with_semicolons(field, sf_filter,
                                                          label)
                return None
            return join_subfields_with_spaces(field, sf_filter, label)

        fgroups = ('system_details', 'physical_description', 'notes',
                   'curriculum_objective')
        marc_stub_rec = SierraMarcRecord(force_utf8=True)
        for fgroup in fgroups:
            marc_stub_rec.add_field(*self.marc_fieldgroups.get(fgroup, []))

        record_parser = MultiFieldMarcRecordParser(marc_stub_rec, {
            '310': {
                'solr_fields': ('current_publication_frequency',
                                'publication_dates_search')
            },
            '321': {
                'solr_fields': ('former_publication_frequency',
                                'publication_dates_search')
            },
            '340': {
                'solr_fields': ('physical_medium', 'type_format_search'),
                'parse_func': join_subfields_with_semicolons
            },
            '342': {
                'solr_fields': ('geospatial_data', 'type_format_search'),
                'parse_func': join_subfields_with_semicolons
            },
            '343': {
                'solr_fields': ('geospatial_data', 'type_format_search'),
                'parse_func': join_subfields_with_semicolons
            },
            '344': {
                'solr_fields': ('audio_characteristics', 'type_format_search'),
                'parse_func': join_subfields_with_semicolons
            },
            '345': {
                'solr_fields': ('projection_characteristics',
                                'type_format_search'),
                'parse_func': join_subfields_with_semicolons
            },
            '346': {
                'solr_fields': ('video_characteristics', 'type_format_search'),
                'parse_func': join_subfields_with_semicolons
            },
            '347': {
                'solr_fields': ('digital_file_characteristics',
                                'type_format_search'),
                'parse_func': join_subfields_with_semicolons
            },
            '348': {
                'subfields': {'include': 'a'},
                'solr_fields': ('type_format_search',),
                'parse_func': get_subfields_as_list
            },
            '351': {
                'solr_fields': ('arrangement_of_materials', 'notes_search')
            },
            '352': {
                'solr_fields': ('graphic_representation', 'type_format_search')
            },
            '370': {
                'subfields': {'include': '3cfgist'},
                'solr_fields': ('physical_description', 'notes_search')
            },
            '382': {
                'solr_fields': ('performance_medium', 'type_format_search'),
                'parse_func': parse_performance_medium
            },
            '385': {
                'subfields': {'include': '3a'},
                'solr_fields': ('audience', 'notes_search'),
                'parse_func': parse_audience
            },
            '386': {
                'subfields': {'include': '3a'},
                'solr_fields': ('creator_demographics', 'notes_search'),
                'parse_func': parse_creator_demographics
            },
            '388': {
                'subfields': {'include': 'a'},
                'solr_fields': ('notes_search',),
                'parse_func': get_subfields_as_list
            },
            '502': {
                'solr_fields': ('dissertation_notes', 'notes_search'),
                'parse_func': parse_502_dissertation_notes
            },
            '505': {
                'solr_fields': ('toc_notes',)
            },
            '508': {
                'solr_fields': ('production_credits', 'responsibility_search')
            },
            '511': {
                'solr_fields': ('performers', 'responsibility_search'),
                'parse_func': parse_511_performers
            },
            '520': {
                'solr_fields': ('summary_notes', 'notes_search'),
                'parse_func': parse_520_summary_notes
            },
            '521': {
                'subfields': {'include': '3a'},
                'solr_fields': ('audience', 'notes_search'),
                'parse_func': parse_audience
            },
            '538': {
                'solr_fields': ('system_details', 'type_format_search'),
                'parse_func': parse_system_details
            },
            '546': {
                'solr_fields': ('language_notes', 'type_format_search'),
            },
            '658': {
                'solr_fields': ('curriculum_objectives', 'notes_search'),
                'parse_func': join_subfields_with_semicolons
            },
            '753': {
                'solr_fields': ('system_details', 'type_format_search'),
                'parse_func': parse_system_details
            },
            'n': {
                'solr_fields': ('notes', 'notes_search'),
                'parse_func': parse_all_other_notes
            },
            'r': {
                'solr_fields': ('physical_description', 'type_format_search')
            },
            'exclude': set(IGNORED_MARC_FIELDS_BY_GROUP_TAG['r']
                           + IGNORED_MARC_FIELDS_BY_GROUP_TAG['n'] 
                           + ('377', '380', '592',))
        }, utils=self.utils)
        return record_parser.parse()

    def get_call_number_info(self):
        """
        Return a dict containing information about call numbers and
        sudoc numbers to load into Solr fields. Note that bib AND item
        call numbers are included, but they are deduplicated.
        """
        call_numbers_display, call_numbers_search = [], []
        sudocs_display, sudocs_search = [], []

        call_numbers = self.r.get_call_numbers() or []

        item_links = [l for l in self.r.bibrecorditemrecordlink_set.all()]
        for link in sorted(item_links, key=lambda l: l.items_display_order):
            item = link.item_record
            if not item.is_suppressed:
                call_numbers.extend(item.get_call_numbers() or [])

        for cn, cntype in call_numbers:
            searchable = make_searchable_callnumber(cn)
            if cntype == 'sudoc':
                if cn not in sudocs_display:
                    sudocs_display.append(cn)
                    sudocs_search.extend(searchable)
            elif cn not in call_numbers_display:
                call_numbers_display.append(cn)
                call_numbers_search.extend(searchable)

        return {
            'call_numbers_display': call_numbers_display or None,
            'call_numbers_search': call_numbers_search or None,
            'sudocs_display': sudocs_display or None,
            'sudocs_search': sudocs_search or None,
        }

    def get_standard_number_info(self):
        isbns_display, issns_display, others_display, search = [], [], [], []
        isbns, issns = [], []
        all_standard_numbers = []

        for f in self.marc_fieldgroups.get('standard_numbers', []):
            for p in StandardControlNumberParser(f).parse():
                nums = [p[k] for k in ('normalized', 'number') if k in p]
                for num in nums:
                    search.append(num)
                    all_standard_numbers.append(num)
                display = format_number_display_val(p)
                if p['type'] == 'isbn':
                    isbns_display.append(display)
                    if p['is_valid'] and nums and nums[0] not in isbns:
                        isbns.append(nums[0])
                elif p['type'] in ('issn', 'issnl'):
                    issns_display.append(display)
                    if p['is_valid'] and nums and nums[0] not in issns:
                        issns.append(nums[0])
                else:
                    others_display.append(display)

        return {
            'isbns_display': isbns_display or None,
            'issns_display': issns_display or None,
            'isbn_numbers': isbns or None,
            'issn_numbers': issns or None,
            'other_standard_numbers_display': others_display or None,
            'all_standard_numbers': all_standard_numbers or None,
            'standard_numbers_search': search or None,
        }

    def compile_control_numbers(self, f):
        for p in StandardControlNumberParser(f).parse():
            nums = [p[k] for k in ('normalized', 'number') if k in p]
            numtype = p['type'] if p['type'] in ('lccn', 'oclc') else 'others'
            oclc_and_suffix = None
            if numtype == 'oclc' and 'oclc_suffix' in p:
                oclc_and_suffix = ''.join((nums[0], p['oclc_suffix']))
            yield {
                'main_number': nums[0],
                'type': numtype,
                'all_numbers': nums,
                'is_valid': p['is_valid'],
                'oclc_and_suffix': oclc_and_suffix,
                'display_val': format_number_display_val(p)
            }

    def get_control_number_info(self):
        disp = {'lccn': [], 'oclc': [], 'others': []}
        num = {'lccn': [], 'oclc': [], 'all': []}
        search = []

        def _put_compiled_into_vars(c, display, numbers, search, prepend=False):
            if c['main_number'] not in numbers['all']:
                if prepend:
                    display[c['type']].insert(0, c['display_val'])
                    numbers['all'] = c['all_numbers'] + numbers['all']
                else:
                    display[c['type']].append(c['display_val'])
                    numbers['all'].extend(c['all_numbers'])
                if c['type'] in ('lccn', 'oclc') and c['is_valid']:
                    numbers[c['type']].append(c['main_number'])
                search.extend(c['all_numbers'])
            if c['oclc_and_suffix']:
                if c['oclc_and_suffix'] not in search:
                    search.append(c['oclc_and_suffix'])
            return display, numbers, search

        deferred = {}
        for f in self.marc_fieldgroups.get('control_numbers', []):
            if f.tag == '001':
                deferred[f.tag] = f.data
            else:
                for c in self.compile_control_numbers(f):
                    args = (disp, num, search)
                    disp, num, search = _put_compiled_into_vars(c, *args)

        if '001' in deferred:
            val = deferred['001']
            is_oclc = re.match(r'(on|ocm|ocn)?\d+(\/.+)?$', val)
            # OCLC numbers in 001 are treated as valid only if
            # there are not already valid OCLC numbers found in
            # 035s that we've already processed.
            is_valid = not is_oclc or len(num['oclc']) == 0
            org_code = 'OCoLC' if is_oclc else None
            if org_code is not None:
                val = '({}){}'.format(org_code, val)
            sftag = 'a' if is_valid else 'z'
            fake035 = make_mfield('035', subfields=[sftag, val])
            for c in self.compile_control_numbers(fake035):
                args, kwargs = (disp, num, search), {}
                if c['type'] == 'oclc':
                    # If this is a valid OCLC number, we want it to
                    # display before any invalid OCLC numbers.
                    kwargs = {'prepend': is_valid}
                disp, num, search = _put_compiled_into_vars(c, *args, **kwargs)

        return {
            'lccn_number': (num['lccn'] or [None])[0],
            'lccns_display': disp['lccn'] or None,
            'oclc_numbers_display': disp['oclc'] or None,
            'other_control_numbers_display': disp['others'] or None,
            'control_numbers_search': search or None,
            'oclc_numbers': num['oclc'] or None,
            'all_control_numbers': num['all'] or None
        }

    def get_games_facets_info(self):
        """
        This maps values from a local notes field in the MARC (592) to
        a set of games-related facets, based on presence of a Media
        Game Facet token string (e.g., 'p1;p2t4;d30t59').
        """

        class NumberLabeler(object):
            def __init__(self, singular, plural):
                self.singular = singular
                self.plural = plural

            def label(self, number):
                if number == 1:
                    return (str(number), self.singular)
                return str(number), self.plural

            def same(self, *labels):
                return all(l in (self.singular, self.plural) for l in labels)

        class MinutesLabeler(object):
            def label(self, number):
                if number < 59:
                    return NumberLabeler('minute', 'minutes').label(number)
                number = int(round(float(number) / float(60)))
                return NumberLabeler('hour', 'hours').label(number)

            def same(self, *labels):
                both_minutes = all(l.startswith('minute') for l in labels)
                both_hours = all(l.startswith('hour') for l in labels)
                return both_minutes or both_hours

        class Bound(object):
            def __init__(self, trigger_value, is_inclusive, template=None):
                self.trigger_value = trigger_value
                self.is_inclusive = is_inclusive
                self.template = template or self.get_default_template()

            def get_default_template(self):
                pass

            def is_triggered(self, comparison_value):
                pass

            def do_outer_bound_number(self, number):
                return number

            def render(self, number, labeler):
                if not self.is_inclusive:
                    number = self.do_outer_bound_number(number)
                number, label = labeler.label(number)
                to_render = ' '.join((str(number), label))
                return self.template.format(to_render)

        class UpperBound(Bound):
            def get_default_template(self):
                if self.is_inclusive:
                    return '{} or more'
                return 'more than {}'

            def is_triggered(self, comparison_value):
                return comparison_value >= self.trigger_value

            def do_outer_bound_number(self, number):
                return number - 1

        class LowerBound(Bound):
            def get_default_template(self):
                if self.is_inclusive:
                    return '{} or less'
                return 'less than {}'

            def is_triggered(self, comparison_value):
                return comparison_value <= self.trigger_value

            def do_outer_bound_number(self, number):
                return number + 1

        class RangeRenderer(object):
            def __init__(self, labeler, lower=None, upper=None):
                self.labeler = labeler
                self.lower = lower
                self.upper = upper

            def render_sort_key(self, start, end=0):
                start = str(start)
                end = str(end) if end else start
                zp = len(str(self.upper.trigger_value)) if self.upper else 10
                return '{}-{}'.format(start.zfill(zp), end.zfill(zp))

            def render_display_value(self, start, end=0):
                snum_to_render, slabel = self.labeler.label(start)
                if not end:
                    return ' '.join((snum_to_render, slabel))

                if self.lower and self.lower.is_triggered(start):
                    return self.lower.render(end, self.labeler)

                if self.upper and self.upper.is_triggered(end):
                    return self.upper.render(start, self.labeler)

                render_stack = [snum_to_render]
                enum_to_render, elabel = self.labeler.label(end)
                if not self.labeler.same(slabel, elabel):
                    render_stack.append(slabel)
                render_stack.extend(['to', enum_to_render, elabel])
                return ' '.join(render_stack)

            def render(self, start, end=0):
                display_val = self.render_display_value(start, end)
                sort_key = self.render_sort_key(start, end)
                return FACET_KEY_SEPARATOR.join((sort_key, display_val))

        def parse_each_592_token(f592s):
            for f in f592s:
                tokenstr = ';'.join(f.get_subfields('a')).lower()
                token_regex = r'([adp])(\d+)(?:t|to)?(\d+)?(?:;+|\s|$)'
                for ttype, start, end in re.findall(token_regex, tokenstr):
                    yield ttype, int(start or 0), int(end or 0)

        values = {'a': [], 'd': [], 'p': []}
        renderers = {
            'a': RangeRenderer(
                NumberLabeler('year', 'years'),
                upper=UpperBound(100, True, template='{} and up')
            ),
            'd': RangeRenderer(
                MinutesLabeler(),
                lower=LowerBound(1, False),
                upper=UpperBound(500, False)
            ),
            'p': RangeRenderer(
                NumberLabeler('player', 'players'),
                upper=UpperBound(99, False)
            )
        }
        if any([loc.code.startswith('czm') for loc in self.r.locations.all()]):
            f592s = self.marc_fieldgroups.get('local_game_note', [])
            for ttype, start, end in parse_each_592_token(f592s):
                renderer = renderers.get(ttype)
                if renderer:
                    values[ttype].append(renderer.render(start, end))

        return {
            'games_ages_facet': values['a'] or None,
            'games_duration_facet': values['d'] or None,
            'games_players_facet': values['p'] or None
        }

    def find_phrases_x_not_in_phrases_y(self, phr_x, phr_y, accessor=None,
                                        finder=None):
        def get(phrase):
            if accessor:
                return accessor(phrase)
            return phrase

        def find_in(wordstr1, wordstr2):
            if finder:
                return finder(wordstr1, wordstr2)
            sc = ' '
            w1 = generate_facet_key(wordstr1, space_char=sc).split(sc)
            w2 = generate_facet_key(wordstr2, space_char=sc).split(sc)
            for i in range(len(w2)):
                if w2[i] == w1[0] and w2[i:i+len(w1)] == w1:
                    return True
            return False

        deduped = []
        for px in phr_x:
            is_dupe = any((py for py in phr_y if find_in(get(px), get(py))))
            if not is_dupe:
                deduped.append(px)
        return deduped

    def combine_phrases(self, phr1, phr2, accessor=None, finder=None):
        """
        Combine two lists of phrases to remove duplicative terms,
        comparing ONLY the two lists against each other, not against
        themselves.

        The purpose of this method is to try to normalize lists of
        search terms to minimize TF inflation from generating search
        terms automatically.

        A duplicative term is one that is fully contained within
        another. "Seeds" is contained in "Certification (Seeds)" and
        so is considered duplicative and would be removed.

        De-duplication happens between the two lists, not within a
        single list. If "Seeds" and "Certification (Seeds)" are in the
        same list they are not compared against each other and thus
        nothing is changed.

        By default, terms are converted to lower case before being
        compared. Punctuation is ignored. Phrases are only compared
        against full words, so "Apple seed" is a duplicate of "Plant
        an apple seed" but not "Apple seeds." If you need different
        behavior, supply your own `finder` function that takes a
        phrase from each list as args and returns True if the first
        is found in the second.

        `phr1` and `phr2` may be lists of data structures, where terms
        to compare are in some sub-element or can be derived. In that
        case, supply the `accessor` function defining how to get terms
        from each element in each list.

        Returns one list of phrases or phrase data structures resulting
        from deduplicating the first against the second and the second
        against the unique phrases from the first.
        """
        xy = self.find_phrases_x_not_in_phrases_y(phr1, phr2, accessor, finder)
        yx = self.find_phrases_x_not_in_phrases_y(phr2, xy, accessor, finder)
        return xy + yx

    def _add_subject_term(self, out_vals, t_heading, t_heading_fvals, t_fvals,
                          t_json, t_search_vals, nf_chars=0, base_t_heading='',
                          allow_search_duplicates=True):
        sep = self.hierarchical_subject_separator
        if out_vals['heading']:
            out_vals['heading'] = sep.join([out_vals['heading'], t_heading])
        else:
            out_vals['heading'] = t_heading

        for ftype, fval in t_fvals:
            fval = format_key_facet_value(fval, nf_chars)
            out_vals['facets'][ftype].add(fval)

        heading_fvals = []
        for fval in t_heading_fvals:
            if base_t_heading:
                fval = sep.join([base_t_heading, fval])
            heading_fvals.append(format_key_facet_value(fval, nf_chars))
        out_vals['facets']['heading'] |= set(heading_fvals)
        new_jsonp = []
        for entry in t_json.get('p', []):
            new_entry = {}
            for key in ('s', 'd', 'v'):
                if key in entry:
                    val = entry[key]
                    if key == 'v':
                        if base_t_heading:
                            val = sep.join([base_t_heading, val])
                        val = format_key_facet_value(val, nf_chars)
                    new_entry[key] = val
            new_jsonp.append(new_entry)
        if out_vals['json'].get('p'):
            out_vals['json']['p'][-1]['s'] = sep
            out_vals['json']['p'].extend(new_jsonp)
        elif new_jsonp:
            out_vals['json'] = {'p': new_jsonp}

        for fieldtype, level, term in t_search_vals:
            if allow_search_duplicates:
                out_vals['search'][fieldtype][level].append(term)
            else:
                terms = out_vals['search'][fieldtype][level]
                new_terms = self.combine_phrases([term], terms)
                out_vals['search'][fieldtype][level] = new_terms
        return out_vals

    def parse_and_compile_subject_field(self, f):
        out_vals = {
            'heading': '',
            'json': {},
            'facets': {'heading': set(), 'topic': set(), 'era': set(),
                       'region': set(), 'genre': set()},
            'search': {'subjects': {'main': [], 'secondary': []},
                       'genres': {'main': [], 'secondary': []}}
        }

        main_term, relators = '', []
        is_nametitle = f.tag in ('600', '610', '611', '630')
        is_fast = 'fast' in f.get_subfields('2')
        is_uncontrolled = f.tag == '653'
        is_for_search_only = f.tag == '692'
        is_genre = f.tag in ('380', '655')
        needs_json = not is_for_search_only
        needs_facets = not is_uncontrolled and not is_for_search_only
        nf_chars = f.indicator1 if f.tag == '630' else 0
        main_term_subfields = 'ab' if is_genre else 'abcdg'
        subdivision_subfields = 'vxyz'
        relator_subfields = 'e4'
        sd_types = {'v': 'genre', 'x': 'topic', 'y': 'era', 'z': 'region'}

        main_term_type = 'topic'
        if is_genre:
            main_term_type = 'genre'
        elif f.tag in ('651', '691'):
            main_term_type = 'region'
        elif f.tag == '648':
            main_term_type ='era'

        sep = self.hierarchical_subject_separator

        if is_nametitle:
            nt_entry = self.parse_nametitle_field(f)
            has_names = bool(nt_entry['names'])
            has_title = bool(nt_entry['title'])
            name_heading = ''

            if has_names:
                name = self.select_best_name(nt_entry['names'], 'combined')
                compiled = name['compiled']
                heading = compiled['heading']
                hfvals = compiled.get('facet_vals', [])
                tfvals = [('topic', v) for v in hfvals]
                tjson = compiled.get('json', {'p': []})
                search = compiled.get('search_vals', []) or [heading]
                level = 'main' if not has_title else 'secondary'
                search = [('subjects', level, v) for v in search]
                params = [out_vals, heading, hfvals, tfvals, tjson, search]

                if heading:
                    out_vals = self._add_subject_term(*params)
                    name_heading = out_vals['heading']
                    relators = name['parsed']['relations']

            if has_title:
                compiled = nt_entry['title']['compiled']
                as_subject = compiled['as_subject']
                heading = as_subject['heading']
                hfvals = as_subject.get('facet_vals', [])
                tfvals = [('topic', v) for v in compiled.get('facet_vals', [])]
                tjson = as_subject.get('json', {'p': []})
                search = [('subjects', 'main', heading)]
                params = [out_vals, heading, hfvals, tfvals, tjson, search]
                kwargs = {'nf_chars': nf_chars, 'base_t_heading': name_heading}
                if compiled['heading']:
                    out_vals = self._add_subject_term(*params, **kwargs)
                    relators = nt_entry['title']['parsed']['relations']
            main_term = out_vals['heading']
        else:
            main_term = ' '.join(pull_from_subfields(f, main_term_subfields))
            main_term = p.strip_ends(main_term)
            tjson, hfvals, tfvals, search = {}, [], [], []

            if main_term:
                if needs_json:
                    if needs_facets:
                        tjson = {'p': [{'d': main_term, 'v': main_term}]}
                    else:
                        tjson = {'p': [{'d': main_term}]}

                if needs_facets:
                    hfvals = [main_term]
                    tfvals = [(main_term_type, main_term)]

                sftype = 'genres' if is_genre else 'subjects'
                slevel = 'secondary' if is_uncontrolled else 'main'
                if main_term not in out_vals['search'][sftype][slevel]:
                    search.append((sftype, slevel, main_term))

                params = [out_vals, main_term, hfvals, tfvals, tjson, search]
                out_vals = self._add_subject_term(*params)
                rel_terms = OrderedDict()
                for tag, val in f.filter_subfields(relator_subfields):
                    for rel_term in self.utils.compile_relator_terms(tag, val):
                        rel_terms[rel_term] = None
                relators = list(rel_terms.keys())

        sd_parents = [main_term] if main_term else []
        for tag, val in f.filter_subfields(subdivision_subfields):
            tjson, hfvals, tfvals, search = {}, [], [], []
            sd_term = p.strip_ends(val)
            sd_type = sd_types[tag]
            if sd_term:
                alts = self.utils.map_subject_subdivision(sd_term, sd_parents,
                                                          sd_type)
                sd_parents.append(sd_term)

                if needs_json:
                    if needs_facets:
                        tjson = {'p': [{'d': sd_term, 'v': sd_term}]}
                    else:
                        tjson = {'p': [{'d': sd_term}]}

                if needs_facets:
                    hfvals = [sd_term]
                    tfvals = alts
                    if is_genre and main_term and sd_type == 'topic':
                        tfvals = list(set(tfvals) - set([('topic', sd_term)]))
                        term = ', '.join((main_term, sd_term))
                        tfvals.append(('genre', term))

                terms = self.combine_phrases([(sd_type, sd_term)], alts,
                                             accessor=lambda x: x[1])
                for ttype, term in terms:
                    if ttype == 'genre' or is_genre:
                        sftype = 'genres'
                    else:
                        sftype = 'subjects'
                    search.append((sftype, 'secondary', term))

                params = [out_vals, sd_term, hfvals, tfvals, tjson, search]
                kwargs = {'base_t_heading': out_vals['heading'],
                          'allow_search_duplicates': False}
                out_vals = self._add_subject_term(*params, **kwargs)
        if relators:
            out_vals['json']['r'] = relators

        return {
            'heading': out_vals['heading'],
            'json': out_vals['json'],
            'facets': {k: list(v) for k, v in out_vals['facets'].items()},
            'search': out_vals['search'],
            'is_genre': is_genre,
        }

    def get_subjects_info(self):
        """
        This extracts all subject and genre headings from relevant 6XX
        fields and generates data for all Solr subject and genre
        fields.
        """
        json = {'subjects': [], 'genres': []}
        facets = {'topic': [], 'era': [], 'region': [], 'genre': []}
        heading_facets = {'subjects': [], 'genres': []}
        search = {
            'subjects': {'exact': [], 'main': [], 'all': []},
            'genres': {'exact': [], 'main': [], 'all': []}
        }

        heading_sets = {'subjects': set(), 'genres': set()}
        hf_sets = {'subjects': set(), 'genres': set()}
        f_sets = {'topic': set(), 'era': set(), 'region': set(), 'genre': set()}

        for f in self.marc_fieldgroups.get('subject_genre', []):
            compiled = self.parse_and_compile_subject_field(f)
            heading = compiled['heading']
            ftype_key = 'genres' if compiled['is_genre'] else 'subjects'
            if heading and heading not in heading_sets[ftype_key]:
                if compiled['json']:
                    json[ftype_key].append(compiled['json'])
                for facet_key, fvals in compiled['facets'].items():
                    if facet_key == 'heading':
                        vals = [v for v in fvals if v not in hf_sets[ftype_key]]
                        heading_facets[ftype_key].extend(vals)
                    else:
                        vals = [v for v in fvals if v not in f_sets[facet_key]]
                        facets[facet_key].extend(vals)
                search[ftype_key]['exact'].append(heading)

                for sftype, sval_groups in compiled['search'].items():
                    groups = {
                        'main': sval_groups['main'],
                        'all': self.combine_phrases(sval_groups['main'],
                                                    sval_groups['secondary'])
                    }
                    for slvl, svals in groups.items():
                        vals = self.combine_phrases(svals, search[sftype][slvl])
                        search[sftype][slvl] = vals

                heading_sets[ftype_key].add(heading)
                for facet_type in f_sets.keys():
                    f_sets[facet_type] = set(facets[facet_type])
                for field_type in hf_sets.keys():
                    hf_sets[field_type] = set(heading_facets[field_type])

        sh_json = [ujson.dumps(v) for v in json['subjects']]
        gh_json = [ujson.dumps(v) for v in json['genres']]
        s_search, g_search = search['subjects'], search['genres']
        return {
            'subject_headings_json': sh_json or None,
            'genre_headings_json': gh_json or None,
            'subject_heading_facet': heading_facets['subjects'] or None,
            'genre_heading_facet': heading_facets['genres'] or None,
            'topic_facet': facets['topic'] or None,
            'era_facet': facets['era'] or None,
            'region_facet': facets['region'] or None,
            'genre_facet': facets['genre'] or None,
            'subjects_search_exact_headings': s_search['exact'] or None,
            'subjects_search_main_terms': s_search['main'] or None,
            'subjects_search_all_terms': s_search['all'] or None,
            'genres_search_exact_headings': g_search['exact'] or None,
            'genres_search_main_terms': g_search['main'] or None,
            'genres_search_all_terms': g_search['all'] or None,
        }

    def get_language_info(self):
        """
        Collect all relevant language information from the record
        (including the 008[35-37], the 041(s), 377(s), and languages
        associated with titles), and return labels for `languages`. In
        addition, if `language_notes` is not already present (from
        parsing one or more 546 fields), generate notes as needed.
        """
        facet, notes = [], []
        needs_notes = not self.bundle.get('language_notes')
        all_languages = OrderedDict()
        categorized = {'a': OrderedDict()}
        tlangs = self.title_languages

        f008 = self.marc_fieldgroups.get('008', [None])[0]
        if f008 is not None and len(f008.data) >= 38:
            lang_code = f008.data[35:38]
            main_lang = settings.MARCDATA.LANGUAGE_CODES.get(lang_code)
            if main_lang:
                all_languages[main_lang] = None
                categorized['a'][main_lang] = None

        for lang in self.title_languages:
            all_languages[lang] = None
            categorized['a'][lang] = None

        for f in self.marc_fieldgroups.get('language_code', []):
            parsed = LanguageParser(f).parse()
            for lang in parsed['languages']:
                all_languages[lang] = None
            for key, langs in parsed['categorized'].items():
                categorized[key] = categorized.get(key, OrderedDict())
                for lang in langs:
                    categorized[key][lang] = None

        facet = list(all_languages.keys())
        if needs_notes:
            categorized = {k: list(odict.keys()) for k, odict in categorized.items()}
            notes = LanguageParser.generate_language_notes_display(categorized)

        return {
            'languages': facet or None,
            'language_notes': notes or None,
        }

    def get_record_boost(self):
        """
        Generate the value for a numeric field (`record_boost`) based
        on, presently, two factors. One, publication year, as a measure
        of recency. Two, `bcode1` (bib type or bib level), as a measure
        of record quality. The idea is that we want to boost more
        recent records and we want to deprioritize minimal records.

        Maximum boost value for pub year is:
        500 + (5 + this_year - 2020); 500 is for things
        published in 2020, with leeway for things published up to 5
        years in the future. >500 years before 2020 is 1. Invalid or
        non-existent pub dates default to 460, or 1980, just to make
        sure they don't get buried.

        For record quality, if bcode1 is `-` or `d` (full record or
        Discovery record), then it gets an extra +500 boost, otherwise
        +0.
        """
        def make_pubyear_boost(this_year, boost_year):
            anchor_boost, anchor_year = 500, 2020
            if (boost_year is not None) and (boost_year <= 5 + this_year):
                boost = anchor_boost - (anchor_year - boost_year)
                if boost < 1:
                    boost = 1
                return boost
            return 460

        pub_boost = make_pubyear_boost(self.this_year, self.year_for_boost)
        q_boost = 500 if self.r.bcode1 in ('-', 'd') else 0
        return {'record_boost': str(pub_boost + q_boost)}

    def compile_linking_field(self, group, f, parsed):
        if group in ('linking_serial_continuity', 'linking_related_resources'):
            rendered = self.render_linking_field_title(parsed, as_search=True)
        else:
            rendered = self.render_linking_field_title(parsed, as_search=False)
        return {
            'json': rendered['json'],
            'search': [rendered['heading']],
            'facet_vals': rendered['facet_vals']
        }

    def _need_linking_field_render(self, rendered_lf, marc_fgroup):
        if marc_fgroup in ('linking_760_762', 'linking_774'):
            wt_key = generate_facet_key(rendered_lf['work_heading'])
            if marc_fgroup == 'linking_760_762':
                return not (wt_key in self.work_title_keys.get('series', []))
            elif marc_fgroup == 'linking_774':
                print(wt_key)
                print((self.work_title_keys.get('included')))
                return not (wt_key in self.work_title_keys.get('included', []))
        return True

    def get_linking_fields(self):
        """
        Generate linking field data for 76X-78X fields.

        760/762 => new related series entries
        774 => new included works entries
        765, 767, 770, 772, 773, 775, 776, 777, 786, and 787
            => `related_resources_linking_json` entries
        780/785 => `serial_continuity_linking_json` entries
        """
        groups = ('linking_760_762', 'linking_774', 'linking_780_785',
                  'linking_other')
        json, search, facet_vals = {}, {}, []
        for group in groups:
            json[group], search[group] = [], []
            as_search = group in ('linking_780_785', 'linking_other')
            for f in self.marc_fieldgroups.get(group, []):
                parsed = LinkingFieldParser(f).parse()
                if parsed['title_parts']:
                    rend = self.render_linking_field(parsed,
                                                     as_search=as_search)
                    if self._need_linking_field_render(rend, group):
                        if as_search:
                            json_dict = rend['json']
                        else:
                            json_dict = self.do_facet_keys(rend['json'])
                            search[group].append(rend['heading'])
                            new_fvals = self.do_facet_keys(rend['facet_vals'])
                            facet_vals.extend(new_fvals)
                        json[group].append(ujson.dumps(json_dict))
        return {
            'included_work_titles_json': json['linking_774'] or None,
            'included_work_titles_search': search['linking_774'] or None,
            'related_series_titles_json': json['linking_760_762'] or None,
            'related_series_titles_search': search['linking_760_762'] or None,
            'serial_continuity_linking_json': json['linking_780_785'] or None,
            'related_resources_linking_json': json['linking_other'] or None,
            'title_series_facet': facet_vals or None
        }

    def render_edition_component(self, parts):
        keys = ('display', 'responsibility', 'value')
        stacks = {k: [] for k in keys}
        for entry in parts:
            render_stack = []
            for key in ('value', 'responsibility'):
                if key in entry:
                    render_stack.append(entry[key])
                    stacks[key].append(entry[key])
            stacks['display'].append(', '.join(render_stack))
        return {k: '; '.join(stacks[k]) for k in keys}

    def compile_edition(self, parsed):
        display = ''
        compiled = {
            'responsibility': [],
            'value': []
        }
        info = parsed['edition_info'] or {}
        if 'editions' in info:
            rendered = self.render_edition_component(info['editions'])
            display = rendered['display']
            if parsed['materials_specified']:
                ms = format_materials_specified(parsed['materials_specified'])
                display = ' '.join([format_display_constants([ms]), display])
            for key in compiled.keys():
                if rendered[key]:
                    compiled[key].append(rendered[key])
            if 'parallel' in info:
                translated = self.render_edition_component(info['parallel'])
                formatted = format_translation(translated['display'])
                display = ' '.join([display, formatted])
                for key in compiled.keys():
                    if translated[key]:
                        compiled[key].append(translated[key])
        compiled['display'] = display
        return compiled

    def get_editions(self):
        """
        Get edition information from the 250, 251, and 254 fields.
        """
        ed_display, ed_search = [], []
        resp_search, fmt_search = [], []

        for f in self.marc_fieldgroups.get('edition', []):
            compiled = self.compile_edition(EditionParser(f).parse())
            print(compiled)
            if compiled['display']:
                ed_display.append(compiled['display'])
            if compiled['responsibility']:
                resp_search.extend(compiled['responsibility'])

            if f.tag == '254':
                fmt_search.extend(compiled['value'])
            else:
                ed_search.extend(compiled['value'])

        return {
            'editions_display': ed_display or None,
            'editions_search': ed_search or None,
            'responsibility_search': resp_search or None,
            'type_format_search': fmt_search or None
        }

    def get_serial_holdings(self):
        """
        Return serial holdings information. Currently this only uses
        the MARC 866 field, but in the future we may expand this to
        include other information from check-in/holdings records.
        """
        library_has_display = []
        sf_filter = {'include': 'az'}
        for f in self.marc_fieldgroups.get('library_has', []):
            val = GenericDisplayFieldParser(f, '; ', sf_filter).parse()
            if val:
                library_has_display.append(val)
        return {
            'library_has_display': library_has_display or None
        }


class DiscoverS2MarcBatch(S2MarcBatch):
    """
    Sierra to MARC converter for Discover.

    This straight up converts the Sierra DB BibRecord record (and
    associated data) to a SierraMarcRecord object.
    """
    def compile_leader(self, r, base):
        try:
            lf = r.record_metadata.leaderfield_set.all()[0]
        except IndexError:
            return base

        return ''.join([
            base[0:5], lf.record_status_code, lf.record_type_code,
            lf.bib_level_code, lf.control_type_code,
            lf.char_encoding_scheme_code, base[10:17], lf.encoding_level_code,
            lf.descriptive_cat_form_code, lf.multipart_level_code, base[20:]
        ])

    def compile_control_fields(self, r):
        mfields = []
        try:
            control_fields = r.record_metadata.controlfield_set.all()
        except Exception as e:
            raise S2MarcError('Skipped. Couldn\'t retrieve control fields. '
                    '({})'.format(e), str(r))
        for cf in control_fields:
            try:
                data = cf.get_data()
                field = make_mfield(cf.get_tag(), data=data)
            except Exception as e:
                raise S2MarcError('Skipped. Couldn\'t create MARC field '
                    'for {}. ({})'.format(cf.get_tag(), e), str(r))
            mfields.append(field)
        return mfields

    def order_varfields(self, varfields):
        groups = []
        vfgroup_ordernum, last_vftag = 0, None
        for vf in sorted(varfields, key=lambda vf: vf.marc_tag):
            if vf.marc_tag:
                if last_vftag and last_vftag != vf.varfield_type_code:
                    vfgroup_ordernum += 1
                sort_key = (vfgroup_ordernum * 1000) + vf.occ_num
                groups.append((sort_key, vf))
                last_vftag = vf.varfield_type_code
        return [vf for _, vf in sorted(groups, key=lambda r: r[0])]

    def compile_varfields(self, r):
        mfields = []
        try:
            varfields = r.record_metadata.varfield_set.all()
        except Exception as e:
            raise S2MarcError('Skipped. Couldn\'t retrieve varfields. '
                              '({})'.format(e), str(r))
        for vf in self.order_varfields(varfields):
            tag, ind1, ind2 = vf.marc_tag, vf.marc_ind1, vf.marc_ind2
            content, field = vf.field_content, None
            try:
                if tag in ['{:03}'.format(num) for num in range(1,10)]:
                    field = make_mfield(tag, data=content)
                else:
                    ind = [ind1, ind2]
                    if not content.startswith('|'):
                        content = ''.join(('|a', content))
                    sf = re.split(r'\|([a-z0-9])', content)[1:]
                    field = make_mfield(tag, indicators=ind, subfields=sf,
                                              group_tag=vf.varfield_type_code)
            except Exception as e:
                raise S2MarcError('Skipped. Couldn\'t create MARC field '
                        'for {}. ({})'.format(vf.marc_tag, e), str(r))
            if field is not None:
                mfields.append(field)
        return mfields

    def compile_original_marc(self, r):
        marc_record = SierraMarcRecord(force_utf8=True)
        marc_record.add_field(*self.compile_control_fields(r))
        marc_record.add_field(*self.compile_varfields(r))
        marc_record.leader = self.compile_leader(r, marc_record.leader)
        return marc_record

    def _one_to_marc(self, r):
        marc_record = self.compile_original_marc(r)
        if not marc_record.fields:
            raise S2MarcError('Skipped. No MARC fields on Bib record.', str(r))
        return marc_record
