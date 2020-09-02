# -*- coding: utf-8 -*- 

"""
Sierra2Marc module for catalog-api `blacklight` app.
"""

from __future__ import unicode_literals
import pymarc
import logging
import re
import ujson
from datetime import datetime
from collections import OrderedDict

from django.conf import settings
from base import models, local_rulesets
from export.sierra2marc import S2MarcBatch, S2MarcError
from blacklight import parsers as p
from utils import helpers, toascii


# These are MARC fields that we are currently not including in public
# catalog records, listed by III field group tag.
IGNORED_MARC_FIELDS_BY_GROUP_TAG = {
    'n': ('539', '901', '959'),
    'r': ('306', '307', '336', '337', '338', '341', '348', '351', '355', '357',
          '377', '380', '381', '383', '384', '385', '386', '387', '388', '389'),
}


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


class MarcParseUtils(object):
    marc_relatorcode_map = settings.MARCDATA.RELATOR_CODES
    marc_sourcecode_map = settings.MARCDATA.STANDARD_ID_SOURCE_CODES
    control_sftags = 'w01256789'
    title_sftags_7xx = 'fhklmoprstvx'

    def compile_relator_terms(self, tag, val):
        if tag == '4':
            term = self.marc_relatorcode_map.get(val, None)
            return [term] if term else []
        return [p.strip_wemi(v) for v in p.strip_ends(val).split(', ')]

    def parse_marc_display_field(self, f):
        """
        Parse `f`, a display field copied/pasted from the MARC
        documentation on either the LC or OCLC website, into the MARC
        field tag, subfield list, and indicator values needed to
        generate a pymarc MARC field object.
        """
        f = re.sub(r'\n\s*', ' ', f)
        pre = re.match(r'(\d{3})\s?([\d\s#][\d\s#])\s?(?=\S)(.*)$', f)
        tag = pre.group(1)
        ind = pre.group(2).replace('#', ' ')

        sf_str = re.sub(r'\s?ǂ(.)\s', r'$\1', pre.group(3))
        if sf_str[0] != '$':
            sf_str = '$a{}'.format(sf_str)
        sfs = re.split(r'[\$\|](.)', sf_str)[1:]
        pr_sfs = ', '.join(["'{}'".format(s.replace("'", r'\'')) for s in sfs])
        printable = "('{}', [{}], '{}')".format(tag, pr_sfs, ind)
        return (tag, sfs, ind, printable)

    def compile_marc_display_field(self, tag, subfields, ind):
        """
        Take the given MARC `tag`, list of `subfields`, and `ind`
        indicator values. Generate a string version of the field for
        display. (Format is the same as what's on the LC website.)
        """
        sf_tuples = zip(subfields[0::2], subfields[1::2])
        sf_str = ''.join([''.join(['$', sft[0], sft[1]]) for sft in sf_tuples])
        return '{} {}{}'.format(tag, ind, sf_str)


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
        self.utils = MarcParseUtils()

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
        for tag, val in self.field:
            if self.parse_subfield(tag, val):
                break
        self.do_post_parse()
        return self.compile_results()


class PersonalNameParser(SequentialMarcFieldParser):
    relator_sftags = 'e4'
    done_sftags = 'fhklmoprstvxz'
    ignore_sftags = 'i'

    def __init__(self, field):
        super(PersonalNameParser, self).__init__(field)
        self.heading_parts = []
        self.relator_terms = OrderedDict()
        self.parsed_name = {}
        self.titles = []
        self.numeration = ''

    def do_relators(self, tag, val):
        for relator_term in self.utils.compile_relator_terms(tag, val):
            self.relator_terms[relator_term] = None

    def do_name(self, tag, val):
        self.parsed_name = p.person_name(val, self.field.indicators)

    def do_titles(self, tag, val):
        self.titles.extend([v for v in p.strip_ends(val).split(', ')])

    def do_numeration(self, tag, val):
        self.numeration = p.strip_ends(val)

    def parse_subfield(self, tag, val):
        if tag in self.done_sftags:
            return True
        elif tag in self.relator_sftags:
            self.do_relators(tag, val)
        elif tag not in self.ignore_sftags:
            self.heading_parts.append(val)
            if tag == 'a':
                self.do_name(tag, val)
            elif tag == 'b':
                self.do_numeration(tag, val)
            elif tag == 'c':
                self.do_titles(tag, val)

    def compile_results(self):
        heading = p.normalize_punctuation(' '.join(self.heading_parts))
        return {
            'heading': p.strip_ends(heading) or None,
            'relations': self.relator_terms.keys() or None,
            'forename': self.parsed_name.get('forename', None),
            'surname': self.parsed_name.get('surname', None),
            'numeration': self.numeration or None,
            'person_titles': self.titles or None,
            'type': 'person'
        }


class OrgEventNameParser(SequentialMarcFieldParser):
    event_info_sftags = 'cdgn'
    done_sftags = 'fhklmoprstvxz'
    ignore_sftags = 'i'

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
        self.parts = {'org': [], 'event': []}
        self._stacks = {'org': [], 'event': []}
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
        if self._event_info:
            event_parts.append({'name': self._build_unit_name('event'),
                                'event_info': self._build_event_info()})
            # Even if this is clearly an event, if it's the first thing
            # in an X10 field, record it as an org as well.
            if self.field_type == 'X10' and not self.parts['org']:
                org_parts.append({'name': self._prev_part_name})
                if self._stacks['org']:
                    self._stacks['org'].pop()
        elif self.field_type == 'X11' and not self.parts['event']:
            event_parts.append({'name': self._build_unit_name('event')})
        else:
            org_parts.append({'name': self._build_unit_name('org')})
        return org_parts, event_parts

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
            self._prev_part_name = '{} {}'.format(self._prev_part_name,
                                                  p.strip_ends(val))
        elif tag == self.subunit_sftag:
            new_org_parts, new_event_parts = self.do_unit()
            self.parts['org'].extend(new_org_parts)
            self.parts['event'].extend(new_event_parts)
            self._event_info = []
            self._prev_part_name = p.strip_ends(val)
        self._prev_tag = tag

    def do_post_parse(self):
        new_org_parts, new_event_parts = self.do_unit()
        self.parts['org'].extend(new_org_parts)
        self.parts['event'].extend(new_event_parts)

    def compile_results(self):
        ret_val = []
        relators = self.relator_terms.keys() or None
        for part_type in ('org', 'event'):
            if self.parts[part_type]:
                ret_val.append({
                    'relations': relators,
                    'heading_parts': self.parts[part_type],
                    'type': 'organization' if part_type == 'org' else 'event',
                    'is_jurisdiction': self.is_jurisdiction
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
        self.materials_specified = []
        self.lock_parallel = False
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

    def start_next_title(self):
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
            if is_parallel and self.titles:
                self.parallel_titles.append(title)
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
        part = p.restore_periods(part)
        if len(self.title_parts):
            part = '; '.join((self.title_parts.pop(), part))
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
            'is_materials_specified': tag == '3' and self.field.tag == '490',
            'is_language_code': tag == 'y',
            'is_bulk_following_incl_dates': is_bdates and self.prev_tag == 'f',
            'subpart_may_need_formatting': tag in 'fgkps'
        }

    def parse_subfield(self, tag, val):
        self.flags = self.get_flags(tag, val)
        if self.flags['is_valid']:
            prot = p.protect_periods(val)

            isbd = r''.join(self.analyzer.isbd_punct_mapping.keys())
            switchp = r'"\'~\.,\)\]\}}'
            is_245bc = self.flags['is_245b'] or self.flags['is_subfield_c']
            if is_245bc or self.field.tag == '490':
                p_switch_re = r'([{}])(\s*[{}]+)(\s|$)'.format(isbd, switchp)
            else:
                p_switch_re = r'([{}])(\s*[{}]+)($)'.format(isbd, switchp)
            prot = re.sub(p_switch_re, r'\2\1\3', prot)

            part, end_punct = self.analyzer.pop_isbd_punct_from_title_part(prot)
            if part:
                if self.flags['is_materials_specified']:
                    self.materials_specified.append(p.restore_periods(part))
                elif self.flags['is_display_text']:
                    self.display_text = p.restore_periods(part)
                elif self.flags['is_main_part']:
                    if self.flags['is_245b']:
                        self.do_compound_title_part(part, False)
                    elif self.field.tag == '490':
                        self.do_titles_and_sors(part, False)
                    else:
                        self.push_title_part(part, self.prev_punct)
                elif self.flags['is_subfield_c']:
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
        self.start_next_title()

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
            'parallel': self.parallel_titles,
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
    title_only_fields = ('130', '240', '243', '730', '740', '830')
    name_title_fields = ('700', '710', '711', '800', '810', '811')
    main_title_fields = ('130', '240', '243')
    nonfiling_char_ind1_fields = ('130', '730', '740')
    nonfiling_char_ind2_fields = ('240', '243', '830')
    nt_title_tags = 'tfklmoprs'
    subpart_tags = 'dgknpr'
    expression_tags = 'flos'

    def __init__(self, field, utils=None):
        super(PreferredTitleParser, self).__init__(field)
        self.utils = utils or MarcParseUtils()
        self.prev_punct = ''
        self.prev_tag = ''
        self.flags = {}
        self.lock_title = False
        self.lock_expression_info = False
        self.seen_subpart = False
        self.primary_title_tag = 't'
        self.materials_specified = []
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
        elif field.tag in self.main_title_fields:
            self.title_type = 'main'

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

    def describe_collective_title(self, main_part):
        norm_part = main_part.lower()
        if not self.title_is_collective:
            is_expl_ct = norm_part in settings.MARCDATA.COLLECTIVE_TITLE_TERMS
            is_legal_ct = re.search(r's, etc\W?$', norm_part)
            is_music_ct = re.search(r'\smusic(\s+\(.+\)\s*)?$', norm_part)
            if is_expl_ct or is_music_ct or is_legal_ct:
                self.title_is_collective = True
        if norm_part in settings.MARCDATA.MUSIC_FORM_TERMS_ALL:
            self.title_is_music_form = True
            is_plural = norm_part in settings.MARCDATA.MUSIC_FORM_TERMS_PLURAL
            self.title_is_collective = is_plural

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
        is_control = tag in self.utils.control_sftags
        is_valid_title_part = self.lock_title and val.strip() and not is_control
        return {
            'is_materials_specified': tag == '3',
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
        self.flags = self.get_flags(tag, val)
        if self.flags['is_materials_specified']:
            self.materials_specified.append(p.strip_ends(val))
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
                    self.describe_collective_title(part)
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

    def __init__(self, field, utils=None):
        super(StandardControlNumberParser, self).__init__(field)
        self.utils = utils or MarcParseUtils()
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
        match = re.search(r'^[^\(]*?(?:\((\w+)\))?\W*(.*)$', oclc_num)
        if match:
            ntype, norm = match.groups()
            if norm:
                if ntype == 'OCoLC':
                    ntype = None
                    norm = re.sub('^[A-Za-z0]+', r'', norm)
                return norm, ntype
        return None, None

    def normalize_lccn(self, lccn):
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


def extract_name_structs_from_field(field):
    if field.tag.endswith('00'):
        return [PersonalNameParser(field).parse()]
    if field.tag.endswith('10') or field.tag.endswith('11'):
        return OrgEventNameParser(field).parse()
    return []


def extract_title_struct_from_field(field):
    return PreferredTitleParser(field).parse()


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


def make_personal_name_variations(forename, surname, ptitles):
    alts = []
    if forename and surname:
        alts.append('{} {}'.format(forename, surname))
    if ptitles:
        if alts:
            namestr = alts.pop()
            alts.append('{}, {}'.format(namestr, ', '.join(ptitles)))
        else:
            namestr = surname or forename
        for i, title in enumerate(ptitles):
            altstr = '{} {}'.format(title, namestr)
            if len(ptitles) > 1:
                other_titles = ', '.join(ptitles[0:i] + ptitles[i+1:])
                altstr = '{}, {}'.format(altstr, other_titles)
            alts.append(altstr)
    return alts


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


def generate_title_key(value, nonfiling_chars=0, space_char=r'-'):
    key = value.lower()
    if nonfiling_chars and len(key) > nonfiling_chars:
        last_nfchar_is_nonword = not key[nonfiling_chars - 1].isalnum()
        if last_nfchar_is_nonword and len(value) > nonfiling_chars:
            key = key[nonfiling_chars:]
    key = toascii.map_from_unicode(key)
    key = re.sub(r'\W+', space_char, key).strip(space_char)
    return key or '~'


def format_title_facet_value(heading, nonfiling_chars=0):
    key = generate_title_key(heading, nonfiling_chars)
    return '!'.join((key, heading))


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
    handled automatically, wherever it occurs in the field.
    """
    def __init__(self, field, separator=' ', sf_filter=None):
        filtered = field.filter_subfields(**sf_filter) if sf_filter else field
        super(GenericDisplayFieldParser, self).__init__(filtered)
        self.separator = separator
        self.original_field = field
        self.sf_filter = sf_filter
        self.value_stack = []
        self.materials_specified_stack = []
        self.sep_is_not_space = bool(re.search(r'\S', separator))

    def handle_other_subfields(self, val):
        if len(self.materials_specified_stack):
            ms_str = format_materials_specified(self.materials_specified_stack)
            val = ' '.join((ms_str, val))
            self.materials_specified_stack = []
        self.value_stack.append(val)

    def parse_subfield(self, tag, val):
        if tag == '3':
            self.materials_specified_stack.append(val)
        else:
            self.handle_other_subfields(val)

    def compile_results(self):
        value_stack = []
        for i, val in enumerate(self.value_stack):
            val = val.strip(self.separator)
            is_last = i == len(self.value_stack) - 1
            if self.sep_is_not_space and not is_last:
                val = p.strip_ends(val, end='right')
            value_stack.append(val)
        result = self.separator.join(value_stack)

        if len(self.materials_specified_stack):
            ms_str = format_materials_specified(self.materials_specified_stack)
            result = ' '.join((result, ms_str))
        return result


class PerformanceMedParser(SequentialMarcFieldParser):
    def __init__(self, field):
        super(PerformanceMedParser, self).__init__(field)
        self.parts = []
        self.part_stack = []
        self.instrument_stack = []
        self.last_part_type = ''
        self.total_performers = None
        self.total_ensembles = None
        self.materials_specified_stack = []

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
        if tag == '3':
            self.materials_specified_stack.append(val)
        elif tag in 'abdp':
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
            'materials_specified': self.materials_specified_stack,
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
    record. The `parse` method returns a dictionary mapping field names
    to value lists that result from parsing the fields on the input
    MARC `record`.

    The `mapping` value (passed to __init__) controls how to translate
    MARC to the output dictionary. It should be a tuple structured as
    such:

        mapping = (
            ('author_search', {
                'fields': {
                    'include': ('a', '100', '700'),
                    'exclude': ('111', '711')
                },
                'subfields': {
                    'default': {'exclude': 'w01258'},
                    '100': {'include': 'acd'},
                }
                'parse_func': lambda field: field.format_field()
            }),
        )

    The first tuple value is the name of the output field (which
    becomes a key in the output dictionary). The second is a dictionary
    of options defining how to get field values from the MARC. These
    options include:

    `fields` -- a dict containing `include` and `exclude` values to
    pass as kwargs to the `filter_fields` method on the record, to get
    a list of fields to process. This is non-optional.

    `subfields` -- a dict mapping MARC field tags to lists of subfield
    filters to pass as kwargs to the `filter_subfields` method during
    processing. A `default` key may be included.

    `parse_func` -- a function or method that parses each individual
    field. It should receive the MARC field object and applicable
    subfield filter, and it should return a string.

    A fallback default subfield filter may also be included, passed on
    initialization (`default_df_filter`). If not included, it falls
    back to the `utils.control_sftags` list.
    """
    def __init__(self, record, mapping, utils=None, default_sf_filter=None):
        self.record = record
        self.mapping = mapping
        self.utils = utils or MarcParseUtils()
        self.default_sf_filter = default_sf_filter or {'exclude':
                                                       utils.control_sftags}

    def default_parse_func(self, field, sf_filter):
        return GenericDisplayFieldParser(field, ' ', sf_filter).parse()

    def parse(self):
        ret_val = {}
        for fname, fdef in self.mapping:
            parse_func = fdef.get('parse_func', self.default_parse_func)
            sfdef = fdef.get('subfields', {})
            default_sff = sfdef.get('default', self.default_sf_filter)
            ret_val[fname] = ret_val.get(fname, [])
            for field in self.record.filter_fields(**fdef['fields']):
                sff = sfdef.get(field.tag, default_sff)
                field_val = parse_func(field, sff)
                if field_val:
                    ret_val[fname].append(field_val)
        return ret_val


class BlacklightASMPipeline(object):
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
        'contributor_info', 'title_info', 'general_3xx_info',
        'general_5xx_info', 'call_number_info', 'standard_number_info',
        'control_number_info',
    ]
    prefix = 'get_'
    access_online_label = 'Online'
    access_physical_label = 'At the Library'
    item_rules = local_rulesets.ITEM_RULES
    bib_rules = local_rulesets.BIB_RULES
    hierarchical_name_separator = ' > '
    hierarchical_subject_separator = ' — '
    utils = MarcParseUtils()

    def __init__(self):
        super(BlacklightASMPipeline, self).__init__()
        self.bundle = {}
        self.name_titles = []

    @property
    def sierra_location_labels(self):
        if not hasattr(self, '_sierra_location_labels'):
            self._sierra_location_labels = {}
            pf = 'locationname_set'
            for loc in models.Location.objects.prefetch_related(pf).all():
                loc_name = loc.locationname_set.all()[0].name
                self._sierra_location_labels[loc.code] = loc_name
        return self._sierra_location_labels

    def do(self, r, marc_record):
        """
        Provide `r`, a base.models.BibRecord instance, and
        `marc_record`, a pymarc Record object (both representing the
        same record). Passes these parameters through each method
        in the `fields` class attribute and returns a dict composed of
        all keys returned by the individual methods.
        """
        self.bundle = {}
        self.name_titles = []
        for fname in self.fields:
            method_name = '{}{}'.format(self.prefix, fname)
            result = getattr(self, method_name)(r, marc_record)
            for k, v in result.items():
                if k in self.bundle and self.bundle[k] and v:
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

    def get_id(self, r, marc_record):
        """
        Return the III Record Number, minus the check digit.
        """
        return { 'id': '.{}'.format(r.record_metadata.get_iii_recnum(False)) }

    def get_suppressed(self, r, marc_record):
        """
        Return 'true' if the record is suppressed, else 'false'.
        """
        return { 'suppressed': 'true' if r.is_suppressed else 'false' }

    def get_date_added(self, r, marc_record):
        """
        Return a date that most closely approximates when the record
        was added to the catalog. E-resources (where all bib locations
        are online) use record_metadata.creation_date_gmt; all others
        use the CAT DATE (cataloged date) of the Bib record. Dates are
        converted to the string format needed by Solr.
        """
        if all((l.code.endswith('www') for l in r.locations.all())):
            cdate = r.record_metadata.creation_date_gmt
        else:
            cdate = r.cataloging_date_gmt
        rval = None if cdate is None else cdate.strftime('%Y-%m-%dT%H:%M:%SZ')
        return { 'date_added': rval }

    def get_item_info(self, r, marc_record):
        """
        Return a dict containing item table information: `items_json`,
        `has_more_items`, and `more_items_json`.
        """
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

    def get_urls_json(self, r, marc_record):
        """
        Return a JSON string representing URLs associated with the
        given record.
        """
        urls_data = []
        for f856 in marc_record.get_fields('856'):
            url = f856.get_subfields('u')
            if url:
                url = self._sanitize_url(url[0])
                note = ' '.join(f856.get_subfields('3', 'z')) or None
                label = ' '.join(f856.get_subfields('y')) or None
                utype = 'fulltext' if f856.indicator2 in ('0', '1') else 'link'

                urls_data.append({'u': url, 'n': note, 'l': label,
                                  't': utype})

        for f962 in marc_record.get_fields('962'):
            url = f962.get_subfields('u')
            if url and not self._url_is_media_cover_image(url[0]):
                title = f962.get_subfields('t') or [None]
                urls_data.append({'u': url[0], 'n': title[0], 'l': None,
                                  't': 'media'})

        urls_json = []
        for ud in urls_data:
            ud['t'] = self.review_url_type(ud, len(urls_data), r)
            urls_json.append(ujson.dumps(ud))

        return { 'urls_json': urls_json }

    def _url_is_media_cover_image(self, url):
        """
        Return True if the given `url` is a UNT Media Library cover
        image.
        """
        return 'library.unt.edu/media/covers' in url

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

    def get_thumbnail_url(self, r, marc_record):
        """
        Try finding a (local) thumbnail URL for this bib record. If it
        exists, it will either be from a cover image scanned by the
        Media Library, or it will be from the Digital Library or
        Portal.
        """
        def _try_media_cover_image(f962s):
            for f962 in f962s:
                urls = f962.get_subfields('u')
                url = self._sanitize_url(urls[0]) if urls else None
                if url and self._url_is_media_cover_image(url):
                    return re.sub(r'^(https?):\/\/(www\.)?', 'https://', url)

        def _try_digital_library_image(f856s):
            for f856 in f856s:
                urls = f856.get_subfields('u')
                url = self._sanitize_url(urls[0]) if urls else None
                if url and self._url_is_from_digital_library(url):
                    url = url.split('?')[0].rstrip('/')
                    url = re.sub(r'^http:', 'https:', url)
                    return '{}/small/'.format(url)

        url = _try_media_cover_image(marc_record.get_fields('962')) or\
              _try_digital_library_image(marc_record.get_fields('856')) or\
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

    def _interpret_coded_date(self, dtype, date1, date2):
        pub_type_map = {
            'i': [('creation', 'Collection created in ')],
            'k': [('creation', 'Collection created in ')],
            'p': [('distribution', 'Released in '),
                  ('creation', 'Created or produced in ')],
            'r': [('distribution', 'Reproduced or reissued in '),
                  ('publication', 'Originally published in ')],
            't': [('publication', ''), ('copyright', '')],
            '046kl': [('creation', '')],
            '046op': [('creation', 'Content originally created in ')]
        }
        default_entry = [('publication', '')]
        coded_dates = []
        date1 = date1[0:4] if len(date1) > 4 else date1
        date2 = date2[0:4] if len(date2) > 4 else date2
        date1_valid = bool(re.search(r'^[\du]+$', date1) and date1 != '0000')
        date2_valid = bool(re.search(r'^[\du]+$', date2))
        if date1_valid:
            if dtype in ('es') or date1 == date2 or not date2_valid:
                date2 = None
            details_list = pub_type_map.get(dtype, default_entry)
            if len(details_list) > 1:
                dates = [date1, date2]
                for i, details in enumerate(details_list):
                    pub_field, label = details
                    coded_dates.append((dates[i], None, pub_field, label))
            else:
                pub_field, label = details_list[0]
                coded_dates.append((date1, date2, pub_field, label))
        return coded_dates

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
                    century = unicode(int(match.groups()[0] or 0) + 1)
                    suffix = century_suffix_map.get(century[-1], 'th')
                    year = '{}{} century'.format(century, suffix)
                else:
                    return '?'
                return '{}{}'.format(the, year)
            return year
        
        disp_y1, disp_y2 = _format_year(year1, the), _format_year(year2, the)
        if disp_y1 is None:
            return ''

        if disp_y2 is None:
            if disp_y1 == '?':
                return 'dates unknown'
            return disp_y1

        if disp_y2 == '9999':
            return '{} to present'.format(disp_y1)

        if disp_y1.endswith('century') and disp_y2.endswith('century'):
            disp_y1 = disp_y1.replace(' century', '')

        return '{} to {}'.format(disp_y1, disp_y2)

    def _make_pub_limit_years(self, described_years):
        """
        Given a *set* of `described_years`, each formatted as in the
        MARC 008, return a tuple of lists--one for the publication year
        facet, one for the publication decade facet, and one for
        searchable publication dates.
        """
        def _year_to_decade_facet(year):
            return '{0}0-{0}9'.format(year[:-1])

        def _year_to_decade_label(year):
            return self._format_years_for_display('{}u'.format(year[:-1]))

        def _century_to_decade_facet(formatted_year):
            # formatted_year would be like '19uu' for 20th century
            return '{0}{1}0-{0}{1}9'.format(formatted_year[:-2], i)

        facet_years, facet_decades = set(), set()
        search_pdates = set()
        this_year = datetime.now().year
        for year in list(described_years):
            if 'u' not in year:
                facet_years.add(year)
                facet_decades.add(_year_to_decade_facet(year))
                search_pdates.add(_year_to_decade_label(year))
                search_pdates.add(self._format_years_for_display(year))
            elif re.search(r'^\d+u$', year):
                for i in range(0, 10):
                    add_year = '{}{}'.format(year[:-1], i)
                    if int(add_year) <= this_year:
                        facet_years.add(add_year)
                        search_pdates.add(add_year)
                facet_decades.add(_year_to_decade_facet(year))
                search_pdates.add(self._format_years_for_display(year))
            elif re.search(r'^\d+uu$', year):
                for i in range(0, 10):
                    add_decade = '{}{}u'.format(year[:-2], i)
                    if int(add_decade[:-1]) <= this_year / 10:
                        facet_decades.add(_year_to_decade_facet(add_decade))
                        search_pdates.add(_year_to_decade_label(add_decade))
                search_pdates.add(self._format_years_for_display(year))
        return (list(facet_years), list(facet_decades), list(search_pdates))

    def get_pub_info(self, r, marc_record):
        """
        Get and handle all the needed publication and related info for
        the given bib and marc record.
        """
        def _strip_unknown_pub(data):
            pub_stripped = p.normalize_punctuation(p.strip_unknown_pub(data))
            if re.search(r'\w', pub_stripped):
                return [pub_stripped]
            return []

        pub_info, described_years, places, publishers = {}, set(), set(), set()
        publication_date_notes = []
        for f26x in marc_record.get_fields('260', '264'):
            years = pull_from_subfields(f26x, 'cg', p.extract_years)
            described_years |= set(years)
            for stype, stext in self._extract_pub_statements_from_26x(f26x):
                pub_info[stype] = pub_info.get(stype, [])
                pub_info[stype].append(stext)

            for place in pull_from_subfields(f26x, 'ae', _strip_unknown_pub):
                place = p.strip_ends(place)
                places.add(p.strip_outer_parentheses(place, True))

            for pub in pull_from_subfields(f26x, 'bf', _strip_unknown_pub):
                pub = p.strip_ends(pub)
                publishers.add(p.strip_outer_parentheses(pub, True))

        for f362 in marc_record.get_fields('362'):
            formatted_date = ' '.join(f362.get_subfields('a'))
            years = p.extract_years(formatted_date)
            described_years |= set(years)
            if f362.indicator1 == '0':
                pub_info['publication'] = pub_info.get('publication', [])
                pub_info['publication'].append(formatted_date)
            else:
                publication_date_notes.append(f362.format_field())

        coded_dates = []
        f008 = (marc_record.get_fields('008') or [None])[0]
        if f008 is not None and len(f008.data) >= 15:
            data = f008.data
            entries = self._interpret_coded_date(data[6], data[7:11],
                                                 data[11:15])
            coded_dates.extend(entries)

        for field in marc_record.get_fields('046'):
            coded_group = group_subfields(field, 'abcde', unique='abcde')
            if coded_group:
                dtype = (coded_group[0].get_subfields('a') or [''])[0]
                date1 = (coded_group[0].get_subfields('c') or [''])[0]
                date2 = (coded_group[0].get_subfields('e') or [''])[0]
                entries = self._interpret_coded_date(dtype, date1, date2)
                coded_dates.extend(entries)

            other_group = group_subfields(field, 'klop', unique='klop')
            if other_group:
                _k = (other_group[0].get_subfields('k') or [''])[0]
                _l = (other_group[0].get_subfields('l') or [''])[0]
                _o = (other_group[0].get_subfields('o') or [''])[0]
                _p = (other_group[0].get_subfields('p') or [''])[0]
                coded_dates.extend(self._interpret_coded_date('046kl', _k, _l))
                coded_dates.extend(self._interpret_coded_date('046op', _o, _p))

        sort, year_display = '', ''
        for i, row in enumerate(coded_dates):
            date1, date2, pub_field, label = row
            if i == 0:
                sort = date1
                year_display = self._format_years_for_display(date1, date2)
            if date1 is not None and date1 not in described_years:
                display_date = self._format_years_for_display(date1, date2,
                                                              the=True)
                if display_date != 'dates unknown':
                    new_stext = '{}{}'.format(label, display_date)
                    pub_info[pub_field] = pub_info.get(pub_field, [])
                    pub_info[pub_field].append(new_stext)
                    described_years.add(date1)

        if not coded_dates and described_years:
            sort = sorted([y for y in described_years])[0]
            year_display = self._format_years_for_display(sort)

        yfacet, dfacet, sdates = self._make_pub_limit_years(described_years)
        
        ret_val = {'{}_display'.format(k): v for k, v in pub_info.items()}
        ret_val.update({
            'publication_sort': sort.replace('u', '-'),
            'publication_year_facet': yfacet,
            'publication_decade_facet': dfacet,
            'publication_year_display': year_display,
            'publication_places_search': list(places),
            'publishers_search': list(publishers),
            'publication_dates_search': sdates,
            'publication_date_notes': publication_date_notes
        })
        return ret_val

    def get_access_info(self, r, marc_record):
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

    def get_resource_type_info(self, r, marc_record):
        rtype_info = self.bib_rules['resource_type'].evaluate(r)
        return {
            'resource_type': rtype_info['resource_type'],
            'resource_type_facet': rtype_info['resource_type_categories'],
            'media_type_facet': rtype_info['media_type_categories']
        }

    def compile_person(self, name_struct):
        heading, relations = name_struct['heading'], name_struct['relations']
        json = {'r': relations} if relations else {}
        json['p'] = [{'d': heading}]
        fn, sn, pt = [name_struct[k] for k in ('forename', 'surname', 
                                               'person_titles')]
        search_vals = [heading] + make_personal_name_variations(fn, sn, pt)
        base_name = '{} {}'.format(fn, sn) if (fn and sn) else (sn or fn)
        rel_search_vals = make_relator_search_variations(base_name, relations)
        return {'heading': heading, 'json': json, 'search_vals': search_vals,
                'relator_search_vals': rel_search_vals,
                'facet_vals': [heading],
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
                json_entry['v'] = heading
            json['p'].append(json_entry)
            facet_vals.append(heading)
            if 'event_info' in part:
                ev_info = part['event_info']
                need_punct_before_ev_info = bool(re.match(r'^\w', ev_info))
                if need_punct_before_ev_info:
                    heading = ', '.join((heading, ev_info))
                    json['p'][-1]['s'] = ', '
                else:
                    heading = ' '.join((heading, ev_info))
                json_entry = {'d': ev_info, 'v': heading}
                json['p'].append(json_entry)
                facet_vals.append(heading)
            if not this_is_last_part:
                json['p'][-1]['s'] = sep
        base_name = ' '.join([h['name'] for h in name_struct['heading_parts']])
        rel_search_vals = make_relator_search_variations(base_name, relations)
        return {'heading': heading, 'json': json, 'search_vals': [heading],
                'relator_search_vals': rel_search_vals,
                'facet_vals': facet_vals,
                'short_author': shorten_name(name_struct)}

    def _prep_author_summary_info(self, names):
        for name in names:
            if name and name['compiled']['heading']:
                return {
                    'full_name': name['compiled']['heading'],
                    'short_name': name['compiled']['short_author'],
                    'is_jd': name['parsed'].get('is_jurisdiction', False),
                    'ntype': name['parsed']['type']
                }
        return {'full_name': '', 'short_name': '', 'is_jd': False, 'ntype': ''}

    def _prep_coll_title_parts(self, orig_title_parts, auth_info, is_mform):
        title_parts = []
        p1 = orig_title_parts[0]
        num_parts = len(orig_title_parts)
        if auth_info['short_name']:
            is_org_event = auth_info['ntype'] != 'person'
            conj = 'by' if is_mform else '' if is_org_event else 'of'
            p1 = format_title_short_author(p1, conj, auth_info['short_name'])
        title_parts.append(p1)
        if num_parts == 1:
            if not auth_info['is_jd']:
                title_parts.append('Complete')
        else:
            title_parts.extend(orig_title_parts[1:])
        return title_parts

    def compile_added_title(self, field, title_struct, names):
        if not title_struct['title_parts']:
            return None

        auth_info = self._prep_author_summary_info(names)
        sep = self.hierarchical_name_separator
        heading = ''
        json = {'a': auth_info['full_name']} if auth_info['full_name'] else {}
        json['p'], facet_vals = [], []

        ms = title_struct['materials_specified']
        dc = title_struct['display_constants']
        ms_str = format_materials_specified(ms) if ms else ''
        dc_str = format_display_constants(dc) if dc else ''
        before = ([ms_str] if ms_str else []) + ([dc_str] if dc_str else [])
        if before:
            json['b'] = ' '.join(before)

        nf_chars = title_struct['nonfiling_chars']
        is_coll = title_struct['is_collective']
        is_mform = title_struct['is_music_form']
        tparts = title_struct['title_parts']
        eparts = title_struct['expression_parts']
        volume = title_struct.get('volume', '')
        issn = title_struct.get('issn', '')
        end_info = [volume] if volume else []
        end_info += ['ISSN: {}'.format(issn)] if issn else []

        if is_coll:
            tparts = self._prep_coll_title_parts(tparts, auth_info, is_mform)

        for i, part in enumerate(tparts):
            this_is_first_part = i == 0
            this_is_last_part = i == len(tparts) - 1
            next_part = None if this_is_last_part else tparts[i + 1]
            d_part = part
            skip = part in ('Complete', 'Selections')

            if this_is_first_part:
                heading = part
                if not is_coll and auth_info['short_name']:
                    conj = 'by' if auth_info['ntype'] == 'person' else ''
                    d_part = format_title_short_author(part, conj,
                                                       auth_info['short_name'])
            if not skip:
                if not this_is_first_part:
                    heading = sep.join((heading, part))

                if next_part in ('Complete', 'Selections'):
                    next_part = '({})'.format(next_part)
                    d_part = ' '.join((d_part, next_part))
                    if not is_coll or is_mform or auth_info['is_jd']:
                        fval = format_title_facet_value(heading, nf_chars)
                        facet_vals.append(fval)
                    heading = ' '.join((heading, next_part))

                fval = format_title_facet_value(heading, nf_chars)
                facet_vals.append(fval)
                json['p'].append({'d': d_part, 'v': fval, 's': sep})

            if json['p'] and this_is_last_part:
                if eparts or end_info:
                    json['p'][-1]['s'] = ' | '
                else:
                    del(json['p'][-1]['s'])

        if eparts:
            exp_str = ', '.join(eparts)
            heading = ' | '.join((heading, exp_str))
            fval = format_title_facet_value(heading, nf_chars)
            json_entry = {'d': exp_str, 'v': fval}
            if end_info:
                json_entry['s'] = ' | '
            json['p'].append(json_entry)
            facet_vals.append(fval)

        if end_info:
            end_info_str = ', '.join(end_info)
            heading = ' | '.join((heading, end_info_str))
            json['p'].append({'d': end_info_str})

        return {
            'auth_info': auth_info,
            'heading': heading,
            'title_key': '' if not len(facet_vals) else facet_vals[-1],
            'json': json,
            'search_vals': [heading],
            'facet_vals': facet_vals
        }

    def parse_name_title_fields(self, marc_record):
        def gather_name_info(field, name):
            ctype = 'person' if name['type'] == 'person' else 'org_or_event'
            compiled = getattr(self, 'compile_{}'.format(ctype))(name)
            if compiled and compiled['heading']:
                return {'field': field, 'parsed': name, 'compiled': compiled}

        def gather_title_info(field, title, names):
            compiled = self.compile_added_title(field, title, names)
            if compiled and compiled['heading']:
                return {'field': field, 'parsed': title, 'compiled': compiled}

        if self.name_titles:
            for entry in self.name_titles:
                yield entry
        else:
            entry = {'names': [], 'title': None}
            for f in marc_record.get_fields('100', '110', '111'):
                names = extract_name_structs_from_field(f)
                name_info = [gather_name_info(f, n) for n in names]
                entry['names'] = [n for n in name_info if n is not None]
                break

            for f in marc_record.get_fields('130', '240', '243'):
                title = extract_title_struct_from_field(f)
                entry['title'] = gather_title_info(f, title, entry['names'])
                break

            self.name_titles = [entry]
            yield entry

            added_fields = marc_record.get_fields('700', '710', '711', '730',
                                                  '740', '800', '810', '811',
                                                  '830')
            for f in added_fields:
                entry = {'names': [], 'title': None}
                names = extract_name_structs_from_field(f)
                title = extract_title_struct_from_field(f)
                name_info = [gather_name_info(f, n) for n in names]
                entry['names'] = [n for n in name_info if n is not None]
                if title:
                    entry['title'] = gather_title_info(f, title, entry['names'])
                self.name_titles.append(entry)
                yield entry

    def get_contributor_info(self, r, marc_record):
        """
        This is responsible for using the 100, 110, 111, 700, 710, 711,
        800, 810, and 811 to determine the entirety of author,
        contributor, and meeting fields.
        """
        author_json, contributors_json, meetings_json = {}, [], []
        author_search, contributors_search, meetings_search = [], [], []
        author_contributor_facet, meeting_facet = [], []
        responsibility_search = []
        author_sort = None
        headings_set = set()

        for entry in self.parse_name_title_fields(marc_record):
            for name in entry['names']:
                compiled = name['compiled']
                field = name['field']
                parsed = name['parsed']
                this_is_event = parsed['type'] == 'event'
                this_is_1XX = field.tag.startswith('1')
                this_is_7XX = field.tag.startswith('7')
                this_is_8XX = field.tag.startswith('8')
                if compiled['heading'] not in headings_set:
                    if this_is_event:
                        meetings_search.extend(compiled['search_vals'])
                        meeting_facet.extend(compiled['facet_vals'])
                        meetings_json.append(compiled['json'])
                    else:
                        have_seen_author = bool(author_contributor_facet)
                        if not have_seen_author:
                            if this_is_1XX or this_is_7XX:
                                author_sort = compiled['heading'].lower()
                            if this_is_1XX:
                                author_json = compiled['json']
                                author_search.extend(compiled['search_vals'])
                        if have_seen_author or this_is_7XX or this_is_8XX:
                            contributors_search.extend(compiled['search_vals'])
                            contributors_json.append(compiled['json'])
                        author_contributor_facet.extend(compiled['facet_vals'])
                    responsibility_search.extend(compiled['relator_search_vals'])
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
            'author_sort': author_sort,
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
                for name in entry['names']:
                    if name['compiled']['heading']:
                        if name['field'].tag in ('100', '110', '111'):
                            main_author = name
                            break

            title = entry['title']
            if title:
                if title['field'].tag in ('130', '240', '243'):
                    parsed_130_240 = entry

                analyzed_entry['is_740'] = title['field'].tag == '740'
                if title['parsed']['type'] in ('analytic', 'main'):
                    analyzed_entry['title_type'] = 'included'
                    auth_info = title['compiled']['auth_info']
                    if auth_info['full_name']:
                        incl_authors.add(auth_info['full_name'])
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
        display, non_trunc = '', ''
        sep = self.hierarchical_name_separator
        if transcribed:
            disp_titles, full_titles = [], []
            for title in transcribed:
                disp_parts, full_parts = [], []
                for disp, full in self.truncate_each_ttitle_part(title):
                    disp_parts.append(disp)
                    full_parts.append(full)
                disp_titles.append(sep.join(disp_parts))
                full_titles.append(sep.join(full_parts))
            display = '; '.join(disp_titles)
            non_trunc = '; '.join(full_titles)
        elif parsed_130_240:
            title = parsed_130_240['title']
            display = title['compiled']['heading']
            nf_chars = title['parsed']['nonfiling_chars']

        non_trunc = '' if non_trunc and display == non_trunc else non_trunc
        search = non_trunc or display or None
        return {
            'display': display,
            'non_truncated': non_trunc or None,
            'search': [search] if search else [],
            'sort': generate_title_key(search, nf_chars) if search else None
        }

    def needs_ttitle(self, f245_ind1, nth_ttitle, total_ttitles, f130_240,
                     total_analytic_titles):
        # If 245 ind1 is 0, then we explicitly don't create an added
        # entry (i.e. facet value) for it.
        if f245_ind1 == '0':
            return False

        if nth_ttitle == 0:
            # If this is the first (or only) title from 245 and there
            # is a 130/240 that is NOT a collective title (i.e. it DOES
            # represent a specific work), then we assume the first
            # title from 245 should not create an added facet because
            # it's likely to duplicate that 130/240.
            if f130_240:
                if not f130_240['title']['parsed']['is_collective']:
                    return False

                # Similarly, if this is the first/only title from 245
                # and there's a 130/240 that is a collective title that
                # is more than just, e.g., "Works > Selections", then
                # we assume it's specific enough that the first 245
                # should not create an added facet.
                tp = f130_240['title']['parsed']['title_parts']
                general_forms = ('Complete', 'Selections')
                if len(tp) > 2 or len(tp) == 2 and tp[1] not in general_forms:
                    return False
            
            # If we're here it means there's either no 130/240 or it's
            # a useless generic collective title. At this point we add
            # the first/only title from the 245 if it's probably not
            # duplicated in a 700-730. I.e., if it's the only title in
            # the 245, then it's probably the title for the whole
            # resource and there won't be an added analytical title for
            # it. (If there were, it would probably be the 130/240.)
            # Or, if there multiple titles in the 245 but there are not
            # enough added analytical titles on the record to cover all
            # the individual titles in the 245, then the later titles 
            # are more likely than the first to be covered, so we
            # should go ahead and add the first.
            return total_ttitles == 1 or total_ttitles > total_analytic_titles
        return total_analytic_titles == 0

    def compile_added_ttitle(self, ttitle, nf_chars, author,
                             needs_author_in_title):
        if not ttitle.get('parts', []):
            return None

        auth_info = self._prep_author_summary_info([author])
        sep = self.hierarchical_name_separator
        heading = ''
        json = {'a': auth_info['full_name']} if auth_info['full_name'] else {}
        json['p'], facet_vals = [], []

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

            facet_val = format_title_facet_value(heading, nf_chars)
            json_entry = {'d': part, 'v': facet_val}
            if not this_is_last_part:
                json_entry['s'] = sep

            json['p'].append(json_entry)
            facet_vals.append(facet_val)

        return {
            'heading': heading,
            'title_key': '' if not len(facet_vals) else facet_vals[-1],
            'json': json,
            'search_vals': [heading],
            'facet_vals': facet_vals
        }

    def _match_name_from_sor(self, nametitle_entries, sor):
        for entry in nametitle_entries:
            for name in entry['names']:
                heading = name['compiled']['heading']
                if heading and p.sor_matches_name_heading(sor, heading):
                    return name

    def get_title_info(self, r, marc_record):
        """
        This is responsible for using the 130, 240, 242, 243, 245, 246,
        247, 490, 700, 710, 711, 730, 740, 800, 810, 811, and 830 to
        determine the entirety of title and series fields. 
        """ 
        main_title_info = {}
        json_fields = {'included': [], 'related': [], 'series': []}
        search_fields = {'included': [], 'related': [], 'series': []}
        title_keys = {'included': set(), 'related': set(), 'series': set()}
        variant_titles_notes, variant_titles_search = [], []
        title_series_facet = []
        title_sort = ''
        responsibility_display, responsibility_search = '', []
        hold_740s = []

        name_titles = self.parse_name_title_fields(marc_record)
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
                json = compiled['json']
                search_vals = compiled['search_vals']
                facet_vals = compiled['facet_vals']
                title_key = compiled['title_key']
                if entry['is_740']:
                    hold_740s.append({
                        'title_type': entry['title_type'],
                        'json': json,
                        'svals': search_vals,
                        'fvals': facet_vals,
                        'title_key': title_key
                    })
                else:
                    json_fields[entry['title_type']].append(json)
                    search_fields[entry['title_type']].extend(search_vals)
                    title_series_facet.extend(facet_vals)
                    title_keys[entry['title_type']].add(title_key)

        f245, parsed_245 = None, {}
        for f in marc_record.get_fields('245'):
            f245 = f
            parsed_245 = TranscribedTitleParser(f).parse()
            break
        transcribed = parsed_245.get('transcribed', [])
        parallel = parsed_245.get('parallel', [])
        nf_chars = parsed_245.get('nonfiling_chars', 0)
        main_title_info = self.compile_main_title(transcribed, nf_chars,
                                                  parsed_130_240)
        sor, author = '', main_author

        for i, ttitle in enumerate(transcribed):
            is_first = i == 0
            if 'responsibility' in ttitle:
                author = '' if (sor or not is_first) else main_author
                sor = ttitle['responsibility']
                responsibility_search.append(sor)

            if self.needs_ttitle(f245.indicator1, i, len(transcribed),
                                 parsed_130_240, num_cont_at):
                if not author and sor:
                    author = self._match_name_from_sor(analyzed_entries, sor)

                # needs_author_in_title = num_iw_authors > 1
                nfc = nf_chars if is_first else 0
                compiled = self.compile_added_ttitle(ttitle, nfc, author, True)
                if compiled is not None:
                    json, pjson = compiled['json'], json_fields['included']
                    sv, psv = compiled['search_vals'], search_fields['included']
                    fv, pfv = compiled['facet_vals'], title_series_facet

                    json_fields['included'] = pjson[:i] + [json] + pjson[i:]
                    search_fields['included'] = psv[:i] + sv + psv[i:]
                    title_series_facet = pfv[:i] + fv + pfv[i:]
                    title_keys['included'].add(compiled['title_key'])

        responsibility_display = '; '.join(responsibility_search)

        for entry in hold_740s:
            if entry['title_key'] not in title_keys[entry['title_type']]:
                json_fields[entry['title_type']].append(entry['json'])
                search_fields[entry['title_type']].extend(entry['svals'])
                title_series_facet.extend(entry['fvals'])
                title_keys[entry['title_type']].add(entry['title_key'])

        for f in marc_record.get_fields('242', '246', '247'):
            parsed = TranscribedTitleParser(f).parse()
            f246_add_notes = f.tag == '246' and f.indicator1 in ('01')
            f247_add_notes = f.tag == '247' and f.indicator2 == '0'
            add_notes = f.tag == '242' or f246_add_notes or f247_add_notes
            display_text = parsed.get('display_text', '')
            for vtitle in parsed.get('transcribed', []):
                if 'parts' in vtitle:
                    t = self.hierarchical_name_separator.join(vtitle['parts'])
                    variant_titles_search.append(t)
                    if add_notes:
                        if display_text:
                            note = '{}: {}'.format(display_text, t)
                        else:
                            note = t
                        variant_titles_notes.append(note)
                if 'responsibility' in vtitle:
                    responsibility_search.append(vtitle['responsibility'])

        for ptitle in parallel:
            if 'parts' in ptitle:
                tstr = self.hierarchical_name_separator.join(ptitle['parts'])
                if tstr not in variant_titles_search:
                    display_text = TranscribedTitleParser.variant_types['1']
                    note = '{}: {}'.format(display_text, tstr)
                    variant_titles_notes = [note] + variant_titles_notes
                    variant_titles_search.append(tstr)

            if 'responsibility' in ptitle:
                sor = ptitle['responsibility']
                if sor not in responsibility_search:
                    responsibility_search.append(ptitle['responsibility'])

        for f in marc_record.get_fields('490'):
            if f.indicator1 == '0':
                pre, end = '', []
                parsed = TranscribedTitleParser(f).parse()
                if 'materials_specified' in parsed:
                    ms = parsed['materials_specified']
                    pre = format_materials_specified(ms)
                if 'issn' in parsed:
                    end.append('ISSN: {}'.format(parsed['issn']))
                if 'lccn' in parsed:
                    end.append('LC Call Number: {}'.format(parsed['lccn']))
                for stitle in parsed['transcribed']:
                    render = [pre] if pre else []
                    parts = stitle.get('parts', [])
                    sor = stitle.get('responsibility')
                    if parts and sor:
                        parts[0] = '{} [{}]'.format(parts[0], sor)
                    render.append(self.hierarchical_name_separator.join(parts))
                    render.extend(['|', '; '.join(end)] if end else [])
                    rendered = ' '.join(render)
                    json_fields['series'].append({'p': [{'d': rendered}]})
                    search_fields['series'].append(rendered)

        iworks_json = [ujson.dumps(v) for v in json_fields['included']]
        rworks_json = [ujson.dumps(v) for v in json_fields['related']]
        series_json = [ujson.dumps(v) for v in json_fields['series']]
        return {
            'title_display': main_title_info['display'] or None,
            'non_truncated_title_display': main_title_info['non_truncated'],
            'included_work_titles_json': iworks_json or None,
            'related_work_titles_json': rworks_json or None,
            'related_series_titles_json': series_json or None,
            'variant_titles_notes': variant_titles_notes or None,
            'main_title_search': main_title_info['search'] or None,
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
                part_type, instruments = clause.items()[0]
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

    def get_general_3xx_info(self, r, marc_record):
        def join_subfields_with_semicolons(field, sf_filter):
            return GenericDisplayFieldParser(field, '; ', sf_filter).parse()

        def parse_performance_medium(field, sf_filter):
            parsed = PerformanceMedParser(field).parse()
            return self.compile_performance_medium(parsed)

        record_parser = MultiFieldMarcRecordParser(marc_record, (
            ('physical_medium', {
                'fields': {'include': ('340',)},
                'parse_func': join_subfields_with_semicolons
            }),
            ('geospatial_data', {
                'fields': {'include': ('342', '343')},
                'parse_func': join_subfields_with_semicolons
            }),
            ('audio_characteristics', {
                'fields': {'include': ('344',)},
                'parse_func': join_subfields_with_semicolons
            }),
            ('projection_characteristics', {
                'fields': {'include': ('345',)},
                'parse_func': join_subfields_with_semicolons
            }),
            ('video_characteristics', {
                'fields': {'include': ('346',)},
                'parse_func': join_subfields_with_semicolons
            }),
            ('digital_file_characteristics', {
                'fields': {'include': ('347',)},
                'parse_func': join_subfields_with_semicolons
            }),
            ('graphic_representation', {
                'fields': {'include': ('352',)}
            }),
            ('performance_medium', {
                'fields': {'include': ('382',)},
                'parse_func': parse_performance_medium
            }),
            ('physical_description', {
                'fields': {
                    'include': ('r', '370'),
                    'exclude': IGNORED_MARC_FIELDS_BY_GROUP_TAG['r'] +
                               ('310', '321', '340', '342', '343', '344', '345',
                                '346', '347', '352', '362', '382')
                }
            })
        ), utils=self.utils)
        return record_parser.parse()

    def get_general_5xx_info(self, r, marc_record):
        def join_subfields_with_spaces(field, sf_filter):
            return GenericDisplayFieldParser(field, ' ', sf_filter).parse()

        def join_subfields_with_semicolons(field, sf_filter):
            return GenericDisplayFieldParser(field, '; ', sf_filter).parse()

        def _generate_display_constant(parse_func, test_val, mapping):
            label = mapping.get(test_val, None)
            if label:
                return '{}: {}'.format(label, parse_func())
            return parse_func()

        def parse_502_dissertation_notes(field, sf_filter):
            if field.get_subfields('a'):
                return join_subfields_with_spaces(field, {'include': 'ago'})
            parsed_dn = DissertationNotesFieldParser(field).parse()
            diss_note = '{}.'.format('. '.join(parsed_dn['note_parts']))
            return p.normalize_punctuation(diss_note)

        def parse_511_performers(field, sf_filter):
            return _generate_display_constant(
                lambda: join_subfields_with_spaces(field, sf_filter),
                field.indicator1,
                {'1': 'Cast'}
            )

        def parse_all_other_notes(field, sf_filter):
            if field.tag == '521':
                val = _generate_display_constant(
                    lambda: join_subfields_with_semicolons(field,
                                                           {'include': '3a'}),
                    field.indicator1,
                    {' ': 'Audience',
                     '0': 'Reading grade level',
                     '1': 'Ages',
                     '2': 'Grades',
                     '3': 'Special audience characteristics',
                     '4': 'Motivation/interest level'}
                )
                source = ', '.join(field.get_subfields('b'))
                if source:
                    val = '{} (source: {})'.format(val, p.strip_ends(source))
                return val

            if field.tag == '583':
                if field.indicator1 == '1':
                    return join_subfields_with_semicolons(field, sf_filter)
                return None

            if field.tag == '588':
                return _generate_display_constant(
                    lambda: join_subfields_with_spaces(field, sf_filter),
                    field.indicator1,
                    {'0': 'Description based on',
                     '1': 'Latest issue consulted'}
                )
            return join_subfields_with_spaces(field, sf_filter)

        record_parser = MultiFieldMarcRecordParser(marc_record, (
            ('performers', {
                'fields': {'include': ('511',)},
                'parse_func': parse_511_performers
            }),
            ('language_notes', {
                'fields': {'include': ('546',)},
                'parse_func': join_subfields_with_spaces
            }),
            ('dissertation_notes', {
                'fields': {'include': ('502',)},
                'parse_func': parse_502_dissertation_notes
            }),
            ('notes', {
                'fields': {
                    'include': ('n', '583'),
                    'exclude': IGNORED_MARC_FIELDS_BY_GROUP_TAG['n'] +
                               ('502', '505', '508', '511', '520', '546',
                                '592'),
                },
                'parse_func': parse_all_other_notes    
            })
        ), utils=self.utils)
        return record_parser.parse()

    def get_call_number_info(self, r, marc_record):
        """
        Return a dict containing information about call numbers and
        sudoc numbers to load into Solr fields. Note that bib AND item
        call numbers are included, but they are deduplicated.
        """
        call_numbers_display, call_numbers_search = [], []
        sudocs_display, sudocs_search = [], []

        call_numbers = r.get_call_numbers() or []

        item_links = [l for l in r.bibrecorditemrecordlink_set.all()]
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

    def get_standard_number_info(self, r, marc_record):
        isbns_display, issns_display, others_display, search = [], [], [], []
        isbns, issns = [], []
        all_standard_numbers = []

        standard_num_fields = ('020', '022', '024', '025', '026', '027', '028',
                               '030', '074', '088')
        for f in marc_record.get_fields(*standard_num_fields):
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

    def get_control_number_info(self, r, marc_record):
        lccns_display, oclc_display, others_display, search = [], [], [], []
        lccn, oclc_numbers = '', [],
        all_control_numbers = []

        control_num_fields = ('010', '016', '035')
        for f in marc_record.get_fields(*control_num_fields):
            for p in StandardControlNumberParser(f).parse():
                nums = [p[k] for k in ('normalized', 'number') if k in p]
                for num in nums:
                    search.append(num)
                    all_control_numbers.append(num)
                display = format_number_display_val(p)
                if p['type'] == 'lccn':
                    lccns_display.append(display)
                    if p['is_valid'] and nums and not lccn:
                        lccn = nums[0]
                elif p['type'] == 'oclc':
                    oclc_display.append(display)
                    if p['is_valid'] and nums and nums[0] not in oclc_numbers:
                        oclc_numbers.append(nums[0])
                else:
                    others_display.append(display)

        return {
            'lccns_display': lccns_display or None,
            'oclc_numbers_display': oclc_display or None,
            'lccn_number': lccn or None,
            'oclc_numbers': oclc_numbers or None,
            'other_control_numbers_display': others_display or None,
            'all_control_numbers': all_control_numbers or None,
            'control_numbers_search': search or None,
        }


class PipelineBundleConverter(object):
    """
    Use this to map a dict to a series of MARC fields/subfields.

    Provide a `mapping` parameter to __init__, or subclass this and
    populate the `mapping` class attribute.

    The mapping should be a tuple, or list, like the one provided.
    Each row models a MARC field instance. The first tuple element is
    the MARC tag. The second is a tuple or list that details what keys
    from the data dict then become subfields. Subfields are assigned
    automatically, starting with 'a'.

    An individual dict key may contain multiple values, which can be
    represented either as repeated instances of the same subfield or
    repeated instances of the field:

        914 $aSubject 1$aSubject 2$aSubject 3
        vs
        914 $aSubject 1
        914 $aSubject 2
        914 $aSubject 3

    Since we're using subfields as granular, fully-independent storage
    slots (not dependent on other subfields), the difference I think is
    cosmetic.

    If a row in the mapping contains one and only one key, then the
    entire field gets repeated for each value. If a row contains
    multiple keys, then they all appear in the same instance of that
    field and repeated values become repeated subfields.

    Whether a field tag is repeated or not, the subfield lettering will
    be sequential:

        ( '909', ('items_json',) ),
        ( '909', ('has_more_items',) ),
        vs
        ( '909', ('items_json', 'has_more_items') ),

    In both cases, 'items_json' is $a and 'has_more_items' is $b. And,
    it's up to you to ensure you don't have more than 26 subfields per
    field.

    Once your mapping is set up, you can use the `do` method (passing
    in a dict with the appropriate keys) to generate a list of pymarc
    Field objects.
    """
    mapping = (
        ( '907', ('id',) ),
        ( '970', ('suppressed', 'date_added', 'access_facet', 'building_facet',
                  'shelf_facet', 'collection_facet', 'resource_type',
                  'resource_type_facet', 'media_type_facet') ),
        ( '971', ('items_json',) ),
        ( '971', ('has_more_items',) ),
        ( '971', ('more_items_json',) ),
        ( '971', ('thumbnail_url', 'urls_json') ),
        ( '971', ('serial_holdings',) ),
        ( '972', ('author_json',) ),
        ( '972', ('contributors_json',) ),
        ( '972', ('meetings_json',) ),
        ( '972', ('author_search',) ),
        ( '972', ('contributors_search',) ),
        ( '972', ('meetings_search',) ),
        ( '972', ('author_contributor_facet',) ),
        ( '972', ('meeting_facet',) ),
        ( '972', ('author_sort',) ),
        ( '972', ('responsibility_search',) ),
        ( '972', ('responsibility_display',) ),
        ( '973', ('title_display', 'non_truncated_title_display') ),
        ( '973', ('included_work_titles_json',) ),
        ( '973', ('related_work_titles_json',) ),
        ( '973', ('related_series_titles_json',) ),
        ( '973', ('variant_titles_notes',) ),
        ( '973', ('main_title_search',) ),
        ( '973', ('included_work_titles_search',) ),
        ( '973', ('related_work_titles_search',) ),
        ( '973', ('related_series_titles_search',) ),
        ( '973', ('variant_titles_search',) ),
        ( '973', ('title_series_facet',) ),
        ( '973', ('title_sort',) ),
        ( '974', ('subjects',) ),
        ( '974', ('subject_topic_facet',) ),
        ( '974', ('subject_region_facet',) ),
        ( '974', ('subject_era_facet',) ),
        ( '974', ('item_genre_facet',) ),
        ( '974', ('subjects_display_jason',) ),
        ( '975', ('call_numbers_display',) ),
        ( '975', ('call_numbers_search',) ),
        ( '975', ('sudocs_display',) ),
        ( '975', ('sudocs_search',) ),
        ( '975', ('isbns_display',) ),
        ( '975', ('issns_display',) ),
        ( '975', ('lccns_display',) ),
        ( '975', ('oclc_numbers_display',) ),
        ( '975', ('isbn_numbers',) ),
        ( '975', ('issn_numbers',) ),
        ( '975', ('lccn_number',) ),
        ( '975', ('oclc_numbers',) ),
        ( '975', ('all_standard_numbers',) ),
        ( '975', ('all_control_numbers',) ),
        ( '975', ('other_standard_numbers_display',) ),
        ( '975', ('other_control_numbers_display',) ),
        ( '975', ('standard_numbers_search',) ),
        ( '975', ('control_numbers_search',) ),
        ( '976', ('publication_sort', 'publication_year_facet',
                  'publication_decade_facet', 'publication_year_display') ),
        ( '976', ('creation_display', 'publication_display',
                  'distribution_display', 'manufacture_display',
                  'copyright_display') ),
        ( '976', ('publication_places_search', 'publishers_search',
                  'publication_dates_search', 'publication_date_notes') ),
        ( '977', ('physical_description', 'physical_medium', 'geospatial_data',
                  'audio_characteristics', 'projection_characteristics',
                  'video_characteristics', 'digital_file_characteristics',
                  'graphic_representation', 'performance_medium', 'performers',
                  'language_notes', 'dissertation_notes', 'notes') ),
    )

    def __init__(self, mapping=None):
        """
        Optionally, pass in a custom `mapping` structure. Default is
        the class attribute `mapping`.
        """
        self.mapping = mapping or self.mapping

    def _increment_sftag(self, sftag):
        return chr(ord(sftag) + 1)

    def _map_row(self, tag, sftag, fnames, bundle):
        repeat_field = True if len(fnames) == 1 else False
        fields, subfields = [], []
        for fname in fnames:
            vals = bundle.get(fname, None)
            vals = vals if isinstance(vals, (list, tuple)) else [vals]
            for v in vals:
                if v is not None:
                    if repeat_field:
                        field = make_mfield(tag, subfields=[sftag, v])
                        fields.append(field)
                    else:
                        subfields.extend([sftag, v])
            sftag = self._increment_sftag(sftag)
        if len(subfields):
            fields.append(make_mfield(tag, subfields=subfields))
        return sftag, fields

    def do(self, bundle):
        """
        Provide `bundle`, a dict of values, where keys match the ones
        given in the mapping. Returns a list of pymarc Field objects.

        If the provided dict does not have a key that appears in the
        mapping, it's fine--that field/subfield is simply skipped.
        """
        fields, tag_tracker = [], {}
        for tag, fnames in self.mapping:
            sftag = tag_tracker.get(tag, 'a')
            sftag, new_fields = self._map_row(tag, sftag, fnames, bundle)
            fields.extend(new_fields)
            tag_tracker[tag] = sftag
        return fields

    def reverse_mapping(self):
        """
        Reverse this object's mapping: get a list of tuples, where each
        tuple is (key, marc_tag, subfield_tag). The list is in order
        based on the mapping.
        """
        reverse, tag_tracker = [], {}
        for tag, fnames in self.mapping:
            sftag = tag_tracker.get(tag, 'a')
            for fname in fnames:
                reverse.append((fname, tag, sftag))
                sftag = self._increment_sftag(sftag)
            tag_tracker[tag] = sftag
        return reverse


class S2MarcBatchBlacklightSolrMarc(S2MarcBatch):
    """
    Sierra to MARC converter for the Blacklight, using SolrMarc.
    """
    custom_data_pipeline = BlacklightASMPipeline()
    to_9xx_converter = PipelineBundleConverter()

    def _record_get_media_game_facet_tokens(self, r, marc_record):
        """
        If this is a Media Library item and has a 592 field with a
        Media Game Facet token string ("p1;p2t4;d30t59"), it returns
        the list of tokens. Returns None if no game facet string is
        found or tokens can't be extracted.
        """
        tokens = []
        if any([loc.code.startswith('czm') for loc in r.locations.all()]):
            for f in marc_record.get_fields('592'):
                for sub_a in f.get_subfields('a'):
                    if re.match(r'^(([adp]\d+(t|to)\d+)|p1)(;|\s|$)', sub_a,
                                re.IGNORECASE):
                        tokens += re.split(r'\W+', sub_a.rstrip('. '))
        return tokens or None

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
                elif tag[0] != '9' or tag in ('962',):
                    # Ignore most existing 9XX fields from Sierra.
                    ind = [ind1, ind2]
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
        return marc_record

    def _one_to_marc(self, r):
        marc_record = self.compile_original_marc(r)
        if not marc_record.fields:
            raise S2MarcError('Skipped. No MARC fields on Bib record.', str(r))

        bundle = self.custom_data_pipeline.do(r, marc_record)
        marc_record.add_field(*self.to_9xx_converter.do(bundle))

        marc_record.remove_fields('001')
        hacked_id = 'a{}'.format(bundle['id'])
        marc_record.add_grouped_field(make_mfield('001', data=hacked_id))

        # If this record has a media game facet field: clean it up,
        # split by semicolon, and put into 910$a (one 910, and one $a
        # per token)
        media_tokens = self._record_get_media_game_facet_tokens(r, marc_record)
        if media_tokens is not None:
            mf_subfield_data = []
            for token in media_tokens:
                mf_subfield_data += ['a', token]
            mf_field = pymarc.field.Field(
                tag='960',
                indicators=[' ', ' '],
                subfields = mf_subfield_data
            )
            marc_record.add_field(mf_field)

        if re.match(r'[0-9]', marc_record.as_marc()[5]):
            raise S2MarcError('Skipped. MARC record exceeds 99,999 bytes.', 
                              str(r))

        return marc_record
