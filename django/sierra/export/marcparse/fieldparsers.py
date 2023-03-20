"""
Contains functions/classes for parsing MARC field data.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import re

try:
    # Python 3
    from itertools import zip_longest
except ImportError:
    # Python 2
    from itertools import izip_longest as zip_longest

from collections import OrderedDict
from django.conf import settings
from six.moves import range

from . import stringparsers as sp, renderers as rend


MARC_SOURCECODE_MAP = settings.MARCDATA.STANDARD_ID_SOURCE_CODES
CONTROL_SFTAGS = 'w01256789'


def explode_subfields(mfield, sftags):
    """
    Get subfields (`sftags`) from the given SierraMarcField object
    (`mfield`) and split them into a tuple, where each tuple value
    contains the list of values for the corresponding subfield tag.
    E.g., subfields 'abc' would return a tuple of 3 lists, the first
    corresponding with all subfield 'a' values from the MARC field, the
    second with subfield 'b' values, and the third with subfield 'c'
    values. Any subfields not present become an empty list.

    Use like this:
        title, subtitle, responsibility = explode_subfields(f245, 'abc')
    """
    return (mfield.get_subfields(tag) for tag in sftags)


def group_subfields(mfield, include='', exclude='', unique='', start='',
                    end='', limit=None):
    """
    Put subfields from the given `mfield` SierraMarcField object into
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
    fieldtype = type(mfield)

    def _include_tag(tag, include, exclude):
        return (not include and not exclude) or (include and tag in include) or\
               (exclude and tag not in exclude)

    def _finish_group(mfield, grouped, group, limit=None):
        if not limit or (len(grouped) < limit - 1):
            grouped.append(fieldtype(mfield.tag, subfields=group,
                                     indicators=mfield.indicators))
            group = []
        return grouped, group

    def _is_repeated_unique(tag, unique, group):
        return tag in unique and tag in [gi[0] for gi in group]

    grouped, group = [], []
    for tag, value in mfield:
        if _include_tag(tag, include, exclude):
            if tag in start or _is_repeated_unique(tag, unique, group):
                grouped, group = _finish_group(mfield, grouped, group, limit)
            group.extend([tag, value])
            if tag in end:
                grouped, group = _finish_group(mfield, grouped, group, limit)
    if group:
        grouped, group = _finish_group(mfield, grouped, group)
    return grouped


def pull_from_subfields(mfield, sftags=None, pull_func=None):
    """
    Extract a list of values from the given SierraMarcField object
    (`mfield`). Optionally specify which `sftags` to pull data from
    and/or a `pull_func` function. The function should take a string
    value (i.e. from one subfield) and return a LIST of values.
    A single flattened list of collective values is returned.
    """
    sftags = tuple(sftags) if sftags else [sf[0] for sf in mfield]
    vals = mfield.get_subfields(*sftags)
    if pull_func is None:
        return vals
    return [v2 for v1 in vals for v2 in pull_func(v1)]


class SequentialMarcFieldParser(object):
    """
    Parse a SierraMarcField obj by parsing subfields sequentially.

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
        return sp.strip_ends(val)

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
        for relator_term in sp.extract_relator_terms(val, tag=='4'):
            self.relator_terms[relator_term] = None

    def do_titles(self, tag, val):
        self.titles.extend([v for v in sp.strip_ends(val).split(', ')])

    def do_numeration(self, tag, val):
        self.numeration = sp.strip_ends(val)

    def do_fuller_form_of_name(self, tag, val):
        ffn = re.sub(r'^(?:.*\()?([^()]+)(?:\).*)?$', r'\1', val)
        self.fuller_form_of_name = sp.strip_ends(ffn)

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
        heading = sp.normalize_punctuation(' '.join(self.heading_parts))
        parsed_name = sp.person_name(self.main_name, self.field.indicators)
        return {
            'heading': sp.strip_ends(heading) or None,
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
        for relator_term in sp.extract_relator_terms(val, tag==4):
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
        return sp.strip_ends(' '.join(self._event_info))

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
            for relator_term in sp.extract_relator_terms(val, tag==4):
                self.relator_terms[relator_term] = None
        elif tag in self.event_info_sftags:
            self._event_info.append(val)
        elif tag == 'a':
            self._prev_part_name = sp.strip_ends(val)
        elif self.sf_is_first_subunit_of_jd_field(tag):
            part = '{} {}'.format(self._prev_part_name, sp.strip_ends(val))
            self._prev_part_name = part
        elif tag == self.subunit_sftag:
            new_org_parts, new_event_parts, new_combined_parts = self.do_unit()
            self.parts['org'].extend(new_org_parts)
            self.parts['event'].extend(new_event_parts)
            self.parts['combined'].extend(new_combined_parts)
            self._event_info = []
            self._prev_part_name = sp.strip_ends(val)
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
            parts = [sp.compress_punctuation(tp) for tp in self.title_parts]
            title['parts'] = parts
            self.title_parts = []
        if self.responsibility:
            no_prev_r = self.titles and 'responsibility' not in self.titles[-1]
            responsibility = sp.compress_punctuation(self.responsibility)
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
        part = sp.restore_periods(part)
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
        vol_sep, volume = rend.format_volume(sp.restore_periods(part))
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
        tstring = sp.normalize_punctuation(tstring, periods_protected=True,
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
                self.do_compound_title_part(
                    title_part, handle_internal_periods)
                do_sor_next = True

    def get_flags(self, tag, val):
        if self.field.tag == '490':
            def_to_newpart = tag == 'a'
        else:
            def_to_newpart = tag == 'n' or (
                tag == 'p' and self.prev_tag != 'n')
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
            prot = sp.protect_periods(val)

            isbd = r''.join(list(self.analyzer.isbd_punct_mapping.keys()))
            switchp = r'"\'~\.,\)\]\}}'
            is_245bc = self.flags['is_245b'] or self.flags['is_subfield_c']
            if is_245bc or self.field.tag == '490':
                p_switch_re = r'([{}])(\s*[{}]+)(\s|$)'.format(isbd, switchp)
            else:
                p_switch_re = r'([{}])(\s*[{}]+)($)'.format(isbd, switchp)
            prot = re.sub(p_switch_re, r'\2\1\3', prot)

            part, end_punct = self.analyzer.pop_isbd_punct_from_title_part(
                prot)
            if part:
                if self.flags['is_display_text']:
                    self.display_text = sp.restore_periods(part)
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
                    lccn = sp.strip_outer_parentheses(part)
                    self.lccn = sp.restore_periods(lccn)

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
            display_text = self.display_text or self.variant_types.get(
                ind2, '')

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

    def __init__(self, field):
        super(PreferredTitleParser, self).__init__(field)
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
        for relator_term in sp.extract_relator_terms(val, tag=='4'):
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
        part_type = self.analyzer.what_type_is_this_part(
            prev_punct, self.flags)
        part = sp.restore_periods(part)
        force_new = self.force_new_part()
        if not force_new and part_type == 'same_part' and len(
                self.title_parts):
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
        is_control = tag in CONTROL_SFTAGS or tag == '3'
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
            for relator_term in sp.extract_relator_terms(val, tag=='4'):
                self.relator_terms[relator_term] = None
        elif self.flags['is_display_const']:
            display_val = sp.strip_ends(sp.strip_wemi(val))
            if display_val.lower() == 'container of':
                self.title_type = 'analytic'
            else:
                self.display_constants.append(display_val)
        elif self.flags['is_valid_title_part']:
            prot = sp.protect_periods(val)
            part, end_punct = self.analyzer.pop_isbd_punct_from_title_part(
                prot)
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
                    self.volume = sp.restore_periods(part)
                elif self.flags['is_issn']:
                    self.issn = sp.restore_periods(part)
                else:
                    if self.flags['is_language']:
                        self.languages = self.parse_languages(part)
                    if self.flags['is_arrangement'] and part.startswith('arr'):
                        part = 'arranged'
                    self.expression_parts.append(sp.restore_periods(part))
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
                sor_chunks = [sp.strip_ends(v) for v in p_chunk.split(' / ')]
                if len(sor_chunks) == 1:
                    if lock_sor:
                        sors.append(sor_chunks[0])
                    else:
                        values.append(sor_chunks[0])
                else:
                    values.append(sor_chunks[0])
                    sors.append(', '.join(sor_chunks[1:]))
                    lock_sor = True

            for i, pair in enumerate(zip_longest(values, sors)):
                value, sor = pair
                entry = {}
                if value:
                    entry['value'] = sp.restore_periods(value)
                if sor:
                    entry['responsibility'] = sp.restore_periods(sor)
                if entry:
                    key = 'editions' if i == 0 else 'parallel'
                    edition_info[key] = edition_info.get(key, [])
                    edition_info[key].append(entry)
        return edition_info

    def parse_subfield(self, tag, val):
        if val:
            val = sp.protect_periods(val).strip()
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
                ed = '; '.join([sp.strip_ends(v) for v in self._edition_stack])
                ed_info = {'editions': [{'value': sp.restore_periods(ed)}]}

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

    def __init__(self, field):
        super(StandardControlNumberParser, self).__init__(field)
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
        match = re.search(r'(?:^|\s)([\dX\-]+)[^(]*(?:\((.+)\))?', isbn)
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
        cleaned = re.sub(r'[()]', r'', sp.strip_ends(qstr))
        return re.split(r'\s*[,;:]\s+', cleaned)

    def generate_validity_label(self, tag):
        if tag == 'z':
            return 'Canceled' if self.ntype == 'issn' else 'Invalid'
        return 'Canceled' if tag == 'm' else 'Incorrect' if tag == 'y' else None

    def generate_type_label(self, ntype):
        label = MARC_SOURCECODE_MAP.get(ntype)
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
                    val, sep, suffix = val.partition(
                        self.oclc_suffix_separator)
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

    def __init__(self, field):
        super(LanguageParser, self).__init__(field)
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
        if tag not in CONTROL_SFTAGS:
            cat = 'a' if self.field.tag == '377' or tag == 'd' else tag
            if self.field.tag == '377' and tag == 'l':
                language = sp.strip_ends(val)
            else:
                language = settings.MARCDATA.LANGUAGE_CODES.get(val)

            if language:
                self.languages[language] = None
                self.categorized[cat] = self.categorized.get(
                    cat, OrderedDict())
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
        protected = sp.protect_periods(title)
        return [sp.restore_periods(tp) for tp in protected.split('. ') if tp]

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
            self.display_label_from_i = sp.strip_ends(sp.strip_wemi(val))
        else:
            val = sp.strip_ends(val)
            if tag == 's':
                self.stitle = val
            elif tag == 't':
                self.ttitle = val
            elif tag == 'g' and self.is_series and self.prev_tag in ('t', 's'):
                self.volume = self.do_volume(val)
            elif tag == 'a':
                name_struct = sp.parse_name_string(val)
                self.short_author = rend.shorten_name(name_struct)
                self.ntype = name_struct['type']
                if name_struct['type'] == 'person':
                    self.author = name_struct['heading']
                else:
                    self.author = ' '.join([
                        hp['name'] for hp in name_struct['heading_parts']
                    ])
            elif tag in self.display_metadata_sftags:
                self.display_metadata.append(sp.strip_outer_parentheses(val))
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
            self.part_stack.append(
                {self.last_part_type: self.instrument_stack})
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

    def try_to_do_degree_statement(self):
        if not self.degree_statement_is_done:
            if self.degree or self.institution or self.date:
                degree_statement = rend.format_degree_statement(
                    self.institution, self.date, self.degree
                )
                self.note_parts.append(degree_statement)
                self.degree_statement_is_done = True

    def parse_subfield(self, tag, val):
        if tag == 'b':
            self.degree = sp.strip_ends(val)
        elif tag == 'c':
            self.institution = sp.strip_ends(val)
        elif tag == 'd':
            self.date = sp.strip_ends(val)
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
            val = sp.strip_ends(val, end='right')
        return ''.join([val, sep])

    def parse_subfield(self, tag, val):
        if tag != '3':
            self.value_stack.append(val)

    def compile_results(self):
        result_stack = []
        if self.materials_specified:
            ms_str = rend.format_materials_specified(self.materials_specified)
            result_stack.append(ms_str)
        if self.label:
            result_stack.append(rend.format_display_constants([self.label]))

        value_stack = []
        for i, val in enumerate(self.value_stack):
            is_last = i == len(self.value_stack) - 1
            if not is_last:
                val = self.add_separator(val)
            value_stack.append(val)
        result_stack.append(''.join(value_stack))
        return ' '.join(result_stack)


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
    simply excludes CONTROL_SFTAGS.

    `parse_func` -- (Optional.) A function or method that parses each
    individual field. It should receive the MARC field object and
    applicable subfield filter, and it should return a string.
    Defaults to the `default_parse_func` method.
    """

    def __init__(self, record, mapping, default_sf_filter=None):
        self.record = record
        self.mapping = mapping
        self.default_sf_filter = default_sf_filter or {'exclude':
                                                       CONTROL_SFTAGS}

    def default_parse_func(self, field, sf_filter):
        return GenericDisplayFieldParser(field, ' ', sf_filter).parse()

    def parse(self):
        ret_val = {}
        for f in self.record.fields:
            fdef = self.mapping.get(f.tag)
            if fdef is None and f.tag not in self.mapping.get(
                    'exclude', set()):
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

