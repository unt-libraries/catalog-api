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
from utils import helpers


# These are MARC fields that we are currently not including in public
# catalog records, listed by III field group tag.
IGNORED_MARC_FIELDS_BY_GROUP_TAG = {
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

    def filter_subfields(self, sftags, exclusionary=False):
        """
        Filter subfields on this field based on the provided `sftags`.
        `sftags` is inclusionary when `exclusionary` is False (default)
        or exclusionary when `exclusionary` is True. This is a
        generator method that yields a (sftag, sfval) tuple.
        """
        incl, excl = not exclusionary, exclusionary
        get_all = not sftags
        for t, v in self:
            if get_all or (incl and t in sftags) or (excl and t not in sftags):
                yield (t, v)


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

    def filter_fields(self, include, exclude):
        """
        Like `get_fields_gen` but lets you provide a list of tags to
        include and a list to exclude. All tags should be ones such as
        what's defined in the `get_fields` docstring.
        """
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
    control_sftags = 'w012356789'
    title_sftags_7xx = 'fhklmoprstvx'

    def compile_relator_terms(self, tag, val):
        if tag == '4':
            term = self.marc_relatorcode_map.get(val, None)
            return [term] if term else []
        return [p.strip_wemi(v) for v in p.strip_ends(val).split(', ')]

    def split7xx_name_title(self, field):
        exclude_sftags = ''.join((self.control_sftags, 'i'))
        return group_subfields(field, exclude=exclude_sftags,
                               start=self.title_sftags_7xx, limit=2)


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
        pass

    def do_post_parse(self):
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
            self.parse_subfield(tag, val)
        self.do_post_parse()
        return self.compile_results()


class PersonalNameParser(SequentialMarcFieldParser):
    relator_sftags = 'e4'

    def __init__(self, field):
        super(PersonalNameParser, self).__init__(field)
        self.heading_parts = []
        self.relator_terms = OrderedDict()
        self.parsed_name = {}
        self.titles = []

    def do_relators(self, tag, val):
        for relator_term in self.utils.compile_relator_terms(tag, val):
            self.relator_terms[relator_term] = None

    def do_name(self, tag, val):
        self.parsed_name = p.person_name(val, self.field.indicators)

    def do_titles(self, tag, val):
        self.titles.extend([v for v in p.strip_ends(val).split(', ')])

    def parse_subfield(self, tag, val):
        if tag in self.relator_sftags:
            self.do_relators(tag, val)
        else:
            self.heading_parts.append(val)
            if tag == 'a':
                self.do_name(tag, val)
            elif tag == 'c':
                self.do_titles(tag, val)

    def compile_results(self):
        heading = p.normalize_punctuation(' '.join(self.heading_parts))
        return {
            'heading': p.strip_ends(heading) or None,
            'relations': self.relator_terms.keys() or None,
            'forename': self.parsed_name.get('forename', None),
            'surname': self.parsed_name.get('surname', None),
            'person_titles': self.titles or None,
            'type': 'person'
        }


class OrgEventNameParser(SequentialMarcFieldParser):
    event_info_sftags = 'cdgn'

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

    def do_relators(self, tag, val):
        for relator_term in self.utils.compile_relator_terms(tag, val):
            self.relator_terms[relator_term] = None

    def sf_is_first_subunit_of_jd_field(self, tag):
        ind1 = self.field.indicator1
        is_jurisdiction = tag == self.subunit_sftag and ind1 == '1'
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
        if tag in (self.relator_sftags):
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
                    'type': 'organization' if part_type == 'org' else 'event'
                })
                relators = None
        return ret_val


def extract_name_structs_from_heading_field(field, utils):
    split_name_title = utils.split7xx_name_title(field)
    if len(split_name_title):
        nfield = split_name_title[0]
        if nfield.tag.endswith('00'):
            return [PersonalNameParser(nfield).parse()]
        return OrgEventNameParser(nfield).parse()
    return []


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
        'contributor_info', 'general_3xx_info',
    ]
    prefix = 'get_'
    access_online_label = 'Online'
    access_physical_label = 'At the Library'
    item_rules = local_rulesets.ITEM_RULES
    bib_rules = local_rulesets.BIB_RULES
    hierarchical_name_separator = ' > '
    hierarchical_subject_separator = ' — '
    utils = MarcParseUtils()

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
        Return the CAT DATE (cataloged date) of the Bib record, in Solr
        date format, as the date the record was added to the catalog.
        """
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
        Return True if there's at least one item attached to this bib
        with an item status ONLINE (w).
        """
        for link in bib.bibrecorditemrecordlink_set.all():
            if link.item_record.item_status_id == 'w':
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
                    century = unicode(int(match.groups()[0]) + 1)
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
                        for l in r.bibrecorditemrecordlink_set.all()]
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
        resource_type = self.bib_rules['resource_type'].evaluate(r)
        rt_categories = {
            'unknown': [],
            'book': ['books'],
            'online_database': ['online_databases'],
            'music_score': ['music_scores'],
            'map': ['maps'],
            'video_film': ['video_film'],
            'audiobook': ['books', 'audio'],
            'music_recording': ['music_recordings', 'audio'],
            'print_graphic': ['images'],
            'software': ['software'],
            'video_game': ['games', 'software'],
            'eresource': ['software'],
            'ebook': ['books'],
            'educational_kit': ['educational_kits'],
            'archival_collection': ['archives_manuscripts'],
            'print_journal': ['journals_periodicals'],
            'object_artifact': ['objects_artifacts'],
            'tabletop_game': ['games', 'objects_artifacts'],
            'equipment': ['equipment', 'objects_artifacts'],
            'score_thesis': ['music_scores', 'theses_dissertations'],
            'manuscript': ['books', 'archives_manuscripts'],
            'ejournal': ['journals_periodicals'],
            'thesis_dissertation': ['theses_dissertations'],
        }

        return {
            'resource_type': resource_type,
            'resource_type_facet': rt_categories[resource_type]
        }

    def compile_person_info(self, name_struct):
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
                'facet_vals': [heading]}

    def compile_org_or_event_info(self, name_struct):
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
                'facet_vals': facet_vals}

    def parse_contributor_fields(self, fields):
        for f in fields:
            for name in extract_name_structs_from_heading_field(f, self.utils):
                if name['type'] == 'person':
                    info = self.compile_person_info(name)
                else:
                    info = self.compile_org_or_event_info(name)
                info['tag'] = f.tag
                info['name_type'] = name['type']
                yield info

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

        fields = marc_record.get_fields('100', '110', '111', '700', '710',
                                        '711', '800', '810', '811')
        for parsed in self.parse_contributor_fields(fields):
            this_is_event = parsed['name_type'] == 'event'
            this_is_1XX = parsed['tag'].startswith('1')
            this_is_7XX = parsed['tag'].startswith('7')
            this_is_8XX = parsed['tag'].startswith('8')
            if this_is_event:
                meetings_search.extend(parsed['search_vals'])
                meeting_facet.extend(parsed['facet_vals'])
                if not this_is_8XX:
                    meetings_json.append(parsed['json'])
            else:
                have_seen_author = bool(author_contributor_facet)
                if not have_seen_author:
                    if this_is_1XX or this_is_7XX:
                        author_sort = parsed['heading'].lower()
                    if this_is_1XX:
                        author_json = parsed['json']
                        author_search.extend(parsed['search_vals'])
                if have_seen_author or this_is_7XX or this_is_8XX:
                    contributors_search.extend(parsed['search_vals'])
                if have_seen_author or this_is_7XX:
                    contributors_json.append(parsed['json'])
                author_contributor_facet.extend(parsed['facet_vals'])
            responsibility_search.extend(parsed['relator_search_vals'])

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

    def get_general_3xx_info(self, r, marc_record):
        def make_subfield_joiner(join_val):
            def joiner(field, filter_, exclusionary):
                filtered = f.filter_subfields(filter_, exclusionary)
                joined = join_val.join([sfval for sftag, sfval in filtered])
                return p.normalize_punctuation(joined)
            return joiner

        semicolon_joiner = make_subfield_joiner('; ')
        space_joiner = make_subfield_joiner(' ')
        mapping = {
            'physical_medium': {
                'include': ('340',),
                'parse_func': semicolon_joiner
            },
            'geospatial_data': {
                'include': ('342', '343'),
                'parse_func': semicolon_joiner
            },
            'audio_characteristics': {
                'include': ('344',),
                'parse_func': semicolon_joiner
            },
            'projection_characteristics': {
                'include': ('345',),
                'parse_func': semicolon_joiner
            },
            'video_characteristics': {
                'include': ('346',),
                'parse_func': semicolon_joiner
            },
            'digital_file_characteristics': {
                'include': ('347',),
                'parse_func': semicolon_joiner
            },
            'graphic_representation': {
                'include': ('352',)
            },
            'physical_description': {
                'include': ('r', '370'),
                'exclude': IGNORED_MARC_FIELDS_BY_GROUP_TAG['r'] +
                           ('340', '342', '343', '344', '345', '346', '347',
                            '352', '382')
            }
        }
        ret_val = {}
        for fname, fdef in mapping.items():
            include = fdef.get('include', ())
            exclude = fdef.get('exclude', ())
            parse_func = fdef.get('parse_func', space_joiner)
            sf_filter = fdef.get('sf_filter', self.utils.control_sftags)
            sf_filter_excl = fdef.get('sf_filter_excl', True)
            ret_val[fname] = []
            for f in marc_record.filter_fields(include, exclude):
                field_val = parse_func(f, sf_filter, sf_filter_excl)
                if field_val:
                    ret_val[fname].append(field_val)
        return ret_val


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
                  'resource_type_facet', 'game_duration_facet',
                  'game_players_facet', 'game_age_facet') ),
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
        ( '973', ('full_title', 'responsibility', 'parallel_titles') ),
        ( '973', ('included_work_titles', 'related_work_titles') ),
        ( '973', ('included_work_titles_display_json',) ),
        ( '973', ('related_work_titles_display_json',) ),
        ( '973', ('series_titles_display_json',) ),
        ( '974', ('subjects',) ),
        ( '974', ('subject_topic_facet',) ),
        ( '974', ('subject_region_facet',) ),
        ( '974', ('subject_era_facet',) ),
        ( '974', ('item_genre_facet',) ),
        ( '974', ('subjects_display_jason',) ),
        ( '975', ('main_call_number', 'main_call_number_sort') ),
        ( '975', ('loc_call_numbers',) ),
        ( '975', ('dewey_call_numbers',) ),
        ( '975', ('sudoc_call_numbers',) ),
        ( '975', ('other_call_numbers',) ),
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
                  'graphic_representation') ),
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
        
        material_type = r.bibrecordproperty_set.all()[0].material.code
        metadata_field = pymarc.field.Field(
                tag='957',
                indicators=[' ', ' '],
                subfields=['d', material_type]
        )
        marc_record.add_field(metadata_field)

        # For each call number in the record, add a 909 field.
        i = 0
        for cn, ctype in r.get_call_numbers():
            subfield_data = []

            if i == 0:
                try:
                    srt = helpers.NormalizedCallNumber(cn, ctype).normalize()
                except helpers.CallNumberError:
                    srt = helpers.NormalizedCallNumber(cn, 'other').normalize()
                subfield_data = ['a', cn, 'b', srt]

            subfield_data.extend([self.cn_type_subfield_mapping[ctype], cn])

            cn_field = pymarc.field.Field(
                tag='959',
                indicators=[' ', ' '],
                subfields=subfield_data
            )
            marc_record.add_field(cn_field)
            i += 1

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
