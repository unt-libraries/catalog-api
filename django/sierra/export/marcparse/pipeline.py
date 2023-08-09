"""
Defines pipelines for converting MARC data for indexing in Solr.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import re
import ujson
from datetime import datetime
from collections import OrderedDict

from django.conf import settings
from six import text_type
from six.moves import range

from base import models, local_rulesets
from export import sierramarc as sm
from . import stringparsers as sp, fieldparsers as fp, renderers as rend


class MarcFieldGrouper(object):
    """
    Use this to parse fields on a SierraMarcRecord object into pre-
    defined groups based on (e.g.) MARC tag. This way, if you are doing
    a lot of operations on a MARC record, where you are issuing many
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


class BibDataPipeline(object):
    """
    This is a one-off class to hold functions/methods for creating the
    document to send to Solr for indexing.

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
    facet_key_separator = '!'
    ignore_fast_headings = True
    ignored_marc_fields_by_group_tag = {
        'n': ('539', '901', '959'),
        'r': ('306', '307', '335', '336', '337', '338', '341', '355', '357',
              '381', '387', '389'),
    }
    subject_sd_patterns = settings.MARCDATA.LCSH_SUBDIVISION_PATTERNS
    subject_sd_term_map = settings.MARCDATA.LCSH_SUBDIVISION_TERM_MAP

    def __init__(self):
        super(BibDataPipeline, self).__init__()
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
        self._sierra_location_labels = None
        self._sorted_items = None

    @property
    def sierra_location_labels(self):
        if self._sierra_location_labels is None:
            self._sierra_location_labels = {}
            pf = 'locationname_set'
            for loc in models.Location.objects.prefetch_related(pf).all():
                loc_name = loc.locationname_set.all()[0].name
                self._sierra_location_labels[loc.code] = loc_name
        return self._sierra_location_labels

    @property
    def sorted_items(self):
        """
        Get a list of items, sorted into a good display order.

        Generally we want items to sort in the intended display order,
        which is set in Sierra and reflected in the database via the
        bib/item link `items_display_order` integer. However, we've
        noticed sometimes there are null and duplicate values in the
        data. This causes two problems. 1) In Py3, null values now raise
        an error (can't use NoneType in comparisons). 2) Without a
        fallback sort value that is both reliable and unique, the sort
        order for duplicate values (including nulls) will be
        unpredictable.

        This is meant to address these issues. 1) For null values, it
        defaults to float('inf'), causing these items to display last.
        2) It adds the item record number as a secondary sort, so
        groups of items with the same display_order value sort by
        record number. All else being equal, record number order is a
        good default order, because it will put things in the order in
        which they were created. Note that this algorithm matches how
        Sierra handles these, both in the staff app and in the WebPAC.

        Also, we're sorting in Python here rather than using a simple
        `order_by` call, to avoid unnecessary database access.
        """
        def _items_sort_key(link):
            if link.items_display_order is None:
                display_order = float('inf')
            else:
                display_order = link.items_display_order
            return (display_order, link.item_record.record_metadata.record_num)

        if self.r and self._sorted_items is None:
            self._sorted_items = [l.item_record for l in sorted(
                self.r.bibrecorditemrecordlink_set.all(),
                key=_items_sort_key
            )]
        return self._sorted_items

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
            self._sorted_items = None

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
        return {'id': self.r.record_metadata.get_iii_recnum(False)}

    def get_suppressed(self):
        """
        Return 'true' if the record is suppressed, else 'false'.
        """
        return {'suppressed': 'true' if self.r.is_suppressed else 'false'}

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
        return {'date_added': rval}

    def get_item_info(self):
        """
        Return a dict containing item table information: `items_json`,
        `has_more_items`, and `more_items_json`.
        """
        r = self.r
        items = []
        for item in self.sorted_items:
            if not item.is_suppressed:
                callnum, vol = self.calculate_item_display_call_number(r, item)
                items.append({
                    'i': str(item.record_metadata.record_num),
                    'b': self.fetch_varfields(item, 'b', only_first=True),
                    'c': callnum,
                    'v': vol,
                    'n': self.fetch_varfields(item, 'p'),
                    'r': self.calculate_item_requestability(item),
                })

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
        if item_rules['is_requestable_through_finding_aid'].evaluate(item):
            urls_data = []
            for f856 in self.marc_fieldgroups.get('url', []):
                search_re = r'\/\/findingaids\.library\.unt\.edu'
                url = f856.get_subfields('u')
                if url and re.search(search_re, url[0]):
                    return 'finding_aid'
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
            return sp.strip_outer_parentheses(sp.strip_ends(statement), True)

        ind2_type_map = {'0': 'creation', '1': 'publication',
                         '2': 'distribution', '3': 'manufacture',
                         '4': 'copyright'}
        ptype = ind2_type_map.get(f26x.indicator2, 'publication')
        statements = []
        for gr in fp.group_subfields(f26x, 'abc', end='c'):
            if f26x.tag == '260':
                d = fp.pull_from_subfields(gr, 'c', sp.split_pdate_and_cdate)
                pdate, cdate = tuple(d[0:2]) if len(d) > 1 else ('', '')
                pdate = sp.normalize_punctuation(pdate)
                cdate = _clean_pub_statement(sp.normalize_cr_symbol(cdate))
                if cdate:
                    statements.append(('copyright', cdate))
            else:
                pdate = (fp.pull_from_subfields(gr, 'c') or [''])[0]
                if ptype == 'copyright':
                    pdate = sp.normalize_cr_symbol(pdate)
            parts = gr.get_subfields('a', 'b') + ([pdate] if pdate else [])
            statement = _clean_pub_statement(' '.join(parts))
            if statement:
                statements.append((ptype, statement))

        for group in fp.group_subfields(f26x, 'efg'):
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
        if (dnum1 and dnum2) and dnum1 > dnum2:
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
                    century = text_type(int(match.groups()[0] or 0) + 1)
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
        def _strip_unknown(data):
            pub_stripped = sp.normalize_punctuation(sp.strip_unknown_pub(data))
            if re.search(r'\w', pub_stripped):
                return [pub_stripped]
            return []

        pub_info, described_years, places, publishers = {}, [], [], []
        publication_date_notes = []
        for f26x in self.marc_fieldgroups.get('publication', []):
            years = fp.pull_from_subfields(
                f26x, 'cg',
                lambda v: sp.extract_years(v, self.year_upper_limit)
            )
            described_years.extend(years)
            for stype, stext in self._extract_pub_statements_from_26x(f26x):
                pub_info[stype] = pub_info.get(stype, [])
                pub_info[stype].append(stext)

            for place in fp.pull_from_subfields(f26x, 'ae', _strip_unknown):
                place = sp.strip_ends(place)
                places.append(sp.strip_outer_parentheses(place, True))

            for pub in fp.pull_from_subfields(f26x, 'bf', _strip_unknown):
                pub = sp.strip_ends(pub)
                publishers.append(sp.strip_outer_parentheses(pub, True))

        for f257 in self.marc_fieldgroups.get('production_country', []):
            places.extend([sp.strip_ends(v) for v in f257.get_subfields('a')])

        for f in self.marc_fieldgroups.get('geographic_info', []):
            place = ' '.join([sf for sf in f.get_subfields(*tuple('abcdfgh'))])
            places.append(place)

        for f362 in self.marc_fieldgroups.get('dates_of_publication', []):
            formatted_date = ' '.join(f362.get_subfields('a'))
            # NOTE: Extracting years from 362s (as below) was leading
            # to falsely extracting volume numbers as years, so we
            # probably should not do that. That's why the next two
            # lines are commented out.
            # years = sp.extract_years(formatted_date, self.year_upper_limit)
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
            coded_group = fp.group_subfields(field, 'abcde', unique='abcde')
            if coded_group:
                dtype = (coded_group[0].get_subfields('a') or [''])[0]
                date1 = (coded_group[0].get_subfields('c') or [''])[0]
                date2 = (coded_group[0].get_subfields('e') or [''])[0]
                entries = self.interpret_coded_date(dtype, date1, date2)
                coded_dates.extend(entries)

            other_group = fp.group_subfields(field, 'klop', unique='klop')
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
        for ystrs, expanded in self._expand_years(
                coded_dates, described_years):
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
        heading, rel = name_struct['heading'], name_struct['relations']
        json = {'r': rel} if rel else {}
        fval = heading or None
        json['p'] = [{'d': heading, 'v': fval}]
        permutator = rend.PersonalNamePermutator(name_struct)
        search_vals = permutator.get_search_permutations()
        base_name = (search_vals or [''])[-1]
        rel_search_vals = rend.make_relator_search_variations(base_name, rel)
        return {'heading': heading, 'json': json, 'search_vals': search_vals,
                'relator_search_vals': rel_search_vals,
                'facet_vals': [fval],
                'short_author': rend.shorten_name(name_struct)}

    def compile_org_or_event(self, name_struct):
        sep = self.hierarchical_name_separator
        heading, rel = '', name_struct['relations']
        json = {'r': rel} if rel else {}
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
        rel_search_vals = rend.make_relator_search_variations(base_name, rel)
        return {'heading': heading, 'json': json, 'search_vals': [],
                'relator_search_vals': rel_search_vals,
                'facet_vals': facet_vals,
                'short_author': rend.shorten_name(name_struct)}

    def select_best_name(self, names, org_event_default='combined'):
        if len(names) == 1:
            return names[0]

        for name in names:
            if name['parsed']['type'] == org_event_default:
                return name

    def do_facet_keys(self, struct, nf_chars=0):
        keysep = self.facet_key_separator
        try:
            struct.get('p')
        except AttributeError:
            new_facet_vals = []
            for fval in struct:
                new_fval = rend.format_key_facet_value(fval, nf_chars, keysep)
                new_facet_vals.append(new_fval)
            return new_facet_vals

        new_p = []
        for entry in struct.get('p', []):
            new_entry = entry
            if 'v' in new_entry:
                new_fval = rend.format_key_facet_value(entry['v'], nf_chars,
                                                       keysep)
                new_entry['v'] = new_fval 
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
            prep = 'by' if is_mform else '' if is_org_event else 'of'
            p1 = rend.format_title_short_author(p1, prep,
                                                author_info['short_name'])
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

        ms_str = rend.format_materials_specified(ms) if ms else ''
        dc_str = rend.format_display_constants(dc) if dc else ''
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
                    prep = 'by' if auth_info['ntype'] == 'person' else ''
                    d_part = rend.format_title_short_author(
                        part, prep, auth_info['short_name']
                    )
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
                    volume_sep, volume = rend.format_volume(volume)
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
            keysep = self.facet_key_separator
            json['a'] = rend.format_key_facet_value(author_info['full_name'],
                                                    keysep=keysep)

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
            kargs = {
                'json': json,
                'facet_vals': facet_vals,
                'heading': heading}
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
                new_jsonp['t'] = ' '.join(
                    tkw.split(' ')[0:20]).replace('"', '')
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
            s_rendered = self.render_authorized_title(
                title_struct, names, True)

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

        names = names or []
        entry = {'names': names, 'title': title}
        if not names:
            if f.tag.endswith('00'):
                names = [fp.PersonalNameParser(f).parse()]
            elif f.tag.endswith('10') or f.tag.endswith('11'):
                names = fp.OrgEventNameParser(f).parse() 
            name_info = [gather_name_info(f, n) for n in names]
            entry['names'] = [n for n in name_info if n is not None]

        if try_title and not title:
            title = fp.PreferredTitleParser(f).parse()
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
                                a_sort = rend.generate_facet_key(
                                    compiled['heading']
                                )
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
        truncator = sp.Truncator([r':\s'], True)
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
                    translation = rend.format_translation('; '.join(ptitles))
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

        sort_key = rend.generate_facet_key(sortable, nf_chars)
        return {
            'display': display,
            'non_truncated': non_trunc or None,
            'search': search,
            'primary_main_title': primary_main_title,
            'sort': sort_key if sortable else None
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
            keysep = self.facet_key_separator
            json['a'] = rend.format_key_facet_value(auth_info['full_name'],
                                                    keysep=keysep)
        facet_vals = []

        for i, res in enumerate(self.truncate_each_ttitle_part(ttitle)):
            part = res[0]
            this_is_first_part = i == 0
            this_is_last_part = i == len(ttitle['parts']) - 1

            if this_is_first_part:
                heading = part
                if needs_author_in_title and auth_info['short_name']:
                    prep = 'by' if auth_info['ntype'] == 'person' else ''
                    part = rend.format_title_short_author(
                        part, prep, auth_info['short_name']
                    )
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
            translation = rend.format_translation('; '.join(ptitles))
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
                if heading and sp.sor_matches_name_heading(sor, heading):
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
        work_title_keys = {'included': set(), 'related': set(),
                           'series': set()}
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
                title_key = rend.generate_facet_key(compiled['title_key'], nfc)
                wt_key = rend.generate_facet_key(compiled['work_title_key'],
                                                 nfc)
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
            parsed_245 = fp.TranscribedTitleParser(f).parse()
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
                    display_text = fp.TranscribedTitleParser.variant_types['1']
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
                    psor_translation = rend.format_translation(psor)
                    sor_display_values.append(
                        ' '.join([sor, psor_translation]))
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
                    t_key = rend.generate_facet_key(compiled['title_key'], nfc)
                    wt_key = rend.generate_facet_key(
                        compiled['work_title_key'], nfc
                    )
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
            parsed = fp.TranscribedTitleParser(f).parse()
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
                parsed = fp.TranscribedTitleParser(f).parse()
                if 'materials_specified' in parsed:
                    ms = parsed['materials_specified']
                    before = rend.format_materials_specified(ms)
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
                    wt_key = rend.generate_facet_key(st_heading)
                    work_title_keys['series'].add(wt_key)
                    if before:
                        new_json['b'] = before
                    if id_parts:
                        args = [None, id_parts]
                        kargs = {'json': new_json, 'heading': st_heading,
                                 'exp_is_part_of_heading': False}
                        result = self.render_title_expression_id(
                            *args, **kargs)
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
                    val_key = rend.generate_facet_key(val)
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
            render_stack, nums = [], OrderedDict()
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
                final_render = ' '.join(
                    ('({})'.format(ms_render), final_render))
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
            return fp.GenericDisplayFieldParser(
                f, ' ', sf_filter, label
            ).parse()

        def join_subfields_with_semicolons(f, sf_filter, label=None):
            return fp.GenericDisplayFieldParser(
                f, '; ', sf_filter, label
            ).parse()

        def get_subfields_as_list(f, sf_filter):
            return [v for sf, v in f.filter_subfields(**sf_filter)]

        def parse_performance_medium(field, sf_filter):
            parsed = fp.PerformanceMedParser(field).parse()
            return self.compile_performance_medium(parsed)

        def parse_502_dissertation_notes(field, sf_filter):
            if field.get_subfields('a'):
                return join_subfields_with_spaces(field, {'include': 'ago'})
            parsed_dn = fp.DissertationNotesFieldParser(field).parse()
            diss_note = '{}.'.format('. '.join(parsed_dn['note_parts']))
            return sp.normalize_punctuation(diss_note)

        def parse_511_performers(field, sf_filter):
            label = 'Cast' if field.indicator1 == '1' else None
            return join_subfields_with_spaces(field, sf_filter, label)

        def parse_520_summary_notes(field, sf_filter):
            class SummaryParser(fp.GenericDisplayFieldParser):
                def parse_subfield(self, tag, val):
                    if tag == 'c':
                        val = sp.strip_brackets(val, keep_inner=True,
                                                to_remove_re=r'',
                                                protect_re=r'')
                        val = sp.strip_ends(val, end='right')
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
                    val = '{} (source: {})'.format(val, sp.strip_ends(source))
            return val

        def parse_creator_demographics(field, sf_filter):
            labels = [sp.strip_ends(sf) for sf in field.get_subfields('i')]
            label = ', '.join(labels) if labels else None
            return join_subfields_with_semicolons(field, sf_filter, label)

        def parse_system_details(field, sf_filter):
            if field.tag == '753':
                return join_subfields_with_semicolons(field, sf_filter)

            class Field538Parser(fp.GenericDisplayFieldParser):
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
        marc_stub_rec = sm.SierraMarcRecord(force_utf8=True)
        for fgroup in fgroups:
            marc_stub_rec.add_field(*self.marc_fieldgroups.get(fgroup, []))

        record_parser = fp.MultiFieldMarcRecordParser(marc_stub_rec, {
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
            'exclude': set(self.ignored_marc_fields_by_group_tag['r']
                           + self.ignored_marc_fields_by_group_tag['n']
                           + ('377', '380', '592',))
        })
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

        for item in self.sorted_items:
            if not item.is_suppressed:
                call_numbers.extend(item.get_call_numbers() or [])

        for cn, cntype in call_numbers:
            searchable = rend.make_searchable_callnumber(cn)
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
            for p in fp.StandardControlNumberParser(f).parse():
                nums = [p[k] for k in ('normalized', 'number') if k in p]
                for num in nums:
                    search.append(num)
                    all_standard_numbers.append(num)
                display = rend.format_number_display_val(p)
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
        for p in fp.StandardControlNumberParser(f).parse():
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
                'display_val': rend.format_number_display_val(p)
            }

    def get_control_number_info(self):
        disp = {'lccn': [], 'oclc': [], 'others': []}
        num = {'lccn': [], 'oclc': [], 'all': []}
        search = []

        def _put_compiled_into_vars(
                c, display, numbers, search, prepend=False):
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
            is_oclc = re.match(r'(on|ocm|ocn)?\d+(/.+)?$', val)
            # OCLC numbers in 001 are treated as valid only if
            # there are not already valid OCLC numbers found in
            # 035s that we've already processed.
            is_valid = not is_oclc or len(num['oclc']) == 0
            org_code = 'OCoLC' if is_oclc else None
            if org_code is not None:
                val = '({}){}'.format(org_code, val)
            sftag = 'a' if is_valid else 'z'
            fake035 = sm.SierraMarcField('035', subfields=[sftag, val])
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
            def __init__(self, labeler, fksep, lower=None, upper=None):
                self.labeler = labeler
                self.lower = lower
                self.upper = upper
                self.facet_key_separator = fksep

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
                return self.facet_key_separator.join((sort_key, display_val))

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
                self.facet_key_separator,
                upper=UpperBound(100, True, template='{} and up')
            ),
            'd': RangeRenderer(
                MinutesLabeler(),
                self.facet_key_separator,
                lower=LowerBound(1, False),
                upper=UpperBound(500, False)
            ),
            'p': RangeRenderer(
                NumberLabeler('player', 'players'),
                self.facet_key_separator,
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
            w1 = rend.generate_facet_key(wordstr1, space_char=sc).split(sc)
            w2 = rend.generate_facet_key(wordstr2, space_char=sc).split(sc)
            for i in range(len(w2)):
                if w2[i] == w1[0] and w2[i:i + len(w1)] == w1:
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
        fksep = self.facet_key_separator
        if out_vals['heading']:
            out_vals['heading'] = sep.join([out_vals['heading'], t_heading])
        else:
            out_vals['heading'] = t_heading

        for ftype, fval in t_fvals:
            fval = rend.format_key_facet_value(fval, nf_chars, fksep)
            out_vals['facets'][ftype].add(fval)

        heading_fvals = []
        for fval in t_heading_fvals:
            if base_t_heading:
                fval = sep.join([base_t_heading, fval])
            heading_fvals.append(rend.format_key_facet_value(fval, nf_chars,
                                                             fksep))
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
                        val = rend.format_key_facet_value(val, nf_chars, fksep)
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
            main_term_type = 'era'

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
            main_term_parts = fp.pull_from_subfields(f, main_term_subfields)
            main_term = ' '.join(main_term_parts)
            main_term = sp.strip_ends(main_term)
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
                    for rel_term in sp.extract_relator_terms(val, tag=='4'):
                        rel_terms[rel_term] = None
                relators = list(rel_terms.keys())

        sd_parents = [main_term] if main_term else []
        for tag, val in f.filter_subfields(subdivision_subfields):
            tjson, hfvals, tfvals, search = {}, [], [], []
            sd_term = sp.strip_ends(val)
            sd_type = sd_types[tag]
            if sd_term:
                mapper = settings.MARCDATA.lcsh_sd_to_facet_values
                alts = mapper(sd_term, sd_parents, sd_type,
                              self.subject_sd_patterns,
                              self.subject_sd_term_map)
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
        f_sets = {'topic': set(), 'era': set(),
                  'region': set(), 'genre': set()}

        for f in self.marc_fieldgroups.get('subject_genre', []):
            compiled = self.parse_and_compile_subject_field(f)
            heading = compiled['heading']
            ftype_key = 'genres' if compiled['is_genre'] else 'subjects'
            if heading and heading not in heading_sets[ftype_key]:
                if compiled['json']:
                    json[ftype_key].append(compiled['json'])
                for facet_key, fvals in compiled['facets'].items():
                    if facet_key == 'heading':
                        vals = [
                            v for v in fvals if v not in hf_sets[ftype_key]]
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
                        vals = self.combine_phrases(
                            svals, search[sftype][slvl])
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
        facet, ln = [], []
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

        # Update 7/24/2023 -- I'm adding a check to make sure that any
        # languages from the title appear in the list of languages for
        # the MARC language codes. If we don't do this we end up with a
        # bunch of garbage in the Language facet.
        for lang in self.title_languages:
            if lang in settings.MARCDATA.LANGUAGES:
                all_languages[lang] = None
                categorized['a'][lang] = None

        for f in self.marc_fieldgroups.get('language_code', []):
            parsed = fp.LanguageParser(f).parse()
            for lang in parsed['languages']:
                all_languages[lang] = None
            for key, langs in parsed['categorized'].items():
                categorized[key] = categorized.get(key, OrderedDict())
                for lang in langs:
                    categorized[key][lang] = None

        facet = list(all_languages.keys())
        if needs_notes:
            categorized = {k: list(odict.keys())
                           for k, odict in categorized.items()}
            ln = fp.LanguageParser.generate_language_notes_display(categorized)

        return {
            'languages': facet or None,
            'language_notes': ln or None,
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
        return {'record_boost': int(pub_boost + q_boost)}

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
            wt_key = rend.generate_facet_key(rendered_lf['work_heading'])
            if marc_fgroup == 'linking_760_762':
                return not (wt_key in self.work_title_keys.get('series', []))
            elif marc_fgroup == 'linking_774':
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
                parsed = fp.LinkingFieldParser(f).parse()
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
                ms = rend.format_materials_specified(
                    parsed['materials_specified']
                )
                to_render = [rend.format_display_constants([ms]), display]
                display = ' '.join(to_render)
            for key in compiled.keys():
                if rendered[key]:
                    compiled[key].append(rendered[key])
            if 'parallel' in info:
                translated = self.render_edition_component(info['parallel'])
                formatted = rend.format_translation(translated['display'])
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
            compiled = self.compile_edition(fp.EditionParser(f).parse())
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
            val = fp.GenericDisplayFieldParser(f, '; ', sf_filter).parse()
            if val:
                library_has_display.append(val)
        return {
            'library_has_display': library_has_display or None
        }

