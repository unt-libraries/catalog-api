"""
Sierra2Marc module for catalog-api `blacklight` app.
"""

from __future__ import unicode_literals
import pymarc
import logging
import re
import ujson
from datetime import datetime

from base import models, local_rulesets
from export.sierra2marc import S2MarcBatch, S2MarcError
from blacklight import parsers as p
from utils import helpers


def make_pmfield(tag, data=None, indicators=None, subfields=None):
    """
    Create a new pymarc Field object with the given parameters.

    `tag` is required. Creates a control field if `data` is not None,
    otherwise creates a variable-length field. `subfields` and
    `indicators` default to blank values.
    """
    kwargs = {'tag': tag}
    if data is None:
        kwargs['indicators'] = indicators or [' ', ' ']
        kwargs['subfields'] = subfields or []
    else:
        kwargs['data'] = data
    return pymarc.field.Field(**kwargs)


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


def group_subfields(pmfield, sftags, uniquetags=None, breaktags=None):
    """
    Put subfields from the given `pmfield` pymarc Field object into
    groupings based on the given `sftags` string. Returns a list of new
    pymarc Field objects, where each represents a grouping.
    """
    grouped, group = [], []
    uniquetags = uniquetags or ''
    breaktags = breaktags or ''
    for tag, value in pmfield:
        if tag in sftags:
            if group and tag in uniquetags and tag in [gi[0] for gi in group]:
                nfield = make_pmfield(pmfield.tag, subfields=group,
                                      indicators=pmfield.indicators)
                grouped.append(nfield)
                group = []
            group.extend([tag, value])
            if tag in breaktags:
                nfield = make_pmfield(pmfield.tag, subfields=group,
                                      indicators=pmfield.indicators)
                grouped.append(nfield)
                group = []
    if group:
        nfield = make_pmfield(pmfield.tag, subfields=group,
                              indicators=pmfield.indicators)
        grouped.append(nfield)
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
        'thumbnail_url', 'pub_info', 'access_info', 'resource_type_info'
    ]
    prefix = 'get_'
    access_online_label = 'Online'
    access_physical_label = 'At the Library'
    item_rules = local_rulesets.ITEM_RULES
    bib_rules = local_rulesets.BIB_RULES

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
        bundle = {}
        for fname in self.fields:
            method_name = '{}{}'.format(self.prefix, fname)
            result = getattr(self, method_name)(r, marc_record)
            for k, v in result.items():
                bundle[k] = v
        return bundle

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

    def get_urls_json(self, r, marc_record):
        """
        Return a JSON string representing URLs associated with the
        given record.
        """
        urls_data = []
        for f856 in marc_record.get_fields('856'):
            url = f856.get_subfields('u')
            if url:
                url = re.sub(r'^([^"]+).*$', r'\1', url[0])
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
                url = f962.get_subfields('u')
                if url and self._url_is_media_cover_image(url[0]):
                    return re.sub(r'^(https?):\/\/(www\.)?', 'https://',
                                  url[0])

        def _try_digital_library_image(f856s):
            for f856 in f856s:
                url = f856.get_subfields('u')
                if url and self._url_is_from_digital_library(url[0]):
                    url = url[0].split('?')[0].rstrip('/')
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
        for gr in group_subfields(f26x, 'abc', breaktags='c'):
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

        coded_dates = []
        f008 = (marc_record.get_fields('008') or [None])[0]
        if f008 is not None and len(f008.data) >= 15:
            data = f008.data
            entries = self._interpret_coded_date(data[6], data[7:11],
                                                 data[11:15])
            coded_dates.extend(entries)

        for field in marc_record.get_fields('046'):
            coded_group = group_subfields(field, 'abcde', uniquetags='abcde')
            if coded_group:
                dtype = (coded_group[0].get_subfields('a') or [''])[0]
                date1 = (coded_group[0].get_subfields('c') or [''])[0]
                date2 = (coded_group[0].get_subfields('e') or [''])[0]
                entries = self._interpret_coded_date(dtype, date1, date2)
                coded_dates.extend(entries)

            other_group = group_subfields(field, 'klop', uniquetags='klop')
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
            'publication_dates_search': sdates
        })
        return ret_val

    def get_access_info(self, r, marc_record):
        accessf, buildingf, shelff, collectionf = set(), set(), set(), set()

        # Note: For now we're just ignoring Bib Locations

        item_rules = self.item_rules
        for link in r.bibrecorditemrecordlink_set.all():
            item = link.item_record
            if item_rules['is_online'].evaluate(item):
                accessf.add(self.access_online_label)
            else:
                shelf = self.sierra_location_labels.get(item.location_id, None)
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
        ( '972', ('author_display_json',) ),
        ( '972', ('contributors_display_json',) ),
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
                  'publication_dates_search') ),
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
                        field = make_pmfield(tag, subfields=[sftag, v])
                        fields.append(field)
                    else:
                        subfields.extend([sftag, v])
            sftag = self._increment_sftag(sftag)
        if len(subfields):
            fields.append(make_pmfield(tag, subfields=subfields))
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
                field = make_pmfield(cf.get_tag(), data=data)
            except Exception as e:
                raise S2MarcError('Skipped. Couldn\'t create MARC field '
                    'for {}. ({})'.format(cf.get_tag(), e), str(r))
            mfields.append(field)
        return mfields

    def compile_varfields(self, r):
        mfields = []
        try:
            varfields = r.record_metadata.varfield_set\
                        .exclude(marc_tag=None)\
                        .exclude(marc_tag='')\
                        .order_by('marc_tag')
        except Exception as e:
            raise S2MarcError('Skipped. Couldn\'t retrieve varfields. '
                              '({})'.format(e), str(r))
        for vf in varfields:
            tag, ind1, ind2 = vf.marc_tag, vf.marc_ind1, vf.marc_ind2
            content, field = vf.field_content, None
            try:
                if tag in ['{:03}'.format(num) for num in range(1,10)]:
                    field = make_pmfield(tag, data=content)
                elif tag[0] != '9' or tag in ('962',):
                    # Ignore most existing 9XX fields from Sierra.
                    ind = [ind1, ind2]
                    sf = re.split(r'\|([a-z0-9])', content)[1:]
                    field = make_pmfield(tag, indicators=ind, subfields=sf)
            except Exception as e:
                raise S2MarcError('Skipped. Couldn\'t create MARC field '
                        'for {}. ({})'.format(vf.marc_tag, e), str(r))
            if field is not None:
                mfields.append(field)
        return mfields

    def compile_original_marc(self, r):
        marc_record = pymarc.record.Record(force_utf8=True)
        marc_record.add_ordered_field(*self.compile_control_fields(r))
        marc_record.add_ordered_field(*self.compile_varfields(r))
        return marc_record

    def _one_to_marc(self, r):
        marc_record = self.compile_original_marc(r)
        if not marc_record.fields:
            raise S2MarcError('Skipped. No MARC fields on Bib record.', str(r))

        bundle = self.custom_data_pipeline.do(r, marc_record)
        marc_record.add_ordered_field(*self.to_9xx_converter.do(bundle))

        marc_record.remove_fields('001')
        hacked_id = 'a{}'.format(bundle['id'])
        marc_record.add_grouped_field(make_pmfield('001', data=hacked_id))
        
        material_type = r.bibrecordproperty_set.all()[0].material.code
        metadata_field = pymarc.field.Field(
                tag='957',
                indicators=[' ', ' '],
                subfields=['d', material_type]
        )
        marc_record.add_ordered_field(metadata_field)

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
            marc_record.add_ordered_field(cn_field)
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
            marc_record.add_ordered_field(mf_field)

        if re.match(r'[0-9]', marc_record.as_marc()[5]):
            raise S2MarcError('Skipped. MARC record exceeds 99,999 bytes.', 
                              str(r))

        return marc_record
