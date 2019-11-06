"""
Sierra2Marc module for catalog-api `blacklight` app.
"""

from __future__ import unicode_literals
import pymarc
import logging
import re
import ujson

from base import models
from export.sierra2marc import S2MarcBatch, S2MarcError
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
        'id', 'suppressed', 'item_info', 'urls_json',
    ]
    prefix = 'get_'

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

    def get_item_info(self, r, marc_record):
        """
        Return a dict containing item table information: `items_json`,
        `has_more_items`, and `more_items_json`.
        """
        items = []
        item_links = [l for l in r.bibrecorditemrecordlink_set.all()]
        for link in sorted(item_links, key=lambda l: l.items_display_order):
            item = link.item_record
            item_id, callnum, barcode, notes, rqbility = '', '', '', [], ''
            callnum = self.calculate_item_display_call_number(item, r)
            item_id = str(item.record_metadata.record_num)
            barcode = self.fetch_varfields(item, 'b', only_first=True)
            notes = self.fetch_varfields(item, 'p')
            rqbility = self.calculate_item_requestability(item)

            items.append({'i': item_id, 'c': callnum, 'b': barcode, 'n': notes,
                          'r': rqbility})

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

    def calculate_item_display_call_number(self, item, bib):
        """
        Sub-method used by `get_item_info` to return the display call
        number for the given `item`.
        """
        cn_string = ''
        item_cn_tuples = item.get_call_numbers()
        bib_cn_tuples = bib.get_call_numbers()

        if len(item_cn_tuples) > 0:
            cn_string = item_cn_tuples[0][0]
        elif len(bib_cn_tuples) > 0:
            cn_string = bib_cn_tuples[0][0]

        vol = self.fetch_varfields(item, 'v', only_first=True)
        if vol is not None:
            cn_string = '{} {}'.format(cn_string, vol)

        if item.copy_num > 1:
            cn_string = '{} c.{}'.format(cn_string, item.copy_num)
        return cn_string or None

    def calculate_item_requestability(self, item):
        """
        Sub-method used by `get_item_info` to return a requestability
        string based on established request rules.
        """
        nr_ruleset = [
            {'location_id': (
                'czmrf', 'czmrs', 'czwww', 'd', 'dcare', 'dfic', 'djuv',
                'dmed', 'dref', 'dresv', 'fip', 'frsco', 'gwww', 'hscfw',
                'ill', 'jlf', 'kmatt', 'kpacs', 'kpeb', 'law', 'lawcl', 'lawh',
                'lawrf', 'lawrs', 'lawtx', 'lawww', 'libr', 'lwww', 'mwww',
                'rzzrf', 'rzzrs', 'sdai', 'sdbi', 'sdmp', 'sdov', 'sdtov',
                'sdvf', 'sdzmr', 'sdzrf', 'sdzrs', 'sdzsd', 'spe', 'spec',
                'swr', 'szmp', 'szzov', 'szzrf', 'szzrs', 'szzsd', 'tamc',
                'test', 'twu', 'txsha', 'unt', 'w1grs', 'w1gwt', 'w1ia',
                'w1ind', 'w2awt', 'w2lan', 'w3dai', 'w3lab', 'w3mfa', 'w3per',
                'w433a', 'w4422', 'w4438', 'w4fil', 'w4mai', 'w4mav', 'w4mbg',
                'w4mfb', 'w4mla', 'w4moc', 'w4mr1', 'w4mr2', 'w4mr3', 'w4mrb',
                'w4mrf', 'w4mrs', 'w4mrx', 'w4mts', 'w4mwf', 'w4mwr', 'w4spe',
                'w4srf', 'wgrc', 'wlmic', 'wlper', 'xprsv', 'xspe', 'xts'
            )},
            {'item_status_id': tuple('efijmnopwyz')},
            {'itype_id': (20, 29, 69, 74, 112)},
            {'itype_id': (7,), '-location_id': ('xmus',) }
        ]
        aeon_ruleset = [
            {'location_id': ('w4mr1', 'w4mr2', 'w4mr3', 'w4mrb', 'w4mrx',
                             'w4spe')}
        ]

        def _item_matches_rule(rule, item):
            for field, valset in rule.items():
                if field[0] == '-':
                    if getattr(item, field[1:]) in valset:
                        return False
                elif getattr(item, field) not in valset:
                    return False
            return True

        if any([_item_matches_rule(rule, item) for rule in aeon_ruleset]):
            return 'aeon'
        if any([_item_matches_rule(rule, item) for rule in nr_ruleset]):
            return None
        return 'catalog'

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
                note = f856.get_subfields('z') or [None]
                label = f856.get_subfields('y') or [None]
                utype = 'fulltext' if f856.indicator2 in ('0', '1') else 'link'
                urls_data.append({'u': url, 'n': note[0], 'l': label[0],
                                  't': utype})

        for f962 in marc_record.get_fields('962'):
            url, thumb = f962.get_subfields('u'), f962.get_subfields('e')
            if url and not thumb:
                title = f962.get_subfields('t') or [None]
                urls_data.append({'u': url[0], 'n': title[0], 'l': None,
                                  't': 'media'})

        urls_json = []
        for ud in urls_data:
            ud['t'] = self.review_url_type(ud, len(urls_data), r)
            urls_json.append(ujson.dumps(ud))

        return { 'urls_json': urls_json }

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
        def _has_mediabooking_url(url):
            return 'mediabook.library.unt.edu' in url

        def _note_indicates_fulltext(note):
            eres_re = r'[Ee]lectronic|[Ee].?[Rr]esource'
            online_re = r'[Oo]nline.?'
            fulltext_re = r'[Ff]ull.?[Tt]ext.?'
            alternatives = r'|'.join([eres_re, online_re, fulltext_re])
            regex = r'(^|\s)({})(\s|$)'.format(alternatives)
            return bool(re.search(regex, note))

        def _has_online_item(bib):
            for link in bib.bibrecorditemrecordlink_set.all():
                if link.item_record.item_status_id == 'w':
                    return True
            return False

        if _has_mediabooking_url(url_data['u']):
            return 'booking'
        if url_data['n'] and _note_indicates_fulltext(url_data['n']):
            return 'fulltext'
        if num_urls == 1 and _has_online_item(bib):
            return 'fulltext'
        return url_data['t']


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
        ( '908', ('suppressed', 'date_added_sort', 'access_facet',
                  'location_facet', 'type_of_item_facet',
                  'game_duration_facet', 'game_players_facet',
                  'game_age_facet', 'recently_added_facet') ),
        ( '909', ('items_json',) ),
        ( '909', ('has_more_items',) ),
        ( '909', ('more_items_json',) ),
        ( '909', ('thumbnail_url', 'urls_json') ),
        ( '909', ('serial_holdings',) ),
        ( '912', ('author_display_json',) ),
        ( '912', ('contributors_display_json',) ),
        ( '913', ('full_title', 'responsibility', 'parallel_titles') ),
        ( '913', ('included_work_titles', 'related_work_titles') ),
        ( '913', ('included_work_titles_display_json',) ),
        ( '913', ('related_work_titles_display_json',) ),
        ( '913', ('series_titles_display_json',) ),
        ( '914', ('subjects',) ),
        ( '914', ('subject_topic_facet',) ),
        ( '914', ('subject_region_facet',) ),
        ( '914', ('subject_era_facet',) ),
        ( '914', ('item_genre_facet',) ),
        ( '914', ('subjects_display_jason',) ),
        ( '915', ('main_call_number', 'main_call_number_sort') ),
        ( '915', ('loc_call_numbers',) ),
        ( '915', ('dewey_call_numbers',) ),
        ( '915', ('sudoc_call_numbers',) ),
        ( '915', ('other_call_numbers',) ),
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
        If this is a Media Library item and has a 590 field with a
        Media Game Facet token string ("p1;p2t4;d30t59"), it returns
        the list of tokens. Returns None if no game facet string is
        found or tokens can't be extracted.
        """
        tokens = []
        if any([loc.code.startswith('czm') for loc in r.locations.all()]):
            for f in marc_record.get_fields('590'):
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
            content = vf.field_content
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
        
        material_type = r.bibrecordproperty_set.all()[0].material.code
        metadata_field = pymarc.field.Field(
                tag='957',
                indicators=[' ', ' '],
                subfields=['d', material_type]
        )
        marc_record.add_ordered_field(metadata_field)
        # Add bib locations to the 911a.
        for loc in r.locations.all():
            loc_field = pymarc.field.Field(
                tag='961',
                indicators=[' ', ' '],
                subfields=['a', loc.code]
            )
            marc_record.add_ordered_field(loc_field)

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
