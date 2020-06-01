"""
`local_rulesets` module for catalog-api `base` app.

Implements base.ruleset classes in order to provide centralization for
Sierra business rules you can't easily get from the DB.

These rules will need to be kept up-to-date both as your business rules
change and as codes/rules in Sierra change, so the goal is to keep this
collected in one place, isolated as much as possible from other code.
"""

from __future__ import unicode_literals
import re

from base import ruleset as r


ITEM_RULES = {
    # `is_online` is True for online/electronic copies.
    'is_online': r.Ruleset([
        ('location_id', r.StrPatternMap({r'www$': True}))
    ], default=False),

    # `is_at_public_location` is True for items that exist at a
    # physical, publicly accessible location.
    'is_at_public_location': r.Ruleset([
        ('location_id', r.StrPatternMap({r'^(czm|f|jlf|k|r|s|w)': True}))
    ], default=False),

    # `building_location` returns the location code value of the
    # physical building an item belongs in, if any.
    'building_location': r.Ruleset([
        ('location_id', r.StrPatternMap({
            r'^w': 'w',
            r'^czm': 'czm',
            r'^f': 'frsco',
            r'^s': 's',
            r'^r': 'r',
            r'(^x|^jlf$)': 'x'
        }, exclude=('spec', 'xprsv', 'xts')))
    ]),

    # `in_collections` returns the names/labels of the collections an
    # item belongs to (returns a set)
    'in_collections': r.Ruleset([
        ('location_id', r.reverse_mapping({
            'Discovery Park Library': (
                'r', 'rfbks', 'rst', 'rzzpb', 'rzzrf', 'rzzrs'
            ),
            'General Collection': (
                'jlf', 'lwww', 's', 's1fdc', 's1ndc', 'smls', 'spe', 'szmp',
                'szzov', 'szzrf', 'szzrs', 'szzsd', 'w', 'w1fdc', 'w1grs',
                'w1ia', 'w1mdc', 'w1mls', 'w1ndc', 'w1upr', 'w3', 'w3big',
                'w3grn', 'w3mfa', 'w3per', 'wlbig', 'wlmic', 'wlper', 'x',
                'xmic'
            ),
            'Government Documents': (
                'gwww', 'sd', 'sd1dc', 'sdai', 'sdbi', 'sdcd', 'sdmc', 'sdmp',
                'sdnb', 'sdndc', 'sdov', 'sdtov', 'sdtx', 'sdus', 'sdvf',
                'sdzmr', 'sdzrf', 'sdzrs', 'sdzsd', 'xdmic', 'xdmp', 'xdoc'
            ),
            'Media Library': ('czm', 'czmrf', 'czmrs', 'czwww', 'xmed'),
            'Music Library': (
                'mwww', 'w433a', 'w4422', 'w4438', 'w4fil', 'w4m', 'w4mai',
                'w4mau', 'w4mav', 'w4mbg', 'w4mfb', 'w4mft', 'w4mla', 'w4moc',
                'w4mov', 'w4mr1', 'w4mr2', 'w4mr3', 'w4mrb', 'w4mrf', 'w4mrs',
                'w4mrx', 'w4mwf', 'xmus'
            ),
            'Special Collections': ('w4spe', 'w4srf', 'xspe'),
            'The Spark (Makerspace)': ('rmak', 'w1mak')
        }))
    ]),

    # `is_requestable_through_catalog` is True if an item is available
    # to be requested in the online catalog.
    'is_requestable_through_catalog': r.Ruleset([
        ('location_id', r.reverse_mapping({
            False: (
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
            )
        }, multi=False)),
        ('item_status_id', r.reverse_mapping({
            False: tuple('efijmnopwyz')
        }, multi=False)),
        ('itype_id', r.reverse_mapping({
            False: (7, 20, 29, 69, 74, 112)
        }, multi=False)),
        (('itype_id', 'location_id'), {(7,'xmus'): True})
    ], default=True),

    # `is_requestable_through_aeon` is True if an item is available to
    # be requested through Aeon, not the online catalog.
    'is_requestable_through_aeon': r.Ruleset([
        ('location_id', r.reverse_mapping({
            True: ('w4mr1', 'w4mr2', 'w4mr3', 'w4mrb', 'w4mrx', 'w4spe')
        }, multi=False))
    ], default=False),

    # `is_at_jlf` is True if an item is at the Joint Library Facility
    # (JLF) and must be requested through ILLiad.
    'is_at_jlf': r.Ruleset([('location_id', {'jlf': True})], default=False)
}


class ResourceTypeDeterminer(object):
    """
    This is a one-off, custom class to let us easily/simply implement
    logic (for use with r.Ruleset objects) that is a little more
    complex. This class should never be instantiated/used directly,
    only through the `BIB_RULES` rule constant.
    """

    bcode2_to_basetype = {
        '-': 'unknown',
        'a': 'book',
        'b': 'database',
        'c': 'score',
        'e': 'map',
        'g': 'video_film',
        'i': 'audio_spoken',
        'j': 'audio_music',
        'k': 'graphic',
        'm': 'software',
        'n': 'ebook',
        'o': 'kit',
        'p': 'archive',
        'q': 'journal',
        'r': 'object',
        's': 'score_thesis',
        't': 'manuscript',
        'y': 'ejournal',
        'z': 'book_thesis',
    }

    # Print types could be Print/Paper or Microform (or online) but
    # default to Print/Paper, absent other information.
    print_types = ('book', 'score', 'kit', 'journal', 'score_thesis',
                   'book_thesis', 'map', 'graphic')

    # Online types are those that are always online/electronic and
    # cannot have microform or paper formats.
    online_types = ('ebook', 'ejournal', 'database')

    # Possible newspaper types are those that could logically have
    # items that are actually newspapers, if 008/21 is 'n'.
    possible_newspaper_types = ('book', 'ebook', 'journal', 'ejournal',
                                'database')

    f007_map = {
        'ss l': (None, 'cassette', None),
        'sd f': (None, 'cd', None),
        'sd d': (None, 'record', '78_RPM'),
        'sd **mc': (None, 'record', '7-inch'),
        'sd **md': (None, 'record', '10-inch'),
        'sd **me': (None, 'record', '12-inch'),
        'sd **m*': (None, 'record', None),
        'sr n': (None, 'streaming', None),
        'cz': (None, 'book', 'Digital Device'),
        'sz': (None, 'book', 'Digital Device'),
        'co ': (None, 'computer', 'CD-ROM'),
        'cot': ('game', 'computer', 'CD-ROM'),
        'cor': ('game', 'console', None),
        'cbr': ('game', 'console', None),
        'coh': ('game', 'handheld', None),
        'cbh': ('game', 'handheld', None),
        'mr ': ('film', None, '16mm Film'),
        'vdv': ('video', 'dvd', None),
        'vdb': ('video', 'bluray', None),
        'go ': ('filmstrip', None, None),
        'vdg': ('video', 'laserdisc', None),
        'vf ': ('video', 'vhs', None),
        'gs ': ('slide', None, None),
        'vzs': ('video', 'streaming', None),
        'he ': (None, None, 'Microfiche'),
        'hd ': (None, None, 'Microfilm'),
        'hg a': (None, None, 'Microopaque'),
        'cr ': (None, None, 'Online'),
        'tb': (None, None, 'Large-Print/Paper'),
        'tc': (None, None, 'Braille'),
        't': (None, None, 'Print/Paper')
    }

    rtype_to_rtype_categories = {
        'book': ['books'],
        'database': ['online_databases'],
        'score': ['music_scores'],
        'map': ['maps'],
        'video': ['video_film'],
        'film': ['video_film'],
        'filmstrip': ['video_film'],
        'slide': ['video_film'],
        'audio': ['audio'],
        'music': ['music_recordings'],
        'spoken': ['spoken_recordings'],
        'graphic': ['images'],
        'software': ['software'],
        'game': ['games'],
        'computer': ['software'],
        'console': ['software'],
        'handheld': ['software'],
        'ebook': ['books'],
        'kit': ['educational_kits'],
        'archive': ['archives_manuscripts'],
        'journal': ['journals_periodicals'],
        'object': ['objects_artifacts'],
        'tabletop': ['objects_artifacts'],
        'equipment': ['equipment', 'objects_artifacts'],
        'manuscript': ['books', 'archives_manuscripts'],
        'ejournal': ['journals_periodicals'],
        'newspaper': ['journals_periodicals'],
        'thesis': ['theses_dissertations']
    }

    rtype_to_mtype_categories = {
        'cassette': ['Audio Cassette Tapes'],
        'cd': ['Audio CDs'],
        'record': ['Audio Records (LPs/EPs)'],
        'streaming': ['Digital Files'],
        'audio_computer': ['Digital Files'],
        'document': ['Digital Files'],
        'database': ['Digital Files'],
        'ebook': ['Digital Files'],
        'ejournal': ['Digital Files'],
        'software': ['Computer Programs (not Games)'],
        'game_computer': ['Computer Games'],
        'game_console': ['Console Games'],
        'game_handheld': ['Handheld Games'],
        'dvd': ['DVDs'],
        'bluray': ['Blu-ray Discs'],
        'filmstrip': ['Filmstrips'],
        'laserdisc': ['Laserdiscs'],
        'vhs': ['VHS Tapes'],
        'slide': ['Slides'],
        'game_tabletop': ['Tabletop Games'],
        'manuscript': ['Manuscripts'],
        'newspaper': ['Newspapers'],
        'archive': ['Archival Collections'],
    }

    format_to_mtype_categories = {
        '78 RPM': ['78 RPM Records'],
        '7-inch': ['7-inch Vinyl Records'],
        '10-inch': ['10-inch Vinyl Records'],
        '12-inch': ['12-inch Vinyl Records'],
        'Digital Device': ['Audiobook Devices', 'Digital Files'],
        'CD-ROM': ['CD-ROMs'],
        '16mm Film': ['16mm Film'],
        'Print/Paper': ['Printed Paper'],
        'Large-Print/Paper': ['Printed Paper', 'Large Print'],
        'Braille': ['Printed Paper', 'Braille'],
        'Microfiche': ['Microforms', 'Microfiche'],
        'Microfilm': ['Microforms', 'Microfilm'],
        'Microopaque': ['Microforms', 'Microopaques'],
        'Online': ['Digital Files'],
    }

    def __call__(self, obj):
        base_type, media, fmt = self.determine_basetype_media_format(obj)
        rtypes = base_type.split('_') + ([media] if media else [])
        categories = self.categorize_resource_type(rtypes, fmt)
        return {
            'resource_type': self.format_resource_type_value(rtypes, fmt),
            'resource_type_categories': categories['resource_type'],
            'media_type_categories': categories['media_type']
        }

    def format_resource_type_value(self, rtypes, fmt):
        rtype_str = '_'.join(rtypes)
        return '|'.join((rtype_str, fmt)) if fmt else rtype_str

    def determine_basetype_media_format(self, obj):
        base_type = self.bcode2_to_basetype.get(obj.bcode2, 'unknown')
        do = getattr(self, 'process_{}'.format(base_type), None)
        return do(obj, base_type) if do else (base_type, None, None)

    def categorize_resource_type(self, rtypes, fmt):
        rtype_cats, mtype_cats = [], []
        rtype_str = '_'.join(rtypes)
        mtype_cats = (self.rtype_to_mtype_categories.get(rtype_str, []) + 
                      self.format_to_mtype_categories.get(fmt, []))
        for rtype in rtypes:
            rtype_cats.extend(self.rtype_to_rtype_categories.get(rtype, []))
            mtype_cats.extend(self.rtype_to_mtype_categories.get(rtype, []))

        if rtype_str in ('game_console', 'game_handheld') and fmt:
            mtype_cats.append('{} Games'.format(fmt))

        return {
            'resource_type': list(set(rtype_cats)),
            'media_type': list(set(mtype_cats)),
        }

    def try_f007_map(self, tests, base_type):
        for test in tests:
            override, media, fmt = self.f007_map.get(test, (None, None, None))
            if override or media or fmt:
                return (override or base_type, media, fmt)
        return (base_type, None, None)

    def get_callnums_from_obj(self, obj):
        for cn, _ in obj.get_call_numbers():
            yield cn

        item_links = [l for l in obj.bibrecorditemrecordlink_set.all()]
        for link in sorted(item_links, key=lambda l: l.items_display_order):
            item = link.item_record
            if not item.is_suppressed:
                for cn, _ in item.get_call_numbers():
                    yield cn

    def get_control_field_from_obj(self, obj, tag):
        cf = obj.record_metadata.controlfield_set.filter(control_num=int(tag))
        return [field.get_data() for field in cf]

    def get_bib_location_codes_from_obj(self, obj):
        return (l.code for l in obj.locations.all())

    def get_printed_online_or_micro_format(self, obj, base_type):
        for f007 in self.get_control_field_from_obj(obj, '007'):
            tests = (f007[0:4], f007[0:3], f007[0])
            fmt = self.try_f007_map(tests, base_type)[2]
            if fmt:
                return fmt
        return 'Print/Paper' if base_type in self.print_types else None

    def get_text_or_print_format(self, obj, base_type):
        fmt = None
        if base_type not in self.online_types:
            fmt = self.get_printed_online_or_micro_format(obj, base_type)
            if fmt == 'Online':
                if base_type == 'book':
                    base_type, fmt = 'ebook', None
                elif base_type == 'journal':
                    base_type, fmt = 'ejournal', None

        if base_type in self.possible_newspaper_types:
            for f008 in self.get_control_field_from_obj(obj, '008'):
                if len(f008) >= 22 and f008[21] == 'n':
                    if base_type in self.online_types:
                        fmt = 'Online'
                    base_type = 'newspaper'
                    break
        return base_type, None, fmt

    def process_book(self, obj, base_type):
        return self.get_text_or_print_format(obj, base_type)

    def process_database(self, obj, base_type):
        return self.get_text_or_print_format(obj, base_type)

    def process_score(self, obj, base_type):
        return self.get_text_or_print_format(obj, base_type)

    def process_map(self, obj, base_type):
        return self.get_text_or_print_format(obj, base_type)

    def process_video_film(self, obj, base_type):
        for f007 in self.get_control_field_from_obj(obj, '007'):
            maybe = self.try_f007_map((f007[0:3],), base_type)
            print maybe
            if maybe != (base_type, None, None):
                base_type, media, fmt = maybe
                if base_type == 'video':
                    cns = (cn.lower() for cn in self.get_callnums_from_obj(obj))
                    if any((cn.startswith('mdvd') or cn.startswith('mvc') 
                            for cn in cns)):
                        base_type = 'video_music'
                return (base_type, media, fmt)
        return (base_type, None, None)

    def process_audio_spoken(self, obj, base_type):
        for f007 in self.get_control_field_from_obj(obj, '007'):
            tests = (f007[0:2], f007[0:4],
                     '{}**{}*'.format(f007[0:3], f007[5:6]))
            maybe = self.try_f007_map(tests, base_type)
            if maybe != (base_type, None, None):
                return maybe
        return ('audio_spoken_book', None, None)

    def process_audio_music(self, obj, base_type):
        for f007 in self.get_control_field_from_obj(obj, '007'):
            tests = (f007[0:4], '{}**{}'.format(f007[0:3], f007[5:7]))
            maybe = self.try_f007_map(tests, base_type)
            if maybe != (base_type, None, None):
                return maybe
        return (base_type, None, None)

    def process_graphic(self, obj, base_type):
        return self.get_text_or_print_format(obj, base_type)

    def process_software(self, obj, base_type):
        def determine_game_platform(obj):
            for cn in self.get_callnums_from_obj(obj):
                if cn.lower().startswith('game'):
                    cn_parts = cn.split(' ', 2)
                    if len(cn_parts) == 3:
                        return cn_parts[2].replace(' ', '_')

        def try_game_007_info(obj, base_type):
            media, fmt = None, None
            for f007 in self.get_control_field_from_obj(obj, '007'):
                base_type, media, fmt = self.try_f007_map((f007[0:3],),
                                                           base_type)
                if base_type == 'game':
                    if media != 'computer':
                        fmt = determine_game_platform(obj)
                    return (base_type, media, fmt)
            return (base_type, media, fmt)

        def try_specific_software_type(obj):
            for f008 in self.get_control_field_from_obj(obj, '008'):
                if f008 and len(f008) >= 27 and f008[26] in 'acdefgh':
                    if f008[26] in 'acdef':
                        return 'document'
                    if f008[26] == 'g':
                        return 'game'
                    if f008[26] == 'h':
                        return 'audio'

        base_type, media, fmt = try_game_007_info(obj, base_type)
        if base_type == 'game':
            return (base_type, media, fmt)

        base_type = try_specific_software_type(obj) or base_type
        return (base_type, 'computer', fmt)

    def process_ebook(self, obj, base_type):
        return self.get_text_or_print_format(obj, base_type)

    def process_kit(self, obj, base_type):
        return self.get_text_or_print_format(obj, base_type)

    def process_journal(self, obj, base_type):
        return self.get_text_or_print_format(obj, base_type)

    def process_object(self, obj, base_type):
        cns = (cn for cn in self.get_callnums_from_obj(obj))
        if any((cn.lower().startswith('boardgame') for cn in cns)):
            return ('game', 'tabletop', None)
        if 'w4spe' in self.get_bib_location_codes_from_obj(obj):
            return (base_type, None, None)
        return ('equipment', None, None)

    def process_score_thesis(self, obj, base_type):
        return self.get_text_or_print_format(obj, base_type)

    def process_manuscript(self, obj, base_type):
        return self.get_text_or_print_format(obj, base_type)

    def process_ejournal(self, obj, base_type):
        return self.get_text_or_print_format(obj, base_type)

    def process_book_thesis(self, obj, base_type):
        return self.get_text_or_print_format(obj, base_type)


BIB_RULES = {
    'resource_type': r.Ruleset([(ResourceTypeDeterminer(), None)]),
}


