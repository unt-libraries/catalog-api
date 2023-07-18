"""
`local_rulesets` module for catalog-api `base` app.

Implements base.ruleset classes in order to provide centralization for
Sierra business rules you can't easily get from the DB.

These rules will need to be kept up-to-date both as your business rules
change and as codes/rules in Sierra change, so the goal is to keep this
collected in one place, isolated as much as possible from other code.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

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
            r'^frsco': 'frsco',
            r'^fip': 'fip',
            r'^fl': 'fl',
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
                'w3grn', 'w3per', 'wlbig', 'wllok', 'wlmic', 'x', 'xmic'
            ),
            'Government Documents': (
                'gwww', 'sd', 'sd1dc', 'sdai', 'sdbi', 'sdcd', 'sdmc', 'sdmp',
                'sdnb', 'sdndc', 'sdov', 'sdtov', 'sdtx', 'sdus', 'sdvf',
                'sdzmr', 'sdzrf', 'sdzrs', 'sdzsd', 'xdmic', 'xdmp', 'xdoc'
            ),
            'Frisco Collection': (
                'fip', 'fl', 'flrs', 'frsco', 'flmak', 'flind', 'flix', 'flgrn'
            ),
            'Media Library': ('czm', 'czmrf', 'czmrs', 'czwww', 'xmed'),
            'Music Library': (
                'mwww', 'w433a', 'w4422', 'w4438', 'w4fil', 'w4lok', 'w4m',
                'w4mai', 'w4mau', 'w4mav', 'w4mbg', 'w4mfb', 'w4mft', 'w4mla',
                'w4moc', 'w4mov', 'w4mr1', 'w4mr2', 'w4mr3', 'w4mrb', 'w4mrf',
                'w4mrs', 'w4mrx', 'w4mwf', 'xmau', 'xmus'
            ),
            'Special Collections': ('pwww', 'w4spc', 'w4spe', 'w4srf', 'xspc',
                                    'xspe'),
            'The Spark (Makerspace)': ('rmak', 'rmkme', 'w1mak', 'w1ind',
                                       'flmak', 'flind', 'flix')
        }))
    ]),

    # `is_requestable_through_catalog` is True if an item is available
    # to be requested in the online catalog.
    'is_requestable_through_catalog': r.Ruleset([
        ('location_id', r.reverse_mapping({
            False: (
                # THESE ARE ****NOT**** REQUESTABLE!
                'czmrf', 'czmrs', 'czwww', 'd', 'dcare', 'dfic', 'djuv',
                'dmed', 'dref', 'dresv', 'fip', 'flind', 'flix', 'flrs',
                'frsco', 'gwww', 'hscfw', 'ill', 'jlf', 'kmats', 'kmatt',
                'kpacs', 'kpeb', 'law', 'lawcl', 'lawh', 'lawrf', 'lawrs',
                'lawtx', 'lawww', 'libr', 'lwww', 'mwww', 'pwww', 'rmkme',
                'rzzrf', 'rzzrs', 'sdai', 'sdbi', 'sdmp', 'sdov', 'sdtov',
                'sdvf', 'sdzmr', 'sdzrf', 'sdzrs', 'sdzsd', 'spe', 'spec',
                'swr', 'szmp', 'szzov', 'szzrf', 'szzrs', 'szzsd', 'tamc',
                'test', 'twu', 'txsha', 'unt', 'w1grs', 'w1gwt', 'w1ia',
                'w1ind', 'w1idl', 'w1ix', 'w2awt', 'w2lan', 'w3dai', 'w3lab',
                'w3per', 'w433a', 'w4422', 'w4438', 'w4fil', 'w4lok', 'w4mai',
                'w4mav', 'w4mbg', 'w4mfb', 'w4mla', 'w4moc', 'w4mr1', 'w4mr2', 
                'w4mr3', 'w4mrb', 'w4mrf', 'w4mrs', 'w4mrx', 'w4mts', 'w4mwf',
                'w4mwr', 'w4spc', 'w4spe', 'w4srf', 'wgrc', 'wllok', 'wlmic',
                'xprsv', 'xspc', 'xspe', 'xts',
            )
        }, multi=False)),
        ('item_status_id', r.reverse_mapping({
            False: tuple('efijmnopwyz')
        }, multi=False)),
        ('itype_id', r.reverse_mapping({
            False: (20, 29, 69, 74, 112)
        }, multi=False)),
        (('itype_id', 'location_id'), r.reverse_mapping({
            False: (
                # Spark / Makerspace (*mak) has several ITYPEs that are
                # not holdable so should be non-requestable.
                (39, 'flmak'), (39, 'rmak'), (39, 'w1mak'),
                (85, 'flmak'), (85, 'rmak'), (85, 'w1mak'),
                (93, 'flmak'), (93, 'rmak'), (93, 'w1mak'),
                (104, 'flmak'), (104, 'rmak'), (104, 'w1mak'),
                (105, 'flmak'), (105, 'rmak'), (105, 'w1mak'),
                (114, 'flmak'), (114, 'rmak'), (114, 'w1mak')
            )
        }, multi=False)),
    ], default=True),

    # `is_requestable_through_aeon` is True if an item is available to
    # be requested through Aeon, not the online catalog.
    'is_requestable_through_aeon': r.Ruleset([
        ('location_id', r.reverse_mapping({
            True: ('w4spe', 'xspe', 'w4mr1', 'w4mr2', 'w4mr3', 'w4mrb',
                   'w4mrx', 'w4mbg', 'w4mwf')
        }, multi=False))
    ], default=False),

    # `is_requestable_via_finding_aid` is True if an item is available
    # to be requested through a finding aid (ultimately through Aeon
    # via the finding aid).
    # NOTE that you may want to reality-check this value against the
    # 856 links to make sure that a finding aid exists for an item
    # before assigning a final value. That is outside the scope of this
    # rule.
    'is_requestable_through_finding_aid': r.Ruleset([
        ('location_id', r.reverse_mapping({
            True: ('w4spc', 'xspc')
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

    paper_types = ('book', 'score', 'map', 'graphic', 'kit', 'journal',
                   'score_thesis', 'manuscript', 'book_thesis', 'newspaper')
    online_types = ('database', 'ebook', 'ejournal')
    newspaper_types = ('book', 'ebook', 'journal', 'ejournal', 'database')
    basetypes_ignore_007 = online_types + ('object',)

    f007_to_format = [(0, 3, {
        'cbh': 'game_handheld_cartridge',
        'cbr': 'game_console_cartridge',
        'co ': 'cdrom',
        'coh': 'game_handheld_cdrom',
        'cor': 'game_console_cdrom',
        'cot': 'game_computer_cdrom',
        'cr ': 'online',
        'cz ': 'digital',
        'mr ': '16mmfilm',
        'vdv': 'dvd',
        'vdb': 'bluray',
        'go ': 'filmstrip',
        'vdg': 'laserdisc',
        'vf ': 'vhs',
        'gs ': 'slide',
        'vzs': 'streaming',
        'he ': 'microfiche',
        'hd ': 'microfilm',
        'hg ': 'microopaque',
        'sz ': 'digital',
        'ta ': 'print',
        'tb ': 'largeprint',
        'tc ': 'braille',
        'td ': 'paper',
        'sd ': [(5, 7, {
            'mc': 'record_7inch',
            'md': 'record_10inch',
            'me': 'record_12inch',
        }), (5, 6, {
            'm': 'record',
        }), (3, 4, {
            'd': 'record_78rpm',
            'f': 'cd',
        })],
        'sr ': [(3, 4, {
            'n': 'streaming',
        })],
        'ss ': [(3, 4, {
            'l': 'cassette',
        })]
    })]

    f008_newspaper = [(21, 22, {
        'n': 'newspaper'
    })]

    f008_to_format = [(23, 24, {
        'a': 'microfilm',
        'b': 'microfiche',
        'c': 'microopaque',
        'd': 'largeprint',
        'f': 'braille',
        'o': 'online',
        'q': 'digital',
        'r': 'print',
        's': 'digital',
    })]

    f008_software_type = [(26, 27, {
        'b': 'software',
        'g': 'game',
        'h': 'audio',
        'i': 'software',
    })]

    formatcombos_to_formats = {
        ('digital', 'online'): ('streaming',),
        ('digital', 'streaming'): ('streaming',),
        ('online', 'streaming'): ('streaming',),
        ('digital', 'online', 'streaming'): ('streaming',),
    }

    basetypeformat_to_rtypesformats = {
        ('software', 'game_handheld_cartridge'): (('game', 'handheld'), ()),
        ('software', 'game_console_cartridge'): (('game', 'console'), ()),
        ('software', 'game_handheld_cdrom'): (('game', 'handheld'), ()),
        ('software', 'game_console_cdrom'): (('game', 'console'), ()),
        ('software', 'game_computer_cdrom'): (('game', 'computer'), ('cdrom',)),
        ('software', 'cdrom'): (('software', 'computer'), ('cdrom',)),
        ('video_film', '16mmfilm'): (('film',), ('16mmfilm',)),
        ('video_film', 'dvd'): (('video', 'dvd',), ()),
        ('video_film', 'bluray'): (('video', 'bluray',), ()),
        ('video_film', 'filmstrip'): (('filmstrip',), ()),
        ('video_film', 'laserdisc'): (('video', 'laserdisc'), ()),
        ('video_film', 'vhs'): (('video', 'vhs'), ()),
        ('video_film', 'slide'): (('slide',), ()),
        ('video_film', 'streaming'): (('video', 'streaming'), ()),
        ('audio_spoken', 'digital'): (('audio', 'spoken', 'book'),
                                      ('digital_device',)),
        ('audio_spoken', 'streaming'): (('audio', 'spoken', 'streaming'), ()),
        ('audio_spoken', 'record_7inch'): (('audio', 'spoken', 'record'),
                                           ('record_7inch',)),
        ('audio_spoken', 'record_10inch'): (('audio', 'spoken', 'record'),
                                            ('record_10inch',)),
        ('audio_spoken', 'record_12inch'): (('audio', 'spoken', 'record'),
                                            ('record_12inch',)),
        ('audio_spoken', 'record_78rpm'): (('audio', 'spoken', 'record'),
                                           ('record_78rpm',)),
        ('audio_spoken', 'record'): (('audio', 'spoken', 'record'), ()),
        ('audio_spoken', 'cd'): (('audio', 'spoken', 'cd'), ()),
        ('audio_spoken', 'cassette'): (('audio', 'spoken', 'cassette'), ()),
        ('audio_music', 'streaming'): (('audio', 'music', 'streaming',), ()),
        ('audio_music', 'record_7inch'): (('audio', 'music', 'record'),
                                          ('record_7inch',)),
        ('audio_music', 'record_10inch'): (('audio', 'music', 'record'),
                                           ('record_10inch',)),
        ('audio_music', 'record_12inch'): (('audio', 'music', 'record'),
                                           ('record_12inch',)),
        ('audio_music', 'record_78rpm'): (('audio', 'music', 'record'),
                                          ('record_78rpm',)),
        ('audio_music', 'record'): (('audio', 'music', 'record'), ()),
        ('audio_music', 'cd'): (('audio', 'music', 'cd'), ()),
        ('audio_music', 'cassette'): (('audio', 'music', 'cassette'), ()),
        ('book', 'online'): (('ebook',), ()),
        ('book', 'streaming'): (('ebook',), ()),
        ('book', 'digital'): (('ebook',), ()),
        ('journal', 'online'): (('ejournal',), ()),
        ('journal', 'streaming'): (('ejournal',), ()),
        ('journal', 'digital'): (('ejournal',), ()),
        ('manuscript', 'print'): (('manuscript',), ('paper',)),
    }
    
    # Update 12/14/2022 -- Added some game platform variations, mainly
    # for specifying unique capitalization. For new-style Media game
    # call numbers, the platform is in all caps, so from now on we will
    # normalize both kinds (old and new) to lower case and then make a
    # display-friendly value with value.title(). However, several game
    # platforms include abbreviations or other unusual capitalization.
    # This is mainly a way to map those to standard display values.
    game_platform_variations = {
        '3ds': '3DS',
        'ds': 'DS',
        'gamecube': 'GameCube',
        'game cube': 'GameCube',
        'nes': 'NES',
        'new 3ds': 'New 3DS',
        'ps': 'PS',
        'ps1': 'PS',
        'playstation': 'PS',
        'ps2': 'PS2',
        'ps3': 'PS3',
        'ps3 move': 'PS3 Move',
        'ps4': 'PS4',
        'ps4 deluxe': 'PS4 Deluxe',
        'ps4 vr': 'PS4 VR',
        'ps5': 'PS5',
        'psp': 'PSP',
        'psvita': 'PSVita',
        'ps vita': 'PSVita',
        'snes': 'SNES',
    }

    format_labels = {
        'game_handheld_cartridge': 'Game Cartridge',
        'game_console_cartridge': 'Game Cartridge',
        'cdrom': 'CD-ROM',
        'game_handheld_cdrom': 'Game CD-ROM',
        'game_console_cdrom': 'Game CD-ROM',
        'game_computer_cdrom': 'Game CD-ROM',
        'tarot': 'Tarot Cards',
        'online': 'Online',
        'digital': 'Digital File',
        'digital_device': 'Digital Device',
        '16mmfilm': '16mm Film',
        'dvd': 'DVD',
        'bluray': 'Blu-ray',
        'filmstrip': 'Filmstrip',
        'laserdisc': 'Laserdisc',
        'vhs': 'VHS',
        'slide': 'Slide',
        'streaming': 'Online',
        'microfiche': 'Microfiche',
        'microfilm': 'Microfilm',
        'microopaque': 'Microopaque',
        'print': 'Print/Paper',
        'paper': 'Paper',
        'largeprint': 'Large-Print/Paper',
        'braille': 'Braille',
        'record': 'Record',
        'record_78rpm': '78 RPM',
        'record_7inch': '7-inch Vinyl',
        'record_10inch': '10-inch Vinyl',
        'record_12inch': '12-inch Vinyl',
        'cd': 'CD',
        'cassette': 'Cassette',
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
        'newspaper': ['journals_periodicals', 'newspapers'],
        'thesis': ['theses_dissertations']
    }

    any_to_mtype_categories = {
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
        'game_computer_cdrom': ['Computer Games'],
        'game_console_cdrom': ['Console Games'],
        'game_handheld_cdrom': ['Handheld Games'],
        'game_console_cartridge': ['Console Games'],
        'game_handheld_cartridge': ['Handheld Games'],
        'dvd': ['DVDs'],
        'bluray': ['Blu-ray Discs'],
        'filmstrip': ['Filmstrips'],
        'laserdisc': ['Laserdiscs'],
        'vhs': ['VHS Tapes'],
        'slide': ['Slides'],
        'game_tabletop': ['Tabletop Games'],
        'tarot': ['Tarot Cards'],
        'manuscript': ['Manuscripts'],
        'archive': ['Archival Collections'],
        'record_78rpm': ['78 RPM Records'],
        'record_7inch': ['7-inch Vinyl Records'],
        'record_10inch': ['10-inch Vinyl Records'],
        'record_12inch': ['12-inch Vinyl Records'],
        'digital_device': ['Audiobook Devices', 'Digital Files'],
        'cdrom': ['CD-ROMs'],
        '16mmfilm': ['16mm Film'],
        'print': ['Paper'],
        'paper': ['Paper'],
        'digital': ['Digital Files'],
        'largeprint': ['Paper', 'Large Print'],
        'braille': ['Paper', 'Braille'],
        'microfiche': ['Microforms', 'Microfiche'],
        'microfilm': ['Microforms', 'Microfilm'],
        'microopaque': ['Microforms', 'Microopaques'],
    }

    def __call__(self, obj):
        rtypes, fmts = self.determine_rtypes_and_formats(obj)
        categories = self.categorize_resource_type(rtypes, fmts)
        return {
            'resource_type': self.format_resource_type_value(rtypes, fmts),
            'resource_type_categories': categories['resource_type'],
            'media_type_categories': categories['media_type']
        }

    def determine_rtypes_and_formats(self, obj):
        rtypes, fmts = [], set()
        base_type = self.bcode2_to_basetype.get(obj.bcode2, 'unknown')
        if base_type in set(self.newspaper_types) | set(self.paper_types):
            f008s = self.get_control_field_from_obj(obj, '008')
            if base_type in self.newspaper_types:
                if self.is_newspaper(obj, f008s):
                    if base_type in self.online_types:
                        fmts = set(['online'])
                    base_type = 'newspaper'
            if base_type in self.paper_types:
                fmts |= set(self.get_ff_formats(obj, '008', f008s))
        if base_type not in self.basetypes_ignore_007:
            f007s = self.get_control_field_from_obj(obj, '007')
            fmts |= set(self.get_ff_formats(obj, '007', f007s))

        if base_type in self.paper_types and not fmts:
            fmts.add('print')

        if len(fmts) > 1:
            combo_key = tuple(sorted(list(fmts)))
            fmts = set(self.formatcombos_to_formats.get(combo_key, fmts))
        if len(fmts) <= 1:
            key = (base_type, list(fmts)[0] if fmts else None)
            res = self.basetypeformat_to_rtypesformats.get(key, (rtypes, fmts))
            rtypes, fmts = (list(res[0]), set(res[1]))

        do = getattr(self, 'process_{}'.format(base_type), None)
        if do:
            rtypes, fmts = do(obj, base_type, rtypes, fmts)

        rtypes = rtypes or base_type.split('_')
        return rtypes, list(fmts)

    def format_resource_type_value(self, rtypes, fmts):
        rtypestr = '_'.join(rtypes)
        fmtstr = ', '.join([self.format_labels.get(f, f)
                           for f in sorted(fmts)])
        return '!'.join((rtypestr, fmtstr)) if fmtstr else rtypestr

    def categorize_resource_type(self, rtypes, fmts):
        rtype_cats, mtype_cats = [], []
        rtype_str = '_'.join(rtypes)
        mtype_cats.extend(self.any_to_mtype_categories.get(rtype_str, []))

        for fmt in fmts:
            mtype_cats.extend(self.any_to_mtype_categories.get(fmt, []))
        for rtype in rtypes:
            rtype_cats.extend(self.rtype_to_rtype_categories.get(rtype, []))
            mtype_cats.extend(self.any_to_mtype_categories.get(rtype, []))

        gtypes = ('game_console', 'game_handheld')
        if rtype_str in gtypes or 'software' in rtypes or 'computer' in rtypes:
            mtype_cats.extend(['{} Games'.format(fmt) for fmt in fmts
                               if fmt not in self.format_labels])

        if 'online' in fmts and 'software' not in rtypes:
            mtype_cats.append('Digital Files')

        return {
            'resource_type': list(set(rtype_cats)),
            'media_type': list(set(mtype_cats)),
        }

    def get_callnums_from_obj(self, obj):
        for cn, _ in obj.get_call_numbers():
            yield cn

        def _item_sort_key(link):
            if link.items_display_order is None:
                display_order = float('inf')
            else:
                display_order = link.items_display_order
            return (display_order, link.item_record.record_metadata.record_num)

        item_links = [l for l in obj.bibrecorditemrecordlink_set.all()]
        for link in sorted(item_links, key=_item_sort_key):
            item = link.item_record
            if not item.is_suppressed:
                for cn, _ in item.get_call_numbers():
                    yield cn

    def get_control_field_from_obj(self, obj, tag):
        cfs = obj.record_metadata.controlfield_set.all()
        return [cf.get_data() for cf in cfs if cf.control_num == int(tag)]

    def get_bib_location_codes_from_obj(self, obj):
        return (l.code for l in obj.locations.all())

    def _map_fixedfield(self, ffmap, ffstr):
        for i, j, mapdef in ffmap:
            if len(ffstr) >= j:
                entry = mapdef.get(ffstr[i:j], None)
                if entry:
                    if not isinstance(entry, list):
                        return entry
                    return self._map_fixedfield(entry, ffstr)

    def get_ff_formats(self, obj, fftag, ffstrs):
        ffmap = self.f007_to_format if fftag == '007' else self.f008_to_format
        entries = []
        for ffstr in ffstrs:
            entry = self._map_fixedfield(ffmap, ffstr)
            if entry:
                entries.append(entry)
        return entries

    def is_newspaper(self, obj, f008s):
        for ffstr in f008s:
            newspaper = self._map_fixedfield(self.f008_newspaper, ffstr)
            if newspaper:
                return True
        return False

    def process_video_film(self, obj, base_type, rtypes, fmts):
        if len(fmts) <= 1:
            cns = (cn.lower() for cn in self.get_callnums_from_obj(obj))
            for cn in cns:
                if cn.startswith('mdvd'):
                    return ['video', 'music', 'dvd'], fmts
                if cn.startswith('mvc'):
                    return ['video', 'music', 'vhs'], fmts
        return rtypes, fmts

    def process_software(self, obj, base_type, rtypes, fmts):
        def determine_game_platforms(obj):
            # Update 12/14/2022 -- Media Library is updating the call
            # number pattern for their games collection. During the
            # transition, both old- and new-style call numbers may
            # appear, so we just need to handle both patterns (see
            # below). I'm also adding the ability to normalize platform
            # names, since there are variations (PSVita vs PsVita), and
            # de-duplicate.

            platforms = []
            norm_platforms = set()
            for cn in self.get_callnums_from_obj(obj):
                norm_cn = cn.lower()
                norm_platform = ''

                # Old-style call numbers: "Game 12345 Switch".
                if norm_cn.startswith('game'):
                    try:
                        norm_platform = norm_cn.split(' ', 2)[2]
                    except IndexError:
                        pass

                # New-style call numbers: "ABC 2016 GAME SWITCH"
                elif ' game ' in norm_cn:
                    norm_platform = norm_cn.split(' game ', 1)[1]

                if norm_platform and norm_platform not in norm_platforms:
                    platform = self.game_platform_variations.get(
                        norm_platform,
                        norm_platform.title()
                    )
                    norm_platforms.add(norm_platform)
                    platforms.append(platform)
            return platforms

        def determine_software_type(obj):
            for f008 in self.get_control_field_from_obj(obj, '008'):
                swtype = self._map_fixedfield(self.f008_software_type, f008)
                if swtype is not None:
                    return swtype
            return 'document'

        hh_fmts = set(['game_handheld_cartridge', 'game_handheld_cdrom'])
        hh_rtypes = ['game', 'handheld']
        con_fmts = set(['game_console_cartridge', 'game_console_cdrom'])
        con_rtypes = ['game', 'console']
        is_console_game = rtypes == con_rtypes or len(con_fmts - fmts) < 2
        is_handheld_game = rtypes == hh_rtypes or len(hh_fmts - fmts) < 2
        if is_console_game or is_handheld_game:
            platforms = set(determine_game_platforms(obj))
            if platforms:
                if is_console_game:
                    fmts -= con_fmts
                if is_handheld_game:
                    fmts -= hh_fmts
            fmts |= platforms
            if is_console_game and is_handheld_game:
                rtypes = con_rtypes
        elif 'game' not in rtypes and 'game_computer_cdrom' not in fmts:
            rtypes = [determine_software_type(obj), 'computer']
        return rtypes, fmts

    def process_object(self, obj, base_type, rtypes, fmts):
        # Update 12/14/2022 -- Media Library is updating the call number
        # pattern for their games collections. Old-style call numbers
        # start with 'Boardgame' and new ones end with ' GAME'. There
        # are also now Tarot card sets, which are a sub-category of
        # tabletop games.
        is_tabletop = False
        is_tarot = False
        for cn in self.get_callnums_from_obj(obj):
            norm_cn = cn.lower()
            if norm_cn.startswith('boardgame') or norm_cn.endswith(' game'):
                is_tabletop = True
            if norm_cn.endswith(' tarot'):
                is_tabletop = True
                is_tarot = True

        if is_tabletop:
            if is_tarot:
                fmts.add('tarot')
            return ['game', 'tabletop'], fmts
        if 'w4spe' in self.get_bib_location_codes_from_obj(obj):
            return ['object'], fmts
        return ['equipment'], fmts


BIB_RULES = {
    'resource_type': r.Ruleset([(ResourceTypeDeterminer(), None)]),
}
