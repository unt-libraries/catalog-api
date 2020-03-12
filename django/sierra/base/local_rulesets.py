"""
`local_rulesets` module for catalog-api `base` app.

Implements base.ruleset classes in order to provide centralization for
Sierra business rules you can't easily get from the DB.

These rules will need to be kept up-to-date both as your business rules
change and as codes/rules in Sierra change, so the goal is to keep this
collected in one place, isolated as much as possible from other code.
"""

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
            'The Factory (Makerspace)': ('rmak', 'w1mak')
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

    rtype_def = {
        'unknown': '-',
        'book': 'a',
        'online_database': 'b',
        'music_score': 'c',
        'map': 'e',
        'video_film': 'g',
        'audiobook': 'i',
        'music_recording': 'j',
        'print_graphic': 'k',
        'software': 'm',
        'video_game': None,
        'eresource': None,
        'ebook': 'n',
        'educational_kit': 'o',
        'archival_collection': 'p',
        'print_journal': 'q',
        'object_artifact': 'r',
        'tabletop_game': None,
        'equipment': None,
        'score_thesis': 's',
        'manuscript': 't',
        'ejournal': 'y',
        'thesis_dissertation': 'z',
    }
    from_bcode2 = {v: k for k, v in rtype_def.items() if v is not None}

    def __call__(self, obj):
        rtype = self.from_bcode2.get(obj.bcode2, 'unknown')
        do = getattr(self, 'process_{}_rtype'.format(rtype), lambda x: None)
        return do(obj) or rtype

    def process_software_rtype(self, obj):
        norm_cns = (cn.lower() for cn in self.get_callnums_from_obj(obj))
        if any([cn.startswith('game') for cn in norm_cns]):
            return 'video_game'
        f008 = self.get_008_from_obj(obj)
        if f008 and len(f008) >= 27:
            if f008[26] in ('a', 'c', 'd', 'e'):
                return 'eresource'
            if f008[26] == 'g':
                return 'video_game'
            if f008[26] == 'h':
                return 'music_recording'

    def process_object_artifact_rtype(self, obj):
        norm_cns = (cn.lower() for cn in self.get_callnums_from_obj(obj))
        if any([cn.startswith('boardgame') for cn in norm_cns]):
            return 'tabletop_game'
        if 'w4spe' in self.get_bib_location_codes_from_obj(obj):
            return 'object_artifact'
        return 'equipment'

    def get_callnums_from_obj(self, obj):
        cns = obj.record_metadata.varfield_set.filter(varfield_type_code='c')
        return (cn.display_field_content() for cn in cns)

    def get_008_from_obj(self, obj):
        f008 = obj.record_metadata.controlfield_set.filter(control_num=8)
        return f008[0].get_data() if len(f008) else None

    def get_bib_location_codes_from_obj(self, obj):
        return (l.code for l in obj.locations.all())


BIB_RULES = {
    'resource_type': r.Ruleset([
        (ResourceTypeDeterminer(),
         {k: k for k in ResourceTypeDeterminer.rtype_def.keys()})
    ])
}
