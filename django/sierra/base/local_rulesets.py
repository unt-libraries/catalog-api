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
