"""
Tests the base.ruleset classes and functions.
"""

from __future__ import unicode_literals
import pytest

from base import local_rulesets as lr

# FIXTURES / TEST DATA

@pytest.fixture
def item_rules():
    return lr.ITEM_RULES


@pytest.fixture
def bib_rules():
    return lr.BIB_RULES


@pytest.fixture
def resource_type_determiner():
    return lr.ResourceTypeDeterminer()


# TESTS

@pytest.mark.parametrize('loc_code, expected', [
    ('czwww', True),
    ('gwww', True),
    ('lawww', True),
    ('lwww', True),
    ('mwww', True),
    ('xwww', True),
    ('w1', False),    
    ('xdoc', False),
    ('w4m', False),
    ('czm', False),
    ('law', False),
])
def test_itemrules_isonline(loc_code, expected, item_rules, mocker):
    """
    Our local ITEM_RULES['is_online'] rule should return the expected
    boolean for items with the given location code, indicating whether
    or not that item is an online copy.
    """
    item = mocker.Mock(location_id=loc_code)
    assert item_rules['is_online'].evaluate(item) == expected


@pytest.mark.parametrize('loc_code, expected', [
    ('czm', True),
    ('czmrf', True),
    ('frsco', True),
    ('jlf', True),
    ('kmatt', True),
    ('r', True),
    ('rzzrs', True),
    ('rmak', True),
    ('s', True),
    ('sdoc', True),
    ('w', True),
    ('w1inf', True),
    ('w4m', True),
    ('unt', False),
    ('ill', False),
    ('czwww', False),
    ('gwww', False),
    ('lawww', False),
    ('lwww', False),
    ('mwww', False),
    ('xwww', False),
    ('x', False),
    ('xmus', False),
    ('xdoc', False),
])
def test_itemrules_isatpubliclocation(loc_code, expected, item_rules, mocker):
    """
    Our local ITEM_RULES['is_at_public_location'] rule should return
    the expected boolean for items with the given location code,
    indicating whether or not that item exists at a (physical) location
    that can be accessed by the public.
    """
    item = mocker.Mock(location_id=loc_code)
    assert item_rules['is_at_public_location'].evaluate(item) == expected


@pytest.mark.parametrize('loc_code, expected', [
    ('czm', 'czm'),
    ('czmrf', 'czm'),
    ('czmrs', 'czm'),
    ('frsco', 'frsco'),
    ('r', 'r'),
    ('rzzrs', 'r'),
    ('s', 's'),
    ('sdoc', 's'),
    ('sdzrs', 's'),
    ('w', 'w'),
    ('w1inf', 'w'),
    ('w3', 'w'),
    ('w4m', 'w'),
    ('x', 'x'),
    ('xmus', 'x'),
    ('xdoc', 'x'),
    ('jlf', 'x'),
    ('spec', None),
    ('xprsv', None),
    ('xts', None),
    ('unt', None),
    ('ill', None),
    ('czwww', None),
    ('gwww', None),
    ('lawww', None),
    ('lwww', None),
    ('mwww', None),
])
def test_itemrules_buildinglocation(loc_code, expected, item_rules, mocker):
    """
    Our local ITEM_RULES['building_location'] rule should return
    the location code value of the physical building an item belongs
    in, if any.
    """
    item = mocker.Mock(location_id=loc_code)
    assert item_rules['building_location'].evaluate(item) == expected


@pytest.mark.parametrize('loc_code, expected', [
    ('czm', ['Media Library']),
    ('czmrf', ['Media Library']),
    ('czwww', ['Media Library']),
    ('jlf', ['General Collection']),
    ('lwww', ['General Collection']),
    ('w', ['General Collection']),
    ('w3', ['General Collection']),
    ('x', ['General Collection']),
    ('xmic', ['General Collection']),
    ('gwww', ['Government Documents']),
    ('sd', ['Government Documents']),
    ('sdtx', ['Government Documents']),
    ('xdoc', ['Government Documents']),
    ('xdmic', ['Government Documents']),
    ('mwww', ['Music Library']),
    ('w4m', ['Music Library']),
    ('xmus', ['Music Library']),
    ('w4spe', ['Special Collections']),
    ('xspe', ['Special Collections']),
    ('rmak', ['The Spark (Makerspace)']),
    ('w1mak', ['The Spark (Makerspace)']),
    ('r', ['Discovery Park Library']),
    ('rfbks', ['Discovery Park Library']),
    ('rst', ['Discovery Park Library']),
])
def test_itemrules_incollections(loc_code, expected, item_rules, mocker):
    """
    Our local ITEM_RULES['in_collections'] rule should return a set of
    names/labels of the collections an item belongs to.
    """
    item = mocker.Mock(location_id=loc_code)
    assert item_rules['in_collections'].evaluate(item) == set(expected)


@pytest.mark.parametrize('loc_code, itype_id, item_status_id, expected', [
    # Note: tests that are commented out represent "normal" policies;
    # currently due to COVID-19 a lot of requesting is restricted. We
    # will update these further as policies change.
    # ('czm', 1, '-', True),
    ('czm', 1, '-', False),
    ('czmrf', 1, '-', False),
    ('x', 7, '-', False),
    ('x', 20, '-', False),
    ('x', 29, '-', False),
    ('x', 69, '-', False),
    ('x', 74, '-', False),
    ('x', 112, '-', False),
    ('xmus', 1, '-', True),
    ('xmus', 7, '-', True),
    ('x', 1, 'e', False),
    ('x', 1, 'f', False),
    ('x', 1, 'i', False),
    ('x', 1, 'j', False),
    ('x', 1, 'm', False),
    ('x', 1, 'n', False),
    ('x', 1, 'o', False),
    ('x', 1, 'p', False),
    ('x', 1, 'w', False),
    ('x', 1, 'y', False),
    ('x', 1, 'z', False),
    ('jlf', 1, '-', False),
    ('w4mr1', 1, '-', False),
    ('w4mr2', 1, '-', False),
    ('w4mr3', 1, '-', False),
    ('w4mrb', 1, '-', False),
    ('w4mrx', 1, '-', False),
    ('w4spe', 1, '-', False),
])
def test_itemrules_isrequestablethroughcatalog(loc_code, itype_id,
                                               item_status_id, expected,
                                               item_rules, mocker):
    """
    Our local ITEM_RULES['is_requestable_through_catalog'] rule should
    return True if an item is available to be requested via the online
    catalog; False if not.
    """
    item = mocker.Mock(location_id=loc_code, itype_id=itype_id,
                       item_status_id=item_status_id)
    result = item_rules['is_requestable_through_catalog'].evaluate(item)
    assert result == expected


@pytest.mark.parametrize('loc_code, expected', [
    # Note: tests that are commented out represent "normal" policies;
    # currently due to COVID-19 a lot of requesting is restricted. We
    # will update these further as policies change.
    ('czm', False),
    ('jlf', False),
    # ('w4mr1', True),
    ('w4mr1', False),
    # ('w4mr2', True),
    ('w4mr2', False),
    # ('w4mr3', True),
    ('w4mr3', False),
    # ('w4mrb', True),
    ('w4mrb', False),
    # ('w4mrx', True),
    ('w4mrx', False),
    ('w4spe', True),
])
def test_itemrules_isrequestablethroughaeon(loc_code, expected, item_rules,
                                            mocker):
    """
    Our local ITEM_RULES['is_requestable_through_aeon'] rule should
    return True if an item is available to be requested via Aeon; False
    if not.
    """
    item = mocker.Mock(location_id=loc_code)
    result = item_rules['is_requestable_through_aeon'].evaluate(item)
    assert result == expected


@pytest.mark.parametrize('loc_code, expected', [
    ('czm', False),
    ('jlf', True),
    ('w4mr1', False),
    ('w4mr2', False),
    ('w4mr3', False),
    ('w4mrb', False),
    ('w4mrx', False),
    ('w4spe', False),
])
def test_itemrules_isatjlf(loc_code, expected, item_rules, mocker):
    """
    Our local ITEM_RULES['is_at_jlf'] rule should return True if an
    item is available to be requested via ILLiad from the JLF.
    """
    item = mocker.Mock(location_id=loc_code)
    assert item_rules['is_at_jlf'].evaluate(item) == expected


@pytest.mark.parametrize('bcode2, cns, f007s, f008_21, f008_23, f008_26, '
                         'bib_locations, expected', [
    ('-', None, None, None, None, None, None, 'unknown'),
    ('a', None, None, None, None, None, None, 'book!Print/Paper'),
    ('a', None, None, None, 'a', None, None, 'book!Microfilm'),
    ('a', None, None, None, 'b', None, None, 'book!Microfiche'),
    ('a', None, None, None, 'c', None, None, 'book!Microopaque'),
    ('a', None, None, None, 'd', None, None, 'book!Large-Print/Paper'),
    ('a', None, None, None, 'f', None, None, 'book!Braille'),
    ('a', None, None, None, 'o', None, None, 'ebook'),
    ('a', None, None, None, 'q', None, None, 'ebook'),
    ('a', None, None, None, 'r', None, None, 'book!Print/Paper'),
    ('a', None, None, None, 's', None, None, 'ebook'),
    ('a', None, None, None, ' ', None, None, 'book!Print/Paper'),
    ('a', None, ['ta '], None, None, None, None, 'book!Print/Paper'),
    ('a', None, ['ta '], None, 'r', None, None, 'book!Print/Paper'),
    ('a', None, ['ta '], None, 'a', None, None, 'book!Microfilm, Print/Paper'),
    ('a', None, ['tb '], None, None, None, None, 'book!Large-Print/Paper'),
    ('a', None, ['tc '], None, None, None, None, 'book!Braille'),
    ('a', None, ['tu '], None, None, None, None, 'book!Print/Paper'),
    ('a', None, ['cr '], None, None, None, None, 'ebook'),
    ('a', None, ['ta '], None, 'o', None, None, 'book!Online, Print/Paper'),
    ('a', None, ['cr '], 'n', None, None, None, 'newspaper!Online'),
    ('a', None, None, 'n', None, None, None, 'newspaper!Print/Paper'),
    ('a', None, ['hd ||||||||||'], 'n', None, None, None,
     'newspaper!Microfilm'),
    ('a', None, ['he ||||||||||'], None, None, None, None, 'book!Microfiche'),
    ('a', None, ['hd ||||||||||'], None, None, None, None, 'book!Microfilm'),
    ('a', None, ['hg a|||||bncn'], None, None, None, None, 'book!Microopaque'),
    ('b', None, None, None, None, None, None, 'database'),
    ('b', None, ['cr '], None, None, None, None, 'database'),
    ('b', None, None, 'n', None, None, None, 'newspaper!Online'),
    ('b', None, ['cr '], 'n', None, None, None, 'newspaper!Online'),
    ('c', None, None, None, None, None, None, 'score!Print/Paper'),
    ('c', None, ['cr '], None, None, None, None, 'score!Online'),
    ('c', None, ['he ||||||||||'], None, None, None, None, 'score!Microfiche'),
    ('e', None, None, None, None, None, None, 'map!Print/Paper'),
    ('e', None, ['cr '], None, None, None, None, 'map!Online'),
    ('g', [], None, None, None, None, None, 'video_film'),
    ('g', ['MP 1234'], ['mr |aaad|||||||||||||||'], None, None, None, None,
     'film!16mm Film'),
    ('g', ['DVD 1234'], ['vdv|vaiz|'], None, None, None, None, 'video_dvd'),
    ('g', ['DVD 1234 Blu-ray'], ['vdb|saiz|'], None, None, None, None,
     'video_bluray'),
    ('g', ['F.S. 123'], ['go |j||f'], None, None, None, None, 'filmstrip'),
    ('g', ['LD 1234'], ['vdg|gaiz|'], None, None, None, None,
     'video_laserdisc'),
    ('g', ['MV 1234'], ['vf |baho|'], None, None, None, None, 'video_vhs'),
    ('g', ['MDVD 1234'], ['vdv|vaiz|'], None, None, None, None,
     'video_music_dvd'),
    ('g', ['MVC 1234'], ['vf |baho|'], None, None, None, None,
     'video_music_vhs'),
    ('g', ['Slide 1234'], ['gs |j||||'], None, None, None, None, 'slide'),
    ('g', ['Online Video'], ['vzs|zazu|'], None, None, None, None,
     'video_streaming'),
    ('g', ['LD 1234', 'DVD 1234'], ['vdg|gaiz|', 'vdv|vaiz|'], None, None, None,
     None, 'video_film!DVD, Laserdisc'),
    ('g', ['LD 1234', 'MDVD 12'], ['vdg|gaiz|', 'vdv|vaiz|'], None, None, None,
     None, 'video_film!DVD, Laserdisc'),
    ('g', ['Online Video'], ['cr ', 'vzs|zazu|'], None, None, None,
     None, 'video_streaming'),
    ('g', ['Online Video'], ['cr ', 'vf |baho|'], None, None, None,
     None, 'video_film!Online, VHS'),
    ('g', [], ['co cga'], None, None, None, None, 'video_film!CD-ROM'),
    ('i', None, None, None, None, None, None, 'audio_spoken'),
    ('i', ['Ph-disc 12'], ['sd ||m|nn||l||'], None, None, None, None,
     'audio_spoken_record'),
    ('i', ['ACD 12'], ['sd f|ngnn|mne|'], None, None, None, None,
     'audio_spoken_cd'),
    ('i', ['Ph-tape 12'], ['ss l|njlc|pn||'], None, None, None, None,
     'audio_spoken_cassette'),
    ('i', ['ADB 12'], ['cz nza||||||||'], None, None, None, None,
     'audio_spoken_book!Digital Device'),
    ('i', ['ADB 12'], ['sz nza||||||||'], None, None, None, None,
     'audio_spoken_book!Digital Device'),
    ('i', ['ADB 12'], ['sz nza||||||||', 'cz nza||||||||'], None, None, None,
     None, 'audio_spoken_book!Digital Device'),
    ('i', ['Online Audio'], ['sz nza||||||||', 'cr nza||||||||'], None, None,
     None, None, 'audio_spoken_streaming'),
    ('i', ['Online Audio'], ['sr n|nnnnnnne|'], None, None, None, None,
     'audio_spoken_streaming'),
    ('j', None, None, None, None, None, None, 'audio_music'),
    ('j', ['Music 12 Cassette'], ['ss l|njlcmpn||'], None, None, None, None,
     'audio_music_cassette'),
    ('j', ['LPCD 12'], ['sd f|ngnnmmne|'], None, None, None, None,
     'audio_music_cd'),
    ('j', ['LPW 12'], ['sd dms|nnmsl||'], None, None, None, None,
     'audio_music_record!78 RPM'),
    ('j', ['LPX 12'], ['sd ||mcnnmpl||'], None, None, None, None,
     'audio_music_record!7-inch Vinyl'),
    ('j', ['LPY 12'], ['sd ||mdnnmpl||'], None, None, None, None,
     'audio_music_record!10-inch Vinyl'),
    ('j', ['LPZ 12'], ['sd ||menn||l||'], None, None, None, None,
     'audio_music_record!12-inch Vinyl'),
    ('j', ['Online Audio'], ['sr n|nnnnnnne|'], None, None, None, None,
     'audio_music_streaming'),
    ('k', None, None, None, None, None, None, 'graphic!Print/Paper'),
    ('k', None, ['cr '], None, None, None, None, 'graphic!Online'),
    ('m', None, None, None, None, None, None, 'document_computer'),
    ('m', ['CD-ROM 1234'], None, None, None, 'b', ['czm'], 'software_computer'),
    ('m', ['CD-ROM 1234'], ['co |g|||||||||'], None, None, 'b', ['czm'],
     'software_computer!CD-ROM'),
    ('m', ['Game 1234'], None, None, None, 'g', ['czm'], 'game_computer'),
    ('m', ['Game 1234'], ['co |g|||||||||'], None, None, 'g', ['czm'],
     'game_computer!CD-ROM'),
    ('m', None, None, None, None, ' ', ['czm'], 'document_computer'),
    ('m', None, ['co |g|||||||||'], None, None, ' ', ['czm'],
     'document_computer!CD-ROM'),
    ('m', ['CD-ROM 1234'], None, None, None, 'g', ['sd'], 'game_computer'),
    ('m', ['CD-ROM 1234'], ['co |g|||||||||'], None, None, 'g', ['sd'],
     'game_computer!CD-ROM'),
    ('m', ['CD-ROM 1234'], None, None, None, 'd', ['sd'], 'document_computer'),
    ('m', ['CD-ROM 1234'], ['co |g|||||||||'], None, None, 'd', ['sd'],
     'document_computer!CD-ROM'),
    ('m', ['LPCD-ROM 1234'], None, None, None, 'h', ['sd'], 'audio_computer'),
    ('m', ['LPCD-ROM 1234'], ['co |g|||||||||'], None, None, 'h', ['sd'],
     'audio_computer!CD-ROM'),
    ('m', ['CD-ROM 1234'], ['cot|ga||||||||'], None, None, None, None,
     'game_computer!CD-ROM'),
    ('m', ['Game 12'], ['cor|ga||||||||'], None, None, None, None,
     'game_console'),
    ('m', ['MT 1234'], ['cor|ga||||||||'], None, None, None, None,
     'game_console'),
    ('m', ['Game 12'], ['cbr|ga||||||||'], None, None, None, None,
     'game_console'),
    ('m', ['Game 12 PS4'], ['cor|ga||||||||'], None, None, None, None,
     'game_console!PS4'),
    ('m', ['Game 12 Xbox 360'], ['cor|ga||||||||'], None, None, None, None,
     'game_console!Xbox 360'),
    ('m', ['Game 12'], ['coh|ga||||||||'], None, None, None, None,
     'game_handheld'),
    ('m', ['MT 1234'], ['cbh|ga||||||||'], None, None, None, None,
     'game_handheld'),
    ('m', ['Game 12'], ['cbh|ga||||||||'], None, None, None, None,
     'game_handheld'),
    ('m', ['Game 12 PSP'], ['cbh|ga||||||||'], None, None, None, None,
     'game_handheld!PSP'),
    ('m', ['Game 12 PSP', 'Game 12 Gameboy'], ['cbh|ga||||||||'], None, None,
     None, None, 'game_handheld!Gameboy, PSP'),
    ('m', ['Game 12 PSP', 'Game 12 Xbox'], ['cbh|ga||||||||', 'cor|ga||||||||'],
     None, None, None, None, 'game_console!PSP, Xbox'),
    ('m', ['Game 12 PSP'], ['cbh|ga||||||||', 'cor|ga||||||||'], None, None,
     None, None, 'game_console!PSP'),
    ('m', ['Game 12'], ['cbh|ga||||||||', 'cor|ga||||||||'], None, None,
     None, None, 'game_console!Game CD-ROM, Game Cartridge'),
    ('m', ['Game 12 PSP', 'Game 12 Xbox'], ['cor|ga||||||||'], None, None, None,
     None, 'game_console!PSP, Xbox'),
    ('n', None, None, None, None, None, None, 'ebook'),
    ('n', None, ['hd ||||||||||'], None, None, None, None, 'ebook'),
    ('n', None, ['cr '], None, None, None, None, 'ebook'),
    ('n', None, ['cr '], 'n', None, None, None, 'newspaper!Online'),
    ('n', None, None, 'n', None, None, None, 'newspaper!Online'),
    ('n', None, ['hd ||||||||||'], 'n', None, None, None,
     'newspaper!Microfilm, Online'),
    ('o', None, None, None, None, None, None, 'kit!Print/Paper'),
    ('p', None, None, None, None, None, None, 'archive'),
    ('q', None, None, None, None, None, None, 'journal!Print/Paper'),
    ('q', None, ['ta '], None, None, None, None, 'journal!Print/Paper'),
    ('q', None, ['tb '], None, None, None, None, 'journal!Large-Print/Paper'),
    ('q', None, ['tc '], None, None, None, None, 'journal!Braille'),
    ('q', None, ['tu '], None, None, None, None, 'journal!Print/Paper'),
    ('q', None, ['cr '], None, None, None, None, 'ejournal'),
    ('q', None, ['cr '], 'n', None, None, None, 'newspaper!Online'),
    ('q', None, None, 'n', None, None, None, 'newspaper!Print/Paper'),
    ('q', None, ['hd ||||||||||'], 'n', None, None, None,
     'newspaper!Microfilm'),
    ('q', None, ['he ||||||||||'], None, None, None, None,
     'journal!Microfiche'),
    ('q', None, ['cr '], None, None, None, None, 'ejournal'),
    ('q', None, ['cr '], 'n', None, None, None, 'newspaper!Online'),
    ('q', None, None, 'n', None, None, None, 'newspaper!Print/Paper'),
    ('q', None, ['hd ||||||||||'], 'n', None, None, None,
     'newspaper!Microfilm'),
    ('r', [], None, None, ' ', None, [''], 'equipment'),
    ('r', ['MT 1234 S32'], None, None, None, ' ', ['w4spe'], 'object'),
    ('r', ['Boardgame 1234'], None, None, None, ' ', ['czm'], 'game_tabletop'),
    ('s', None, None, None, None, None, None, 'score_thesis!Print/Paper'),
    ('t', None, None, None, None, None, None, 'manuscript!Paper'),
    ('t', None, ['hd ||||||||||'], None, None, None, None,
     'manuscript!Microfilm'),
    ('y', None, None, None, None, None, None, 'ejournal'),
    ('y', None, ['hd ||||||||||'], None, None, None, None, 'ejournal'),
    ('y', None, ['cr '], None, None, None, None, 'ejournal'),
    ('y', None, ['cr '], 'n', None, None, None, 'newspaper!Online'),
    ('y', None, None, 'n', None, None, None, 'newspaper!Online'),
    ('y', None, ['hd ||||||||||'], 'n', None, None, None,
     'newspaper!Microfilm, Online'),
    ('z', None, None, None, None, None, None, 'book_thesis!Print/Paper'),
    ('z', None, ['cr '], None, None, None, None, 'book_thesis!Online'),
])
def test_bibrules_resourcetype(bcode2, cns, f007s, f008_21, f008_23, f008_26,
                               bib_locations, expected, bib_rules, mocker):
    """
    Our local BIB_RULES['resource_type'] rule should return the expected
    type.
    """
    bib = mocker.Mock(bcode2=bcode2)
    if cns is not None:
        cn_tuples = [(cn, None) for cn in cns]
        bib.get_call_numbers.return_value = cn_tuples
        items = [mocker.Mock(**{'is_suppressed': False,
                                'get_call_numbers.return_value': [ct]})
                 for ct in cn_tuples]
        links = [mocker.Mock(**{'items_display_order': i, 'item_record': item})
                 for i, item in enumerate(items)]
        bib.bibrecorditemrecordlink_set.all.return_value = links

    def side_effect(control_num=None):
        if control_num == 7:
            if f007s:
                return [mocker.Mock(**{'get_data.return_value': f007})
                        for f007 in f007s]
            return []
        if control_num == 8:
            f008 = '{}{} {}  {}'.format(' ' * 21, f008_21 or ' ',
                                        f008_23 or ' ', f008_26 or ' ')
            return [mocker.Mock(**{'get_data.return_value': f008})]

    bib.record_metadata.controlfield_set.filter.side_effect = side_effect

    if bib_locations is not None:
        rval = [mocker.Mock(code=bl) for bl in bib_locations]
        bib.locations.all.return_value = rval
    lr.ResourceTypeDeterminer()(bib)
    assert bib_rules['resource_type'].evaluate(bib)['resource_type'] == expected


@pytest.mark.parametrize('rtypes, fmts, exp_rtypes, exp_mtypes', [
    (['book'], ['print'], ['books'], ['Paper']),
    (['book'], ['largeprint'], ['books'], ['Paper', 'Large Print']),
    (['book'], ['braille'], ['books'], ['Paper', 'Braille']),
    (['book'], ['microfiche'], ['books'], ['Microforms', 'Microfiche']),
    (['book'], ['microfilm'], ['books'], ['Microforms', 'Microfilm']),
    (['book'], ['microopaque'], ['books'], ['Microforms', 'Microopaques']),
    (['book'], ['print', 'microfiche'], ['books'],
     ['Paper', 'Microforms', 'Microfiche']),
    (['database'], [], ['online_databases'], ['Digital Files']),
    (['score'], ['print'], ['music_scores'], ['Paper']),
    (['score'], ['microfiche'], ['music_scores'], ['Microforms', 'Microfiche']),
    (['map'], ['print'], ['maps'], ['Paper']),
    (['map'], ['online'], ['maps'], ['Digital Files']),
    (['film'], ['16mmfilm'], ['video_film'], ['16mm Film']),
    (['video', 'dvd'], [], ['video_film'], ['DVDs']),
    (['video', 'bluray'], [], ['video_film'], ['Blu-ray Discs']),
    (['filmstrip'], [], ['video_film'], ['Filmstrips']),
    (['video', 'laserdisc'], [], ['video_film'], ['Laserdiscs']),
    (['video', 'vhs'], [], ['video_film'], ['VHS Tapes']),
    (['video', 'music', 'dvd'], [], ['video_film', 'music_recordings'],
     ['DVDs']),
    (['video', 'film'], ['dvd', 'bluray'], ['video_film'],
     ['DVDs', 'Blu-ray Discs']),
    (['video', 'music', 'vhs'], [], ['video_film', 'music_recordings'],
     ['VHS Tapes']),
    (['video', 'film'], ['cdrom'], ['video_film'], ['CD-ROMs']),
    (['slide'], [], ['video_film'], ['Slides']),
    (['video', 'streaming'], [], ['video_film'], ['Digital Files']),
    (['audio', 'spoken', 'record'], [], ['audio', 'spoken_recordings'],
     ['Audio Records (LPs/EPs)']),
    (['audio', 'spoken', 'cd'], [], ['audio', 'spoken_recordings'],
     ['Audio CDs']),
    (['audio', 'spoken', 'cassette'], [], ['audio', 'spoken_recordings'],
     ['Audio Cassette Tapes']),
    (['audio', 'spoken', 'book'], ['digital_device'],
     ['audio', 'books', 'spoken_recordings'],
     ['Audiobook Devices', 'Digital Files']),
    (['audio', 'spoken', 'streaming'], [], ['audio', 'spoken_recordings'],
     ['Digital Files']),
    (['audio', 'spoken', 'book'], [], ['audio', 'books', 'spoken_recordings'],
     []),
    (['audio', 'music', 'cassette'], [], ['audio', 'music_recordings'],
     ['Audio Cassette Tapes']),
    (['audio', 'music', 'cd'], [], ['audio', 'music_recordings'],
     ['Audio CDs']),
    (['audio', 'music', 'record'], ['record_78rpm'], ['audio', 'music_recordings'],
     ['Audio Records (LPs/EPs)', '78 RPM Records']),
    (['audio', 'music', 'record'], ['record_7inch'], ['audio', 'music_recordings'],
     ['Audio Records (LPs/EPs)', '7-inch Vinyl Records']),
    (['audio', 'music', 'record'], ['record_10inch'], ['audio', 'music_recordings'],
     ['Audio Records (LPs/EPs)', '10-inch Vinyl Records']),
    (['audio', 'music', 'record'], ['record_12inch'], ['audio', 'music_recordings'],
     ['Audio Records (LPs/EPs)', '12-inch Vinyl Records']),
    (['audio', 'music', 'streaming'], [], ['audio', 'music_recordings'],
     ['Digital Files']),
    (['audio', 'music'], [], ['audio', 'music_recordings'], []),
    (['graphic'], ['print'], ['images'], ['Paper']),
    (['graphic'], ['online'], ['images'], ['Digital Files']),
    (['audio', 'computer'], [], ['audio', 'software'], ['Digital Files']),
    (['audio', 'computer'], ['cdrom'], ['audio', 'software'],
     ['CD-ROMs', 'Digital Files']),
    (['document', 'computer'], [], ['software'], ['Digital Files']),
    (['document', 'computer'], ['cdrom'], ['software'],
     ['CD-ROMs', 'Digital Files']),
    (['document', 'computer'], ['online'], ['software'], ['Digital Files']),
    (['software', 'computer'], ['cdrom'], ['software'],
     ['CD-ROMs', 'Computer Programs (not Games)']),
    (['software', 'computer'], ['online'], ['software'],
     ['Computer Programs (not Games)']),
    (['game', 'computer'], ['cdrom', 'paper'], ['software', 'games'],
     ['CD-ROMs', 'Computer Games', 'Paper']),
    (['game', 'computer'], [], ['software', 'games'], ['Computer Games']),
    (['game', 'console'], [], ['software', 'games'], ['Console Games']),
    (['game', 'console'], ['PS3'], ['software', 'games'],
     ['Console Games', 'PS3 Games']),
    (['game', 'console'], ['PS3', 'PSP'], ['software', 'games'],
     ['Console Games', 'PS3 Games', 'PSP Games']),
    (['game', 'console'], ['Blah blah'], ['software', 'games'],
     ['Console Games', 'Blah blah Games']),
    (['game', 'handheld'], [], ['software', 'games'], ['Handheld Games']),
    (['game', 'handheld'], ['PSP'], ['software', 'games'],
     ['Handheld Games', 'PSP Games']),
    (['game', 'handheld'], ['Blah blah'], ['software', 'games'],
     ['Handheld Games', 'Blah blah Games']),
    (['ebook'], [], ['books'], ['Digital Files']),
    (['kit'], ['print'], ['educational_kits'], ['Paper']),
    (['archive'], [], ['archives_manuscripts'], ['Archival Collections']),
    (['journal'], ['print'], ['journals_periodicals'], ['Paper']),
    (['journal'], ['microfilm'], ['journals_periodicals'],
     ['Microforms', 'Microfilm']),
    (['object'], [], ['objects_artifacts'], []),
    (['equipment'], [], ['objects_artifacts', 'equipment'], []),
    (['game', 'tabletop'],[], ['objects_artifacts', 'games'],
     ['Tabletop Games']),
    (['score', 'thesis'], ['print'],
     ['music_scores', 'theses_dissertations'], ['Paper']),
    (['manuscript'], ['paper'], ['books', 'archives_manuscripts'],
     ['Manuscripts', 'Paper']),
    (['ejournal'], [], ['journals_periodicals'], ['Digital Files']),
    (['book', 'thesis'], ['online'], ['books', 'theses_dissertations'],
     ['Digital Files']),
    (['book', 'thesis'], ['print'], ['books', 'theses_dissertations'],
     ['Paper']),
    (['newspaper'], ['print'], ['journals_periodicals', 'newspapers'],
     ['Paper']),
    (['newspaper'], ['online'], ['journals_periodicals', 'newspapers'],
     ['Digital Files']),
    (['newspaper'], ['microfilm'], ['journals_periodicals', 'newspapers'],
     ['Microforms', 'Microfilm']),
    #([''], None, [''], ['']),
])
def test_bibrules_resourcetype_categories(rtypes, fmts, exp_rtypes, exp_mtypes,
                                          resource_type_determiner):
    """
    ResourceTypeDeterminer.categorize_resource_type should return the
    expected resource_type and media_type category values given the
    provided parameters.
    """
    val = resource_type_determiner.categorize_resource_type(rtypes, fmts)
    assert set(val['resource_type']) == set(exp_rtypes)
    assert set(val['media_type']) == set(exp_mtypes)
