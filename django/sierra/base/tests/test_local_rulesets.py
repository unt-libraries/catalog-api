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
    ('czm', 1, '-', True),
    ('czmrf', 1, '-', False),
    ('czm', 7, '-', False),
    ('czm', 20, '-', False),
    ('czm', 29, '-', False),
    ('czm', 69, '-', False),
    ('czm', 74, '-', False),
    ('czm', 112, '-', False),
    ('xmus', 1, '-', True),
    ('xmus', 7, '-', True),
    ('czm', 1, 'e', False),
    ('czm', 1, 'f', False),
    ('czm', 1, 'i', False),
    ('czm', 1, 'j', False),
    ('czm', 1, 'm', False),
    ('czm', 1, 'n', False),
    ('czm', 1, 'o', False),
    ('czm', 1, 'p', False),
    ('czm', 1, 'w', False),
    ('czm', 1, 'y', False),
    ('czm', 1, 'z', False),
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
    ('czm', False),
    ('jlf', False),
    ('w4mr1', True),
    ('w4mr2', True),
    ('w4mr3', True),
    ('w4mrb', True),
    ('w4mrx', True),
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


@pytest.mark.parametrize('bcode2, cns, f007s, f008_21, f008_26, bib_locations, '
                         'expected', [
    ('-', None, None, None, None, None, 'unknown'),
    ('a', None, None, None, None, None, 'book|Print/Paper'),
    ('a', None, ['ta'], None, None, None, 'book|Print/Paper'),
    ('a', None, ['tb'], None, None, None, 'book|Large-Print/Paper'),
    ('a', None, ['tc'], None, None, None, 'book|Braille'),
    ('a', None, ['tu'], None, None, None, 'book|Print/Paper'),
    ('a', None, ['cr '], None, None, None, 'ebook'),
    ('a', None, ['cr '], 'n', None, None, 'newspaper|Online'),
    ('a', None, None, 'n', None, None, 'newspaper|Print/Paper'),
    ('a', None, ['hd ||||||||||'], 'n', None, None, 'newspaper|Microfilm'),
    ('a', None, ['he ||||||||||'], None, None, None, 'book|Microfiche'),
    ('a', None, ['hd ||||||||||'], None, None, None, 'book|Microfilm'),
    ('a', None, ['hg a|||||bncn'], None, None, None, 'book|Microopaque'),
    ('b', None, None, None, None, None, 'database'),
    ('b', None, ['cr '], None, None, None, 'database'),
    ('b', None, None, 'n', None, None, 'newspaper|Online'),
    ('b', None, ['cr '], 'n', None, None, 'newspaper|Online'),
    ('c', None, None, None, None, None, 'score|Print/Paper'),
    ('c', None, ['cr '], None, None, None, 'score|Online'),
    ('c', None, ['he ||||||||||'], None, None, None, 'score|Microfiche'),
    ('e', None, None, None, None, None, 'map|Print/Paper'),
    ('e', None, ['cr '], None, None, None, 'map|Online'),
    ('g', None, None, None, None, None, 'video_film'),
    ('g', ['MP 1234'], ['mr |aaad|||||||||||||||'], None, None, None,
     'film|16mm Film'),
    ('g', ['DVD 1234'], ['vdv|vaiz|'], None, None, None, 'video_dvd'),
    ('g', ['DVD 1234 Blu-ray'], ['vdb|saiz|'], None, None, None,
     'video_bluray'),
    ('g', ['F.S. 123'], ['go |j||f'], None, None, None, 'filmstrip'),
    ('g', ['LD 1234'], ['vdg|gaiz|'], None, None, None, 'video_laserdisc'),
    ('g', ['MV 1234'], ['vf |baho|'], None, None, None, 'video_vhs'),
    ('g', ['MDVD 1234'], ['vdv|vaiz|'], None, None, None, 'video_music_dvd'),
    ('g', ['MVC 1234'], ['vf |baho|'], None, None, None, 'video_music_vhs'),
    ('g', ['Slide 1234'], ['gs |j||||'], None, None, None, 'slide'),
    ('g', ['Online Video'], ['vzs|zazu|'], None, None, None, 'video_streaming'),
    ('i', None, None, None, None, None, 'audio_spoken_book'),
    ('i', ['Ph-disc 12'], ['sd ||m|nn||l||'], None, None, None,
     'audio_spoken_record'),
    ('i', ['ACD 12'], ['sd f|ngnn|mne|'], None, None, None, 'audio_spoken_cd'),
    ('i', ['Ph-tape 12'], ['ss l|njlc|pn||'], None, None, None,
     'audio_spoken_cassette'),
    ('i', ['ADB 12'], ['cz nza||||||||'], None, None, None,
     'audio_spoken_book|Digital Device'),
    ('i', ['ADB 12'], ['sz nza||||||||'], None, None, None,
     'audio_spoken_book|Digital Device'),
    ('i', ['ADB 12'], ['sz nza||||||||', 'cz nza||||||||'], None, None, None,
     'audio_spoken_book|Digital Device'),
    ('i', ['Online Audio'], ['sr n|nnnnnnne|'], None, None, None,
     'audio_spoken_streaming'),
    ('j', None, None, None, None, None, 'audio_music'),
    ('j', ['Music 12 Cassette'], ['ss l|njlcmpn||'], None, None, None,
     'audio_music_cassette'),
    ('j', ['LPCD 12'], ['sd f|ngnnmmne|'], None, None, None, 'audio_music_cd'),
    ('j', ['LPW 12'], ['sd dms|nnmsl||'], None, None, None,
     'audio_music_record|78 RPM'),
    ('j', ['LPX 12'], ['sd ||mcnnmpl||'], None, None, None,
     'audio_music_record|7-inch'),
    ('j', ['LPY 12'], ['sd ||mdnnmpl||'], None, None, None,
     'audio_music_record|10-inch'),
    ('j', ['LPZ 12'], ['sd ||menn||l||'], None, None, None,
     'audio_music_record|12-inch'),
    ('j', ['Online Audio'], ['sr n|nnnnnnne|'], None, None, None,
     'audio_music_streaming'),
    ('k', None, None, None, None, None, 'graphic|Print/Paper'),
    ('k', None, ['cr '], None, None, None, 'graphic|Online'),
    ('m', None, None, None, None, None, 'document_computer'),
    ('m', ['CD-ROM 1234'], None, None, 'b', ['czm'], 'software_computer'),
    ('m', ['CD-ROM 1234'], ['co |g|||||||||'], None, 'b', ['czm'],
     'software_computer|CD-ROM'),
    ('m', ['Game 1234'], None, None, 'g', ['czm'], 'game_computer'),
    ('m', ['Game 1234'], ['co |g|||||||||'], None, 'g', ['czm'],
     'game_computer|CD-ROM'),
    ('m', None, None, None, ' ', ['czm'], 'document_computer'),
    ('m', None, ['co |g|||||||||'], None, ' ', ['czm'],
     'document_computer|CD-ROM'),
    ('m', ['CD-ROM 1234'], None, None, 'g', ['sd'], 'game_computer'),
    ('m', ['CD-ROM 1234'], ['co |g|||||||||'], None, 'g', ['sd'],
     'game_computer|CD-ROM'),
    ('m', ['CD-ROM 1234'], None, None, 'd', ['sd'], 'document_computer'),
    ('m', ['CD-ROM 1234'], ['co |g|||||||||'], None, 'd', ['sd'],
     'document_computer|CD-ROM'),
    ('m', ['LPCD-ROM 1234'], None, None, 'h', ['sd'], 'audio_computer'),
    ('m', ['LPCD-ROM 1234'], ['co |g|||||||||'], None, 'h', ['sd'],
     'audio_computer|CD-ROM'),
    ('m', ['CD-ROM 1234'], ['cot|ga||||||||'], None, None, None,
     'game_computer|CD-ROM'),
    ('m', ['Game 12'], ['cor|ga||||||||'], None, None, None, 'game_console'),
    ('m', ['MT 1234'], ['cor|ga||||||||'], None, None, None, 'game_console'),
    ('m', ['Game 12'], ['cbr|ga||||||||'], None, None, None, 'game_console'),
    ('m', ['Game 12 PS4'], ['cor|ga||||||||'], None, None, None,
     'game_console|PS4'),
    ('m', ['Game 12 Xbox 360'], ['cor|ga||||||||'], None, None, None,
     'game_console|Xbox 360'),
    ('m', ['Game 12'], ['coh|ga||||||||'], None, None, None, 'game_handheld'),
    ('m', ['MT 1234'], ['cbh|ga||||||||'], None, None, None, 'game_handheld'),
    ('m', ['Game 12'], ['cbh|ga||||||||'], None, None, None, 'game_handheld'),
    ('m', ['Game 12 PSP'], ['cbh|ga||||||||'], None, None, None,
     'game_handheld|PSP'),
    ('n', None, None, None, None, None, 'ebook'),
    ('n', None, ['hd ||||||||||'], None, None, None, 'ebook'),
    ('n', None, ['cr '], None, None, None, 'ebook'),
    ('n', None, ['cr '], 'n', None, None, 'newspaper|Online'),
    ('n', None, None, 'n', None, None, 'newspaper|Online'),
    ('n', None, ['hd ||||||||||'], 'n', None, None, 'newspaper|Online'),
    ('o', None, None, None, None, None, 'kit|Print/Paper'),
    ('p', None, None, None, None, None, 'archive'),
    ('q', None, None, None, None, None, 'journal|Print/Paper'),
    ('q', None, ['ta'], None, None, None, 'journal|Print/Paper'),
    ('q', None, ['tb'], None, None, None, 'journal|Large-Print/Paper'),
    ('q', None, ['tc'], None, None, None, 'journal|Braille'),
    ('q', None, ['tu'], None, None, None, 'journal|Print/Paper'),
    ('q', None, ['cr '], None, None, None, 'ejournal'),
    ('q', None, ['cr '], 'n', None, None, 'newspaper|Online'),
    ('q', None, None, 'n', None, None, 'newspaper|Print/Paper'),
    ('q', None, ['hd ||||||||||'], 'n', None, None, 'newspaper|Microfilm'),
    ('q', None, ['he ||||||||||'], None, None, None, 'journal|Microfiche'),
    ('q', None, ['cr '], None, None, None, 'ejournal'),
    ('q', None, ['cr '], 'n', None, None, 'newspaper|Online'),
    ('q', None, None, 'n', None, None, 'newspaper|Print/Paper'),
    ('q', None, ['hd ||||||||||'], 'n', None, None, 'newspaper|Microfilm'),
    ('r', [], None, None, ' ', [''], 'equipment'),
    ('r', ['MT 1234 S32'], None, None, ' ', ['w4spe'], 'object'),
    ('r', ['Boardgame 1234'], None, None, ' ', ['czm'], 'game_tabletop'),
    ('s', None, None, None, None, None, 'score_thesis|Print/Paper'),
    ('t', None, None, None, None, None, 'manuscript'),
    ('y', None, None, None, None, None, 'ejournal'),
    ('y', None, ['hd ||||||||||'], None, None, None, 'ejournal'),
    ('y', None, ['cr '], None, None, None, 'ejournal'),
    ('y', None, ['cr '], 'n', None, None, 'newspaper|Online'),
    ('y', None, None, 'n', None, None, 'newspaper|Online'),
    ('y', None, ['hd ||||||||||'], 'n', None, None, 'newspaper|Online'),
    ('z', None, None, None, None, None, 'book_thesis|Print/Paper'),
    ('z', None, ['cr '], None, None, None, 'book_thesis|Online'),
])
def test_bibrules_resourcetype(bcode2, cns, f007s, f008_21, f008_26,
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
            f008 = '{}{}{}{}'.format(' ' * 21, f008_21 or ' ', ' ' * 4,
                                     f008_26 or ' ')
            return [mocker.Mock(**{'get_data.return_value': f008})]

    bib.record_metadata.controlfield_set.filter.side_effect = side_effect

    if bib_locations is not None:
        rval = [mocker.Mock(code=bl) for bl in bib_locations]
        bib.locations.all.return_value = rval
    assert bib_rules['resource_type'].evaluate(bib)['resource_type'] == expected


@pytest.mark.parametrize('rtypes, fmt, exp_rtypes, exp_mtypes', [
    (['book'], 'Print/Paper', ['books'], ['Printed Paper']),
    (['book'], 'Large-Print/Paper', ['books'],
     ['Printed Paper', 'Large Print']),
    (['book'], 'Braille', ['books'], ['Printed Paper', 'Braille']),
    (['book'], 'Microfiche', ['books'], ['Microforms', 'Microfiche']),
    (['book'], 'Microfilm', ['books'], ['Microforms', 'Microfilm']),
    (['book'], 'Microopaque', ['books'], ['Microforms', 'Microopaques']),
    (['database'], None, ['online_databases'], ['Digital Files']),
    (['score'], 'Print/Paper', ['music_scores'], ['Printed Paper']),
    (['score'], 'Microfiche', ['music_scores'], ['Microforms', 'Microfiche']),
    (['map'], 'Print/Paper', ['maps'], ['Printed Paper']),
    (['map'], 'Online', ['maps'], ['Digital Files']),
    (['film'], '16mm Film', ['video_film'], ['16mm Film']),
    (['video', 'dvd'], None, ['video_film'], ['DVDs']),
    (['video', 'bluray'], None, ['video_film'], ['Blu-ray Discs']),
    (['filmstrip'], None, ['video_film'], ['Filmstrips']),
    (['video', 'laserdisc'], None, ['video_film'], ['Laserdiscs']),
    (['video', 'vhs'], None, ['video_film'], ['VHS Tapes']),
    (['video', 'music', 'dvd'], None, ['video_film', 'music_recordings'],
     ['DVDs']),
    (['video', 'music', 'vhs'], None, ['video_film', 'music_recordings'],
     ['VHS Tapes']),
    (['slide'], None, ['video_film'], ['Slides']),
    (['video', 'streaming'], None, ['video_film'], ['Digital Files']),
    (['audio', 'spoken', 'record'], None, ['audio', 'spoken_recordings'],
     ['Audio Records (LPs/EPs)']),
    (['audio', 'spoken', 'cd'], None, ['audio', 'spoken_recordings'],
     ['Audio CDs']),
    (['audio', 'spoken', 'cassette'], None, ['audio', 'spoken_recordings'],
     ['Audio Cassette Tapes']),
    (['audio', 'spoken', 'book'], 'Digital Device',
     ['audio', 'books', 'spoken_recordings'],
     ['Audiobook Devices', 'Digital Files']),
    (['audio', 'spoken', 'streaming'], None, ['audio', 'spoken_recordings'],
     ['Digital Files']),
    (['audio', 'spoken', 'book'], None, ['audio', 'books', 'spoken_recordings'],
     []),
    (['audio', 'music', 'cassette'], None, ['audio', 'music_recordings'],
     ['Audio Cassette Tapes']),
    (['audio', 'music', 'cd'], None, ['audio', 'music_recordings'],
     ['Audio CDs']),
    (['audio', 'music', 'record'], '78 RPM', ['audio', 'music_recordings'],
     ['Audio Records (LPs/EPs)', '78 RPM Records']),
    (['audio', 'music', 'record'], '7-inch', ['audio', 'music_recordings'],
     ['Audio Records (LPs/EPs)', '7-inch Vinyl Records']),
    (['audio', 'music', 'record'], '10-inch', ['audio', 'music_recordings'],
     ['Audio Records (LPs/EPs)', '10-inch Vinyl Records']),
    (['audio', 'music', 'record'], '12-inch', ['audio', 'music_recordings'],
     ['Audio Records (LPs/EPs)', '12-inch Vinyl Records']),
    (['audio', 'music', 'streaming'], None, ['audio', 'music_recordings'],
     ['Digital Files']),
    (['audio', 'music'], None, ['audio', 'music_recordings'], []),
    (['graphic'], 'Print/Paper', ['images'], ['Printed Paper']),
    (['graphic'], 'Online', ['images'], ['Digital Files']),
    (['audio', 'computer'], None, ['audio', 'software'], ['Digital Files']),
    (['audio', 'computer'], 'CD-ROM', ['audio', 'software'],
     ['CD-ROMs', 'Digital Files']),
    (['document', 'computer'], None, ['software'], ['Digital Files']),
    (['document', 'computer'], 'CD-ROM', ['software'],
     ['CD-ROMs', 'Digital Files']),
    (['document', 'computer'], 'Online', ['software'], ['Digital Files']),
    (['software', 'computer'], 'CD-ROM', ['software'],
     ['CD-ROMs', 'Computer Programs (not Games)']),
    (['software', 'computer'], 'Online', ['software'],
     ['Computer Programs (not Games)']),
    (['game', 'computer'], 'CD-ROM', ['software', 'games'],
     ['CD-ROMs', 'Computer Games']),
    (['game', 'computer'], None, ['software', 'games'], ['Computer Games']),
    (['game', 'console'], None, ['software', 'games'], ['Console Games']),
    (['game', 'console'], 'PS3', ['software', 'games'],
     ['Console Games', 'PS3 Games']),
    (['game', 'console'], 'Blah blah', ['software', 'games'],
     ['Console Games', 'Blah blah Games']),
    (['game', 'handheld'], None, ['software', 'games'], ['Handheld Games']),
    (['game', 'handheld'], 'PSP', ['software', 'games'],
     ['Handheld Games', 'PSP Games']),
    (['game', 'handheld'], 'Blah blah', ['software', 'games'],
     ['Handheld Games', 'Blah blah Games']),
    (['ebook'], None, ['books'], ['Digital Files']),
    (['kit'], 'Print/Paper', ['educational_kits'], ['Printed Paper']),
    (['archive'], None, ['archives_manuscripts'], ['Archival Collections']),
    (['journal'], 'Print/Paper', ['journals_periodicals'], ['Printed Paper']),
    (['journal'], 'Microfilm', ['journals_periodicals'],
     ['Microforms', 'Microfilm']),
    (['object'], None, ['objects_artifacts'], []),
    (['equipment'], None, ['objects_artifacts', 'equipment'], []),
    (['game', 'tabletop'], None, ['objects_artifacts', 'games'],
     ['Tabletop Games']),
    (['score', 'thesis'], 'Print/Paper',
     ['music_scores', 'theses_dissertations'], ['Printed Paper']),
    (['manuscript'], None, ['books', 'archives_manuscripts'], ['Manuscripts']),
    (['ejournal'], None, ['journals_periodicals'], ['Digital Files']),
    (['book', 'thesis'], 'Online', ['books', 'theses_dissertations'],
     ['Digital Files']),
    (['book', 'thesis'], 'Print/Paper', ['books', 'theses_dissertations'],
     ['Printed Paper']),
    (['newspaper'], 'Print/Paper', ['journals_periodicals'],
     ['Newspapers', 'Printed Paper']),
    (['newspaper'], 'Online', ['journals_periodicals'],
     ['Newspapers', 'Digital Files']),
    (['newspaper'], 'Microfilm', ['journals_periodicals'],
     ['Newspapers', 'Microforms', 'Microfilm']),
    #([''], None, [''], ['']),
])
def test_bibrules_resourcetype_categories(rtypes, fmt, exp_rtypes, exp_mtypes,
                                          resource_type_determiner):
    """
    ResourceTypeDeterminer.categorize_resource_type should return the
    expected resource_type and media_type category values given the
    provided parameters.
    """
    val = resource_type_determiner.categorize_resource_type(rtypes, fmt)
    assert set(val['resource_type']) == set(exp_rtypes)
    assert set(val['media_type']) == set(exp_mtypes)
