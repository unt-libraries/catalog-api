# -*- coding: utf-8 -*-

"""
Tests the utils.toascii module.
"""

from __future__ import unicode_literals
import pytest

from utils import toascii


@pytest.mark.parametrize('text, expected', [
    ('', ''),
    ('abcdefg. 12345', 'abcdefg. 12345'),
    ('An ḃfuil do ċroí ag bualaḋ ó ḟaitíos an ġrá a ṁeall lena ṗóg éada ó ṡlí '
     'do leasa ṫú',
     'An bfuil do croi ag bualad o faitios an gra a meall lena pog eada o sli '
     'do leasa tu'),
    ('Falsches Üben von Xylophonmusik quält jeden größeren Zwerg',
     'Falsches Uben von Xylophonmusik qualt jeden grosseren Zwerg'),
    ('Příliš žluťoučký kůň úpěl ďábelské kódy',
     'Prilis zlutoucky kun upel dabelske kody'),
    ('Les naïfs ægithales hâtifs pondant à Noël où il gèle sont sûrs d\'être '
     'déçus en voyant leurs drôles d\'œufs abîmés',
     'Les naifs aegithales hatifs pondant a Noel ou il gele sont surs d\'etre '
     'decus en voyant leurs droles d\'oeufs abimes'),
    ('Blåbærsyltetøy', 'Blabaersyltetoy'),
    ('Sævör grét áðan því úlpan var ónýt',
     'Saevor gret adan thvi ulpan var onyt')

])
def test_mapfromunicode_maps_correctly(text, expected):
    """
    The `map_from_unicode` function should map the input unicode text
    to the expected output.
    """
    assert toascii.map_from_unicode(text) == expected
