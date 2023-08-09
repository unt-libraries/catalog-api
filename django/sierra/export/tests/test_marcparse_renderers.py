# -*- coding: utf-8 -*-

"""
Tests the export.marcparse.renderers classes/functions.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import pytest

from export.marcparse import fieldparsers as fp, renderers as rend


# FIXTURES AND TEST DATA
pytestmark = pytest.mark.django_db(databases=['sierra'])


# TESTS
# Note: Currently most of the rendering functions in
# export.marcparse.renderers lack direct tests. If they are not tested
# here, they are tested indirectly via the pipeline tests.

@pytest.mark.parametrize('raw_marcfields, expected', [
    (['700 0#$a***,$cMadame de'],
     ['', 'Madame de', 'Madame, Madame de']),
    (['700 1#$aPompadour,$cMadame de'],
     ['Pompadour', 'Madame Pompadour Madame de Pompadour',
      'Madame Pompadour, Madame de Pompadour']),
    (['700 1#$aWinchilsea, Anne Finch,$cCountess of'],
     ['Winchilsea, Anne Finch Winchilsea, A.F Winchilsea',
      'Countess Anne Winchilsea Countess of Winchilsea',
      'Countess Anne Finch Winchilsea, Countess of Winchilsea', ]),
    (['700 1#$aPeng + Hu,$eeditor.'],
     ['Peng Hu', 'Peng Hu', 'Peng Hu']),
    (['100 0#$aH. D.$q(Hilda Doolittle),$d1886-1961.'],
     ['H.D', 'Hilda Doolittle', 'Hilda Doolittle', 'H.D']),
    (['100 1#$aGresham, G. A.$q(Geoffrey Austin)'],
     ['Gresham, Geoffrey Austin Gresham, G.A Gresham', 'Geoffrey Gresham',
      'G.A Gresham']),
    (['100 1#$aSmith, Elizabeth$q(Ann Elizabeth)'],
     ['Smith, Elizabeth', 'Smith, E', 'Smith, Ann Elizabeth Smith, A.E Smith',
      'Ann Smith', 'Elizabeth Smith']),
    (['700 1#$aE., Sheila$q(Escovedo),$d1959-'],
     ['E, Sheila E, S.E', 'Escovedo, Sheila Escovedo, S Escovedo',
      'Sheila Escovedo', 'Sheila E']),
    (['100 1#$aBeeton,$cMrs.$q(Isabella Mary),$d1836-1865.'],
     ['Beeton, Isabella Mary Beeton, I.M Beeton', 'Mrs Isabella Beeton',
      'Mrs Beeton']),
    (['100 1#$aHutchison, Thomas W.$q(Thomas William),$eauthor$4aut'],
     ['Hutchison, Thomas W Hutchison, Thomas William Hutchison, T.W Hutchison',
      'Thomas Hutchison', 'Thomas W Hutchison']),
    (['600 10$aKoh, Tommy T. B.$q(Tommy Thong Bee),$d1937-'],
     ['Koh, Tommy T.B Koh, Tommy Thong Bee Koh, T.T.B Koh', 'Tommy Koh',
      'Tommy T.B Koh']),
    (['600 11$aMagellan, Ferdinand,$dd 1521.'],
     ['Magellan, Ferdinand Magellan, F Magellan', 'Ferdinand Magellan',
      'Ferdinand Magellan']),
    (['600 00$aGautama Buddha$vEarly works to 1800.'],
     ['Gautama Buddha', 'Gautama Buddha', 'Gautama Buddha']),
    (['100 00$aThomas,$cAquinas, Saint,$d1225?-1274.'],
     ['Thomas', 'Saint Thomas Aquinas', 'Saint Thomas, Aquinas']),
    (['100 1#$aSeuss,$cDr.'],
     ['Seuss', 'Dr Seuss', 'Dr Seuss']),
    (['100 1#$aBeethoven, Ludwig van,$d1770-1827$c(Spirit)'],
     ['Beethoven, Ludwig van Beethoven, L.v Beethoven',
      'Ludwig van Beethoven Spirit', 'Ludwig van Beethoven, Spirit']),
    (['100 1#$aMasséna, André,$cprince d\'Essling,$d1758-1817.'],
     ['Masséna, André Masséna, A Masséna',
      'André Masséna prince d Essling',
      'André Masséna, prince d Essling']),
    (['100 1#$aWalle-Lissnijder,$cvan de.'],
     ['Walle Lissnijder', 'van de Walle Lissnijder',
      'van de Walle Lissnijder']),
    (['700 0#$aCharles Edward,$cPrince, grandson of James II, King of England,'
      '$d1720-1788.'],
     ['Charles Edward',
      'Prince Charles Edward grandson of James II King of England',
      'Prince Charles Edward, grandson of James II, King of England']),
    (['100 0#$aJohn Paul$bII,$cPope,$d1920-'],
     ['John Paul', 'Pope John Paul II', 'Pope John Paul II']),
    (['100 0#$aJohn$bII Comnenus,$cEmperor of the East,$d1088-1143.'],
     ['John', 'Emperor John II Comnenus Emperor of the East',
      'Emperor John II Comnenus, Emperor of the East']),
    (['100 1#$aSaxon, Joseph$q(Irv).'],
     ['Saxon, Joseph Saxon, J Saxon, Irv Saxon, I Saxon', 'Joseph Saxon',
      'Joseph Irv Saxon']),
    (['100 1#$aSaxon, Joseph (Irv).'],
     ['Saxon, Joseph Saxon, J Saxon, Irv Saxon, I Saxon', 'Joseph Saxon',
      'Joseph Irv Saxon']),
    (['100 1#$aSaxon, J. (Irv)$q(Joseph).'],
     ['Saxon, Joseph Saxon, J Saxon, Irv Saxon, I Saxon', 'Joseph Saxon',
      'J Irv Saxon']),
    (['100 1#$aBannister, D.$q{17} (Donald)'],
     ['Bannister, Donald Bannister, D Bannister', 'Donald Bannister',
      'D Bannister']),
    (['100 1#$aBannister,$qD. (Donald)'],
     ['Bannister, Donald Bannister, D Bannister', 'Donald Bannister',
      'Bannister']),
    (['100 1#$aBannister, D.$q(Donald) 1908-'],
     ['Bannister, Donald Bannister, D Bannister', 'Donald Bannister',
      'D Bannister']),
    (['100 1#$aBannister, D.$qDonald'],
     ['Bannister, Donald Bannister, D Bannister', 'Donald Bannister',
      'D Bannister']),
])
def test_personalnamepermutator_getsearchperms(raw_marcfields, expected,
                                               fieldstrings_to_fields):
    """
    The `get_search_permutations` method of the PersonalNamePermutator
    class should return the expected list of search permutations for
    the name in the give MARC field input.
    """
    field = fieldstrings_to_fields(raw_marcfields)[0]
    parsed_name = fp.PersonalNameParser(field).parse()
    permutator = rend.PersonalNamePermutator(parsed_name)
    result = permutator.get_search_permutations()
    print(result)
    assert result == expected


@pytest.mark.parametrize('fparams, expected', [
    (('100', ['a', 'Adams, Henry,', 'd', '1838-1918.'], '1 '), ['Adams, H.']),
    (('100', ['a', 'Chopin, Frédéric', 'd', '1810-1849.'], '1 '),
     ['Chopin, F.']),
    (('100', ['a', 'Riaño, Juan Facundo,', 'd', '1828-1901.'], '1 '),
     ['Riaño, J.F.']),
    (('100', ['a', 'Fowler, T. M.', 'q', '(Thaddeus Mortimer),',
              'd', '1842-1922.'], '1 '),
     ['Fowler, T.M.']),
    (('100', ['a', 'Isidore of Seville.'], '0 '), ['Isidore of Seville']),
    (('100', ['a', 'Vérez-Peraza, Elena,', 'd', '1919-'], '1 '),
     ['Vérez-Peraza, E.']),
    (('100', ['a', 'John', 'b', 'II Comnenus,', 'c', 'Emperor of the East,',
              'd', '1088-1143.'], '0 '),
     ['John II Comnenus, Emperor of the East']),
    (('100', ['a', 'John Paul', 'b', 'II,', 'c', 'Pope,',
              'd', '1920-'], '0 '),
     ['John Paul II, Pope']),
    (('100', ['a', 'Beeton,', 'c', 'Mrs.', 'q', '(Isabella Mary),',
              'd', '1836-1865.'], '1 '),
     ['Beeton, Mrs.']),
    (('100', ['a', 'Black Foot,', 'c', 'Chief,', 'd', 'd. 1877',
              'c', '(Spirit)'], '0 '),
     ['Black Foot, Chief (Spirit)']),
    (('100', ['a', 'Thomas,', 'c', 'Aquinas, Saint,', 'd', '1225?-1274.'],
        '0 '),
     ['Thomas, Aquinas, Saint']),
    (('110', ['a', 'United States.', 'b', 'Court of Appeals (2nd Circuit)'],
        '1 '),
     ['United States Court of Appeals (2nd Circuit)']),
    (('110', ['a', 'Catholic Church.', 'b', 'Province of Baltimore (Md.).',
              'b', 'Provincial Council.'], '2 '),
     ['Catholic Church ... Provincial Council']),
    (('110', ['a', 'United States.', 'b', 'Congress.',
              'b', 'Joint Committee on the Library.'], '1 '),
     ['United States Congress, Joint Committee on the Library']),
    (('110', ['a', 'Catholic Church.',
              'b', 'Concilium Plenarium Americae Latinae',
              'd', '(1899 :', 'c', 'Rome, Italy)'], '2 '),
     ['Catholic Church',
      'Catholic Church, Concilium Plenarium Americae Latinae']),
    (('111', ['a', 'Governor\'s Conference on Aging (N.Y.)',
              'd', '(1982 :', 'c', 'Albany, N.Y.)'], '2 '),
     ['Governor\'s Conference on Aging (N.Y.)']),
    (('111', ['a', 'Esto \'84', 'd', '(1984 :', 'c', 'Toronto, Ont).',
              'e', 'Raamatunaituse Komitee.'], '2 '),
     ['Esto \'84', 'Esto \'84, Raamatunaituse Komitee'])
])
def test_shortenname(fparams, expected, params_to_fields):
    """
    The `shorten_name` function should return the expected shortened
    version of a name when passed a structure from a NameParser
    resulting from the given `fparams` data.
    """
    field = params_to_fields([fparams])[0]
    if field.tag.endswith('00'):
        parsed = [fp.PersonalNameParser(field).parse()]
    else:
        parsed = fp.OrgEventNameParser(field).parse()
    result = [rend.shorten_name(n) for n in parsed]
    assert set(result) == set(expected)


@pytest.mark.parametrize('fval, nf_chars, expected', [
    ('', 0, '~'),
    ('$', 0, '~'),
    ('日本食品化学学会誌', 0, '~'),
    ('$1000', 0, '1000'),
    ('1000', 0, '1000'),
    ('[A] whatever', 0, 'a-whatever'),
    ('[A] whatever', 4, 'whatever'),
    ('[A] whatever', 1, 'a-whatever'),
    ('[A] whatever!', 1, 'a-whatever'),
    ('Romeo and Juliet', 4, 'romeo-and-juliet'),
    ('Lastname, Firstname, 1800-1922', 0, 'lastname-firstname-1800-1922'),
])
def test_generatefacetkey(fval, nf_chars, expected):
    """
    The `generate_facet_key` function should return the expected key
    string when passed the given facet value string and number of non-
    filing characters (`nf_chars`).
    """
    assert rend.generate_facet_key(fval, nf_chars) == expected

