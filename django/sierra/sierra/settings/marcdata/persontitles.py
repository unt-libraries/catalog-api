# -*- coding: utf-8 -*- 

from __future__ import unicode_literals

"""
Contains data related to personal titles
"""

# PERSON_PRETITLES includes a list of individual titles that should
# (or can) be placed before a person's name, that may appear in the
# list of titles in $c in a MARC X00 field. This includes:
# - Honorifics, such as 'Dr' and 'Mrs'.
#   See https://en.wikipedia.org/wiki/English_honorifics
# - Imperial, royal, and noble titles: 'King', 'Baron', 'Sir'
#   See https://en.wikipedia.org/wiki/Imperial,_royal_and_noble_ranks
# - Nobiliary particle, for names that are surnames only, such as
#   100 1#$aWalle-Lissnijder,$cvan de -- (van de Walle-Lissnijder)
#   See https://en.wikipedia.org/wiki/Nobiliary_particle
# 
# Before comparing to a value in this list, normalize the comparison
# value by stripping punctuation and converting to lower case.
#
# This is not a complete list by any means; we'll add values that are
# missing as we run across them.
PERSON_PRETITLES = set([
    'master', 'mr', 'miss', 'ms', 'mrs', 'mx', 'sir', 'mistress', 'madam',
    'maam', 'ma am', 'dame', 'lord', 'lady', 'the honourable', 'the honorable',
    'the right honourable', 'the right honorable', 'the most honourable',
    'the most honorable', 'the hon', 'hon', 'dr', 'doctor', 'professor',
    'excellency', 'his excellency', 'her excellency', 'chancellor',
    'vice chancellor', 'principal', 'president', 'warden', 'dean', 'regent',
    'rector', 'provost', 'director', 'chief executive', 'his holiness', 'hh',
    'pope', 'pope emeritus', 'his all holiness', 'hah', 'his beatitude',
    'his most eminent highness', 'hmeh', 'his eminence', 'he',
    'most reverend eminence', 'the most reverend', 'the most rev',
    'the most revd', 'his grace', 'the right reverend', 'the rt rev',
    'the rt revd', 'his lordship', 'the reverend', 'reverend', 'the rev', 'rev',
    'the revd', 'revd', 'father', 'fr', 'pastor', 'pr', 'brother', 'br',
    'sister', 'sr', 'elder', 'saint', 'rabbi', 'cantor', 'chief rabbi',
    'grand rabbi', 'rebbetzin', 'imam', 'imām', 'shaykh', 'muftī', 'mufti',
    'hāfiz', 'hafiz', 'hāfizah', 'hafizah', 'qārī', 'qari', 'mawlānā',
    'mawlana', 'hājī', 'haji', 'sayyid', 'sayyidah', 'sharif', 'venerable',
    'ven', 'eminent', 'emi', 'of', 'af', 'von', 'de', 'd', 'du', 'des', 'zu',
    'van', 'den', 'der', 'van de', 'van der', 'van den', 'emperor', 'empress',
    'king emperor', 'queen empress', 'kaiser', 'tsar', 'tsarina', 'high king',
    'high queen', 'great king', 'great queen' 'king', 'queen', 'archduke',
    'archduchess', 'tsesarevich', 'grand prince', 'grand princess',
    'grand duke', 'grand duchess', 'prince-elector', 'prince', 'princess',
    'crown prince', 'crown princess', 'foreign prince', 'prince du sang',
    'infante', 'infanta', 'dauphin', 'dauphine', 'królewicz', 'krolewicz',
    'królewna', 'krolewna', 'jarl', 'tsarevich', 'tsarevna', 'duke', 'duchess',
    'herzog', 'knyaz', 'princely count', 'sovereign prince',
    'sovereign princess', 'fürst', 'furst', 'fürstin', 'furstin', 'boyar',
    'marquess', 'marquis', 'marchioness', 'margrave', 'marcher lord',
    'landgrave', 'count palatine', 'count', 'countess', 'earl', 'graf',
    'châtelain', 'chatelain', 'castellan', 'burgrave', 'burggrave', 'viscount',
    'viscountess', 'vidame', 'baron', 'baroness', 'freiherr', 'advocatus',
    'thane', 'lendmann', 'baronet', 'baronetess', 'seigneur', 'laird',
    'lord of the manor', 'gentleman', 'maid', 'don',
])
