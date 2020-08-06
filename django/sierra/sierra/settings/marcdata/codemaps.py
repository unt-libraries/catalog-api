# -*- coding: utf-8 -*-
"""
Define various mappings of (standard) codes to values.
"""

from __future__ import unicode_literals


# MARC Relator codes, from https://www.loc.gov/marc/relators/relacode.html
RELATOR_CODES = {
    'abr': 'abridger',
    'acp': 'art copyist',
    'act': 'actor',
    'adi': 'art director',
    'adp': 'adapter',
    'aft': 'author of afterword, colophon, etc.',
    'anl': 'analyst',
    'anm': 'animator',
    'ann': 'annotator',
    'ant': 'bibliographic antecedent',
    'ape': 'appellee',
    'apl': 'appellant',
    'app': 'applicant',
    'aqt': 'author in quotations or text abstracts',
    'arc': 'architect',
    'ard': 'artistic director',
    'arr': 'arranger',
    'art': 'artist',
    'asg': 'assignee',
    'asn': 'associated name',
    'ato': 'autographer',
    'att': 'attributed name',
    'auc': 'auctioneer',
    'aud': 'author of dialog',
    'aui': 'author of introduction, etc.',
    'aus': 'screenwriter',
    'aut': 'author',
    'bdd': 'binding designer',
    'bjd': 'bookjacket designer',
    'bkd': 'book designer',
    'bkp': 'book producer',
    'blw': 'blurb writer',
    'bnd': 'binder',
    'bpd': 'bookplate designer',
    'brd': 'broadcaster',
    'brl': 'braille embosser',
    'bsl': 'bookseller',
    'cas': 'caster',
    'ccp': 'conceptor',
    'chr': 'choreographer',
    'clb': 'collaborator',
    'cli': 'client',
    'cll': 'calligrapher',
    'clr': 'colorist',
    'clt': 'collotyper',
    'cmm': 'commentator',
    'cmp': 'composer',
    'cmt': 'compositor',
    'cnd': 'conductor',
    'cng': 'cinematographer',
    'cns': 'censor',
    'coe': 'contestant-appellee',
    'col': 'collector',
    'com': 'compiler',
    'con': 'conservator',
    'cor': 'collection registrar',
    'cos': 'contestant',
    'cot': 'contestant-appellant',
    'cou': 'court governed',
    'cov': 'cover designer',
    'cpc': 'copyright claimant',
    'cpe': 'complainant-appellee',
    'cph': 'copyright holder',
    'cpl': 'complainant',
    'cpt': 'complainant-appellant',
    'cre': 'creator',
    'crp': 'correspondent',
    'crr': 'corrector',
    'crt': 'court reporter',
    'csl': 'consultant',
    'csp': 'consultant to a project',
    'cst': 'costume designer',
    'ctb': 'contributor',
    'cte': 'contestee-appellee',
    'ctg': 'cartographer',
    'ctr': 'contractor',
    'cts': 'contestee',
    'ctt': 'contestee-appellant',
    'cur': 'curator',
    'cwt': 'commentator for written text',
    'dbp': 'distribution place',
    'dfd': 'defendant',
    'dfe': 'defendant-appellee',
    'dft': 'defendant-appellant',
    'dgg': 'degree granting institution',
    'dgs': 'degree supervisor',
    'dis': 'dissertant',
    'dln': 'delineator',
    'dnc': 'dancer',
    'dnr': 'donor',
    'dpc': 'depicted',
    'dpt': 'depositor',
    'drm': 'draftsman',
    'drt': 'director',
    'dsr': 'designer',
    'dst': 'distributor',
    'dtc': 'data contributor',
    'dte': 'dedicatee',
    'dtm': 'data manager',
    'dto': 'dedicator',
    'dub': 'dubious author',
    'edc': 'editor of compilation',
    'edm': 'editor of moving image work',
    'edt': 'editor',
    'egr': 'engraver',
    'elg': 'electrician',
    'elt': 'electrotyper',
    'eng': 'engineer',
    'enj': 'enacting jurisdiction',
    'etr': 'etcher',
    'evp': 'event place',
    'exp': 'expert',
    'fac': 'facsimilist',
    'fds': 'film distributor',
    'fld': 'field director',
    'flm': 'film editor',
    'fmd': 'film director',
    'fmk': 'filmmaker',
    'fmo': 'former owner',
    'fmp': 'film producer',
    'fnd': 'funder',
    'fpy': 'first party',
    'frg': 'forger',
    'gis': 'geographic information specialist',
    'grt': 'graphic technician',
    'his': 'host institution',
    'hnr': 'honoree',
    'hst': 'host',
    'ill': 'illustrator',
    'ilu': 'illuminator',
    'ins': 'inscriber',
    'inv': 'inventor',
    'isb': 'issuing body',
    'itr': 'instrumentalist',
    'ive': 'interviewee',
    'ivr': 'interviewer',
    'jud': 'judge',
    'jug': 'jurisdiction governed',
    'lbr': 'laboratory',
    'lbt': 'librettist',
    'ldr': 'laboratory director',
    'led': 'lead',
    'lee': 'libelee-appellee',
    'lel': 'libelee',
    'len': 'lender',
    'let': 'libelee-appellant',
    'lgd': 'lighting designer',
    'lie': 'libelant-appellee',
    'lil': 'libelant',
    'lit': 'libelant-appellant',
    'lsa': 'landscape architect',
    'lse': 'licensee',
    'lso': 'licensor',
    'ltg': 'lithographer',
    'lyr': 'lyricist',
    'mcp': 'music copyist',
    'mdc': 'metadata contact',
    'med': 'medium',
    'mfp': 'manufacture place',
    'mfr': 'manufacturer',
    'mod': 'moderator',
    'mon': 'monitor',
    'mrb': 'marbler',
    'mrk': 'markup editor',
    'msd': 'musical director',
    'mte': 'metal-engraver',
    'mtk': 'minute taker',
    'mus': 'musician',
    'nrt': 'narrator',
    'opn': 'opponent',
    'org': 'originator',
    'orm': 'organizer',
    'osp': 'onscreen presenter',
    'oth': 'other',
    'own': 'owner',
    'pan': 'panelist',
    'pat': 'patron',
    'pbd': 'publishing director',
    'pbl': 'publisher',
    'pdr': 'project director',
    'pfr': 'proofreader',
    'pht': 'photographer',
    'plt': 'platemaker',
    'pma': 'permitting agency',
    'pmn': 'production manager',
    'pop': 'printer of plates',
    'ppm': 'papermaker',
    'ppt': 'puppeteer',
    'pra': 'praeses',
    'prc': 'process contact',
    'prd': 'production personnel',
    'pre': 'presenter',
    'prf': 'performer',
    'prg': 'programmer',
    'prm': 'printmaker',
    'prn': 'production company',
    'pro': 'producer',
    'prp': 'production place',
    'prs': 'production designer',
    'prt': 'printer',
    'prv': 'provider',
    'pta': 'patent applicant',
    'pte': 'plaintiff-appellee',
    'ptf': 'plaintiff',
    'pth': 'patent holder',
    'ptt': 'plaintiff-appellant',
    'pup': 'publication place',
    'rbr': 'rubricator',
    'rcd': 'recordist',
    'rce': 'recording engineer',
    'rcp': 'addressee',
    'rdd': 'radio director',
    'red': 'redaktor',
    'ren': 'renderer',
    'res': 'researcher',
    'rev': 'reviewer',
    'rpc': 'radio producer',
    'rps': 'repository',
    'rpt': 'reporter',
    'rpy': 'responsible party',
    'rse': 'respondent-appellee',
    'rsg': 'restager',
    'rsp': 'respondent',
    'rsr': 'restorationist',
    'rst': 'respondent-appellant',
    'rth': 'research team head',
    'rtm': 'research team member',
    'sad': 'scientific advisor',
    'sce': 'scenarist',
    'scl': 'sculptor',
    'scr': 'scribe',
    'sds': 'sound designer',
    'sec': 'secretary',
    'sgd': 'stage director',
    'sgn': 'signer',
    'sht': 'supporting host',
    'sll': 'seller',
    'sng': 'singer',
    'spk': 'speaker',
    'spn': 'sponsor',
    'spy': 'second party',
    'srv': 'surveyor',
    'std': 'set designer',
    'stg': 'setting',
    'stl': 'storyteller',
    'stm': 'stage manager',
    'stn': 'standards body',
    'str': 'stereotyper',
    'tcd': 'technical director',
    'tch': 'teacher',
    'ths': 'thesis advisor',
    'tld': 'television director',
    'tlp': 'television producer',
    'trc': 'transcriber',
    'trl': 'translator',
    'tyd': 'type designer',
    'tyg': 'typographer',
    'uvp': 'university place',
    'vac': 'voice actor',
    'vdg': 'videographer',
    'voc': 'vocalist',
    'wac': 'writer of added commentary',
    'wal': 'writer of added lyrics',
    'wam': 'writer of accompanying material',
    'wat': 'writer of added text',
    'wdc': 'woodcutter',
    'wde': 'wood engraver',
    'win': 'writer of introduction',
    'wit': 'witness',
    'wpr': 'writer of preface',
    'wst': 'writer of supplementary textual content'
}

# MARC Language Codes,
# from https://www.loc.gov/marc/languages/language_code.html
LANGUAGE_CODES = {
    'aar': 'Afar',
    'abk': 'Abkhaz',
    'ace': 'Achinese',
    'ach': 'Acoli',
    'ada': 'Adangme',
    'ady': 'Adygei',
    'afa': 'Afroasiatic (Other)',
    'afh': 'Afrihili (Artificial language)',
    'afr': 'Afrikaans',
    'ain': 'Ainu',
    'ajm': 'Aljamía',
    'aka': 'Akan',
    'akk': 'Akkadian',
    'alb': 'Albanian',
    'ale': 'Aleut',
    'alg': 'Algonquian (Other)',
    'alt': 'Altai',
    'amh': 'Amharic',
    'ang': 'English, Old (ca. 450-1100)',
    'anp': 'Angika',
    'apa': 'Apache languages',
    'ara': 'Arabic',
    'arc': 'Aramaic',
    'arg': 'Aragonese',
    'arm': 'Armenian',
    'arn': 'Mapuche',
    'arp': 'Arapaho',
    'art': 'Artificial (Other)',
    'arw': 'Arawak',
    'asm': 'Assamese',
    'ast': 'Bable',
    'ath': 'Athapascan (Other)',
    'aus': 'Australian languages',
    'ava': 'Avaric',
    'ave': 'Avestan',
    'awa': 'Awadhi',
    'aym': 'Aymara',
    'aze': 'Azerbaijani',
    'bad': 'Banda languages',
    'bai': 'Bamileke languages',
    'bak': 'Bashkir',
    'bal': 'Baluchi',
    'bam': 'Bambara',
    'ban': 'Balinese',
    'baq': 'Basque',
    'bas': 'Basa',
    'bat': 'Baltic (Other)',
    'bej': 'Beja',
    'bel': 'Belarusian',
    'bem': 'Bemba',
    'ben': 'Bengali',
    'ber': 'Berber (Other)',
    'bho': 'Bhojpuri',
    'bih': 'Bihari (Other)',
    'bik': 'Bikol',
    'bin': 'Edo',
    'bis': 'Bislama',
    'bla': 'Siksika',
    'bnt': 'Bantu (Other)',
    'bos': 'Bosnian',
    'bra': 'Braj',
    'bre': 'Breton',
    'btk': 'Batak',
    'bua': 'Buriat',
    'bug': 'Bugis',
    'bul': 'Bulgarian',
    'bur': 'Burmese',
    'byn': 'Bilin',
    'cad': 'Caddo',
    'cai': 'Central American Indian (Other)',
    'cam': 'Khmer',
    'car': 'Carib',
    'cat': 'Catalan',
    'cau': 'Caucasian (Other)',
    'ceb': 'Cebuano',
    'cel': 'Celtic (Other)',
    'cha': 'Chamorro',
    'chb': 'Chibcha',
    'che': 'Chechen',
    'chg': 'Chagatai',
    'chi': 'Chinese',
    'chk': 'Chuukese',
    'chm': 'Mari',
    'chn': 'Chinook jargon',
    'cho': 'Choctaw',
    'chp': 'Chipewyan',
    'chr': 'Cherokee',
    'chu': 'Church Slavic',
    'chv': 'Chuvash',
    'chy': 'Cheyenne',
    'cmc': 'Chamic languages',
    'cnr': 'Montenegrin',
    'cop': 'Coptic',
    'cor': 'Cornish',
    'cos': 'Corsican',
    'cpe': 'Creoles and Pidgins, English-based (Other)',
    'cpf': 'Creoles and Pidgins, French-based (Other)',
    'cpp': 'Creoles and Pidgins, Portuguese-based (Other)',
    'cre': 'Cree',
    'crh': 'Crimean Tatar',
    'crp': 'Creoles and Pidgins (Other)',
    'csb': 'Kashubian',
    'cus': 'Cushitic (Other)',
    'cze': 'Czech',
    'dak': 'Dakota',
    'dan': 'Danish',
    'dar': 'Dargwa',
    'day': 'Dayak',
    'del': 'Delaware',
    'den': 'Slavey',
    'dgr': 'Dogrib',
    'din': 'Dinka',
    'div': 'Divehi',
    'doi': 'Dogri',
    'dra': 'Dravidian (Other)',
    'dsb': 'Lower Sorbian',
    'dua': 'Duala',
    'dum': 'Dutch, Middle (ca. 1050-1350)',
    'dut': 'Dutch',
    'dyu': 'Dyula',
    'dzo': 'Dzongkha',
    'efi': 'Efik',
    'egy': 'Egyptian',
    'eka': 'Ekajuk',
    'elx': 'Elamite',
    'eng': 'English',
    'enm': 'English, Middle (1100-1500)',
    'epo': 'Esperanto',
    'esk': 'Eskimo languages',
    'esp': 'Esperanto',
    'est': 'Estonian',
    'eth': 'Ethiopic',
    'ewe': 'Ewe',
    'ewo': 'Ewondo',
    'fan': 'Fang',
    'fao': 'Faroese',
    'far': 'Faroese',
    'fat': 'Fanti',
    'fij': 'Fijian',
    'fil': 'Filipino',
    'fin': 'Finnish',
    'fiu': 'Finno-Ugrian (Other)',
    'fon': 'Fon',
    'fre': 'French',
    'fri': 'Frisian',
    'frm': 'French, Middle (ca. 1300-1600)',
    'fro': 'French, Old (ca. 842-1300)',
    'frr': 'North Frisian',
    'frs': 'East Frisian',
    'fry': 'Frisian',
    'ful': 'Fula',
    'fur': 'Friulian',
    'gaa': 'Gã',
    'gae': 'Scottish Gaelix',
    'gag': 'Galician',
    'gal': 'Oromo',
    'gay': 'Gayo',
    'gba': 'Gbaya',
    'gem': 'Germanic (Other)',
    'geo': 'Georgian',
    'ger': 'German',
    'gez': 'Ethiopic',
    'gil': 'Gilbertese',
    'gla': 'Scottish Gaelic',
    'gle': 'Irish',
    'glg': 'Galician',
    'glv': 'Manx',
    'gmh': 'German, Middle High (ca. 1050-1500)',
    'goh': 'German, Old High (ca. 750-1050)',
    'gon': 'Gondi',
    'gor': 'Gorontalo',
    'got': 'Gothic',
    'grb': 'Grebo',
    'grc': 'Greek, Ancient (to 1453)',
    'gre': 'Greek, Modern (1453-)',
    'grn': 'Guarani',
    'gsw': 'Swiss German',
    'gua': 'Guarani',
    'guj': 'Gujarati',
    'gwi': 'Gwich\'in',
    'hai': 'Haida',
    'hat': 'Haitian French Creole',
    'hau': 'Hausa',
    'haw': 'Hawaiian',
    'heb': 'Hebrew',
    'her': 'Herero',
    'hil': 'Hiligaynon',
    'him': 'Western Pahari languages',
    'hin': 'Hindi',
    'hit': 'Hittite',
    'hmn': 'Hmong',
    'hmo': 'Hiri Motu',
    'hrv': 'Croatian',
    'hsb': 'Upper Sorbian',
    'hun': 'Hungarian',
    'hup': 'Hupa',
    'iba': 'Iban',
    'ibo': 'Igbo',
    'ice': 'Icelandic',
    'ido': 'Ido',
    'iii': 'Sichuan Yi',
    'ijo': 'Ijo',
    'iku': 'Inuktitut',
    'ile': 'Interlingue',
    'ilo': 'Iloko',
    'ina': 'Interlingua (International Auxiliary Language Association)',
    'inc': 'Indic (Other)',
    'ind': 'Indonesian',
    'ine': 'Indo-European (Other)',
    'inh': 'Ingush',
    'int': 'Interlingua (International Auxiliary Language Association)',
    'ipk': 'Inupiaq',
    'ira': 'Iranian (Other)',
    'iri': 'Irish',
    'iro': 'Iroquoian (Other)',
    'ita': 'Italian',
    'jav': 'Javanese',
    'jbo': 'Lojban (Artificial language)',
    'jpn': 'Japanese',
    'jpr': 'Judeo-Persian',
    'jrb': 'Judeo-Arabic',
    'kaa': 'Kara-Kalpak',
    'kab': 'Kabyle',
    'kac': 'Kachin',
    'kal': 'Kalâtdlisut',
    'kam': 'Kamba',
    'kan': 'Kannada',
    'kar': 'Karen languages',
    'kas': 'Kashmiri',
    'kau': 'Kanuri',
    'kaw': 'Kawi',
    'kaz': 'Kazakh',
    'kbd': 'Kabardian',
    'kha': 'Khasi',
    'khi': 'Khoisan (Other)',
    'khm': 'Khmer',
    'kho': 'Khotanese',
    'kik': 'Kikuyu',
    'kin': 'Kinyarwanda',
    'kir': 'Kyrgyz',
    'kmb': 'Kimbundu',
    'kok': 'Konkani',
    'kom': 'Komi',
    'kon': 'Kongo',
    'kor': 'Korean',
    'kos': 'Kosraean',
    'kpe': 'Kpelle',
    'krc': 'Karachay-Balkar',
    'krl': 'Karelian',
    'kro': 'Kru (Other)',
    'kru': 'Kurukh',
    'kua': 'Kuanyama',
    'kum': 'Kumyk',
    'kur': 'Kurdish',
    'kus': 'Kusaie',
    'kut': 'Kootenai',
    'lad': 'Ladino',
    'lah': 'Lahndā',
    'lam': 'Lamba (Zambia and Congo)',
    'lan': 'Occitan (post 1500)',
    'lao': 'Lao',
    'lap': 'Sami',
    'lat': 'Latin',
    'lav': 'Latvian',
    'lez': 'Lezgian',
    'lim': 'Limburgish',
    'lin': 'Lingala',
    'lit': 'Lithuanian',
    'lol': 'Mongo-Nkundu',
    'loz': 'Lozi',
    'ltz': 'Luxembourgish',
    'lua': 'Luba-Lulua',
    'lub': 'Luba-Katanga',
    'lug': 'Ganda',
    'lui': 'Luiseño',
    'lun': 'Lunda',
    'luo': 'Luo (Kenya and Tanzania)',
    'lus': 'Lushai',
    'mac': 'Macedonian',
    'mad': 'Madurese',
    'mag': 'Magahi',
    'mah': 'Marshallese',
    'mai': 'Maithili',
    'mak': 'Makasar',
    'mal': 'Malayalam',
    'man': 'Mandingo',
    'mao': 'Maori',
    'map': 'Austronesian (Other)',
    'mar': 'Marathi',
    'mas': 'Maasai',
    'max': 'Manx',
    'may': 'Malay',
    'mdf': 'Moksha',
    'mdr': 'Mandar',
    'men': 'Mende',
    'mga': 'Irish, Middle (ca. 1100-1550)',
    'mic': 'Micmac',
    'min': 'Minangkabau',
    'mis': 'Miscellaneous languages',
    'mkh': 'Mon-Khmer (Other)',
    'mla': 'Malagasy',
    'mlg': 'Malagasy',
    'mlt': 'Maltese',
    'mnc': 'Manchu',
    'mni': 'Manipuri',
    'mno': 'Manobo languages',
    'moh': 'Mohawk',
    'mol': 'Moldavian',
    'mon': 'Mongolian',
    'mos': 'Mooré',
    'mul': None,
    'mun': 'Munda (Other)',
    'mus': 'Creek',
    'mwl': 'Mirandese',
    'mwr': 'Marwari',
    'myn': 'Mayan languages',
    'myv': 'Erzya',
    'nah': 'Nahuatl',
    'nai': 'North American Indian (Other)',
    'nap': 'Neapolitan Italian',
    'nau': 'Nauru',
    'nav': 'Navajo',
    'nbl': 'Ndebele (South Africa)',
    'nde': 'Ndebele (Zimbabwe)',
    'ndo': 'Ndonga',
    'nds': 'Low German',
    'nep': 'Nepali',
    'new': 'Newari',
    'nia': 'Nias',
    'nic': 'Niger-Kordofanian (Other)',
    'niu': 'Niuean',
    'nno': 'Norwegian (Nynorsk)',
    'nob': 'Norwegian (Bokmål)',
    'nog': 'Nogai',
    'non': 'Old Norse',
    'nor': 'Norwegian',
    'nqo': 'N\'Ko',
    'nso': 'Northern Sotho',
    'nub': 'Nubian languages',
    'nwc': 'Newari, Old',
    'nya': 'Nyanja',
    'nym': 'Nyamwezi',
    'nyn': 'Nyankole',
    'nyo': 'Nyoro',
    'nzi': 'Nzima',
    'oci': 'Occitan (post-1500)',
    'oji': 'Ojibwa',
    'ori': 'Oriya',
    'orm': 'Oromo',
    'osa': 'Osage',
    'oss': 'Ossetic',
    'ota': 'Turkish, Ottoman',
    'oto': 'Otomian languages',
    'paa': 'Papuan (Other)',
    'pag': 'Pangasinan',
    'pal': 'Pahlavi',
    'pam': 'Pampanga',
    'pan': 'Panjabi',
    'pap': 'Papiamento',
    'pau': 'Palauan',
    'peo': 'Old Persian (ca. 600-400 B.C.)',
    'per': 'Persian',
    'phi': 'Philippine (Other)',
    'phn': 'Phoenician',
    'pli': 'Pali',
    'pol': 'Polish',
    'pon': 'Pohnpeian',
    'por': 'Portuguese',
    'pra': 'Prakrit languages',
    'pro': 'Provençal (to 1500)',
    'pus': 'Pushto',
    'que': 'Quechua',
    'raj': 'Rajasthani',
    'rap': 'Rapanui',
    'rar': 'Rarotongan',
    'roa': 'Romance (Other)',
    'roh': 'Raeto-Romance',
    'rom': 'Romani',
    'rum': 'Romanian',
    'run': 'Rundi',
    'rup': 'Aromanian',
    'rus': 'Russian',
    'sad': 'Sandawe',
    'sag': 'Sango (Ubangi Creole)',
    'sah': 'Yakut',
    'sai': 'South American Indian (Other)',
    'sal': 'Salishan languages',
    'sam': 'Samaritan Aramaic',
    'san': 'Sanskrit',
    'sao': 'Samoan',
    'sas': 'Sasak',
    'sat': 'Santali',
    'scc': 'Serbian',
    'scn': 'Sicilian Italian',
    'sco': 'Scots',
    'scr': 'Croatian',
    'sel': 'Selkup',
    'sem': 'Semitic (Other)',
    'sga': 'Irish, Old (to 1100)',
    'sgn': 'Sign languages',
    'shn': 'Shan',
    'sho': 'Shona',
    'sid': 'Sidamo',
    'sin': 'Sinhalese',
    'sio': 'Siouan (Other)',
    'sit': 'Sino-Tibetan (Other)',
    'sla': 'Slavic (Other)',
    'slo': 'Slovak',
    'slv': 'Slovenian',
    'sma': 'Southern Sami',
    'sme': 'Northern Sami',
    'smi': 'Sami',
    'smj': 'Lule Sami',
    'smn': 'Inari Sami',
    'smo': 'Samoan',
    'sms': 'Skolt Sami',
    'sna': 'Shona',
    'snd': 'Sindhi',
    'snh': 'Sinhalese',
    'snk': 'Soninke',
    'sog': 'Sogdian',
    'som': 'Somali',
    'son': 'Songhai',
    'sot': 'Sotho',
    'spa': 'Spanish',
    'srd': 'Sardinian',
    'srn': 'Sranan',
    'srp': 'Serbian',
    'srr': 'Serer',
    'ssa': 'Nilo-Saharan (Other)',
    'sso': 'Sotho',
    'ssw': 'Swazi',
    'suk': 'Sukuma',
    'sun': 'Sundanese',
    'sus': 'Susu',
    'sux': 'Sumerian',
    'swa': 'Swahili',
    'swe': 'Swedish',
    'swz': 'Swazi',
    'syc': 'Syriac',
    'syr': 'Syriac, Modern',
    'tag': 'Tagalog',
    'tah': 'Tahitian',
    'tai': 'Tai (Other)',
    'taj': 'Tajik',
    'tam': 'Tamil',
    'tar': 'Tatar',
    'tat': 'Tatar',
    'tel': 'Telugu',
    'tem': 'Temne',
    'ter': 'Terena',
    'tet': 'Tetum',
    'tgk': 'Tajik',
    'tgl': 'Tagalog',
    'tha': 'Thai',
    'tib': 'Tibetan',
    'tig': 'Tigré',
    'tir': 'Tigrinya',
    'tiv': 'Tiv',
    'tkl': 'Tokelauan',
    'tlh': 'Klingon (Artificial language)',
    'tli': 'Tlingit',
    'tmh': 'Tamashek',
    'tog': 'Tonga (Nyasa)',
    'ton': 'Tongan',
    'tpi': 'Tok Pisin',
    'tru': 'Truk',
    'tsi': 'Tsimshian',
    'tsn': 'Tswana',
    'tso': 'Tsonga',
    'tsw': 'Tswana',
    'tuk': 'Turkmen',
    'tum': 'Tumbuka',
    'tup': 'Tupi languages',
    'tur': 'Turkish',
    'tut': 'Altaic (Other)',
    'tvl': 'Tuvaluan',
    'twi': 'Twi',
    'tyv': 'Tuvinian',
    'udm': 'Udmurt',
    'uga': 'Ugaritic',
    'uig': 'Uighur',
    'ukr': 'Ukrainian',
    'umb': 'Umbundu',
    'und': None,
    'urd': 'Urdu',
    'uzb': 'Uzbek',
    'vai': 'Vai',
    'ven': 'Venda',
    'vie': 'Vietnamese',
    'vol': 'Volapük',
    'vot': 'Votic',
    'wak': 'Wakashan languages',
    'wal': 'Wolayta',
    'war': 'Waray',
    'was': 'Washoe',
    'wel': 'Welsh',
    'wen': 'Sorbian (Other)',
    'wln': 'Walloon',
    'wol': 'Wolof',
    'xal': 'Oirat',
    'xho': 'Xhosa',
    'yao': 'Yao (Africa)',
    'yap': 'Yapese',
    'yid': 'Yiddish',
    'yor': 'Yoruba',
    'ypk': 'Yupik languages',
    'zap': 'Zapotec',
    'zbl': 'Blissymbolics',
    'zen': 'Zenaga',
    'zha': 'Zhuang',
    'znd': 'Zande languages',
    'zul': 'Zulu',
    'zun': 'Zuni',
    'zxx': None,
    'zza': 'Zaza',
}
