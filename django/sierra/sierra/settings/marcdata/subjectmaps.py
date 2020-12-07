"""
Map subject headings to subject facet values.
"""

from __future__ import unicode_literals
import re


war_words = '(?:war|revolution)'


# Use LCSH_SUBDIVISION_PATTERNS to match certain pattern-based LCSH
# subdivisions and map those to topic facet values.
# Note that the third sub-entry in each main entry is an example
# subdivision string that matches the given pattern (first sub-entry).
# This is here for testing purposes.
# TO USE: Use the `lcsh_sd_to_facet_values` convenience function.
LCSH_SUBDIVISION_PATTERNS = [
    [r'annexation to (.+)', 
        [('topic', 'Annexation (International law)'), ('region', '{}')],
        'Annexation to the United States'],
    [r'art and(?: the)? {}'.format(war_words),
        [('topic','Art and war')],
        'Art and the war'],
    [r'claims vs (.+)',
        [('topic', 'Claims'), ('topic', '{}')],
        'Claims vs. John Doe'],
    [r'dependency on (?!foreign countries)(.+)',
        [('topic', 'Dependency'), ('region', '{}')],
        'Dependency on the United States'],
    [r'education and(?: the)? {}'.format(war_words),
        [('topic', 'War and education')],
        'Education and the War'],
    [r'(elections, .+)',
        [('topic', 'Elections'), ('topic', '{}')],
        'Elections, 2016'],
    [r'(eruption, .+)',
        [('topic', 'Volcanic eruptions'), ('topic', '{}')],
        'Eruption, 1980'],
    [r'(explosion, .+)',
        [('topic', 'Explosions'), ('topic', '{}')],
        'Explosion, 2019'],
    [r'(fire, .+)',
        [('topic', 'Fires'), ('topic', '{}')],
        'Fire, 2020'],
    [r'literature and(?: the)? {}'.format(war_words),
        [('topic', 'War and literature')],
        'Literature and War'],
    [r'mass media and(?: the)? {}'.format(war_words),
        [('topic', 'Mass media and war')],
        'Mass media and the revolution'],
    [r'motion pictures and(?: the)? {}'.format(war_words),
        [('topic', 'Anti-war films'), ('topic', 'War films'),
         ('topic', 'Motion pictures and war'),
         ('topic', 'War and motion pictures')],
        'Motion pictures and the war'],
    [r'music and(?: the)? {}'.format(war_words),
        [('topic', 'Music and war')],
        'Music and the war'],
    [r'paraphrases, (.+)',
        [('topic', 'Paraphrases'), ('topic', '{} language')],
        'Paraphrases, French'],
    [r'participation, (.+)',
        [('topic', '{} participation [in wars]')],
        'Participation, Bhuddist'],
    [r'radio broadcasting and(?: the)? {}'.format(war_words),
        [('topic', 'Radio broadcasting and war')],
        'Radio broadcasting and the war'],
    [r'relations with (.+)',
        [('topic', 'Interpersonal relations'), ('topic', '{}')],
        'Relations with employees'],
    [r'(riot, .+)',
        [('topic', 'Riots'), ('topic', '{}')],
        'Riot, 1979'],
    [r'(student strike, .+)',
        [('topic', 'Student strikes'), ('topic', '{}')],
        'Student strike, 1984'],
    [r'television and(?: the)? {}'.format(war_words),
        [('topic', 'Anti-war television programs'),
         ('topic', 'War television programs'), ('topic', 'Television and war'),
         ('topic', 'War and television')],
         'Television and the war'],
    [r'theater and(?: the)? {}'.format(war_words),
        [('topic', 'War and theater')],
        'Theater and the war'],
    [r'transliteration into (.+)',
        [('topic', 'Transliteration'), ('topic', '{} language')],
        'Translisteration into English'],
]


# Use LCSH_SUBDIVISION_TERM_MAP to map individual subdivisions (which
# may be ambiguous or otherwise not great as standalone subjects) to a
# better term or set of terms. In most cases the mapped terms are non-
# subdivision LCSH headings. The idea is that, rather than doing a
# naive mapping of complex subjects, where subdivisions translate
# exactly to topics, each subdivision (in some cases considering multi-
# subdivision subdivisions) may map to one or more different topics, as
# found below.
#
# Examples:
#
#   "Corn -- Diseases and pests -- Control" maps to:
#
#     Corn
#     Agricultural pests
#     Plant diseases
#     Plant parasites
#     Pest control
#
#   "Skyscrapers -- Heating and ventilation -- Control" maps to:
#
#     Skyscrapers
#     Heating
#     Ventilation
#     Control (Heating)
#
# To use, split an LCSH subject into separate terms (based on "--" or
# MARC subfield). Leave the main/first term as-is. For each subsequent
# term (subdivision):
# 
#   - Call the `lcsh_to_facet_values` function on that subdivision.
#     Pass in any parent subdivisions as the `sd_parents` argument. Use
#     the subfield tag to determine the right `default_type` -- $x is
#     topic, $y is region, $z is era, and $v is form.
#   - Return value will be a list of (`facet_type`, `new_term`) tuples.
#   - Add this subdivision to `sd_parents` (to track parents for the
#     next subdivision).
LCSH_SUBDIVISION_TERM_MAP = {
    '16th century': {
        'parents': {
            'economic conditions': [
                'Economic history',
            ],
            'history': [
                'History, Modern',
            ],
            'history military': [
                'Military history, Modern',
            ],
            'history of doctrines': [
                'Theology, Doctrinal',
                'History',
            ],
            'intellectual life': [
                'Intellectual life',
                'History',
            ],
            'politics and government': [
                'World politics',
            ],
            'religion': [
                'Religious history',
            ],
            'social conditions': [
                'Social history',
            ],
            'social life and customs': [
                'Manners and customs',
                'History',
            ],
        },
    },
    '17th century': {
        'parents': {
            'economic conditions': [
                'Economic history',
            ],
            'history': [
                'History, Modern',
            ],
            'history military': [
                'Military history, Modern',
            ],
            'history of doctrines': [
                'Theology, Doctrinal',
                'History',
            ],
            'intellectual life': [
                'Intellectual life',
                'History',
            ],
            'politics and government': [
                'World politics',
            ],
            'religion': [
                'Religious history',
            ],
            'social conditions': [
                'Social history',
            ],
            'social life and customs': [
                'Manners and customs',
                'History',
            ],
        },
    },
    '18th century': {
        'parents': {
            'economic conditions': [
                'Economic history',
            ],
            'history': [
                'History, Modern',
            ],
            'history military': [
                'Military history, Modern',
            ],
            'history of doctrines': [
                'Theology, Doctrinal',
                'History',
            ],
            'intellectual life': [
                'Intellectual life',
                'History',
            ],
            'politics and government': [
                'World politics',
            ],
            'religion': [
                'Religious history',
            ],
            'social conditions': [
                'Social history',
            ],
            'social life and customs': [
                'Manners and customs',
                'History',
            ],
        },
    },
    '19th century': {
        'parents': {
            'economic conditions': [
                'Economic history',
            ],
            'history': [
                'History, Modern',
            ],
            'history military': [
                'Military history, Modern',
            ],
            'history of doctrines': [
                'Theology, Doctrinal',
                'History',
            ],
            'intellectual life': [
                'Intellectual life',
                'History',
            ],
            'politics and government': [
                'World politics',
            ],
            'religion': [
                'Religious history',
            ],
            'social conditions': [
                'Social history',
            ],
            'social life and customs': [
                'Manners and customs',
                'History',
            ],
        },
    },
    '20th century': {
        'parents': {
            'civilization': [
                'Civilization, Modern',
            ],
            'economic conditions': [
                'Economic history',
            ],
            'history': [
                'History, Modern',
            ],
            'history military': [
                'Military history, Modern',
            ],
            'history naval': [
                'Naval history, Modern',
            ],
            'history of doctrines': [
                'Theology, Doctrinal',
                'History',
            ],
            'intellectual life': [
                'Intellectual life',
                'History',
            ],
            'politics and government': [
                'World politics',
            ],
            'religion': [
                'Religious history',
            ],
            'social conditions': [
                'Social history',
            ],
            'social life and customs': [
                'Manners and customs',
            ],
        },
    },
    '21st century': {
        'parents': {
            'civilization': [
                'Civilization, Modern',
            ],
            'economic conditions': [
                'Economic history',
            ],
            'history': [
                'History, Modern',
            ],
            'history military': [
                'Military history, Modern',
            ],
            'history naval': [
                'Naval history, Modern',
            ],
            'history of doctrines': [
                'Theology, Doctrinal',
                'History',
            ],
            'intellectual life': [
                'Intellectual life',
                'History',
            ],
            'politics and government': [
                'World politics',
            ],
            'religion': [
                'Religious history',
            ],
            'social conditions': [
                'Social history',
            ],
            'social life and customs': [
                'Manners and customs',
            ],
        },
    },
    'abandonment': {
        'parents': {
            'nests': [
                'Abandonment of nests',
            ],
        },
    },
    'abbreviations of titles': {
        'headings': [
            'Abbreviations of titles (Periodicals)',
        ]
    },
    'abdication': {
        'headings': [
            'Abdication [of popes, kings, rulers, etc.]',
        ],
    },
    'ability testing': {
        'headings': [
            'Ability',
            'Testing',
        ],
    },
    'abnormalities': {
        'headings': [
            'Abnormalities (Biology)',
        ],
    },
    'absolute constructions': {
        'headings': [
            'Absolute constructions (Grammar)',
        ],
    },
    'absorption and adsorption': {
        'headings': [
            'Absorption',
            'Adsorption',
        ],
    },
    'abstracting and indexing': {
        'headings': [
            'Abstracting',
            'Indexing',
        ],
    },
    'abuse of': {
        'headings': [
            'Offenses against the person',
        ],
    },
    'accidents': {
        'headings': [
            'Accidents [e.g., industrial, space vehicles, etc.]',
        ],
    },
    'accreditation': {
        'headings': [
            'Accreditation (Education)',
        ],
    },
    'acoustic properties': {
        'headings': [
            'Acoustic properties [of chemicals, materials, etc.]',
            'Sound',
        ],
    },
    'acoustics': {
        'headings': [
            'Acoustics and physics (Music)',
        ],
    },
    'acquisition': {
        'headings': [
            'Language acquisition',
        ],
    },
    'activity programs': {
        'parents': {
            'study and teaching': [
                'Activity programs in education',
            ],
            'study and teaching early childhood': [
                'Activity programs in education',
            ],
            'study and teaching elementary': [
                'Activity programs in education',
            ],
            'study and teaching higher': [
                'Activity programs in education',
            ],
            'study and teaching middle school': [
                'Activity programs in education',
            ],
            'study and teaching preschool': [
                'Activity programs in education',
            ],
            'study and teaching primary': [
                'Activity programs in education',
            ],
            'study and teaching secondary': [
                'Activity programs in education',
            ],
        },
    },
    'adaptation': {
        'headings': [
            'Adaptation (Physiology)',
        ],
    },
    'adaptations': {
        'headings': [
            'Adaptations (Literature)',
        ],
    },
    'address forms of': {
        'headings': [
            'Forms of address',
        ],
    },
    'adjectivals': {
        'headings': [
            'Adjectivals (Grammar)',
        ],
    },
    'adjective': {
        'headings': [
            'Adjective',
        ],
    },
    'adjuvant treatment': {
        'headings': [
            'Combined modality therapy',
        ],
    },
    'administration': {
        'parents': {
            'drugs': [
                'Administration (Drugs)',
            ],
            'colonies': [
                'Administration of colonies',
            ],
            'therapeutic use': [
                'Administration (Drugs)',
            ],
        },
    },
    'admission': {
        'headings': [
            'Universities and colleges',
            'Admission into universities and colleges',
        ],
    },
    'adverb': {
        'headings': [
            'Adverb (Grammar)',
        ],
    },
    'adverbials': {
        'headings': [
            'Adverbials (Grammar)',
        ],
    },
    'adversaries': {
        'headings': [
            'Enemies',
            'Adversaries',
        ],
    },
    'aerial exploration': {
        'headings': [
            'Discoveries in geography',
            'Aerial exploration',
        ],
    },
    'aerial film and video footage': {
        'headings': [
            'Aerial cinematography',
            'Aerial videography',
        ],
    },
    'aerial operations': {
        'headings': [
            'Aeronautics, Military',
            'Air warfare',
            'Naval aviation',
        ],
    },
    'affixes': {
        'headings': [
            'Affixes (Grammar)',
        ],
    },
    'age': {
        'headings': [
            'Age [of plants, animals, etc.]',
        ],
    },
    'age determination': {
        'headings': [
            'Age determination [of plants, animals, etc.]',
        ],
    },
    'age differences': {
        'headings': [
            'Language and languages',
            'Age differences (Linguistics)',
        ],
    },
    'age factors': {
        'headings': [
            'Age factors in disease',
        ],
    },
    'agonists': {
        'headings': [
            'Chemical agonists',
        ],
    },
    'agreement': {
        'headings': [
            'Agreement (Grammar)',
        ],
    },
    'air police': {
        'headings': [
            'Military police',
            'Air police',
        ],
    },
    'alcohol use': {
        'headings': [
            'Alcoholism',
            'Drinking of alcoholic beverages',
        ],
    },
    'allergenicity': {
        'headings': [
            'Allergens',
        ],
    },
    'alluvial plain': {
        'headings': [
            'Alluvial plains',
        ],
    },
    'alternative treatment': {
        'headings': [
            'Alternative medicine',
        ],
    },
    'amphibious operations': {
        'headings': [
            'Amphibious warfare',
        ],
    },
    'analysis': {
        'headings': [
            'Chemistry, Analytic',
        ],
    },
    'analysis appreciation': {
        'headings': [
            'Music appreciation',
            'Musical analysis',
        ],
    },
    'anaphora': {
        'headings': [
            'Anaphora (Linguistics)',
        ],
    },
    'anatomy': {
        'headings': [
            'Anatomy (Biology)',
        ],
    },
    'animacy': {
        'headings': [
            'Animacy (Grammar)',
        ],
    },
    'animal models': {
        'headings': [
            'Animal models (Diseases)',
        ],
    },
    'anniversaries etc': {
        'headings': [
            'Anniversaries',
        ],
    },
    'antagonists': {
        'headings': [
            'Chemical inhibitors',
        ],
    },
    'antiaircraft artillery operations': {
        'headings': [
            'Antiaircraft artillery',
        ],
    },
    'antiquities': {
        'headings': [
            'Antiquities',
            'Classical antiquities',
            'Archaeological sites',
        ],
    },
    'antiquities byzantine': {
        'headings': [
            'Byzantine antiquities',
        ],
    },
    'antiquities celtic': {
        'headings': [
            'Celtic antiquities',
        ],
    },
    'antiquities germanic': {
        'headings': [
            'Germanic antiquities',
        ],
    },
    'antiquities phoenician': {
        'headings': [
            'Phoenician antiquities',
        ],
    },
    'antiquities roman': {
        'headings': [
            'Rome',
            'Antiquities, Roman',
        ],
    },
    'antiquities slavic': {
        'headings': [
            'Slavic antiquities',
        ],
    },
    'antiquities turkish': {
        'headings': [
            'Turkish antiquities',
        ],
    },
    'apologetic works': {
        'headings': [
            'Apologetics',
        ],
    },
    'appointment call and election': {
        'headings': [
            'Appointment, call, and election (Clergy)',
        ],
    },
    'appointments and retirements': {
        'headings': [
            'Retired military personnel',
            'Military appointments',
            'Military retirements',
        ],
    },
    'apposition': {
        'headings': [
            'Apposition (Grammar)',
        ],
    },
    'appreciation': {
        'headings': [
            'Appreciation [of art, music, literature, etc.]',
        ],
    },
    'appropriations and expenditures': {
        'headings': [
            'Expenditures, Public',
            'Finance, Public',
            'Appropriations and expenditures',
        ],
    },
    'archaeological collections': {
        'headings': [
            'Archaeological museums and collections',
        ],
    },
    'archaisms': {
        'headings': [
            'Archaisms (Linguistics)',
        ],
    },
    'area': {
        'headings': [
            'Area measurement',
        ],
    },
    'art collections': {
        'headings': [
            'Art',
            'Art collections',
        ],
    },
    'art patronage': {
        'headings': [
            'Art patronage',
            'Art patrons',
        ],
    },
    'article': {
        'headings': [
            'Article',
        ],
    },
    'artificial growing media': {
        'headings': [
            'Plant growing media, Artificial',
        ],
    },
    'artillery operations': {
        'headings': [
            'Artillery',
        ],
    },
    'aspect': {
        'headings': [
            'Aspect (Grammar)',
        ],
    },
    'aspiration': {
        'headings': [
            'Aspiration (Phonetics)',
        ],
    },
    'assassination attempts': {
        'headings': [
            'Attempted assassination',
        ],
    },
    'asyndeton': {
        'headings': [
            'Asyndeton (Grammar)',
        ],
    },
    'atlases': {
        'headings': [
            'Atlases, Scientific',
        ],
    },
    'atrocities': {
        'headings': [
            'Atrocities',
            'Military atrocities',
        ],
    },
    'attendance': {
        'parents': {
            'congresses': [
                'Attendance of congresses and conventions',
            ],
        },
    },
    'attitudes': {
        'headings': [
            'Attitude (Psychology)',
            'Public opinion',
        ],
    },
    'audio adaptations': {
        'headings': [
            'Audio adaptations of literature',
        ],
    },
    'audiocassette catalogs': {
        'headings': [
            'Audiocassettes',
            'Catalogs',
        ],
    },
    'audiotape catalogs': {
        'headings': [
            'Audiotapes',
            'Catalogs',
        ],
    },
    'audiovisual aids': {
        'headings': [
            'Audio-visual materials',
            'Audio-visual aids',
        ],
    },
    'augmentatives': {
        'headings': [
            'Augmentatives (Grammar)',
        ],
    },
    'autonomous communities': {
        'headings': [
            'Spanish autonomous communities',
        ],
    },
    'autonomous regions': {
        'headings': [
            'Chinese autonomous regions',
        ],
    },
    'autonomy and independence movements': {
        'parents': {
            'history': [
                'Separatist movements',
                'Autonomy and independence movements',
            ],
        },
    },
    'autopsy': {
        'headings': [
            'Veterinary autopsy',
        ],
    },
    'auxiliary verbs': {
        'headings': [
            'Auxiliaries (Grammar)',
        ],
    },
    'aviation': {
        'headings': [
            'Aeronautics, Military',
        ],
    },
    'aviation supplies and stores': {
        'headings': [
            'Equipment and supplies (Military aeronautics)',
        ],
    },
    'ayurvedic treatment': {
        'headings': [
            'Medicine, Ayurvedic',
        ],
    },
    'bands': {
        'headings': [
            'Bands (Music)',
        ],
    },
    'barracks and quarters': {
        'headings': [
            'Barracks',
        ],
    },
    'barrierfree design': {
        'parents': {
            'buildings': [
                'School buildings',
                'Barrier-free design',
            ],
        },
    },
    'batteries': {
        'headings': [
            'Storage batteries',
        ],
    },
    'bearings': {
        'headings': [
            'Bearings (Machinery)',
        ],
    },
    'behavior': {
        'headings': [
            'Animal behavior',
        ],
    },
    'benefices': {
        'headings': [
            'Benefices, Ecclesiastical',
        ],
    },
    'biblical teaching': {
        'headings': [
            'Bible',
            'Theology',
            'Biblical teaching',
        ],
    },
    'bilingual method': {
        'parents': {
            'study and teaching': [
                'Bilingual method (Language teaching)',
            ],
        },
    },
    'binomial': {
        'headings': [
            'Binomial (Linguistics)',
        ],
    },
    'biological control': {
        'headings': [
            'Biological pest control',
        ],
    },
    'birth': {
        'headings': [
            'Birthdays',
        ],
    },
    'birthplace': {
        'headings': [
            'Birthplaces',
        ],
    },
    'blockades': {
        'headings': [
            'Blockade',
        ],
    },
    'boats': {
        'headings': [
            'Boats and boating',
        ],
    },
    'bodies': {
        'headings': [
            'Bodies of motor vehicles',
        ],
    },
    'bonding': {
        'headings': [
            'Bonding (Employees)',
        ],
    },
    'bonsai collections': {
        'headings': [
            'Bonsai',
            'Catalogs',
        ],
    },
    'book reviews': {
        'headings': [
            'Books',
            'Book reviews',
        ],
    },
    'books and reading': {
        'headings': [
            'Books and reading',
            'Reading interests',
        ],
    },
    'boundaries': {
        'headings': [
            'Boundaries',
            'Boundary disputes',
        ],
    },
    'breeding': {
        'headings': [
            'Reproduction',
            'Breeding (Agriculture)',
        ],
    },
    'cadmium content': {
        'headings': [
            'Cadmium',
        ],
    },
    'camouflage': {
        'headings': [
            'Camouflage (Military science)',
        ],
    },
    'campaigns': {
        'headings': [
            'Battles',
            'Military campaigns',
        ],
    },
    'cannibalism': {
        'headings': [
            'Cannibalism in animals',
        ],
    },
    'capital and capitol': {
        'headings': [
            'Capitals (Cities)',
            'Capitols',
        ],
    },
    'carbon content': {
        'headings': [
            'Carbon',
            'Carbon content [of plants, animals, etc.]',
        ],
    },
    'carcasses': {
        'headings': [
            'Animal carcasses',
        ],
    },
    'care and hygiene': {
        'headings': [
            'Hygiene',
        ],
    },
    'caricatures and cartoons': {
        'headings': [
            'Caricatures and cartoons',
            'Wit and humor, Pictorial',
        ],
    },
    'case': {
        'headings': [
            'Case (Grammar)',
        ],
    },
    'cases': {
        'headings': [
            'Law reports, digests, etc.',
        ],
    },
    'casualties': {
        'headings': [
            'Battle casualties',
            'War casualties',
        ],
    },
    'catalogs': {
        'parents': {
            'bibliography': [
                'Catalogs, Bibliographic',
            ],
            'curricula': [
                'Universities and colleges',
                'Catalogs',
            ],
        },
    },
    'catalogs and collections': {
        'headings': [
            'Catalogs',
        ],
    },
    'caucuses': {
        'headings': [
            'Caucus',
            'Legislative service organizations',
        ],
    },
    'causative': {
        'headings': [
            'Causative (Linguistics)',
        ],
    },
    'causes': {
        'headings': [
            'Causes (War)',
        ],
    },
    'cavalry operations': {
        'headings': [
            'Cavalry',
        ],
    },
    'cdrom catalogs': {
        'headings': [
            'CD-ROMs',
            'Catalogs',
        ],
    },
    'censures': {
        'headings': [
            'Censures of public officers',
        ],
    },
    'certification': {
        'headings': [
            'Certification (Occupations)',
        ],
        'parents': {
            'seeds': [
                'Certification (Seeds)',
            ],
        },
    },
    'channelization': {
        'headings': [
            'Stream channelization',
        ],
    },
    'channels': {
        'headings': [
            'River channels',
        ],
    },
    'chaplains': {
        'headings': [
            'Chaplains',
            'Military chaplains',
        ],
    },
    'characters': {
        'headings': [
            'Characters and characteristics in literature',
        ],
    },
    'charters grants privileges': {
        'headings': [
            'Charters',
        ],
    },
    'charts diagrams etc': {
        'parents': {
            'fingering': [
                'Fingering charts (Musical instruments)',
            ],
        },
    },
    'chemical defenses': {
        'headings': [
            'Chemical defenses (Organisms)',
        ],
    },
    'chemical ecology': {
        'headings': [
            'Plant chemical ecology',
        ],
    },
    'chemotaxonomy': {
        'headings': [
            'Plant chemotaxonomy',
        ],
    },
    'chiropractic treatment': {
        'headings': [
            'Chiropractic',
        ],
    },
    'choral organizations': {
        'headings': [
            'Choral societies',
        ],
    },
    'chord diagrams': {
        'headings': [
            'Chords (Music)',
            'Tablature (Music)',
        ],
    },
    'cipher': {
        'headings': [
            'Ciphers',
        ],
    },
    'circulation': {
        'headings': [
            'Serial publications',
            'Circulation of serial publications',
        ],
    },
    'citizen participation': {
        'headings': [
            'Political participation',
            'Social action',
        ],
    },
    'civic action': {
        'headings': [
            'Armed Forces',
            'Civic action in armed forces',
        ],
    },
    'civilian relief': {
        'headings': [
            'Charities',
            'Civilian relief',
        ],
    },
    'cladistic analysis': {
        'headings': [
            'Cladistic analysis (Plants)',
        ],
    },
    'classifiers': {
        'headings': [
            'Classifiers (Linguistics)',
        ],
    },
    'clauses': {
        'headings': [
            'Clauses (Grammar)',
        ],
    },
    'climate': {
        'headings': [
            'Climatology',
        ],
    },
    'climatic factors': {
        'headings': [
            'Bioclimatology',
            'Climatology',
        ],
        'parents': {
            'behavior': [
                'Bioclimatology',
            ],
            'diseases': [
                'Bioclimatology',
            ],
            'feeding and feeds': [
                'Meteorology, Agricultural',
            ],
            'geographical distribution': [
                'Climatic factors of bigeography',
            ],
            'metabolism': [
                'Bioclimatology',
            ],
            'migration': [
                'Bioclimatology',
            ],
            'reproduction': [
                'Climatic factors of animal populations',
                'Bioclimatology',
            ],
            'seeds': [
                'Crops and climate',
                'Meteorology, Agricultural',
            ],
            'storage': [
                'Crops and climate',
                'Meteorology, Agricultural',
            ],
        },
    },
    'clitics': {
        'headings': [
            'Clitics (Grammar)',
        ],
    },
    'clones': {
        'headings': [
            'Clones (Plants)',
        ],
    },
    'clothing': {
        'headings': [
            'Clothing and dress',
        ],
    },
    'clutches': {
        'headings': [
            'Clutches (Machinery)',
        ],
    },
    'cobalt content': {
        'headings': [
            'Cobalt',
        ],
    },
    'collaboration': {
        'parents': {
            'authorship': [
                'Collaboration (Authorship)',
            ],
        },
    },
    'collection and preservation': {
        'headings': [
            'Collectors and collecting',
            'Preservation of materials',
            'Preservation of natural history specimens',
        ],
        'parents': {
            'antiquities': [
                'Collection and preservation of antiquities',
            ],
        },
    },
    'collective nouns': {
        'headings': [
            'Collective nouns (Grammar)',
        ],
    },
    'colonial forces': {
        'headings': [
            'Colonies',
            'Armies, Colonial',
        ],
    },
    'colonial influence': {
        'headings': [
            'Colonies',
            'Colonial influence',
        ],
    },
    'color': {
        'headings': [
            'Color [of plants, animals, etc.]',
        ],
    },
    'combat sustainability': {
        'headings': [
            'Combat sustainability (Military science)',
        ],
    },
    'committees': {
        'headings': [
            'Committees (Legislative bodies)',
        ],
    },
    'communication systems': {
        'headings': [
            'Telecommunication systems',
        ],
    },
    'communications': {
        'headings': [
            'Communications, Military',
            'Signals and signaling',
        ],
    },
    'compact disc catalogs': {
        'headings': [
            'Compact discs',
            'Catalogs',
        ],
    },
    'comparative clauses': {
        'headings': [
            'Comparative clauses (Grammar)',
        ],
    },
    'comparison': {
        'headings': [
            'Comparison (Grammar)',
        ],
    },
    'competitions': {
        'headings': [
            'Contests',
        ],
    },
    'complement': {
        'headings': [
            'Complement (Grammar)',
        ],
    },
    'complications': {
        'headings': [
            'Diseases',
            'Complications of diseases',
        ],
    },
    'composition': {
        'headings': [
            'Composition of natural substances',
        ],
    },
    'compound words': {
        'headings': [
            'Compound words (Grammar)',
        ],
    },
    'compression testing': {
        'headings': [
            'Testing',
            'Compression testing (Materials)',
        ],
    },
    'concessive clauses': {
        'headings': [
            'Concessive clauses (Grammar)',
        ],
    },
    'conditionals': {
        'headings': [
            'Conditionals (Grammar)',
        ],
    },
    'conference committees': {
        'headings': [
            'Legislative conference committees',
        ],
    },
    'confiscations and contributions': {
        'headings': [
            'Confiscations',
        ],
    },
    'congresses': {
        'headings': [
            'Congresses and conventions',
        ],
    },
    'conjunctions': {
        'headings': [
            'Conjunctions (Grammar)',
        ],
    },
    'connectives': {
        'headings': [
            'Connectives (Grammar)',
        ],
    },
    'conscript labor': {
        'headings': [
            'Forced labor',
            'Conscript labor',
        ],
    },
    'conservation': {
        'headings': [
            'Conservation of natural resources',
            'Nature conservation',
        ],
        'parents': {
            'habitat': [
                'Habitat conservation',
            ],
        },
    },
    'conservation and restoration': {
        'headings': [
            'Conservation and restoration',
            'Preservation and restoration',
            'Restoration and conservation',
        ],
    },
    'construction': {
        'headings': [
            'Construction (Musical instruments)',
        ],
    },
    'context': {
        'headings': [
            'Context (Linguistics)',
        ],
    },
    'contraction': {
        'headings': [
            'Muscle contraction',
        ],
    },
    'control': {
        'headings': [
            'Pest control',
        ],
        'parents': {
            'air conditioning': [
                'Control (Air conditioning)',
            ],
            'diseases and pests': [
                'Pest control',
            ],
            'heating and ventilation': [
                'Control (Heating)',
            ],
            'parasites': [
                'Pest control',
            ],
            'predators of': [
                'Control of predatory animals',
            ],
        },
    },
    'controlled release': {
        'headings': [
            'Controlled release (Drugs)',
        ],
    },
    'cooperative marketing': {
        'headings': [
            'Cooperative marketing of farm produce',
        ],
    },
    'coordinate constructions': {
        'headings': [
            'Coordinate constructions (Grammar)',
        ],
    },
    'copying': {
        'headings': [
            'Copying (Art)',
        ],
    },
    'coronation': {
        'headings': [
            'Coronations',
        ],
    },
    'corrosion': {
        'headings': [
            'Corrosion and anti-corrosives',
        ],
    },
    'corrupt practices': {
        'headings': [
            'Corruption',
        ],
    },
    'cost of operation': {
        'headings': [
            'Cost',
        ],
    },
    'costs': {
        'headings': [
            'Cost',
        ],
    },
    'counseling of': {
        'headings': [
            'Counseling',
        ],
    },
    'counterfeit money': {
        'headings': [
            'Counterfeits and conterfeiting',
        ],
    },
    'counting': {
        'headings': [
            'Counting [e.g. of plants and animals]',
        ],
    },
    'court and courtiers': {
        'headings': [
            'Courts and courtiers',
        ],
    },
    'cracking': {
        'headings': [
            'Fracture mechanics',
        ],
    },
    'crankshafts': {
        'parents': {
            'motors': [
                'Cranks and crankshafts',
            ],
        },
    },
    'crimes against': {
        'headings': [
            'Crime',
            'Offenses against the person',
        ],
    },
    'criminal provisions': {
        'headings': [
            'Criminal law',
        ],
    },
    'cryopreservation': {
        'headings': [
            'Cryopreservation of organs, tissues, etc.',
        ],
    },
    'cryotherapy': {
        'headings': [
            'Cold',
            'Therapeutic use of cold',
        ],
    },
    'cult': {
        'headings': [
            'Cults',
        ],
    },
    'cultural assimilation': {
        'headings': [
            'Acculturation',
            'Assimilation (Sociology)',
        ],
    },
    'cultural control': {
        'headings': [
            'Cultural pest control',
        ],
    },
    'cultures and culture media': {
        'headings': [
            'Culture media (Biology)',
            'Cultures (Biology)',
            'Organ culture',
        ],
    },
    'curricula': {
        'headings': [
            'Curricula (Education)',
        ],
    },
    'customs and practices': {
        'headings': [
            'Rites and ceremonies',
        ],
    },
    'cuttings': {
        'headings': [
            'Plant cuttings',
        ],
    },
    'cysts': {
        'headings': [
            'Cysts (Pathology)',
        ],
    },
    'cytopathology': {
        'headings': [
            'Pathology, Cellular',
        ],
    },
    'cytotaxonomy': {
        'headings': [
            'Cytotaxonomy',
            'Plant cytotaxonomy',
        ],
    },
    'data processing': {
        'headings': [
            'Electronic data processing',
        ],
    },
    'data tape catalogs': {
        'headings': [
            'Data tapes',
            'Catalogs',
        ],
    },
    'death and burial': {
        'headings': [
            'Burial',
            'Death',
        ],
    },
    'death mask': {
        'headings': [
            'Masks (Sculpture)',
        ],
    },
    'decay': {
        'headings': [
            'Radioactive decay',
        ],
    },
    'deception': {
        'headings': [
            'Deception (Military science)',
        ],
    },
    'decontamination': {
        'headings': [
            'Decontamination [from gases, chemicals, etc.]',
        ],
    },
    'decoration': {
        'parents': {
            'housing': [
                'Decoration and ornament',
            ],
        },
    },
    'defenses': {
        'parents': {
            'larvae': [
                'Animal defenses',
            ],
        },
    },
    'definiteness': {
        'headings': [
            'Definiteness (Linguistics)',
        ],
    },
    'degrees': {
        'headings': [
            'Degrees, Academic',
        ],
    },
    'deixis': {
        'headings': [
            'Deixis (Grammar)',
        ],
    },
    'deletion': {
        'headings': [
            'Deletion (Grammar)',
        ],
    },
    'demonstratives': {
        'headings': [
            'Demonstratives (Grammar)',
        ],
    },
    'denaturation': {
        'headings': [
            'Denaturation of proteins',
        ],
    },
    'denominative': {
        'headings': [
            'Denominative (Grammar)',
        ],
    },
    'dependency on foreign countries': {
        'headings': [
            'Dependency',
        ],
    },
    'deposition': {
        'parents': {
            'clergy': [
                'Deposition (Clergy)',
            ],
        },
    },
    'derivatives': {
        'headings': [
            'Derivatives (Chemicals)',
        ],
    },
    'description and travel': {
        'headings': [
            'Travel',
            'Voyages and travels',
        ],
    },
    'desertions': {
        'headings': [
            'Desertion, Military',
        ],
    },
    'design and construction': {
        'headings': [
            'Engineering design',
            'Design and construction',
        ],
    },
    'destruction and pillage': {
        'headings': [
            'Pillage',
        ],
    },
    'detection': {
        'headings': [
            'Detection of animals (Zoology)',
        ],
    },
    'deterioration': {
        'headings': [
            'Deterioration (Materials)',
        ],
    },
    'determiners': {
        'headings': [
            'Determiners (Grammar)',
        ],
    },
    'diagnostic use': {
        'headings': [
            'Diagnosis',
        ],
    },
    'dictionaries': {
        'headings': [
            'Encyclopedias and dictionaries',
        ],
        'parents': {
            'abbreviations': [
                'Abbreviations',
            ],
            'acronyms': [
                'Acronyms',
            ],
        },
    },
    'dictionaries juvenile': {
        'headings': [
            "Children's encyclopedias and dictionaries",
        ],
    },
    'differentiation': {
        'headings': [
            'Differentiation (Developmental biology)',
        ],
    },
    'digests': {
        'headings': [
            'Law reports, digests, etc.',
        ],
    },
    'digitization': {
        'headings': [
            'Digitization (Library materials)',
        ],
    },
    'diminutives': {
        'headings': [
            'Diminutives (Grammar)',
        ],
    },
    'diplomatic history': {
        'headings': [
            'Diplomacy',
        ],
    },
    'direct object': {
        'headings': [
            'Direct object (Grammar)',
        ],
    },
    'discography': {
        'headings': [
            'Sound recordings',
            'Catalogs',
            'Discography',
        ],
    },
    'discovery and exploration': {
        'headings': [
            'Discoveries in geography',
            'Voyages and travels',
        ],
    },
    'disease and pest resistance': {
        'headings': [
            'Resistance to diseases and pests (Plants)',
        ],
    },
    'disease resistance': {
        'headings': [
            'Natural immunity',
        ],
    },
    'diseasefree stock': {
        'headings': [
            'Stocks (Horticulture)',
        ],
    },
    'diseases': {
        'parents': {
            'employees': [
                'Occupational diseases',
            ],
        },
    },
    'diseases and pests': {
        'headings': [
            'Agricultural pests',
            'Plant diseases',
            'Plant parasites',
        ],
    },
    'dislocation': {
        'headings': [
            'Dislocations',
        ],
    },
    'dismissal of': {
        'headings': [
            'Dismissal (Employees)',
        ],
    },
    'dispersal': {
        'headings': [
            'Dispersal (Biology)',
        ],
    },
    'dissertations': {
        'headings': [
            'Dissertations, Academic',
        ],
    },
    'dissimilation': {
        'headings': [
            'Dissimilation (Phonetics)',
        ],
    },
    'distances etc': {
        'headings': [
            'Distances',
        ],
    },
    'doctrines': {
        'headings': [
            'Theology, Doctrinal',
        ],
    },
    'dormancy': {
        'headings': [
            'Dormancy (Biology)',
        ],
    },
    'doseresponse relationship': {
        'headings': [
            'Dose-response relationship (Biochemistry)',
        ],
    },
    'dosimetric treatment': {
        'headings': [
            'Medicine, Dosimetric',
        ],
    },
    'drawings': {
        'headings': [
            'Drawing',
            'Mechanical drawing',
        ],
    },
    'drill and tactics': {
        'headings': [
            'Miltary drill and tactics',
        ],
        'parents': {
            'artillery': [
                'Artillery drill and tactics',
            ],
            'cavalry': [
                'Cavalry drill and tactics',
            ],
            'infantry': [
                'Infantry drill and tactics',
            ],
        },
    },
    'drought tolerance': {
        'headings': [
            'Drought tolerance (Plants)',
        ],
    },
    'drug use': {
        'headings': [
            'Drug abuse',
            'Drug utilization',
        ],
    },
    'drying': {
        'headings': [
            'Drying [of materials, plants, etc.]',
        ],
    },
    'dwellings': {
        'headings': [
            'Architecture, Domestic',
            'Dwellings',
        ],
    },
    'dynamic testing': {
        'parents': {
            'materials': [
                'Testing',
                'Dynamic testing (Materials)',
            ],
        },
    },
    'early': {
        'parents': {
            'bibliography': [
                'Early bibliography',
            ],
        },
    },
    'early works to 1700': {
        'headings': [
            'Early printed books',
        ],
        'parents': {
            'maps': [
                'Early maps',
            ],
        },
    },
    'early works to 1800': {
        'headings': [
            'Early printed books',
        ],
        'parents': {
            'maps': [
                'Early maps',
            ],
        },
    },
    'earthquake effects': {
        'headings': [
            'Earthquakes',
            'Earthquake damage',
        ],
    },
    'eclectic treatment': {
        'headings': [
            'Medicine, Eclectic',
        ],
    },
    'ecology': {
        'parents': {
            'larvae': [
                'Ecology',
                'Animal ecology',
            ],
            'predators of': [
                'Ecology',
                'Animal ecology',
            ],
        },
    },
    'economic aspects': {
        'headings': [
            'Economics',
        ],
    },
    'economic conditions': {
        'headings': [
            'Economics',
        ],
    },
    'economic integration': {
        'headings': [
            'International economic integration',
        ],
    },
    'education': {
        'parents': {
            'kings and rulers': [
                'Education of princes',
            ],
        },
    },
    'education continuing education': {
        'headings': [
            'Continuing education',
        ],
    },
    'education early childhood': {
        'headings': [
            'Early childhood education',
        ],
    },
    'education graduate': {
        'headings': [
            'Universities and colleges',
            'Graduate work',
        ],
    },
    'education middle school': {
        'headings': [
            'Middle school education',
        ],
    },
    'effect of acid deposition on': {
        'headings': [
            'Effect of acid deposition [on plants]',
            'Acid deposition',
        ],
    },
    'effect of acid precipitation on': {
        'headings': [
            'Acid precipitation (Meteorology)',
            'Effect of acid precipitation [on plants and animals]',
        ],
    },
    'effect of air pollution on': {
        'headings': [
            'Effect of air pollution [on plants]',
            'Air',
            'Pollution',
        ],
    },
    'effect of aircraft on': {
        'headings': [
            'Effect of aircraft [on animals]',
        ],
    },
    'effect of altitude on': {
        'headings': [
            'Altitude, Influence of',
            'Effect of altitude [on plants and animals]',
        ],
    },
    'effect of aluminum sulfate on': {
        'headings': [
            'Effect of aluminum sulfate [on plants]',
            'Aluminum sulfate',
        ],
    },
    'effect of arsenic on': {
        'headings': [
            'Effect of arsenic [on plants]',
            'Arsenic',
        ],
    },
    'effect of atmospheric carbon dioxide on': {
        'headings': [
            'Effect of atmospheric carbon dioxide [on plants]',
            'Atmospheric carbon dioxide',
        ],
    },
    'effect of atmospheric deposition on': {
        'headings': [
            'Effect of atmospheric deposition [on plants]',
            'Atmospheric deposition',
        ],
    },
    'effect of atmospheric nitrogen dioxide on': {
        'headings': [
            'Effect of atmospheric nitrogen dioxide [on plants]',
            'Atmospheric nitrogen dioxide',
        ],
    },
    'effect of atmospheric ozone on': {
        'headings': [
            'Effect of atmospheric ozone [on plants]',
            'Atmospheric ozone',
        ],
    },
    'effect of automation on': {
        'headings': [
            'Automation',
            'Effect of automation [on employees]',
        ],
    },
    'effect of browsing on': {
        'headings': [
            'Browse (Animal food)',
            'Browsing (Animal behavior)',
            'Effect of animal browsing [on plants]',
        ],
    },
    'effect of cadmium on': {
        'headings': [
            'Effect of cadmium [on plants]',
            'Cadmium',
        ],
    },
    'effect of calcium on': {
        'headings': [
            'Effect of calcium [on plants]',
            'Calcium',
        ],
    },
    'effect of chemicals on': {
        'headings': [
            'Chemicals',
            'Physiology',
            'Effect of chemicals [on human or animal physiology]',
        ],
    },
    'effect of cold on': {
        'headings': [
            'Cold',
            'Effect of cold [on plants and animals]',
        ],
    },
    'effect of contaminated sediments on': {
        'headings': [
            'Contaminated sediments',
            'Effect of contaminated sediments',
        ],
    },
    'effect of dams on': {
        'headings': [
            'Dams',
            'Ecological effect of dams',
        ],
    },
    'effect of dichlorophenoxyacetic acid on': {
        'headings': [
            'Effect of dichlorophenoxyacetic acid [on plants]',
            'Dichlorophenoxyacetic acid',
        ],
    },
    'effect of dredging on': {
        'headings': [
            'Dredging',
            'Effect on dredging [on plants and animals]',
        ],
    },
    'effect of drought on': {
        'headings': [
            'Droughts',
            'Effect of drought [on plants]',
        ],
    },
    'effect of drugs on': {
        'headings': [
            'Pharmacology',
        ],
        'parents': {
            'receptors': [
                'Physiological effect of drugs',
            ],
        },
    },
    'effect of energy development on': {
        'headings': [
            'Energy development',
            'Ecological effect of energy development',
        ],
    },
    'effect of environment on': {
        'headings': [
            'Environmental effect [on materials, machinery, etc.]',
        ],
    },
    'effect of ethephon on': {
        'headings': [
            'Effect of ethephon [on plants]',
            'Ethephon',
        ],
    },
    'effect of exotic animals on': {
        'headings': [
            'Exotic animals',
            'Ecological effect of exotic animals',
        ],
    },
    'effect of factory and trade waste on': {
        'headings': [
            'Effect of factory and trade waste',
            'Pollution',
        ],
    },
    'effect of ferrous sulfate on': {
        'headings': [
            'Effect of ferrous sulfate [on plants]',
            'Ferrous sulfate',
        ],
    },
    'effect of fires on': {
        'headings': [
            'Fires',
            'Effect of fires [on plants and animals]',
        ],
    },
    'effect of fishing on': {
        'headings': [
            'Fisheries',
            'Ecological effect of fishing',
        ],
    },
    'effect of floods on': {
        'headings': [
            'Floods',
            'Effect of floods [on plants]',
        ],
    },
    'effect of fluorides on': {
        'headings': [
            'Flourides',
            'Effect of fluorides [on plants]',
        ],
    },
    'effect of fluorine on': {
        'headings': [
            'Fluorine',
            'Effect of fluorine [on plants]',
        ],
    },
    'effect of forest management on': {
        'headings': [
            'Forest management',
            'Effect of forest management',
        ],
    },
    'effect of freezes on': {
        'headings': [
            'Freezes (Meteorology)',
            'Effect of freezes [on plants]',
        ],
    },
    'effect of gamma rays on': {
        'headings': [
            'Gamma rays',
            'Effect of gamma rays [on plants]',
        ],
    },
    'effect of gases on': {
        'headings': [
            'Gases',
            'Effect of gases [on plants]',
        ],
    },
    'effect of global warming on': {
        'headings': [
            'Global warming',
            'Effect of global warming [on plants and animals]',
        ],
    },
    'effect of glyphosate on': {
        'headings': [
            'Glyphosate',
            'Effect of glyphosate [on plants]',
        ],
    },
    'effect of grazing on': {
        'headings': [
            'Effect of grazing [on plants]',
            'Grazing',
        ],
    },
    'effect of greenhouse gases on': {
        'headings': [
            'Greenhouse gases',
            'Effect of greenhouse gases [on plants]',
        ],
    },
    'effect of habitat modification on': {
        'headings': [
            'Habitat (Ecology)',
            'Habitat modification (Ecology)',
            'Ecological effect of habitat modification',
        ],
    },
    'effect of heat on': {
        'headings': [
            'Heat',
            'Physiological effect of heat',
        ],
    },
    'effect of heavy metals on': {
        'headings': [
            'Heavy metals',
            'Effect of heavy metals [on plants, physiology, etc.]',
        ],
    },
    'effect of human beings on': {
        'headings': [
            'Human beings',
            'Human-animal relationships',
            'Effect of human beings [on animals]',
        ],
    },
    'effect of hunting on': {
        'headings': [
            'Hunting',
            'Ecological effect of hunting',
        ],
    },
    'effect of ice on': {
        'headings': [
            'Ice',
            'Effect of ice [on plants]',
        ],
    },
    'effect of implants on': {
        'headings': [
            'Implants, Artificial',
            'Physiological effect of artificial implants',
        ],
    },
    'effect of imprisonment on': {
        'headings': [
            'Imprisonment',
            'Effect of imprisonment',
        ],
    },
    'effect of inflation on': {
        'headings': [
            'Inflation (Finance)',
            'Effect of inflation',
        ],
    },
    'effect of insecticides on': {
        'headings': [
            'Insecticides',
            'Physiological effect of insecticides',
        ],
    },
    'effect of iron on': {
        'headings': [
            'Iron',
            'Effect of iron [on plants]',
        ],
    },
    'effect of lasers on': {
        'headings': [
            'Laser beams',
            'Lasers',
            'Effect of lasers [on materials]',
        ],
    },
    'effect of light on': {
        'headings': [
            'Light',
            'Effect of light [on plants, physiology, etc.]',
        ],
    },
    'effect of logging on': {
        'headings': [
            'Logging',
            'Effect of logging',
        ],
    },
    'effect of magnesium on': {
        'headings': [
            'Magnesium',
            'Effect of magnesium [on plants]',
        ],
    },
    'effect of magnetism on': {
        'headings': [
            'Magnetic fields',
            'Effect of magnetic fields [on plants, physiology, etc.]',
        ],
    },
    'effect of manganese on': {
        'headings': [
            'Manganese',
            'Effect of manganese [on plants]',
        ],
    },
    'effect of metals on': {
        'headings': [
            'Metals',
            'Physiological effect of metals',
        ],
    },
    'effect of minerals on': {
        'headings': [
            'Minerals',
            'Effect of minerals [on plants]',
        ],
    },
    'effect of mining on': {
        'headings': [
            'Mines and mineral resources',
            'Mineral industries',
            'Ecological effect of mining',
        ],
    },
    'effect of music on': {
        'headings': [
            'Music',
            'Effect of music [on animals]',
        ],
    },
    'effect of noise on': {
        'headings': [
            'Noise',
            'Effect of noise [on animals, physiology, etc.]',
        ],
    },
    'effect of odors on': {
        'headings': [
            'Odors',
            'Effect of odors [on animals, physiology, etc.]',
        ],
    },
    'effect of offroad vehicles on': {
        'headings': [
            'Off-road vehicles',
            'Ecological effect of off-road vehicles',
        ],
    },
    'effect of oil spills on': {
        'headings': [
            'Oil spills',
            'Oil spills and wildlife',
            'Ecological effect of oil spills',
        ],
    },
    'effect of oxygen on': {
        'headings': [
            'Oxygen',
            'Effect of oxygen [on plants]',
        ],
    },
    'effect of ozone on': {
        'headings': [
            'Ozone',
            'Effect of ozone [on plants]',
        ],
    },
    'effect of pesticides on': {
        'headings': [
            'Pesticides',
            'Pesticides and wildlife',
            'Effect of pesticides [on ecology, physiology, etc.]',
        ],
    },
    'effect of poaching on': {
        'headings': [
            'Poaching',
            'Ecological effect of poaching',
        ],
    },
    'effect of pollution on': {
        'headings': [
            'Pollution',
            'Effect of pollution [on ecology, physiology, etc.]',
        ],
    },
    'effect of potassium on': {
        'headings': [
            'Potassium',
            'Effect of potassium [on plants]',
        ],
    },
    'effect of predation on': {
        'headings': [
            'Predation (Biology)',
            'Ecological effect of predation',
        ],
    },
    'effect of radiation on': {
        'headings': [
            'Radiation',
            'Effect of radiation [on plants, animals, materials, etc.]',
        ],
    },
    'effect of radioactive pollution on': {
        'headings': [
            'Radioactive pollution',
            'Effect of radioactive pollution [on ecology, physiology, etc.]',
        ],
    },
    'effect of roads on': {
        'headings': [
            'Roads',
            'Ecological effect of roads',
        ],
    },
    'effect of salt on': {
        'headings': [
            'Salt',
            'Effect of salt [on plants, physiology, etc.]',
        ],
    },
    'effect of sediments on': {
        'headings': [
            'Sediments (Geology)',
            'Ecological effect of sediments',
        ],
    },
    'effect of selenium on': {
        'headings': [
            'Selenium',
            'Physiological effect of selenium',
        ],
    },
    'effect of soil acidity on': {
        'headings': [
            'Soil acidity',
            'Effect of soil acidity [on plants]',
        ],
    },
    'effect of soil moisture on': {
        'headings': [
            'Soil moisture',
            'Effect of soil moisture [on plants]',
        ],
    },
    'effect of sound on': {
        'headings': [
            'Sound',
            'Effect of sound [on animals, physiology, etc.]',
        ],
    },
    'effect of space flight on': {
        'headings': [
            'Space flight',
            'Physiological effect of space flight',
        ],
    },
    'effect of storms on': {
        'headings': [
            'Storms',
            'Ecological effect of storms',
        ],
    },
    'effect of stray currents on': {
        'headings': [
            'Stray currents',
            'Physiological effect of stray currents',
        ],
    },
    'effect of stress on': {
        'headings': [
            'Stress (Physiology)',
            'Effect of stress [on plants, physiology, etc.]',
        ],
    },
    'effect of sulfates on': {
        'headings': [
            'Sulfates',
            'Effect of sulfates [on plants]',
        ],
    },
    'effect of sulfur on': {
        'headings': [
            'Sulfur',
            'Effect of sulfur [on plants]',
        ],
    },
    'effect of surface active agents on': {
        'headings': [
            'Surface active agents',
            'Physiological effect of surface active agents',
        ],
    },
    'effect of technological innovations on': {
        'headings': [
            'Technological innovations',
            'Effect of technological innovations [on people]',
        ],
    },
    'effect of temperature on': {
        'headings': [
            'Temperature',
            'Effect of temperature [on plants, physiology, etc.]',
        ],
    },
    'effect of thermal pollution on': {
        'headings': [
            'Thermal pollution of rivers, lakes, etc.',
            'Effect of thermal pollution [on plants]',
        ],
    },
    'effect of trampling on': {
        'headings': [
            'Trampling',
            'Effect of trampling [on plants]',
        ],
    },
    'effect of trichloroethylene on': {
        'headings': [
            'Trichloroethylene',
            'Effect of trichloroethylene [on plants]',
        ],
    },
    'effect of turbidity on': {
        'headings': [
            'Effect of turbidity [on plants]',
            'Turbidity',
        ],
    },
    'effect of ultraviolet radiation on': {
        'headings': [
            'Ultraviolet radiation',
            'Effect of ultraviolet radiation [on plants, physiology, etc.]',
        ],
    },
    'effect of vibration on': {
        'headings': [
            'Vibration',
            'Physiological effect of vibration',
        ],
    },
    'effect of volcanic eruptions on': {
        'headings': [
            'Effect of volcanic eruptions [on plants, animals, ecology, etc.]',
            'Volcanic eruptions',
        ],
    },
    'effect of water acidification on': {
        'headings': [
            'Water acidification',
            'Effect of water acidification [on animals, ecology, etc.]',
        ],
    },
    'effect of water currents on': {
        'headings': [
            'Water currents',
            'Effect of water currents [on animals, ecology, etc.]',
        ],
    },
    'effect of water levels on': {
        'headings': [
            'Effect of water levels [on plants, animals, ecology, etc.]',
            'Water levels',
        ],
    },
    'effect of water pollution on': {
        'headings': [
            'Pollution',
            'Water',
            'Effect of water pollution [on physiology, ecology, etc.]',
        ],
    },
    'effect of water quality on': {
        'headings': [
            'Water quality',
            'Effect of water quality [on animals, ecology, etc.]',
        ],
    },
    'effect of water waves on': {
        'headings': [
            'Water waves',
            'Effect of water waves [on plants]',
        ],
    },
    'effect of wind on': {
        'headings': [
            'Wind',
            'Effect of wind [on plants]',
        ],
    },
    'effect of wind power plants on': {
        'headings': [
            'Wind power plants',
            'Ecological effect of wind power plants',
        ],
    },
    'effectiveness': {
        'headings': [
            'Drug effectiveness',
        ],
    },
    'elastic properties': {
        'headings': [
            'Elasticity',
        ],
    },
    'electric equipment': {
        'headings': [
            'Electric apparatus and appliances',
        ],
    },
    'electric properties': {
        'headings': [
            'Electric properties [of chemicals, materials, etc.]',
        ],
    },
    'electronic equipment': {
        'headings': [
            'Electronic apparatus and appliances',
        ],
    },
    'ellipsis': {
        'headings': [
            'Ellipsis (Grammar)',
        ],
    },
    'embouchure': {
        'headings': [
            'Embouchure (Musical instruments)',
        ],
    },
    'embryos': {
        'headings': [
            'Embryos',
            'Embryology',
        ],
    },
    'emphasis': {
        'headings': [
            'Emphasis (Linguistics)',
        ],
    },
    'enclitics': {
        'headings': [
            'Enclitics (Grammar)',
        ],
    },
    'encyclopedias': {
        'headings': [
            'Encyclopedias and dictionaries',
        ],
    },
    'encyclopedias juvenile': {
        'headings': [
            "Children's encyclopedias and dictionaries",
        ],
    },
    'endocrine aspects': {
        'headings': [
            'Endocrinology',
        ],
        'parents': {
            'development': [
                'Endocrinology',
                'Developmental endocrinology',
            ],
        },
    },
    'engineering and construction': {
        'headings': [
            'Military engineering',
        ],
    },
    'environmental aspects': {
        'headings': [
            'Environment',
        ],
    },
    'environmental enrichment': {
        'headings': [
            'Environmental enrichment (Animal culture)',
        ],
    },
    'equipment': {
        'headings': [
            'Military supplies',
        ],
    },
    'ergative constructions': {
        'headings': [
            'Ergative constructions (Grammar)',
        ],
    },
    'errors of usage': {
        'headings': [
            'Errors',
            'Language and languages',
            'Error analysis (Linguistics)',
        ],
    },
    'eruptions': {
        'headings': [
            'Volcanic eruptions',
        ],
    },
    'estate': {
        'headings': [
            "Decedents' estates",
        ],
    },
    'ethnic identity': {
        'headings': [
            'Ethnicity',
        ],
    },
    'ethnological collections': {
        'headings': [
            'Ethnological museums and collections',
        ],
    },
    'etiology': {
        'headings': [
            'Causes and theories of causation (Diseases)',
        ],
    },
    'evolution': {
        'headings': [
            'Evolution (Biology)',
        ],
    },
    'examination': {
        'headings': [
            'Physical diagnosis',
        ],
    },
    'examinations questions etc': {
        'headings': [
            'Examinations',
            'Questions and answers',
        ],
    },
    'exclamations': {
        'headings': [
            'Exclamations (Grammar)',
        ],
    },
    'exhaust gas': {
        'parents': {
            'motors': [
                'Internal combustion engines',
                'Exhaust gas',
            ],
            'motors diesel': [
                'Diesel motor exhaust gas',
                'Exhaust gas',
            ],
        },
    },
    'exhibitions': {
        'parents': {
            'bibliography': [
                'Books',
                'History',
                'Exhibitions',
            ],
        },
    },
    'exile': {
        'headings': [
            'Exiles',
        ],
    },
    'existential constructions': {
        'headings': [
            'Existential constructions (Grammar)',
        ],
    },
    'expansion and contraction': {
        'headings': [
            'Expansion of solids',
        ],
    },
    'expertising': {
        'headings': [
            'Expertising [of art, architecture, etc.]',
        ],
    },
    'explication': {
        'headings': [
            'Explication, Literary',
        ],
    },
    'extrusion': {
        'headings': [
            'Extrusion process',
        ],
    },
    'faculty': {
        'headings': [
            'Faculty (Education)',
        ],
    },
    'family': {
        'headings': [
            'Families',
            'Genealogy',
        ],
    },
    'family relationships': {
        'headings': [
            'Families',
        ],
    },
    'fatigue': {
        'headings': [
            'Fatigue (Materials)',
        ],
    },
    'feces': {
        'headings': [
            'Animal droppings',
        ],
    },
    'feeding and feeds': {
        'headings': [
            'Food (Animals)',
            'Feeds',
        ],
    },
    'fees': {
        'headings': [
            'Fees, Professional',
        ],
    },
    'fertilization': {
        'headings': [
            'Lake fertilization',
        ],
    },
    'fetuses': {
        'headings': [
            'Fetus',
        ],
    },
    'field service': {
        'headings': [
            'Field service (Military science)',
        ],
    },
    'fieldwork': {
        'headings': [
            'Fieldwork (Educational method)',
        ],
    },
    'film catalogs': {
        'headings': [
            'Motion pictures',
            'Catalogs',
        ],
    },
    'finishing': {
        'headings': [
            'Finishes and finishing',
        ],
    },
    'fire use': {
        'headings': [
            'Firemaking',
            'Fire use',
        ],
    },
    'fires and fire prevention': {
        'headings': [
            'Fire prevention',
            'Fires',
        ],
    },
    'first editions': {
        'headings': [
            'First editions [of literature]',
            'Rare books',
        ],
    },
    'first performances': {
        'headings': [
            'First performances [of music]',
        ],
    },
    'flight': {
        'headings': [
            'Animal flight',
        ],
    },
    'flowering': {
        'headings': [
            'Flowering (Plants)',
        ],
    },
    'flowering time': {
        'headings': [
            'Flowering time (Plants)',
        ],
    },
    'fluid capacities': {
        'headings': [
            'Fluid capacities (Vehicles)',
        ],
    },
    'fluorescence': {
        'headings': [
            'Biofluorescence',
        ],
    },
    'forced repatriation': {
        'headings': [
            'Repatriation',
        ],
    },
    'foreign auxiliaries': {
        'parents': {
            'military police': [
                'Auxiliary military police',
            ],
        },
    },
    'foreign bodies': {
        'headings': [
            'Foreign bodies (Surgery)',
        ],
    },
    'foreign economic relations': {
        'headings': [
            'International economic relations',
        ],
    },
    'foreign elements': {
        'headings': [
            'Foreign elements [in a language]',
            'Etymology',
        ],
    },
    'foreign ownership': {
        'headings': [
            'Alien property',
            'Investments, Foreign',
        ],
    },
    'foreign public opinion': {
        'headings': [
            'Public opinion, Foreign',
        ],
    },
    'foreign relations': {
        'headings': [
            'Diplomacy',
            'International relations',
        ],
    },
    'foreign relations administration': {
        'headings': [
            'International relations',
        ],
    },
    'foreign service': {
        'headings': [
            'Diplomatic and consular service',
        ],
    },
    'foreign students': {
        'headings': [
            'Students, Foreign',
        ],
    },
    'forgeries': {
        'headings': [
            'Forgery',
        ],
    },
    'forms': {
        'headings': [
            'Forms, blanks, etc.',
        ],
    },
    'formulae receipts prescriptions': {
        'headings': [
            'Formulae, receipts, prescriptions (Traditional medicine)',
        ],
    },
    'fracture': {
        'headings': [
            'Fracture mechanics',
        ],
    },
    'freedom of debate': {
        'headings': [
            'Freedom of debate (Legislative bodies)',
        ],
    },
    'freshmen': {
        'headings': [
            'College freshmen',
        ],
    },
    'friends and associates': {
        'headings': [
            'Friends [of specific people]',
        ],
    },
    'frost damage': {
        'headings': [
            'Frost damage (Plants)',
        ],
    },
    'frost protection': {
        'headings': [
            'Frost protection (Plants)',
        ],
    },
    'frost resistance': {
        'headings': [
            'Frost resistance (Plants)',
        ],
    },
    'fuel consumption': {
        'headings': [
            'Fuel',
            'Energy consumption',
        ],
    },
    'fuel supplies': {
        'headings': [
            'Fuel',
        ],
    },
    'fuel systems': {
        'headings': [
            'Fuel',
            'Fuel systems',
        ],
    },
    'function words': {
        'headings': [
            'Function words (Grammar)',
        ],
    },
    'funds and scholarships': {
        'headings': [
            'Scholarships',
            'Student loans',
        ],
    },
    'funeral customs and rites': {
        'headings': [
            'Funeral rites and ceremonies',
        ],
    },
    'galvanomagnetic properties': {
        'headings': [
            'Galvanomagnetic effects',
        ],
    },
    'gender': {
        'headings': [
            'Gender (Grammar)',
        ],
    },
    'genetic aspects': {
        'headings': [
            'Genetics',
            'Genetic disorders',
            'Medical genetics',
        ],
        'parents': {
            'aging': [
                'Genetics',
            ],
            'behavior': [
                'Genetics',
                'Animal behavior genetics',
            ],
            'color': [
                'Genetics',
            ],
            'disease and pest resistance': [
                'Genetics',
                'Plant genetics',
            ],
            'effect of air pollution on': [
                'Genetics',
                'Plant genetics',
            ],
            'effect of fires on': [
                'Genetics',
                'Plant genetics',
            ],
            'immunology': [
                'Genetics',
                'Immunogenetics',
            ],
            'insect resistance': [
                'Genetics',
                'Plant genetics',
            ],
            'metabolism': [
                'Genetics',
                'Biochemical genetics',
            ],
            'metamorphosis': [
                'Genetics',
            ],
        },
    },
    'geographical distribution': {
        'headings': [
            'Biogeography',
        ],
        'parents': {
            'diseases and pests': [
                'Biogeography',
                'Zoogeography',
            ],
            'eggs': [
                'Biogeography',
                'Zoogeography',
                'Egg distribution (Zoogeography)',
            ],
            'larvae': [
                'Biogeography',
                'Zoogeography',
            ],
        },
    },
    'gerund': {
        'headings': [
            'Gerund (Grammar)',
        ],
    },
    'gerundive': {
        'headings': [
            'Gerundive (Grammar)',
        ],
    },
    'gold discoveries': {
        'headings': [
            'Gold mines and mining',
        ],
    },
    'gradation': {
        'headings': [
            'Gradation (Grammar)',
        ],
    },
    'graded lists': {
        'parents': {
            'bibliography': [
                'Graded lists (Music bibliography)',
            ],
        },
    },
    'grading': {
        'headings': [
            'Grading [e.g. of commercial products]',
        ],
    },
    'graduate work': {
        'headings': [
            'Universities and colleges',
            'Graduate work',
        ],
    },
    'grammar generative': {
        'headings': [
            'Generative grammar',
        ],
    },
    'grammatical categories': {
        'headings': [
            'Grammatical categories (Grammar)',
        ],
    },
    'grammaticalization': {
        'headings': [
            'Grammaticalization (Grammar)',
        ],
    },
    'grooming': {
        'headings': [
            'Animal grooming',
        ],
    },
    'ground support': {
        'headings': [
            'Ground support (Military aeronautics)',
        ],
        'parents': {
            'aviation': [
                'Ground support (Military aeronautics)',
            ],
        },
    },
    'growing media': {
        'headings': [
            'Plant growing media',
        ],
    },
    'habitat': {
        'headings': [
            'Habitat (Ecology)',
        ],
    },
    'habitations': {
        'headings': [
            'Animal habitations',
        ],
    },
    'halflife': {
        'parents': {
            'isotopes': [
                'Half-life (Nuclear physics)',
            ],
        },
    },
    'handling': {
        'headings': [
            'Handling [plants and animals]',
        ],
        'parents': {
            'carcasses': [
                'Handling of animal carcasses',
            ],
            'manure': [
                'Manure handling',
            ],
        },
    },
    'hardiness': {
        'headings': [
            'Hardiness (Plants)',
        ],
    },
    'harmonics': {
        'headings': [
            'Harmonics (Music)',
        ],
    },
    'headquarters': {
        'headings': [
            'Military headquarters',
        ],
    },
    'health and hygiene': {
        'headings': [
            'Health',
            'Hygiene',
            'Public health',
        ],
        'parents': {
            'employees': [
                'Health',
                'Hygiene',
                'Industrial hygiene',
            ],
        },
    },
    'health aspects': {
        'headings': [
            'Health',
            'Public health',
        ],
    },
    'health promotion services': {
        'headings': [
            'Health promotion',
        ],
    },
    'heat treatment': {
        'headings': [
            'Heat treatment (Materials)',
        ],
    },
    'heating': {
        'headings': [
            'Heating (Materials)',
        ],
    },
    'heating and ventilation': {
        'headings': [
            'Heating',
            'Ventilation',
        ],
    },
    'heavy metal content': {
        'headings': [
            'Heavy metals',
        ],
    },
    'heirloom varieties': {
        'headings': [
            'Heirloom varieties (Plants)',
        ],
    },
    'herbarium': {
        'headings': [
            'Herbaria',
        ],
    },
    'herbicide injuries': {
        'headings': [
            'Herbicide injuries (Crops)',
        ],
    },
    'hiatus': {
        'headings': [
            'Hiatus (Linguistics)',
        ],
    },
    'histochemistry': {
        'headings': [
            'Histochemistry',
            'Plant histochemistry',
        ],
    },
    'histopathology': {
        'headings': [
            'Histology, Pathological',
        ],
    },
    'history and criticism': {
        'headings': [
            'History',
            'Criticism',
        ],
        'parents': {
            'biography': [
                'Biography as a literary form',
            ],
        },
    },
    'history local': {
        'headings': [
            'Local history',
        ],
    },
    'history military': {
        'headings': [
            'Military history',
        ],
    },
    'history naval': {
        'headings': [
            'Naval battles',
            'Naval history',
        ],
    },
    'history of doctrines': {
        'headings': [
            'Theology, Doctrinal',
            'History',
        ],
    },
    'home care': {
        'headings': [
            'Home care services',
        ],
    },
    'home range': {
        'headings': [
            'Home range (Animal geography)',
        ],
    },
    'homeopathic treatment': {
        'headings': [
            'Homeopathy',
        ],
    },
    'homing': {
        'headings': [
            'Animal homing',
        ],
    },
    'honor system': {
        'headings': [
            'Honor system (Higher education)',
        ],
    },
    'honorific': {
        'headings': [
            'Honorific (Grammar)',
        ],
    },
    'humor': {
        'headings': [
            'Wit and humor',
        ],
        'parents': {
            'social aspects': [
                'Social satire',
                'Wit and humor',
                'Satire',
            ],
            'social conditions': [
                'Social satire',
                'Wit and humor',
                'Satire',
            ],
            'social life and customs': [
                'Social satire',
                'Wit and humor',
                'Satire',
            ],
        },
    },
    'hurricane effects': {
        'headings': [
            'Hurricane damage',
        ],
    },
    'hydatids': {
        'headings': [
            'Echinococcosis',
        ],
    },
    'hydraulic equipment': {
        'headings': [
            'Hydraulic machinery',
            'Hydraulic equipment',
        ],
    },
    'identification': {
        'headings': [
            'Identification [e.g. of plants, animals, etc.]',
        ],
    },
    'ideophone': {
        'headings': [
            'Ideophone (Grammar)',
        ],
    },
    'illustrations': {
        'headings': [
            'Illustration of books',
        ],
    },
    'imaging': {
        'headings': [
            'Diagnostic imaging',
        ],
    },
    'immersion method': {
        'parents': {
            'study and teaching': [
                'Language and languages',
                'Immersion method (Language teaching)',
            ],
        },
    },
    'immunological aspects': {
        'headings': [
            'Immunology',
        ],
        'parents': {
            'transplantation': [
                'Transplantation immunology',
            ],
        },
    },
    'immunology': {
        'headings': [
            'Immunology',
        ],
    },
    'impeachment': {
        'headings': [
            'Impeachments',
        ],
    },
    'imperative': {
        'headings': [
            'Imperative (Grammar)',
        ],
    },
    'implements': {
        'headings': [
            'Implements, utensils, etc.',
        ],
    },
    'in art': {
        'headings': [
            'Art',
            'Themes, motives [in art, literature, etc.]',
        ],
    },
    'in bookplates': {
        'headings': [
            'Bookplates',
        ],
    },
    'in literature': {
        'headings': [
            'Comparative literature',
            'Themes, motives [in art, literature, etc.]',
        ],
    },
    'in mass media': {
        'headings': [
            'Mass media',
        ],
    },
    'in motion pictures': {
        'headings': [
            'Motion pictures',
            'Themes, motives [in art, literature, etc.]',
        ],
    },
    'in opera': {
        'headings': [
            'Opera',
            'Themes, motives [in art, literature, etc.]',
        ],
    },
    'in popular culture': {
        'headings': [
            'Popular culture',
        ],
    },
    'inaugurations': {
        'headings': [
            'Inauguration',
        ],
    },
    'indian troops': {
        'headings': [
            'Indian military personnel',
            'Indians in military service',
            'Indians in the Armed Forces',
        ],
    },
    'indicative': {
        'headings': [
            'Indicative (Grammar)',
        ],
    },
    'indirect discourse': {
        'headings': [
            'Indirect discourse (Grammar)',
        ],
    },
    'indirect object': {
        'headings': [
            'Indirect object (Grammar)',
        ],
    },
    'industrial applications': {
        'headings': [
            'Chemistry, Technical',
            'Industrial applications [of chemicals, materials, etc.]',
        ],
    },
    'infancy': {
        'headings': [
            'Animal infancy',
        ],
    },
    'infections': {
        'headings': [
            'Infection',
        ],
    },
    'infertility': {
        'headings': [
            'Infertility in animals',
        ],
    },
    'infinitival constructions': {
        'headings': [
            'Infinitival constructions (Grammar)',
        ],
    },
    'infinitive': {
        'headings': [
            'Infinitive (Grammar)',
        ],
    },
    'infixes': {
        'headings': [
            'Infixes (Grammar)',
        ],
    },
    'inflection': {
        'headings': [
            'Inflection (Grammar)',
        ],
    },
    'influence on foreign languages': {
        'headings': [
            'Language and languages',
            'Foreign elements [in a language]',
            'Etymology',
        ],
    },
    'inhibitors': {
        'headings': [
            'Chemical inhibitors',
            'Enzyme inhibitors',
        ],
    },
    'innervation': {
        'headings': [
            'Nerves, Peripheral',
            'Nervous system',
        ],
    },
    'inoculation': {
        'headings': [
            'Plant inoculation',
        ],
    },
    'insect resistance': {
        'headings': [
            'Insect resistance (Plants)',
        ],
    },
    'inservice training': {
        'headings': [
            'Employee training',
            'In-service training',
        ],
    },
    'installation': {
        'headings': [
            'Installation of equipment',
        ],
        'parents': {
            'clergy': [
                'Installation (Clergy)',
            ],
        },
    },
    'instruction and study': {
        'headings': [
            'Instruction and study (Music)',
        ],
    },
    'instruments': {
        'headings': [
            'Scientific apparatus and instruments',
        ],
        'parents': {
            'surgery': [
                'Surgical instruments and apparatus',
            ],
        },
    },
    'insurance requirements': {
        'headings': [
            'Insurance',
        ],
    },
    'integrated control': {
        'headings': [
            'Integrated pest control',
        ],
    },
    'intelligence testing': {
        'headings': [
            'Intelligence tests',
        ],
    },
    'intensification': {
        'headings': [
            'Intensification (Linguistics)',
        ],
    },
    'interjections': {
        'headings': [
            'Interjections (Grammar)',
        ],
    },
    'intermediate care': {
        'headings': [
            'Intermediate care (Nursing care)',
        ],
    },
    'international status': {
        'headings': [
            'International law',
        ],
    },
    'international unification': {
        'headings': [
            'International unification (Law)',
        ],
    },
    'interpretation': {
        'headings': [
            'Interpretation [of tests and exams]',
        ],
    },
    'interpretation and construction': {
        'headings': [
            'Interpretation and construction (Law)',
        ],
    },
    'interpretation phrasing dynamics etc': {
        'headings': [
            'Interpretation of phrasing, dynamics, etc. (Music)',
        ],
    },
    'interrogative': {
        'headings': [
            'Interrogative (Grammar)',
        ],
    },
    'intonation': {
        'headings': [
            'Intonation [in phonetics, musical pitch, etc.]',
        ],
    },
    'introductions': {
        'headings': [
            'Introductions [of sacred books]',
        ],
    },
    'investigation': {
        'parents': {
            'accidents': [
                'Accident investigation',
            ],
        },
    },
    'isotopes': {
        'headings': [
            'Isotopes',
            'Isotopic forms of elements',
        ],
    },
    'jargon': {
        'headings': [
            'Jargon (Terminology)',
        ],
    },
    'jewelry': {
        'headings': [
            'Jewelry',
            'Ethnic jewelry',
        ],
    },
    'judging': {
        'headings': [
            'Judging (Livestock)',
        ],
    },
    'jumping': {
        'headings': [
            'Animal jumping',
        ],
    },
    'juvenile drama': {
        'headings': [
            "Children's plays",
            'Juvenile drama',
        ],
    },
    'juvenile fiction': {
        'headings': [
            "Children's stories",
            'Juvenile fiction',
        ],
    },
    'juvenile films': {
        'headings': [
            'Motion pictures for children',
        ],
    },
    'juvenile humor': {
        'headings': [
            'Wit and humor, Juvenile',
            'Juvenile humor',
        ],
    },
    'juvenile literature': {
        'headings': [
            "Children's literature",
            'Juvenile literature',
        ],
    },
    'juvenile poetry': {
        'headings': [
            "Children's poetry",
            'Juvenile poetry',
        ],
    },
    'juvenile software': {
        'headings': [
            "Children's software",
        ],
    },
    'knock': {
        'headings': [
            'Knock (Motors)',
        ],
    },
    'knowledge': {
        'headings': [
            'Knowledge and learning',
        ],
    },
    'labeling': {
        'headings': [
            'Labels',
        ],
    },
    'labiality': {
        'headings': [
            'Labiality (Phonetics)',
        ],
    },
    'language': {
        'headings': [
            'Language and languages',
        ],
    },
    'languages': {
        'headings': [
            'Language and languages',
        ],
    },
    'laser surgery': {
        'headings': [
            'Lasers in surgery',
        ],
    },
    'law and legislation': {
        'parents': {
            'crimes against': [
                'Criminal law',
                'Law and legislation',
            ],
        },
    },
    'leadership': {
        'headings': [
            'Leadership (Legislative bodies)',
        ],
    },
    'leaves and furloughs': {
        'headings': [
            'Furloughs',
            'Military leaves and furloughs',
        ],
    },
    'lexicology historical': {
        'headings': [
            'Historical lexicology',
        ],
    },
    'library': {
        'headings': [
            'Libraries',
        ],
    },
    'licenses': {
        'parents': {
            'collection and preservation': [
                'Licenses for collection and preservation of animals',
            ],
        },
    },
    'life cycles': {
        'headings': [
            'Life cycles (Biology)',
        ],
    },
    'life skills assessment': {
        'headings': [
            'Life skills',
            'Evaluation',
        ],
    },
    'life skills guides': {
        'headings': [
            'Life skills',
            'Handbooks, manuals, etc.',
        ],
    },
    'literary collections': {
        'headings': [
            'Anthologies',
            'Literature',
            'Collections',
        ],
    },
    'literary style': {
        'headings': [
            'Style, Literary',
        ],
    },
    'literatures': {
        'headings': [
            'Literature',
        ],
    },
    'liturgical use': {
        'headings': [
            'Liturgics',
        ],
    },
    'liturgy': {
        'headings': [
            'Liturgics',
        ],
    },
    'location': {
        'headings': [
            'Location [of industries, crops, etc.]',
        ],
    },
    'locative constructions': {
        'headings': [
            'Locative constructions (Grammar)',
        ],
    },
    'locomotion': {
        'headings': [
            'Animal locomotion',
        ],
    },
    'longevity': {
        'headings': [
            'Longevity [of plants and animals]',
        ],
    },
    'longitudinal studies': {
        'headings': [
            'Longitudinal method',
        ],
    },
    'longterm care': {
        'headings': [
            'Long-term care of the sick',
        ],
    },
    'losses': {
        'headings': [
            'Losses (Agriculture)',
        ],
    },
    'lubrication': {
        'headings': [
            'Lubrication and lubricants',
        ],
    },
    'machinery': {
        'headings': [
            'Agricultural machinery',
        ],
        'parents': {
            'harvesting': [
                'Harvesting machinery',
            ],
            'threshing': [
                'Threshing machines',
            ],
            'transplanting': [
                'Transplanting machines',
            ],
        },
    },
    'magnetic properties': {
        'headings': [
            'Magnetic properties (Materials)',
        ],
    },
    'maintenance and repair': {
        'headings': [
            'Maintenance',
            'Repairing',
        ],
    },
    'maneuvers': {
        'headings': [
            'Military maneuvers',
        ],
    },
    'manure': {
        'headings': [
            'Manures',
        ],
    },
    'marginal notes': {
        'parents': {
            'library': [
                'Marginalia',
            ],
        },
    },
    'markedness': {
        'headings': [
            'Markedness (Linguistics)',
        ],
    },
    'marking': {
        'headings': [
            'Animal marking',
        ],
    },
    'markings': {
        'parents': {
            'firearms': [
                'Markings (Firearms)',
            ],
        },
    },
    'mechanical properties': {
        'headings': [
            'Mechanical properties [of materials, living things, etc.]',
        ],
    },
    'mechanism of action': {
        'headings': [
            'Mechanism of action (Biochemistry)',
        ],
    },
    'medals badges decorations etc': {
        'headings': [
            'Medals',
            'Military decorations',
        ],
    },
    'medical care': {
        'parents': {
            'employees': [
                'Occupational health services',
            ],
        },
    },
    'medicine': {
        'headings': [
            'Traditional medicine',
        ],
    },
    'memorizing': {
        'headings': [
            'Memory',
        ],
    },
    'mercury content': {
        'headings': [
            'Mercury',
        ],
    },
    'mergers': {
        'headings': [
            'Consolidation and merger of corporations',
        ],
    },
    'methods': {
        'headings': [
            'Methods (Music)',
        ],
    },
    'metrics and rhythmics': {
        'headings': [
            'Rhythm',
            'Versification',
        ],
    },
    'microbiology': {
        'parents': {
            'germplasm resources': [
                'Microbiology',
                'Veterinary microbiology',
            ],
        },
    },
    'microform catalogs': {
        'headings': [
            'Microforms',
            'Catalogs',
        ],
    },
    'micropropagation': {
        'headings': [
            'Plant micropropagation',
        ],
    },
    'migration': {
        'headings': [
            'Animal migration',
        ],
    },
    'migrations': {
        'headings': [
            'Migrations of nations',
        ],
    },
    'military aspects': {
        'headings': [
            'Industrial mobilization',
            'Military art and science',
        ],
    },
    'military leadership': {
        'headings': [
            'Command of troops',
        ],
    },
    'milling': {
        'headings': [
            'Milling [e.g. plants, grain]',
            'Mills and mill-work',
        ],
    },
    'miscellanea': {
        'headings': [
            'Curiosities and wonders',
            'Miscellanea',
        ],
    },
    'mnemonic devices': {
        'headings': [
            'Mnemonics',
        ],
    },
    'mobilization': {
        'headings': [
            'Mobilization (Armed Forces)',
        ],
    },
    'modality': {
        'headings': [
            'Modality (Linguistics)',
        ],
    },
    'molecular aspects': {
        'headings': [
            'Molecular biology',
        ],
        'parents': {
            'parasites': [
                'Molecular biology',
                'Molecular parasitology',
            ],
        },
    },
    'molecular genetics': {
        'headings': [
            'Molecular genetics',
        ],
    },
    'monitoring': {
        'headings': [
            'Monitoring [of vegetation and wildlife]',
        ],
        'parents': {
            'diseases and pests': [
                'Monitoring agricultural pests',
                'Epidemiology of plant diseases',
            ],
        },
    },
    'monuments': {
        'headings': [
            'Monuments',
            'War memorials',
        ],
    },
    'mood': {
        'headings': [
            'Mood (Grammar)',
        ],
    },
    'moral and ethical aspects': {
        'headings': [
            'Ethics',
        ],
    },
    'morphology': {
        'parents': {
            'dialects': [
                'Morphology (Grammar)',
            ],
            'pollen': [
                'Plant morphology',
            ],
            'seeds': [
                'Plant morphology',
            ],
            'spermatozoa': [
                'Morphology (Animals)',
            ],
            'spores': [
                'Plant morphology',
            ],
        },
    },
    'morphosyntax': {
        'headings': [
            'Morphosyntax (Grammar)',
        ],
    },
    'motility': {
        'parents': {
            'embryos': [
                'Motility (Embryos)',
            ],
            'spermatozoa': [
                'Motility (Spermatozoa)',
            ],
        },
    },
    'motors diesel': {
        'headings': [
            'Diesel motor',
        ],
    },
    'movements': {
        'headings': [
            'Biomechanics',
            'Animal mechanics',
        ],
    },
    'musical instrument collections': {
        'headings': [
            'Musical instruments',
            'Catalogs',
        ],
    },
    'mutation': {
        'headings': [
            'Mutation (Phonetics)',
        ],
    },
    'mutation breeding': {
        'headings': [
            'Plant mutation breeding',
        ],
    },
    'mutual intelligibility': {
        'headings': [
            'Languages, Modern',
            'Mutual intelligibility',
        ],
    },
    'name': {
        'headings': [
            'Names',
        ],
    },
    'names': {
        'parents': {
            'etymology': [
                'Onomastics',
                'Names',
            ],
        },
    },
    'nasality': {
        'headings': [
            'Nasality (Phonetics)',
        ],
    },
    'natural history collections': {
        'headings': [
            'Natural history',
            'Catalogs',
        ],
    },
    'naval operations': {
        'headings': [
            'Naval battles',
        ],
    },
    'nazi persecution': {
        'headings': [
            'Persecution',
        ],
    },
    'negatives': {
        'headings': [
            'Negatives (Grammar)',
        ],
    },
    'nests': {
        'headings': [
            'Animal habitations',
            'Nest building',
        ],
    },
    'neutralization': {
        'headings': [
            'Neutralization (Linguistics)',
        ],
    },
    'new words': {
        'headings': [
            'Words, New',
        ],
    },
    'nitrogen content': {
        'headings': [
            'Nitrogen',
        ],
    },
    'nominals': {
        'headings': [
            'Nominals (Grammar)',
        ],
    },
    'nomograms': {
        'headings': [
            'Nomography (Mathematics)',
        ],
    },
    'noncommissioned officers': {
        'headings': [
            'Armies',
            'Non-commissioned officers',
        ],
    },
    'notebooks sketchbooks etc': {
        'headings': [
            'Notebooks',
        ],
    },
    'noun': {
        'headings': [
            'Noun (Grammar)',
        ],
    },
    'noun phrase': {
        'headings': [
            'Noun phrase (Grammar)',
        ],
    },
    'null subject': {
        'headings': [
            'Null subject (Grammar)',
        ],
    },
    'number': {
        'headings': [
            'Number (Grammar)',
        ],
    },
    'numismatic collections': {
        'headings': [
            'Numismatics',
            'Private collections',
        ],
    },
    'nursing': {
        'parents': {
            'surgery': [
                'Surgical nursing',
            ],
        },
    },
    'nutritional aspects': {
        'headings': [
            'Nutrition',
            'Diet in disease',
        ],
        'parents': {
            'diseases and pests': [
                'Nutrition',
            ],
        },
    },
    'obscene words': {
        'headings': [
            'Words, Obscene',
        ],
    },
    'observations': {
        'headings': [
            'Observation (Scientific method)',
        ],
    },
    'occupations': {
        'headings': [
            'Occupations (Monasticism and religious orders)',
        ],
    },
    'occupied territories': {
        'headings': [
            'Military occupation',
        ],
    },
    'odor': {
        'headings': [
            'Odors',
        ],
    },
    'odor control': {
        'parents': {
            'housing': [
                'Animal housing',
                'Odor control',
            ],
        },
    },
    'officers': {
        'headings': [
            'Officers (Armed forces)',
        ],
    },
    'officials and employees honorary': {
        'headings': [
            'Honorary officials and employees',
        ],
    },
    'officials and employees retired': {
        'headings': [
            'Civil service pensioners',
        ],
    },
    'on postage stamps': {
        'headings': [
            'Postage stamps',
        ],
    },
    'on television': {
        'headings': [
            'Television programs',
        ],
    },
    'onomatopoeic words': {
        'headings': [
            'Onomatopoeia',
        ],
    },
    'open admission': {
        'headings': [
            'Universities and colleges',
            'Open admission',
        ],
    },
    'operational readiness': {
        'headings': [
            'Operational readiness (Military science)',
        ],
    },
    'operations other than war': {
        'headings': [
            'Operations other than war (Armed forces)',
        ],
    },
    'optical properties': {
        'headings': [
            'Optical properties (Materials)',
        ],
        'parents': {
            'surfaces': [
                'Surfaces (Technology)',
                'Optical properties (Materials)',
            ],
        },
    },
    'orbit': {
        'headings': [
            'Orbits (Artificial satellites)',
        ],
    },
    'orchestras': {
        'headings': [
            'Orchestra',
        ],
    },
    'ordnance and ordnance stores': {
        'headings': [
            'Ordnance',
        ],
    },
    'organizing': {
        'parents': {
            'labor unions': [
                'Organizing (Labor unions)',
            ],
        },
    },
    'organs': {
        'headings': [
            'Organ (Musical instrument)',
        ],
    },
    'orientation': {
        'headings': [
            'Animal orientation',
        ],
    },
    'osmotic potential': {
        'headings': [
            'Osmotic potential [of plants]',
        ],
    },
    'outlines syllabi etc': {
        'headings': [
            'Outlines',
        ],
    },
    'overdose': {
        'headings': [
            'Overdose (Drugs)',
        ],
    },
    'ownership': {
        'headings': [
            'Stock ownership',
        ],
    },
    'packing': {
        'headings': [
            'Packing for shipment',
        ],
    },
    'padding': {
        'parents': {
            'instrument panels': [
                'Instrument panel padding',
            ],
        },
    },
    'painting': {
        'headings': [
            'Painting, Industrial',
        ],
    },
    'painting of vessels': {
        'headings': [
            'Painting, Structural',
        ],
    },
    'parallelism': {
        'headings': [
            'Parallelism (Linguistics)',
        ],
    },
    'paramours': {
        'parents': {
            'kings and rulers': [
                'Paramours [of kings and rulers]',
            ],
        },
    },
    'parasites': {
        'headings': [
            'Parasites',
            'Medical parasitology',
            'Parasitic diseases',
        ],
    },
    'parenthetical constructions': {
        'headings': [
            'Parenthetical constructions (Grammar)',
        ],
    },
    'parking': {
        'headings': [
            'Campus parking',
        ],
    },
    'parodies imitations etc': {
        'headings': [
            'Parodies',
        ],
    },
    'parsing': {
        'headings': [
            'Parsing (Grammar)',
        ],
    },
    'participation deaf': {
        'headings': [
            'Deaf soldiers',
        ],
    },
    'participation female': {
        'headings': [
            'Women soldiers',
        ],
    },
    'participation gay': {
        'headings': [
            'Gay military personnel',
        ],
    },
    'participation jewish': {
        'headings': [
            'Jewish soldiers',
        ],
    },
    'participation juvenile': {
        'headings': [
            'Child soldiers',
        ],
    },
    'participle': {
        'headings': [
            'Participle (Grammar)',
        ],
    },
    'particles': {
        'headings': [
            'Particles (Grammar)',
        ],
    },
    'partitives': {
        'headings': [
            'Partitives (Grammar)',
        ],
    },
    'parts': {
        'headings': [
            'Machine parts',
        ],
    },
    'party work': {
        'headings': [
            'Party work (Politics)',
        ],
    },
    'passenger lists': {
        'headings': [
            'Passenger lists [of ships]',
        ],
    },
    'passive voice': {
        'headings': [
            'Passive voice (Grammar)',
        ],
    },
    'pastoral counseling of': {
        'headings': [
            'Pastoral counseling',
        ],
    },
    'pathogenesis': {
        'headings': [
            'Pathology',
        ],
    },
    'pathogens': {
        'headings': [
            'Pathogenic microorganisms',
        ],
    },
    'pathophysiology': {
        'headings': [
            'Physiology, Pathological',
        ],
    },
    'pay allowances etc': {
        'headings': [
            'Military pay',
        ],
    },
    'pedigrees': {
        'headings': [
            'Animal pedigrees',
        ],
    },
    'pejoration': {
        'headings': [
            'Pejoration (Linguistics)',
        ],
    },
    'periodization': {
        'headings': [
            'Periodization (Literature)',
        ],
        'parents': {
            'history': [
                'Periodization (History)',
            ],
        },
    },
    'person': {
        'headings': [
            'Person (Grammar)',
        ],
    },
    'philosophy': {
        'headings': [
            'Applied philosophy',
            'Philosophy',
        ],
    },
    'philosophy and aesthetics': {
        'headings': [
            'Philosophy and aesthetics (Music)',
        ],
    },
    'phonology comparative': {
        'headings': [
            'Phonology, Comparative (Grammar)',
        ],
    },
    'photographic identification': {
        'headings': [
            'Photographic identification [of animals]',
        ],
    },
    'photography': {
        'headings': [
            'War photography',
        ],
    },
    'physical training': {
        'headings': [
            'Physical education and training',
        ],
    },
    'physiological aspects': {
        'headings': [
            'Physiology',
        ],
        'parents': {
            'exercise': [
                'Physiology',
                'Human physiology',
            ],
        },
    },
    'physiological effect': {
        'headings': [
            'Physiology',
        ],
    },
    'physiological transport': {
        'headings': [
            'Biological transport',
        ],
    },
    'pistons and piston rings': {
        'parents': {
            'motors': [
                'Piston rings',
                'Pistons',
            ],
        },
    },
    'planting': {
        'headings': [
            'Planting (Plant culture)',
        ],
    },
    'plastic properties': {
        'headings': [
            'Plasticity',
        ],
    },
    'pneumatic equipment': {
        'headings': [
            'Pneumatic machinery',
        ],
    },
    'political activity': {
        'headings': [
            'Political participation',
        ],
    },
    'political aspects': {
        'headings': [
            'Political science',
        ],
    },
    'politics and government': {
        'headings': [
            'Political science',
            'Politics, Practical',
            'Public administration',
        ],
    },
    'population': {
        'headings': [
            'Demography',
            'Population',
        ],
    },
    'population regeneration': {
        'headings': [
            'Plant population regeneration',
        ],
    },
    'positioning': {
        'parents': {
            'radiography': [
                'Positioning (Medical radiography)',
            ],
        },
    },
    'positions': {
        'headings': [
            'Civil service positions',
        ],
    },
    'possessives': {
        'headings': [
            'Possessives (Grammar)',
        ],
    },
    'postal service': {
        'headings': [
            'Postal service',
            'Military postal service',
        ],
    },
    'poster collections': {
        'headings': [
            'Posters',
            'Private collections',
        ],
    },
    'postharvest losses': {
        'headings': [
            'Losses (Agriculture)',
            'Postharvest losses (Crops)',
        ],
    },
    'postharvest physiology': {
        'headings': [
            'Postharvest physiology (Crops)',
        ],
    },
    'postharvest technology': {
        'headings': [
            'Agricultural processing',
            'Postharvest technology (Crops)',
        ],
    },
    'postpositions': {
        'headings': [
            'Postpositions (Grammar)',
        ],
    },
    'power supply': {
        'headings': [
            'Electric power',
        ],
    },
    'power utilization': {
        'headings': [
            'Water-power',
        ],
    },
    'powers and duties': {
        'headings': [
            'Legislative power',
        ],
    },
    'practice': {
        'headings': [
            'Professional practice',
        ],
    },
    'prayers and devotions': {
        'headings': [
            'Devotional exercises',
            'Devotional literature',
            'Prayer books',
            'Prayers',
        ],
    },
    'predators of': {
        'headings': [
            'Predatory animals',
        ],
        'parents': {
            'seeds': [
                'Granivores',
            ],
        },
    },
    'pregnancy': {
        'headings': [
            'Pregnancy in animals',
        ],
    },
    'preharvest sprouting': {
        'headings': [
            'Germination',
        ],
    },
    'prepositional phrases': {
        'headings': [
            'Prepositional phrases (Grammar)',
        ],
    },
    'prepositions': {
        'headings': [
            'Prepositions (Grammar)',
        ],
    },
    'preservation': {
        'headings': [
            'Preservation of materials',
        ],
    },
    'presiding officers': {
        'headings': [
            'Presiding officers (Legislative bodies)',
        ],
    },
    'press coverage': {
        'headings': [
            'Press',
        ],
    },
    'prevention': {
        'headings': [
            'Prevention [of crime, diseases, fires, etc.]',
        ],
    },
    'prisoners and prisons': {
        'headings': [
            'Prisoners of war',
            'Military prisons',
        ],
    },
    'prisons': {
        'headings': [
            'Military prisons',
        ],
    },
    'prizes etc': {
        'headings': [
            'Prizes (Property captured at sea)',
        ],
    },
    'processing': {
        'headings': [
            'Agricultural processing',
        ],
    },
    'production and direction': {
        'headings': [
            'Production and direction [performing arts, motion pictures, etc.]',
        ],
    },
    'productivity': {
        'headings': [
            'Livestock productivity',
        ],
    },
    'professional staff': {
        'headings': [
            'Professional employees',
            'Universities and colleges',
        ],
    },
    'programming': {
        'headings': [
            'Computer programming',
        ],
    },
    'pronominals': {
        'headings': [
            'Pronominals (Grammar)',
        ],
    },
    'pronoun': {
        'headings': [
            'Pronoun (Grammar)',
        ],
    },
    'pronunciation by foreign speakers': {
        'headings': [
            'Pronunciation',
        ],
    },
    'propaganda': {
        'headings': [
            'Propaganda',
            'War propaganda',
        ],
    },
    'propagation': {
        'headings': [
            'Plant propagation',
        ],
    },
    'properties': {
        'headings': [
            'Properties [of chemicals, materials, etc.]',
        ],
    },
    'prosodic analysis': {
        'headings': [
            'Prosodic analysis (Linguistics)',
        ],
    },
    'protest movements': {
        'headings': [
            'Anti-war demonstrations',
            'Protest movements',
        ],
    },
    'provenances': {
        'headings': [
            'Provenances (Plants, Cultivated)',
        ],
    },
    'provincialisms': {
        'headings': [
            'Dialects',
        ],
    },
    'psychic aspects': {
        'headings': [
            'Parapsychology',
        ],
    },
    'psychological aspects': {
        'headings': [
            'Psychology',
        ],
    },
    'psychological testing': {
        'headings': [
            'Psychological tests',
        ],
    },
    'psychosomatic aspects': {
        'headings': [
            'Medicine, Psychosomatic',
        ],
    },
    'psychotropic effects': {
        'headings': [
            'Psychopharmacology',
        ],
    },
    'public services': {
        'headings': [
            'Universities and colleges',
            'Public services',
        ],
    },
    'publication and distribution': {
        'headings': [
            'Publishers and publishing',
        ],
    },
    'publication of proceedings': {
        'headings': [
            'Publication of proceedings (Legislative bodies)',
            'Printing, Legislative',
        ],
    },
    'publishing': {
        'headings': [
            'Publishers and publishing',
        ],
    },
    'purges': {
        'headings': [
            'Political purges',
        ],
    },
    'purification': {
        'headings': [
            'Purification (Chemistry)',
        ],
    },
    'qualifications': {
        'headings': [
            'Qualifications (Legislative bodies)',
        ],
    },
    'quality': {
        'headings': [
            'Quality [e.g. of plants, animals, etc.]',
        ],
    },
    'quantifiers': {
        'headings': [
            'Quantifiers (Grammar)',
        ],
    },
    'quantity': {
        'headings': [
            'Duration (Phonetics)',
        ],
    },
    'quarantine': {
        'headings': [
            'Quarantine, Veterinary',
        ],
    },
    'quotations maxims etc': {
        'headings': [
            'Aphorisms and apothegms',
            'Epigrams',
            'Maxims',
            'Proverbs',
            'Quotations',
        ],
    },
    'radiation preservation': {
        'headings': [
            'Radiation preservation of food',
        ],
    },
    'radio equipment': {
        'headings': [
            'Radio',
        ],
    },
    'radio tracking': {
        'headings': [
            'Animal radio tracking',
        ],
    },
    'radionuclide imaging': {
        'headings': [
            'Radioisotope scanning',
        ],
    },
    'rating of': {
        'headings': [
            'Rating (Employees)',
            'Evaluation',
        ],
    },
    'reactivity': {
        'headings': [
            'Reactivity (Chemistry)',
        ],
    },
    'receptors': {
        'headings': [
            'Cell receptors',
        ],
    },
    'reconnaissance operations': {
        'headings': [
            'Military reconnaissance',
            'Reconnaissance operations',
        ],
    },
    'records and correspondence': {
        'headings': [
            'Business records',
            'Commercial correspondence',
        ],
    },
    'recreational use': {
        'headings': [
            'Recreation',
            'Outdoor recreation',
        ],
    },
    'recruiting': {
        'headings': [
            'Recruiting (Employees)',
        ],
    },
    'recruiting enlistment etc': {
        'headings': [
            'Recruiting and enlistment',
        ],
    },
    'recursion': {
        'headings': [
            'Recursion (Linguistics)',
        ],
    },
    'recycling': {
        'headings': [
            'Recycling (Waste, etc.)',
        ],
    },
    'reduplication': {
        'headings': [
            'Reduplication (Linguistics)',
        ],
    },
    'reference': {
        'headings': [
            'Reference (Linguistics)',
        ],
    },
    'reflexives': {
        'headings': [
            'Reflexives (Grammar)',
        ],
    },
    'regeneration': {
        'headings': [
            'Regeneration (Biology)',
        ],
    },
    'regional disparities': {
        'parents': {
            'economic conditions': [
                'Regional economic disparities',
            ],
        },
    },
    'regions': {
        'headings': [
            'Regions (Administrative and political divisions)',
        ],
    },
    'regulation': {
        'headings': [
            'Regulation [of lakes and rivers]',
        ],
        'parents': {
            'growth': [
                'Regulation [of growth]',
            ],
            'metabolism': [
                'Biological control systems',
                'Regulation (Biology)',
            ],
            'reproduction': [
                'Regulation (Biology)',
            ],
            'secretion': [
                'Biological control systems',
                'Regulation (Biology)',
            ],
            'synthesis': [
                'Biological control systems',
                'Regulation (Biology)',
            ],
            'vocalization': [
                'Biological control systems',
                'Regulation (Biology)',
            ],
        },
    },
    'reimplantation': {
        'headings': [
            'Reimplantation (Surgery)',
        ],
    },
    'reinstatement': {
        'headings': [
            'Reinstatement (Employees)',
        ],
    },
    'reintroduction': {
        'headings': [
            'Reintroduction (Ecology)',
        ],
    },
    'relapse': {
        'headings': [
            'Relapse (Diseases)',
        ],
    },
    'relations with men': {
        'headings': [
            'Man-woman relationships',
        ],
    },
    'relations with women': {
        'headings': [
            'Man-woman relationships',
        ],
    },
    'relative clauses': {
        'headings': [
            'Relative clauses (Grammar)',
        ],
    },
    'reliability': {
        'headings': [
            'Reliability (Engineering)',
        ],
    },
    'religious aspects': {
        'headings': [
            'Religion',
        ]
    },
    'remodeling': {
        'headings': [
            'Renovation (Architecture)',
        ],
    },
    'remodeling for other use': {
        'headings': [
            'Renovation (Architecture)',
        ],
    },
    'reorganization': {
        'headings': [
            'Reorganization [e.g. of corporations, armed forces, etc.]',
        ],
    },
    'reparations': {
        'headings': [
            'Reparations for historical injustices',
            'War reparations',
        ],
    },
    'reporters and reporting': {
        'headings': [
            'Legislative reporting',
        ],
    },
    'reporting': {
        'headings': [
            'Reporting (Diseases)',
        ],
        'parents': {
            'defects': [
                'Reporting of defects',
            ],
            'toxicology': [
                'Reporting (Diseases)',
            ],
        },
    },
    'reporting to': {
        'headings': [
            'Reporting to [types of employees]',
        ],
    },
    'reserves': {
        'headings': [
            'Reserves (Armed forces)',
        ],
    },
    'residues': {
        'headings': [
            'Crop residues',
        ],
    },
    'resignation': {
        'headings': [
            'Resignation (Employees)',
        ],
    },
    'resignation from office': {
        'headings': [
            'Resignation (Employees)',
        ],
    },
    'resolutions': {
        'headings': [
            'Resolutions, Legislative',
        ],
    },
    'resultative constructions': {
        'headings': [
            'Resultative constructions (Grammar)',
        ],
    },
    'revival': {
        'headings': [
            'Language revival',
        ],
    },
    'ripening': {
        'headings': [
            'Plant phenology',
        ],
    },
    'rituals': {
        'headings': [
            'Rites and ceremonies',
        ],
    },
    'robots': {
        'headings': [
            'Military robots',
        ],
    },
    'rules and practice': {
        'headings': [
            'Administrative agencies',
            'Rules and practice',
            'Parliamentary practice',
        ],
    },
    'rupture': {
        'headings': [
            'Rupture of organs, tissues, etc.',
        ],
    },
    'safety appliances': {
        'headings': [
            'Safety appliances',
            'Industrial safety',
        ],
    },
    'safety measures': {
        'headings': [
            'Industrial safety',
            'Safety measures',
        ],
    },
    'salaries etc': {
        'headings': [
            'Wages',
            'Salaries',
        ],
    },
    'sanitary affairs': {
        'headings': [
            'Sanitation',
        ],
    },
    'scheduled tribes': {
        'headings': [
            'Scheduled tribes (India)',
        ],
    },
    'scholarships fellowships etc': {
        'headings': [
            'Scholarships',
        ],
    },
    'scientific apparatus collections': {
        'headings': [
            'Scientific apparatus and instruments',
            'Private collections',
        ],
    },
    'scouts and scouting': {
        'headings': [
            'Scouting (Reconnaissance)',
        ],
    },
    'scrapping': {
        'headings': [
            'Salvage (Waste, etc.)',
        ],
    },
    'sea life': {
        'headings': [
            'Seafaring life',
        ],
    },
    'seal': {
        'headings': [
            'Seals (Numismatics)',
        ],
    },
    'seasonal distribution': {
        'headings': [
            'Seasons',
        ],
    },
    'seasonal variations': {
        'headings': [
            'Seasonal variations',
            'Seasons',
        ],
    },
    'seats': {
        'headings': [
            'Seating (Furniture)',
        ],
    },
    'secretions': {
        'headings': [
            'Body fluids',
            'Secretion',
        ],
    },
    'secular employment': {
        'parents': {
            'clergy': [
                'Secular employment of clergy',
            ],
        },
    },
    'security measures': {
        'headings': [
            'Security systems',
        ],
    },
    'selection': {
        'headings': [
            'Selection (Organisms)',
        ],
    },
    'selection indexes': {
        'parents': {
            'breeding': [
                'Selection indexes (Animal breeding)',
            ],
        },
    },
    'selfregulation': {
        'headings': [
            'Self-regulation [of industry]',
        ],
    },
    'seniority system': {
        'parents': {
            'committees': [
                'Seniority system of legislative committees',
            ],
        },
    },
    'sentences': {
        'headings': [
            'Sentences (Grammar)',
        ],
    },
    'separation': {
        'headings': [
            'Separation (Technology)',
        ],
    },
    'service life': {
        'headings': [
            'Service life (Engineering)',
        ],
    },
    'services for': {
        'headings': [
            'Services',
        ],
    },
    'settings': {
        'headings': [
            'Setting (Literature)',
        ],
    },
    'sex factors': {
        'headings': [
            'Sex factors in disease',
        ],
    },
    'sexual behavior': {
        'headings': [
            'Sex',
            'Sexual behavior',
        ],
    },
    'side effects': {
        'headings': [
            'Side effects (Drugs)',
        ],
    },
    'signers': {
        'headings': [
            'Signers of historic documents',
        ],
    },
    'silica content': {
        'headings': [
            'Silica',
        ],
    },
    'skid resistance': {
        'headings': [
            'Surfaces (Technology)',
            'Skid resistance (Materials)',
        ],
    },
    'slide collections': {
        'headings': [
            'Slides (Photography)',
            'Private collections',
        ],
    },
    'slides': {
        'headings': [
            'Slides (Photography)',
        ],
    },
    'social conditions': {
        'headings': [
            'Social history',
        ],
    },
    'social life and customs': {
        'headings': [
            'Manners and customs',
        ],
    },
    'social services': {
        'headings': [
            'Military social work',
        ],
    },
    'societies and clubs': {
        'headings': [
            'Clubs',
            'Societies',
        ],
    },
    'societies etc': {
        'headings': [
            'Associations, institutions, etc.',
            'Learned institutions and societies',
            'Societies',
        ],
    },
    'sociological aspects': {
        'headings': [
            'Sociology',
        ],
    },
    'software': {
        'headings': [
            'Computer software',
        ],
    },
    'songs and music': {
        'headings': [
            'Songs',
        ],
    },
    'sonorants': {
        'headings': [
            'Sonorants (Phonetics)',
        ],
    },
    'sources': {
        'parents': {
            'biography': [
                'Sources',
                'Biographical sources',
            ],
        },
    },
    'spacing': {
        'headings': [
            'Plant spacing',
        ],
    },
    'spectra': {
        'headings': [
            'Spectrum analysis',
        ],
    },
    'spiritual life': {
        'headings': [
            'Monastic and religious life',
        ],
    },
    'spores': {
        'headings': [
            'Plant spores',
        ],
    },
    'springs and suspension': {
        'headings': [
            'Springs (Mechanism)',
        ],
    },
    'spurious and doubtful works': {
        'headings': [
            'Authorship, Disputed',
        ],
    },
    'standardization': {
        'headings': [
            'Standard language',
        ],
    },
    'states': {
        'headings': [
            'State governments',
        ],
    },
    'statistics medical': {
        'headings': [
            'Medical statistics',
        ],
    },
    'statistics vital': {
        'headings': [
            'Vital statistics',
        ],
    },
    'stories plots etc': {
        'headings': [
            'Plots (Drama, novel, etc.)',
        ],
    },
    'strategic aspects': {
        'headings': [
            'Strategic aspects [of individual places]',
        ],
    },
    'structure': {
        'headings': [
            'Chemical structure',
        ],
    },
    'structureactivity relationships': {
        'headings': [
            'Structure-activity relationships (Biochemistry)',
        ],
    },
    'study and teaching': {
        'headings': [
            'Study skills',
            'Teaching',
        ],
    },
    'study and teaching continuing education': {
        'headings': [
            'Continuing education',
        ],
    },
    'study and teaching early childhood': {
        'headings': [
            'Early childhood education',
        ],
    },
    'study and teaching elementary': {
        'headings': [
            'Education, Elementary',
        ],
    },
    'study and teaching graduate': {
        'headings': [
            'Universities and colleges',
            'Graduate work',
        ],
    },
    'study and teaching higher': {
        'headings': [
            'Education, Higher',
        ],
    },
    'study and teaching internship': {
        'headings': [
            'Internship programs',
        ],
    },
    'study and teaching middle school': {
        'headings': [
            'Middle school education',
        ],
    },
    'study and teaching preschool': {
        'headings': [
            'Education, Preschool',
        ],
    },
    'study and teaching primary': {
        'headings': [
            'Education, Primary',
        ],
    },
    'study and teaching secondary': {
        'headings': [
            'Education, Secondary',
        ],
    },
    'style': {
        'headings': [
            'Style, Literary',
        ],
    },
    'style manuals': {
        'parents': {
            'authorship': [
                'Style manuals (Authorship)',
            ],
        },
    },
    'subjectless constructions': {
        'headings': [
            'Subjectless constructions (Grammar)',
        ],
    },
    'subjunctive': {
        'headings': [
            'Subjunctive (Grammar)',
        ],
    },
    'submarine': {
        'parents': {
            'naval operations': [
                'Submarine warfare',
            ],
        },
    },
    'subordinate constructions': {
        'headings': [
            'Subordinate constructions (Grammar)',
        ],
    },
    'substance use': {
        'headings': [
            'Substance abuse',
        ],
    },
    'substitution': {
        'headings': [
            'Substitution (Grammar)',
        ],
    },
    'suffixes and prefixes': {
        'headings': [
            'Suffixes and prefixes (Grammar)',
        ],
    },
    'suicidal behavior': {
        'headings': [
            'Suicide',
            'Suicidal behavior',
        ],
    },
    'summering': {
        'headings': [
            'Summering (Animals)',
        ],
    },
    'supervision of': {
        'headings': [
            'Supervision (Employees)',
        ],
    },
    'suppletion': {
        'headings': [
            'Suppletion (Grammar)',
        ],
    },
    'supplies and stores': {
        'headings': [
            'Military supplies',
        ],
    },
    'supply and demand': {
        'headings': [
            'Supply and demand',
        ],
        'parents': {
            'employees': [
                'Labor supply',
                'Supply and demand',
            ],
        },
    },
    'surfaces': {
        'headings': [
            'Surfaces (Technology)',
        ],
    },
    'susceptibility': {
        'headings': [
            'Disease susceptibility',
        ],
    },
    'suspension': {
        'headings': [
            'Suspension (Employees)',
        ],
    },
    'switchreference': {
        'headings': [
            'Switch-reference (Grammar)',
        ],
    },
    'symbolic aspects': {
        'headings': [
            'Symbolism',
        ],
    },
    'symbolic representation': {
        'headings': [
            'Symbolism',
        ],
    },
    'symbols': {
        'parents': {
            'maps': [
                'Symbols (Maps)',
            ],
        },
    },
    'synonyms and antonyms': {
        'headings': [
            'Synonyms',
            'Antonyms',
        ],
    },
    'synthesis': {
        'headings': [
            'Synthesis (Organic chemistry)',
        ],
    },
    'tables': {
        'headings': [
            'Tables (Systematic lists)',
        ],
    },
    'telephone directories': {
        'headings': [
            'Directories (Telephone)',
        ],
    },
    'television broadcasting of proceedings': {
        'headings': [
            'Television broadcasting of proceedings (Legislative bodies)',
        ],
    },
    'temperature': {
        'headings': [
            'Temperature (Plants)',
        ],
    },
    'tempo': {
        'headings': [
            'Tempo (Phonetics)',
        ],
    },
    'temporal clauses': {
        'headings': [
            'Temporal clauses (Grammar)',
        ],
    },
    'temporal constructions': {
        'headings': [
            'Temporal constructions (Grammar)',
        ],
    },
    'tense': {
        'headings': [
            'Tense (Grammar)',
        ],
    },
    'term of office': {
        'headings': [
            'Legislators',
            'Term of office [of legislators]',
        ],
    },
    'terminology': {
        'headings': [
            'Terms and phrases',
            'Terminology',
        ],
    },
    'territorial questions': {
        'headings': [
            'Boundaries',
            'Boundary disputes',
        ],
    },
    'territoriality': {
        'headings': [
            'Territoriality (Zoology)',
        ],
    },
    'territories and possessions': {
        'headings': [
            'Non-self-governing territories',
            'Territories and possessions [of countries]',
        ],
    },
    'texts': {
        'parents': {
            'liturgy': [
                'Liturgies',
            ],
        },
    },
    'texture': {
        'headings': [
            'Texture (Materials)',
        ],
    },
    'thematic catalogs': {
        'headings': [
            'Music',
            'Thematic catalogs',
        ],
    },
    'therapeutic use': {
        'headings': [
            'Therapeutics',
        ],
    },
    'thermal properties': {
        'headings': [
            'Thermal properties [of chemicals, materials, etc.]',
        ],
    },
    'thermography': {
        'headings': [
            'Medical thermography',
        ],
    },
    'threshold limit values': {
        'headings': [
            'Threshold limit values (Industrial toxicology)',
        ],
    },
    'titles': {
        'headings': [
            'Titles of honor and nobility',
        ],
    },
    'to 1500': {
        'parents': {
            'history': [
                'History, Ancient',
                'Middle Ages',
            ],
        },
    },
    'tomb': {
        'headings': [
            'Tombs',
        ],
    },
    'topic and comment': {
        'headings': [
            'Topic and comment (Grammar)',
        ],
    },
    'toxicology': {
        'headings': [
            'Poisoning',
            'Toxicology',
        ],
    },
    'training': {
        'headings': [
            'Training [of plants and animals]',
        ],
    },
    'training of': {
        'headings': [
            'Employee training',
        ],
    },
    'transcription': {
        'headings': [
            'Transcription',
            'Transcribing services',
        ],
    },
    'transfer': {
        'headings': [   
            'Transfer (Employees)',
        ],
    },
    'transitivity': {
        'headings': [
            'Transitivity (Grammar)',
        ],
    },
    'translating': {
        'headings': [
            'Translating and interpreting',
            'Translating services',
        ],
    },
    'transmission': {
        'headings': [
            'Transmission (Diseases)',
        ],
    },
    'transmutation': {
        'headings': [
            'Transmutation (Linguistics)',
        ],
    },
    'transplantation': {
        'headings': [
            'Transplantation of organs, tissues, etc.',
        ],
        'parents': {
            'embryos': [
                'Embryo transplantation',
            ],
        },
    },
    'transplanting': {
        'headings': [
            'Transplanting (Plant culture)',
        ],
    },
    'transport of sick and wounded': {
        'headings': [
            'Ambulance service',
            'Transportation, Military',
        ],
    },
    'transport properties': {
        'headings': [
            'Transport theory',
            'Transport properties [of chemicals, materials, etc.]',
        ],
    },
    'transport service': {
        'headings': [
            'Transportation, Military',
        ],
    },
    'travel': {
        'headings': [
            'Travel',
            'Voyages and travels',
        ],
    },
    'treatment': {
        'headings': [
            'Therapeutics',
        ],
    },
    'trials litigation etc': {
        'headings': [
            'Trials',
            'Litigation',
        ],
    },
    'trials of vessels': {
        'headings': [
            'Ship trials',
        ],
    },
    'tribal citizenship': {
        'headings': [
            'Citizenship',
            'Citizenship (Tribes)',
        ],
    },
    'trophies': {
        'headings': [
            'Military trophies',
        ],
    },
    'trypanotolerance': {
        'headings': [
            'Immunological tolerance',
        ],
    },
    'turbochargers': {
        'parents': {
            'motors diesel': [
                'Turbochargers (Diesel motors)',
            ],
        },
    },
    'turnover': {
        'parents': {
            'officials and employees': [
                'Labor turnover',
            ],
        },
    },
    'type specimens': {
        'headings': [
            'Type specimens (Natural history)',
        ],
    },
    'ultrasonic imaging': {
        'headings': [
            'Diagnostic ultrasonic imaging',
        ],
    },
    'underground movements': {
        'headings': [
            'Underground movements, War',
        ],
    },
    'union lists': {
        'headings': [
            'Catalogs, Union',
        ],
    },
    'unit cohesion': {
        'headings': [
            'Unit cohesion (Military science)',
        ],
    },
    'use studies': {
        'headings': [
            'Use studies (Information resources)',
        ],
    },
    'utilization': {
        'headings': [
            'Utilization [of natural resources]',
        ],
    },
    'validity': {
        'headings': [
            'Validity (Examinations)',
        ],
    },
    'valves': {
        'headings': [
            'Valves (Motors)',
        ],
    },
    'vapor lock': {
        'headings': [
            'Vapor lock (Fuel systems)',
        ],
    },
    'variation': {
        'parents': {
            'clones': [
                'Variation in Clones (Plants)',
            ],
        },
    },
    'varieties': {
        'headings': [
            'Plant varieties',
        ],
    },
    'verb': {
        'headings': [
            'Verb (Grammar)',
        ],
    },
    'verb phrase': {
        'headings': [
            'Verb phrase (Grammar)',
        ],
    },
    'verbals': {
        'headings': [
            'Verbals (Grammar)',
        ],
    },
    'vertical distribution': {
        'headings': [
            'Vertical distribution (Aquatic biology)',
        ],
    },
    'veterinary service': {
        'headings': [
            'Veterinary service, Military',
        ],
    },
    'viability': {
        'headings': [
            'Viability (Seeds)',
        ],
    },
    'video catalogs': {
        'headings': [
            'Video tapes',
            'Catalogs',
        ],
    },
    'violence against': {
        'headings': [
            'Assault and battery',
            'Violence',
        ],
    },
    'virus diseases': {
        'headings': [
            'Viruses',
        ],
    },
    'viruses': {
        'headings': [
            'Viruses',
        ],
    },
    'vitality': {
        'headings': [
            'Vitality (Plants)',
        ],
    },
    'vocational guidance': {
        'headings': [
            'Occupations',
            'Professions',
            'Vocational guidance',
        ],
    },
    'voice': {
        'headings': [
            'Voice (Grammar)',
        ],
    },
    'voivodeships': {
        'headings': [
            'Polish voivodeships',
        ],
    },
    'voting': {
        'headings': [
            'Voting (Legislative bodies)',
        ],
    },
    'vowel gradation': {
        'headings': [
            'Vowel gradation (Grammar)',
        ],
    },
    'vowel reduction': {
        'headings': [
            'Vowel reduction (Grammar)',
        ],
    },
    'warfare': {
        'headings': [
            'Military art and science',
        ],
    },
    'wars': {
        'headings': [
            'War',
        ],
    },
    'water requirements': {
        'headings': [
            'Water requirements (Agriculture)',
        ],
    },
    'watersupply': {
        'headings': [
            'Water-supply',
        ],
    },
    'weight': {
        'headings': [
            'Weight (Physics)',
        ],
    },
    'will': {
        'headings': [
            'Wills',
        ],
    },
    'wintering': {
        'headings': [
            'Wintering (Animals)',
        ],
    },
    'word formation': {
        'headings': [
            'Word formation (Grammar)',
        ],
    },
    'workload': {
        'headings': [
            'Workload (Employees)',
        ],
    },
    'writing skill': {
        'headings': [
            'Literary style',
        ],
    },
    'yearbooks': {
        'parents': {
            'students': [
                'School yearbooks',
            ],
        },
    },
    'yellow pages': {
        'parents': {
            'telephone directories': [
                'Directories (Telephone)',
                'Yellow pages',
            ],
        },
    },
    'yields': {
        'headings': [
            'Crop yields',
        ],
    },
    'youth': {
        'headings': [
            'Youth and war',
        ],
    },
}


def lcsh_sd_to_facet_values(subdivision, sd_parents=[], default_type='topic',
                            pattern_map=LCSH_SUBDIVISION_PATTERNS,
                            term_map=LCSH_SUBDIVISION_TERM_MAP):
    """
    Convenience/utility function to map the given LCSH `subdivision`
    heading to a group of terms for subject faceting, based on LCSH.
    Returns a list of tuples, [(`facet_type`, `term_value`)]. The
    `facet_type` is just a suggestion and is one of, 'topic', 'region',
    'era', or 'form'.

    The `sd_parents` parameter is used for sub-subdivisions and should
    contain a list of the parent subdivisions of `subdivisions`,
    excluding the first term. I.e.:

    "Corn -- Diseases and pests -- Control"

    "Corn" is the main term (not a subdivision). "Diseases and pests"
    is the first subdivision and has no `sd_parents`. "Control" is the
    second subdivision and has `sd_parents` == ['Diseases and pests'].

    The `default_type` should be whatever facet type designation (i.e.
    topic, region, era, or form) to assign by default.
    """
    def normalize_subdivision(sd):
        return ''.join([ch for ch in sd if ch.isalnum() or ch == ' ']).lower()

    def by_mapping(subdivision, sd_parents):
        norm = normalize_subdivision(subdivision)
        match = term_map.get(norm, None)
        if match:
            parents = match.get('parents', {})
            for sd_parent in sd_parents:
                sd_parent = normalize_subdivision(sd_parent)
                if sd_parent in parents:
                    return parents[sd_parent]
            return match.get('headings')

    def by_pattern(subdivision):
        for pattern, new_terms, _ in pattern_map:
            match = re.search(pattern, subdivision, flags=re.IGNORECASE)
            if match:
                return_values = []
                for ftype, term in new_terms:
                    if '{}' in term:
                        term = term.format(match.group(1))
                        if term:
                            if re.search(r'[A-Z]', term):
                                term = re.sub(r'^([^A-Z]+)', r'', term)
                            else:
                                term = term.capitalize()
                    return_values.append((ftype, term))
                return return_values

    mapped = by_mapping(subdivision, sd_parents)
    if mapped:
        if default_type == 'era':
            return [('topic', v) for v in mapped] + [('era', subdivision)]
        return [(default_type, v) for v in mapped]

    mapped = by_pattern(subdivision)
    if mapped:
        return mapped
    return [(default_type, subdivision)]
