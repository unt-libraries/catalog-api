"""
Get regexes for helping clean up punctuation in MARC data.
"""

# When found in square brackets, these phrases are removed entirely.
BRACKET_DATA_REMOVE = [
    'et al\.'
]
BRACKET_DATA_REMOVE_REGEX = r'({})'.format('|'.join(BRACKET_DATA_REMOVE))

# When found in square brackets, brackets surrounding these phrases are
# retained.
BRACKET_DATA_PROTECT = [
    r'i\.\s*e\.[^\]]*',
    r'sic\.?'
]
BRACKET_DATA_PROTECT_REGEX = r'({})'.format('|'.join(BRACKET_DATA_PROTECT))

# Regular Expression for matching Roman Numerals
ROMAN_NUMERAL_REGEX = r'(?=[MDCLXVI])M*(C[MD]|D?C{0,3})(X[CL]|L?X{0,3})(I[XV]|V?I{0,3})'

# Regular Expression for matching typical MARC ending punctuation
ENDING_PUNCTUATION_REGEX = r'[\.\/;:,]'

# Regular Expression for matching punctuation that should not have
# whitespace to its immediate left
NO_LEFT_WHITESPACE_PUNCTUATION_REGEX = r'[\.;:,]'
