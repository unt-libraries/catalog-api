from .base import *

# These are the production settings loaded by Apache.

if len(ALLOWED_HOSTS) == 0:
    raise_setting_error('ALLOWED_HOSTS')

if STATIC_ROOT is None:
    raise_setting_error('STATIC_ROOT')

DEBUG = False
TEMPLATE_DEBUG = False

# SolrMarc
SOLRMARC_CONFIG_FILE = 'production_config.properties'

# The logging setup from base.py will be used by default, but you can set
# up your own loggers here, if you'd like, to override the default setup.
#
# LOGGING = {}