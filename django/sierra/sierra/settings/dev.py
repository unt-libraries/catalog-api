# MOST of the settings you'll need to set will be in your .env file,
# which is kept out of version control, or your environment variables.
# See .env.template for instructions.

from .base import *

DEBUG = True

# The logging setup from base.py will be used by default, but you can set
# up your own loggers here, if you'd like, to override the default setup.
#
# LOGGING = {}

LOGGING['loggers']['django'] = {
    'handlers': ['console'],
    'level': 'INFO',
    'propagate': False
}

ALLOWED_HOSTS += ['localhost', '127.0.0.1']

DEBUG_TOOLBAR_PATCH_SETTINGS = False

INSTALLED_APPS += (
    'debug_toolbar',
    'sierra'
)

MIDDLEWARE += (
    'debug_toolbar.middleware.DebugToolbarMiddleware',
)

# Django debug toolbar user computer ips
DEBUG_TOOLBAR_CONFIG = {
    'SHOW_TOOLBAR_CALLBACK': lambda x: True,
}
