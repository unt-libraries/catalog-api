# MOST of the settings you'll need to set will be in your settings.json
# which is kept out of version control. Copy settings_template.json to
# settings.json, and set the appropriate config settings for your local
# environment in settings.json.

from .base import *

DEBUG = True
TEMPLATE_DEBUG = True

# The logging setup from base.py will be used by default, but you can set
# up your own loggers here, if you'd like, to override the default setup.
#
# LOGGING = {}
