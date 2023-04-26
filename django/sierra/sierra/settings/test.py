from .base import *

# Check required TEST settings
required = ['TEST_SIERRA_DB_USER', 'TEST_SIERRA_DB_PASSWORD',
            'TEST_DEFAULT_DB_USER', 'TEST_DEFAULT_DB_PASSWORD']

for setting in required:
    if get_env_variable(setting) is None:
        raise_setting_error(setting)

DEBUG = True
TESTING = True

TIME_ZONE = 'America/Chicago'

ALLOWED_HOSTS += (
    'testserver',
)

INSTALLED_APPS += (
    'base.tests.vcftestmodels',
    'sierra.tests.testmodels',
    'sierra'
)

DATABASES = {
    'sierra': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': get_env_variable('TEST_SIERRA_DB_NAME', 'sierra_test'),
        'USER': get_env_variable('TEST_SIERRA_DB_USER'),
        'PASSWORD': get_env_variable('TEST_SIERRA_DB_PASSWORD'),
        'HOST': get_env_variable('TEST_SIERRA_DB_HOST', '127.0.0.1'),
        'PORT': get_env_variable('TEST_SIERRA_DB_PORT', '5432'),
        'TEST': {
            'NAME': get_env_variable('TEST_SIERRA_DB_NAME', 'sierra_test')
        }
    },
    'default': {
        'ENGINE': get_env_variable('TEST_DEFAULT_DB_ENGINE',
                                   'django.db.backends.mysql'),
        'NAME': get_env_variable('TEST_DEFAULT_DB_NAME', 'capi_test'),
        'USER': get_env_variable('TEST_DEFAULT_DB_USER'),
        'PASSWORD': get_env_variable('TEST_DEFAULT_DB_PASSWORD'),
        'HOST': get_env_variable('TEST_DEFAULT_DB_HOST', '127.0.0.1'),
        'PORT': get_env_variable('TEST_DEFAULT_DB_PORT', '3307'),
        'TEST': {
            'NAME': get_env_variable('TEST_DEFAULT_DB_NAME', 'capi_test')
        }
    }
}

# CELERY settings
REDIS_CELERY_PORT = get_env_variable('TEST_REDIS_CELERY_PORT', '6279')
REDIS_CELERY_HOST = get_env_variable('TEST_REDIS_CELERY_HOST', '127.0.0.1')
REDIS_CELERY_PASSWORD = get_env_variable('TEST_REDIS_CELERY_PASSWORD')
if REDIS_CELERY_PASSWORD:
    rc_pw = f':{REDIS_CELERY_PASSWORD}@'
else:
    rc_pw = ''
redis_celery_url = f'redis://{rc_pw}{REDIS_CELERY_HOST}:{REDIS_CELERY_PORT}/0'
CELERY_BROKER_URL = redis_celery_url
CELERY_RESULT_BACKEND = redis_celery_url
CELERY_WORKER_SEND_TASK_EVENTS = True

# Other Redis settings
REDIS_CONNECTION = {
    'host': get_env_variable('TEST_REDIS_APPDATA_HOST', '127.0.0.1'),
    'port': get_env_variable('TEST_REDIS_APPDATA_PORT', '6280'),
    'db': get_env_variable('TEST_REDIS_APPDATA_DATABASE', 0),
    'password': get_env_variable('TEST_REDIS_APPDATA_PASSWORD')
}

# Solr settings
SOLR_PORT_FOR_UPDATE = get_env_variable('TEST_SOLR_PORT_FOR_UPDATE', '8883')
SOLR_PORT_FOR_SEARCH = get_env_variable('TEST_SOLR_PORT_FOR_SEARCH', '8884')
SOLR_HOST_FOR_UPDATE = get_env_variable('TEST_SOLR_HOST_FOR_UPDATE', '127.0.0.1')
SOLR_HOST_FOR_SEARCH = get_env_variable('TEST_SOLR_HOST_FOR_SEARCH', '127.0.0.1')
solr_base_url_for_update = f'http://{SOLR_HOST_FOR_UPDATE}:{SOLR_PORT_FOR_UPDATE}/solr'
solr_base_url_for_search = f'http://{SOLR_HOST_FOR_SEARCH}:{SOLR_PORT_FOR_SEARCH}/solr'

SOLR_HAYSTACK_URL_FOR_UPDATE = get_env_variable(
    'TEST_SOLR_HAYSTACK_UPDATE_URL',
    f'{solr_base_url_for_update}/haystack'
)
SOLR_HAYSTACK_URL_FOR_SEARCH = get_env_variable(
    'TEST_SOLR_HAYSTACK_SEARCH_URL',
    f'{solr_base_url_for_search}/haystack'
)
SOLR_DISCOVER01_URL_FOR_UPDATE = get_env_variable(
    'TEST_SOLR_DISCOVER01_UPDATE_URL',
    f'{solr_base_url_for_update}/discover-01'
)
SOLR_DISCOVER01_URL_FOR_SEARCH = get_env_variable(
    'TEST_SOLR_DISCOVER01_SEARCH_URL',
    f'{solr_base_url_for_search}/discover-01'
)
SOLR_DISCOVER02_URL_FOR_UPDATE = get_env_variable(
    'TEST_SOLR_DISCOVER02_UPDATE_URL',
    f'{solr_base_url_for_update}/discover-02'
)
SOLR_DISCOVER02_URL_FOR_SEARCH = get_env_variable(
    'TEST_SOLR_DISCOVER02_SEARCH_URL',
    f'{solr_base_url_for_search}/discover-02'
)

HAYSTACK_CONNECTIONS['default']['URL'] = SOLR_HAYSTACK_URL_FOR_UPDATE
HAYSTACK_CONNECTIONS['haystack|update']['URL'] = SOLR_HAYSTACK_URL_FOR_UPDATE
HAYSTACK_CONNECTIONS['haystack|search']['URL'] = SOLR_HAYSTACK_URL_FOR_SEARCH
HAYSTACK_CONNECTIONS['discover-01|update']['URL'] = SOLR_DISCOVER01_URL_FOR_UPDATE
HAYSTACK_CONNECTIONS['discover-01|search']['URL'] = SOLR_DISCOVER01_URL_FOR_SEARCH
HAYSTACK_CONNECTIONS['discover-02|update']['URL'] = SOLR_DISCOVER02_URL_FOR_UPDATE
HAYSTACK_CONNECTIONS['discover-02|search']['URL'] = SOLR_DISCOVER02_URL_FOR_SEARCH
