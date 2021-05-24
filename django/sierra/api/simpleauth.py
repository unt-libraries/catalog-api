from __future__ import absolute_import
import hashlib

from rest_framework import authentication
from rest_framework import exceptions

from . import models
from utils import redisobjs as ro


# set up logger, for debugging
import logging
logger = logging.getLogger('sierra.custom')


class SimpleSignatureAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request):
        ret_val = None
        username = request.META.get('HTTP_X_USERNAME', None)
        timestamp = request.META.get('HTTP_X_TIMESTAMP', None)
        client_signature = request.META.get('HTTP_AUTHORIZATION', 'Basic ')
        client_signature = client_signature.split('Basic ')[1]
        body = request.body

        if username and timestamp and client_signature:
            try:
                api_user = models.APIUser.objects.get(user__username=username)
            except models.APIUser.DoesNotExist:
                raise exceptions.AuthenticationFailed('Incorrect username or '
                                                      'password.')

            user_timestamp = ro.RedisObject('user_timestamp', username)
            last_ts = user_timestamp.get() or 0

            if last_ts >= float(timestamp):
                raise exceptions.AuthenticationFailed('Timestamp invalid.')

            secret = api_user.secret
            user = api_user.user

            hasher = hashlib.sha256('{}{}{}{}'.format(username, secret,
                                                      timestamp,
                                                      body).encode('utf-8'))
            server_signature = hasher.hexdigest()

            if server_signature != client_signature:
                raise exceptions.AuthenticationFailed('Incorrect username or '
                                                      'password.')
            else:
                user_timestamp.set(float(timestamp))
                ret_val = (user, None)

        return ret_val



