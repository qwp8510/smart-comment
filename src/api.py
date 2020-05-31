import os
from os.path import join, abspath, dirname, exists
import logging
import requests
import json
import time
from requests.exceptions import HTTPError
from .config import Config
from urllib.parse import urlparse, urlencode, parse_qs
from urllib.request import  urlopen

DEFAULT_TIMEOUT = (15, 15)
logger = logging.getLogger(__name__)
CURRENT_PATH = dirname(abspath(__file__))

class Api():
    def __init__(self, host, path):
        self.host = host
        self.path = path
        self.url = host + path
        self._sess = None

    @property
    def sess(self):
        """Get HTTP session.

        Returns:
            A `requests.Session` object.

        """
        if not self._sess:
            self._sess = requests.Session()
        return self._sess

    def get(self, params=None, timeout=DEFAULT_TIMEOUT):
        response = self.sess.get(self.url, params=params, timeout=timeout)
        response.raise_for_status()
        return response.json()

    def post(self, timeout=DEFAULT_TIMEOUT, *args, **kwargs):
        response = self.sess.post(self.url, timeout=timeout, *args, **kwargs)
        response.raise_for_status()
        return response.json()

    def patch(self, id, timeout=DEFAULT_TIMEOUT, **kwargs):
        url = join(self.url, id)
        response = self.sess.patch(url, timeout=timeout, **kwargs)
        response.raise_for_status()
        return response.json()

    def put(self, id, timeout=DEFAULT_TIMEOUT, *args, **kwargs):
        url = join(self.url, id)
        response = self.sess.put(url, timeout=timeout, *args, **kwargs)
        response.raise_for_status()
        return response.json()

    def delete(self, id, timeout=DEFAULT_TIMEOUT, *args, **kwargs):
        url = join(self.url, id)
        response = self.sess.delete(url, timeout=timeout, *args, **kwargs)
        response.raise_for_status()
        return response.json()

class Login(Api):
    userName = 'admin'
    password = 'defaultisnotroot'

    def __init__(self, host, cachePath):
        self.host = host
        self.cachePath = cachePath
        lpConfig = Config(join(CURRENT_PATH, 'lp_config.json')).content
        self.userName = lpConfig.get('userName', self.userName)
        self.password = lpConfig.get('password', self.password)
        self.Config = Config(join(cachePath, 'portal.json'))
        super(Login, self).__init__(host=host, path='Users/login')

    def tokenTimeExpire(self, cacheTime):
        # 604800 = one week
        if abs(time.time() - cacheTime) >= 604800:
            return True

    @property
    def login(self):
        params = {
            'username': self.userName,
            'password': self.password
        }
        accessToken = self.post(data=params)['id']
        data = {
            'referenceTime': time.time(),
            'access_token': accessToken
        }
        self.Config.set_config(data)
        return accessToken

    @property
    def token(self):
        cacheDir = join(self.cachePath, 'portal.json')
        if exists(cacheDir):
            cacheConfig = self.Config.content
            if not self.tokenTimeExpire(cacheConfig.get('referenceTime', 0)): 
                return cacheConfig['access_token']
            else:
                return self.login
        else:
            return self.login

class OwnerApi(Api):
    def __init__(self, host, path, tokenPath='/tmp/smart_comment'):
        self.login = Login(host=host, cachePath=tokenPath)
        super(OwnerApi, self).__init__(host=host, path=path)

    def update_paramsToken(self, params):
        params = params or {}
        params['access_token'] = self.login.token
        return params
    
    def get(self, params=None):
        try:
            return super(OwnerApi, self).get(params=self.update_paramsToken(params))
        except HTTPError as e:
            if e.response.status_code != 401:
                raise
        except Exception as e:
            logger.error('OwnerApi.get fail {}'.format(e))

    def post(self, params=None, **kwargs):
        try:
            return super(OwnerApi, self).post(params=self.update_paramsToken(params), **kwargs)
        except HTTPError as e:
            if e.response.status_code != 401:
                raise
        except Exception as e:
            logger.error('OwnerApi.get fail {}'.format(e))

    def patch(self, id, params=None, jsonData=None):
        """
        Args:
            id: primary id,
            params: params,
            jsonData: post body encoded to 'application/json'
        
        """

        try:
            return super(OwnerApi, self).post(id=id, params=self.update_paramsToken(params), json=jsonData)
        except HTTPError as e:
            if e.response.status_code != 401:
                raise
        except Exception as e:
            logger.error('OwnerApi.get fail {}'.format(e))

    def put(self, id, params=None, jsonData=None):
        """
        Args:
            id: primary id,
            params: params,
            jsonData: post body encoded to 'application/json'
        
        """
        
        try:
            return super(OwnerApi, self).put(id=id, params=self.update_paramsToken(params), json=jsonData)
        except HTTPError as e:
            if e.response.status_code != 401:
                raise
        except Exception as e:
            logger.error('OwnerApi.get fail {}'.format(e))

    def delete(self, id, params=None, jsonData=None):
        """
        Args:
            id: primary id,
            params: params,
            jsonData: post body encoded to 'application/json'
        
        """
        
        try:
            return super(OwnerApi, self).delete(id=id, params=self.update_paramsToken(params), json=jsonData)
        except HTTPError as e:
            if e.response.status_code != 401:
                raise
        except Exception as e:
            logger.error('OwnerApi.get fail {}'.format(e))

