import logging
import argparse
import json

from ..api import OwnerApi
from ..config import Config


class ChannelApi(OwnerApi):
    def __init__(self, host, path):
        super(ChannelApi, self).__init__(
            host=host, path=path
        )
    
    def get(self, params=None):
        data = super(ChannelApi, self).get(params={"filter": json.dumps(params)})
        return data

    def push(self, data):
        super(ChannelApi, self).post(json=data)