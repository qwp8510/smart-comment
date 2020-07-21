import pymongo
import logging
import json
from os.path import join, abspath, dirname
from requests import Timeout

from config import Config

logger = logging.getLogger(__name__)
# mongodb = 'mongodb+srv://weichen:defaultisnotroot@raw-comment-chinese-erjue.gcp.mongodb.net/test?retryWrites=true&w=majority'
CURRENT_PATH = dirname(abspath(__file__))
mongodb = 'mongodb+srv://{}:{}@{}-erjue.gcp.mongodb.net/test?retryWrites=true&w=majority'

class Mongodb():
    def __init__(self, cluster_name, db_name, collection_name):
        self.cluster_name = cluster_name
        self.db_name = db_name
        self.collection_name = collection_name
        self.db = self._db

    @property
    def _db(self):
        try:
            mdConfig = Config(join(CURRENT_PATH, 'md_config.json')).content
            cluster = mongodb.format(
                mdConfig.get('userName', 'weichen'),
                mdConfig.get('password', 'defaultisnotroot'),
                self.cluster_name
            )
            return pymongo.MongoClient(cluster)[self.db_name]
        except Timeout:
            logger.error('connect Mongodb with {} {} fail'.format(self.cluster_name, self.db_name))

    @property
    def _collection(self):
        return self.db[self.collection_name]

    def _get(self, filter_params={}):
        results = self._collection.find(filter_params)
        return results
    
    def _insert_one(self, postMessage):
        try:
            self._collection.insert_one(postMessage)
        except:
            logger.debug("insert {} to {} collection fail".format(postMessage, self.collection_name))

    def _insert_many(self, postMessages):
        try:
            self._collection.insert_many(postMessages)
        except:
            logger.debug("insert {} to {} collection fail, insert many format should be [{ },..]".format(postMessages, self.collection_name))

    def _delete_one(self, deleteMessage):
        try:
            logger.warning('deleting deleteMessage')
            self._collection.delete_one(deleteMessage)
        except:
            logger.debug('delete {} to {} collection fail'.format(deleteMessage, self.collection_name))

    def _update_one(self, updateMessage):
        try:
            self._collection.update_one(updateMessage)
        except:
            logger.debug("update {} to {} collection fail".format(updateMessage, self.collection_name))
