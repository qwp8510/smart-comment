import logging
import argparse
import time
from os import path

from smart_features.models import SmartFeatures
from eyescomment.md import Mongodb
from eyescomment.config import Config
from eyescomment.youtube import YoutubeChannel


logger = logging.getLogger(__name__)
CURRENT_PATH = path.dirname(path.abspath(__file__))


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cluster', default='raw-comment-chinese',
                        help='Mongodb cluster')
    parser.add_argument('--db', default='comment-chinese',
                        help='Mongodb database')
    parser.add_argument('--collection', default=None,
                        help='Mongodb collection')
    parser.add_argument('--channels-id', nargs='+', default=[],
                        help='youtube channel')
    return parser.parse_args()


class MdCommentSentimentUpdater(Mongodb):
    def __init__(self, cluster, db, collection=None):
        super().__init__(
            cluster_name=cluster, db_name=db, collection_name=collection
        )
        self._model = None

    @property
    def model(self):
        if not self._model:
            self._model = SmartFeatures.model('eyesComment', 'predict')()
        return self._model

    def _get_mongodb_collection(self, channel_id):
        return 'comment-{}'.format(channel_id)

    def predict_sentiment_score(self, text):
        try:
            return self.model.predict(text)
        except Exception as err:
            logger.error('predict_sentiment_score fail with {}'.format(err))

    def _enrich_sentiment_data(self, text):
        return {'sentimentScore': self.predict_sentiment_score(text)}

    def update(self, filter_obj, text):
        self.update_one(filter_obj, self._enrich_sentiment_data(text))

    def update_video(self, video_id):
        for data in self.get({'sentimentScore': {'$exists': True}, 'videoId': video_id}):
            self.update(data.get('_id'), data.get('text'))

    def update_channels(self, channels_id, is_update_all=False):
        get_filter = {}
        if not is_update_all:
            get_filter = {'sentimentScore': {'$exists': False}}
        for channel_id in channels_id:
            self.collection_name = self._get_mongodb_collection(channel_id)
            for data in self.get(get_filter):
                self.update(data.get('_id'), data.get('text'))
                time.sleep(0.05)


def main():
    args = _parse_args()
    Config.set_dir(path.join(CURRENT_PATH, 'config.json'))
    if not args.channels_id:
        yt_channels = YoutubeChannel(
            host=Config.instance().get('PORTAL_SERVER'),
            cache_path=Config.instance().get('CACHE_DIR'),
            filter_params={"fields": {"channelId": True}})
        args.channels_id = [channel_dict.get('channelId') for channel_dict in yt_channels]
    MdCommentSentimentUpdater(
        cluster=args.cluster, db=args.db).update_channels(args.channels_id)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s')
    main()
