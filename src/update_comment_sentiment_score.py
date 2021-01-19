import logging
import argparse
from os import path

from sentiment.sentiment_score import SentimentScore
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
    parser.add_argument('--update-all', action='store_true',
                        help='update all comment sentiment score, \
                              include comment with sentiment score')
    return parser.parse_args()


class MdCommentSentimentUpdater(Mongodb):
    def __init__(self, cluster, db, collection=None):
        super().__init__(
            cluster_name=cluster, db_name=db, collection_name=collection
        )
        self.sentiment_score = SentimentScore()

    def _get_mongodb_collection(self, channel_id):
        return 'comment-{}'.format(channel_id)

    def _valid_message(self, msg):
        return msg.get('sentimentScore', 'field not exist') is not None

    def update(self, filter_obj, text):
        try:
            msg = self.sentiment_score.get(text)
            if self._valid_message(msg):
                self.update_one({'_id': filter_obj}, msg)
                logger.info('updating id:{} text: {}, score: {}'.format(
                    filter_obj, text, msg.get('sentimentScore')))
            else:
                logger.warning('id {}, text {} got invalid message {}'.format(
                    filter_obj, text, msg))
        except Exception as err:
            logger.error('predict_sentiment_score fail with {}'.format(err))

    def update_video(self, video_id):
        for data in self.get({'sentimentScore': {'$exists': True}, 'videoId': video_id}):
            self.update(data.get('_id'), data.get('text'))

    def update_channels(self, channels_id, is_update_all=False):
        get_filter = {}
        if not is_update_all:
            get_filter = {'sentimentScore': {'$exists': False}}
        for channel_id in channels_id:
            self.collection_name = self._get_mongodb_collection(channel_id)
            cursor = self.get(get_filter, True)
            for data in cursor:
                self.update(data.get('_id'), data.get('text'))
            cursor.close()


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
        cluster=args.cluster, db=args.db).update_channels(args.channels_id, args.update_all)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s')
    main()
