import logging
import argparse
import pandas as pd
import json
from time import gmtime, strftime
from os import path
from itertools import chain
from eyescomment.api import OwnerApi
from eyescomment.config import Config
from eyescomment.youtube.channel_api import ChannelApi
from eyescomment.youtube_api import YoutubeApi
from eyescomment.md import Mongodb


logger = logging.getLogger(__name__)
CURRENT_PATH = path.dirname(path.abspath(__file__))
TRAIN_DIR = path.join(
    CURRENT_PATH, 'smart_feautres/eyesComment/data/unlabel')
FEATURES_LABEL = 'feature.json'


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cluster', default='raw-comment-chinese',
                        help='Mongodb cluster')
    parser.add_argument('--db', default='comment-chinese',
                        help='Mongodb database')
    parser.add_argument('--collection', default=None,
                        help='Mongodb collection')
    parser.add_argument('--video-id', default=None,
                        help='youtube video unique id')
    parser.add_argument('--channel-id', nargs='+', default=None,
                        help='youtube channel')
    parser.add_argument('--youtube-api-key',
                        default='AIzaSyBKWCDhu4PumaIgwie_hHw602uOHFWgR1o',
                        help='youtube api key')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show results only, do not plubic')
    parser.add_argument('--save', action='store_true',
                        help='save to csv')
    return parser.parse_args()


class VideoApi(OwnerApi):
    def __init__(self, path):
        super(VideoApi, self).__init__(
            host=Config.instance().get('PORTAL_SERVER'),
            path=path
        )

    def get(self, params=None):
        data = super(VideoApi, self).get(params={"filter": json.dumps(params)})
        return data


class AbstractFeatures():
    features_label_dir = path.join(
        CURRENT_PATH, 'features_Label', FEATURES_LABEL)
    frontKeys = []
    filterKeys = ['publishedAt', 'updatedAt'] + frontKeys

    @property
    def featuresDict(self):
        with open(self.features_label_dir, 'r') as js:
            f = json.load(js)
        return f

    def get_column(self, features):
        return self.frontKeys + list(pd.unique(list(features.values())))

    def filterSeries(self, series):
        for key in self.filterKeys:
            if key in series.keys():
                del series[key]
        return series

    def trans_seriesFeatures(self, currentSeries, featuresDict):
        """ transfer current series key to key of json file
        Args:
            currentSeries: the current series
            featuresDict: json file with new feature
        Return:
            filter series
        """

        currentSeries = self.filterSeries(currentSeries)
        for key in currentSeries.keys():
            if key in self.filterKeys:
                del currentSeries[key]
            else:
                transKey = featuresDict[key]
                if transKey in currentSeries:
                    continue
                currentSeries[transKey] = currentSeries[key]
                del currentSeries[key]
        return currentSeries

    def gen_trainingData(self, series):
        df = pd.DataFrame()
        featuresDict = self.featuresDict
        featuresColumn = self.get_column(featuresDict)
        for data in series:
            for data_values in data.values():
                for value in data_values:
                    # logging.info('saving {} policy_name dataframe to csv'.format(ser['video_id']))
                    transSer = self.trans_seriesFeatures(value, featuresDict)
                    newDf = pd.DataFrame(transSer, columns=featuresColumn, index=[0])
                    df = pd.concat([df, newDf], ignore_index=True)
        return df

    def save(self, file_dir, series):
        df = self.gen_trainingData(series)
        file_path = gen_file_path(file_dir)
        df.fillna(0).to_csv(file_path, index=False, encoding='utf-8-sig')


def gen_file_path(file_dir):
    date = strftime("%Y-%m-%d-%H-%M", gmtime())
    file_name = "{}_.csv".format(date)
    return path.join(file_dir, file_name)


def get_video_id(channel_id):
    channel_api = ChannelApi(
        host=Config.instance().get('PORTAL_SERVER'),
        target_path='Youtube_videos',
        cache_path=Config.instance().get('CACHE_DIR'))
    channels_detail = channel_api.get(params={"where": {"channelId": channel_id}})
    for channel_detail in channels_detail:
        yield channel_detail['videoId']


def get_channel_id():
    channel_api = ChannelApi(
        host=Config.instance().get('PORTAL_SERVER'),
        target_path='Youtube_videos',
        cache_path=Config.instance().get('CACHE_DIR'))
    video_ids = channel_api.get(params={"fields": {"channelId": True}})
    ids = [video_id['channelId'] for video_id in video_ids]
    logger.info('loading channelId id from db')
    return ids


class YoutubeApiHandler(YoutubeApi):
    def __init__(self, key):
        self.key = key
        super().__init__(apiKey=key)

    def get_comment_detail(self, channel_id):
        for video_id in get_video_id(channel_id):
            print('video:', video_id)
            yield video_id, self.gen_comment(video_id, 50)

    def get_videos_comment(self, channels_id):
        for channel_id in channels_id:
            yield channel_id, dict(self.get_comment_detail(channel_id))


class Handler(Mongodb):
    def __init__(
        self,
        cluster='raw-comment-chinese',
        db='comment-chinese',
        collection=None
    ):
        self.cluster = cluster
        self.db = db
        self.collection = collection

    def gen_collection(self, channel_id):
        return 'comment-{}'.format(channel_id)

    def push_comment_detail(self, comment_detail):
        for video_id, video_comments_detail in comment_detail.items():
            post_message = list(chain(*video_comments_detail.values()))
            if post_message:
                self.insert_many(post_message)
                logger.info(
                    'pushing key: {} video comment detail to mongodb: {}'.format(
                        video_id, video_comments_detail)
                )
            else:
                logger.debug(
                    'fial pushing key:{} video comment detail to mongodb: {}'.format(
                        video_id, video_comments_detail)
                )

    def push_collection(self, comments_detail):
        for channel_id, channel_comments_detail in comments_detail.items():
            if not self.collection:
                self.collection = self.gen_collection(channel_id)
            super().__init__(
                cluster_name=self.cluster, db_name=self.db, collection_name=self.collection
            )
            self.push_comment_detail(channel_comments_detail)


def main():
    args = _parse_args()
    Config.set_dir(path.join(CURRENT_PATH, 'config.json'))
    if not args.channel_id:
        args.channel_id = get_channel_id()
    YoutubeApihandler = YoutubeApiHandler(args.youtube_api_key)
    comments_detail = dict(YoutubeApihandler.get_videos_comment(args.channel_id))
    if args.save:
        AbstractFeatures().save(TRAIN_DIR, comments_detail)
    if not args.dry_run:
        Handler(
            cluster=args.cluster,
            db=args.db,
            collection=args.collection
        ).push_collection(comments_detail)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
    )
    main()
