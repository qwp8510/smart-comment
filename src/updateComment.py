import logging
import argparse
import pandas as pd
import json
from time import gmtime, strftime
from os.path import join, abspath, dirname
from requests.exceptions import HTTPError

from api import OwnerApi
from config import Config
from youtube.channelApi import ChannelApi
from youtube.youtubeApi import MongoYoutube

logger = logging.getLogger(__name__)
CURRENT_PATH = dirname(abspath(__file__))
TRAIN_DIR = join(CURRENT_PATH, 'eyesComment/data/unlabel')
FEATURESLABEL = 'feature.json'
APIKEY = [
    'AIzaSyDDa9SL4Rk4oVGj6rHHqzmZmJSIewGCUgg',
    'AIzaSyCGokxpLFG-7M259tOp7-q7fsqYKqvmQNE',
    'AIzaSyD08pO1kEyZ1t7RXQuAyUFlOTyJO68FZYg',
    'AIzaSyBOWzgpes4ryDn0BHthJjj7vcGr1VlpndA',
    'AIzaSyBaFMdTVrz6pJhSosmWNMaailKVWElkjIw'
]
COUNT = 0

def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--video-id', default=None,
                        help='youtube video unique id')
    parser.add_argument('--channel-id', default=None,
                        help='youtube channel')
    parser.add_argument('--youtube-api-key', default='AIzaSyBKWCDhu4PumaIgwie_hHw602uOHFWgR1o',
                        help='youtube api key')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show results only, do not plubic')
    parser.add_argument('--save', action='store_true',
                        help='save to csv')
    return parser.parse_args()

class VideoApi(OwnerApi):
    def __init__(self, path):
        super(VideoApi, self).__init__(
            host=Config(join(CURRENT_PATH, 'lp_config.json')).content['PORTAL_SERVER'], path=path
        )
    
    def get(self, params=None):
        data = super(VideoApi, self).get(params={"filter": json.dumps(params)})
        return data

class AbstractFeatures():
    featuresLabel_dir = join(CURRENT_PATH, 'featuresLabel', FEATURESLABEL)
    frontKeys = []
    filterKeys = ['publishedAt', 'updatedAt'] + frontKeys

    @property
    def featuresDict(self):
        with open(self.featuresLabel_dir, 'r') as js:
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
                    newDf = pd.DataFrame(transSer, columns = featuresColumn, index=[0])
                    df = pd.concat([df, newDf], ignore_index=True)
        return df

    def save(self, fileDIR, series):
        df = self.gen_trainingData(series)
        FILE_DIR = gen_filePath(fileDIR)
        df.fillna(0).to_csv(FILE_DIR, index=False, encoding='utf-8-sig')   

def gen_filePath(fileDir):
    date = strftime("%Y-%m-%d-%H-%M", gmtime())
    fileName = "{}_.csv".format(date)
    filePath = join(fileDir, fileName)
    return filePath

def get_video_id(channel_ids):
    channelApi = ChannelApi(Config(join(CURRENT_PATH, 'lp_config.json')).content['PORTAL_SERVER'], 'Youtube_videos')
    for video_id in channelApi.get(params={"fields": {"videoId": True, "channelId": True}}):
        if video_id['channelId'] in channel_ids:
            yield video_id['videoId']


def get_channel_id():
    channelApi = ChannelApi(Config(join(CURRENT_PATH, 'lp_config.json')).content['PORTAL_SERVER'], 'Youtube_videos')
    video_ids = channelApi.get(params={"fields": {"videoId": True}})
    ids = [video_id['videoId'] for video_id in video_ids]
    logger.info('loading video id from db')
    return ids

class HandleYoutubeApi(MongoYoutube):
    def __init__(
        self,
        key,
        cluster='raw-comment-chinese',
        db='comment-chinese',
        collection='raw-comment'
    ):
        self.key = key
        self.cluster = cluster
        self.dbName = db
        self.collection = collection
        super(HandleYoutubeApi, self).__init__(
            key=key, cluster=cluster, db=db, collection=collection
        )

    def get_comment_detail(self, channel_id, video_id):
        global COUNT
        if video_id:
            yield self.gen_comment(video_id, 50)
        else:
            for video_id in get_video_id(channel_id):
                try:
                    yield self.gen_comment(video_id, 50)
                except Exception as e:
                    logger.warning('fail with exception in get_comment_detail: {}'.format(e))
                    super(HandleYoutubeApi, self).__init__(
                        key=APIKEY[COUNT], cluster=self.cluster, db=self.dbName, collection=self.collection
                    )
                    print(APIKEY[COUNT])
                    COUNT += 1
                    yield self.gen_comment(video_id, 50)
    
    def push_commentDetail(self, commentDetail):
        for video_id, detail in commentDetail.items():
            self.push_comment(detail)
            logger.info('pushing key: {} video comment detail to mongodb: {} '.format(video_id, detail))

    def push(self, commentDetails):
        for commentDetail in commentDetails:
            self.push_commentDetail(commentDetail)

def main():
    args = _parse_args()
    if not args.channel_id:
        args.channel_id = get_channel_id()
    handleYoutubeApi = HandleYoutubeApi(args.youtube_api_key, collection='raw-comment-2020.04.21')
    commentDetails = handleYoutubeApi.get_comment_detail(args.channel_id, args.video_id)
    if args.save:
        AbstractFeatures().save(TRAIN_DIR, commentDetails)
    if not args.dry_run:
        handleYoutubeApi.push(commentDetails)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
    )
    main()