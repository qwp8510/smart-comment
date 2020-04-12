import logging
import argparse
import pandas as pd
import json
from time import gmtime, strftime
from os.path import join, abspath, dirname
from api import OwnerApi
from youtubeApi import MongoYoutube
from config import Config

# KEY = 'AIzaSyBKWCDhu4PumaIgwie_hHw602uOHFWgR1o'
# videourl = 'https://www.youtube.com/watch?v=Azr2SA2Ers4'
logger = logging.getLogger(__name__)
CURRENT_PATH = dirname(abspath(__file__))
TRAIN_DIR = join(CURRENT_PATH, 'data', 'unlabel')
FEATURESLABEL = 'feature.json'

def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='localhost',
                        help='Elasticsearch host (default: "localhost").')
    parser.add_argument('--port', type=int, default=9200,
                        help='Elasticsearch port (default: 9200).')
    parser.add_argument('--youtube-url',
                        help='open weather map api key')
    parser.add_argument('--youtube-api-key', default='AIzaSyBKWCDhu4PumaIgwie_hHw602uOHFWgR1o',
                        help='open weather map api key')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show results only, do not plubic')
    return parser.parse_args()

class ChannelApi(OwnerApi):
    def __init__(self):
        super(ChannelApi, self).__init__(
            host=Config(CURRENT_PATH, 'lp_config.json').content['PORTAL_SERVER'], path='Youtube_channels'
        )
    
    def get(self):
        data = super(ChannelApi, self).get()
        return data


def get_channelDetail():
    data = ChannelApi().get()
    logger.info('data: {}'.format(data))
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
        for serDict in series.values():
            for ser in serDict:
                # logging.info('saving {} policy_name dataframe to csv'.format(ser['videoId']))
                transSer = self.trans_seriesFeatures(ser, featuresDict)
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

def main():
    args = _parse_args()
    youtubeApi = MongoYoutube(
        key=args.youtube_api_key,
        cluster='raw-comment-chinese',
        db='comment-chinese',
        collection='raw-comment'
    )

    commentDetail = youtubeApi.gen_comment(args.youtube_url, 1)
    if args.youtube_url:
        AbstractFeatures().save(TRAIN_DIR, commentDetail)
    for videoId, comment in commentDetail.items():
        if not args.dry_run:
            youtubeApi.push_comment(comment, args.dry_run)
            logger.info('succedd pushing video Id: {} to mongodb'.format(videoId))
        
            logger.info('---got youtube url--- {}'.format(args.youtube_url))
    # channelDetail = get_channelDetail()

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
    )
    main()