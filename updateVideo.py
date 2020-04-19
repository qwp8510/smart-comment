import logging
import argparse
import json
from os.path import join, abspath, dirname
from api import OwnerApi
from youtubeApi import MongoYoutube
from config import Config

# mainKey = 'AIzaSyBKWCDhu4PumaIgwie_hHw602uOHFWgR1o'
# backKEY = 'AIzaSyDDa9SL4Rk4oVGj6rHHqzmZmJSIewGCUgg'
# videourl = 'https://www.youtube.com/watch?v=Azr2SA2Ers4'
logger = logging.getLogger(__name__)
CURRENT_PATH = dirname(abspath(__file__))

def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--youtube-api-key', default='AIzaSyDDa9SL4Rk4oVGj6rHHqzmZmJSIewGCUgg',
                        help='youtube api key')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show results only, do not plubic')
    return parser.parse_args()

class ChannelApi(OwnerApi):
    def __init__(self, path):
        super(ChannelApi, self).__init__(
            host=Config(CURRENT_PATH, 'lp_config.json').content['PORTAL_SERVER'], path=path
        )
    
    def get(self, params=None):
        data = super(ChannelApi, self).get(params={"filter": json.dumps(params)})
        return data

    def push(self, data):
        super(ChannelApi, self).post(json=data)

def get_channelDetail():
    data = ChannelApi('Youtube_channels').get()
    logger.info('data: {}'.format(data))
    return data

def push_channelVideo(videoData):
    try:
        ChannelApi('Youtube_videos').push(data=videoData)
    except Exception as e:
        logger.warning('warning occure push_channelVideo: {}'.format(e))

def get_videoId():
    videoId = ChannelApi('Youtube_videos').get(params={"fields": {"videoId": True}})
    ids = [id['videoId'] for id in videoId]
    logger.info('loading video id from db')
    return ids

def videoIdExist(videoId, videoId_db):
    if videoId in videoId_db:
        return True

def main():
    args = _parse_args()
    youtubeApi = MongoYoutube(
        key=args.youtube_api_key,
        cluster='raw-comment-chinese',
        db='comment-chinese',
        collection='raw-comment'
    )
    channelDetail = get_channelDetail()
    videoId_db = get_videoId()
    for channel in channelDetail[:2]:
        videoDetail = youtubeApi.gen_channelVideo(channel['channelId'], maxResult=40)
        for key, detail in videoDetail.items():
            if not videoIdExist(key, videoId_db):
                logger.info("push data: {}".format(detail))
                push_channelVideo(detail)
            else:
                logger.info("Skip due to videoId '{}' exit".format(key))

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
    )
    main()