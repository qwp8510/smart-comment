import logging
from os.path import join, abspath, dirname
from api import OwnerApi
from youtubeApi import MongoYoutube
from config import Config

KEY = 'AIzaSyBKWCDhu4PumaIgwie_hHw602uOHFWgR1o'
videourl = 'https://www.youtube.com/watch?v=Azr2SA2Ers4'
logger = logging.getLogger(__name__)
CURRENT_PATH = dirname(abspath(__file__))

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


def main():
    youtubeApi = MongoYoutube(
        key=KEY,
        cluster='raw-comment-chinese',
        db='comment-chinese',
        collection='raw-comment'
    )
    # commentDetail = youtubeApi.gen_comment(videourl, 1)
    # for videoId, comment in commentDetail.items():
    #     youtubeApi._insert_many(comment)
    #     logger.info('succedd pushing video Id: {} to mongodb'.format(videoId))
    channelDetail = get_channelDetail()

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
    )
    main()