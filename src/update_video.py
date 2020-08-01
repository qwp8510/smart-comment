import logging
import argparse
import json
from os.path import join, abspath, dirname
from api import OwnerApi
from config import Config
from youtube.channel_api import ChannelApi
from youtube.youtube_api import YoutubeApi


logger = logging.getLogger(__name__)
CURRENT_PATH = dirname(abspath(__file__))


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--youtube-api-key', default='AIzaSyDDa9SL4Rk4oVGj6rHHqzmZmJSIewGCUgg',
                        help='youtube api key')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show results only, do not plubic')
    return parser.parse_args()


def get_channel_detail():
    data = ChannelApi(Config(join(CURRENT_PATH, 'lp_config.json')).content['PORTAL_SERVER'], 'Youtube_channels').get()
    logger.info('data: {}'.format(data))
    return data


def push_channel_video(videoData):
    try:
        ChannelApi(Config(join(CURRENT_PATH, 'lp_config.json')).content['PORTAL_SERVER'], 'Youtube_videos').push(data=videoData)
    except Exception as e:
        logger.warning('warning occure push_channel_video: {}'.format(e))


def get_video_id():
    channel_api = ChannelApi(Config(join(CURRENT_PATH, 'lp_config.json')).content['PORTAL_SERVER'], 'Youtube_videos')
    video_ids = channel_api.get(params={"fields": {"videoId": True}})
    ids = [video_id['videoId'] for video_id in video_ids]
    logger.info('loading video id from db')
    return ids


def video_id_exist(video_id, video_id_db):
    if video_id in video_id_db:
        return True


def main():
    args = _parse_args()
    youtube_api = YoutubeApi(args.youtube_api_key)
    channel_detail = get_channel_detail()
    video_id_db = get_video_id()
    for channel in channel_detail:
        video_detail = youtube_api.gen_channel_video(channel['channelId'], max_result=40)
        if args.dry_run:  return 
        for key, detail in video_detail.items():
            if not video_id_exist(key, video_id_db):
                logger.info("push data: {}".format(detail))
                push_channel_video(detail)
            else:
                logger.info("Skip due to videoId '{}' exit".format(key))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
    )
    main()