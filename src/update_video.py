import logging
import argparse
from os import path
from eyescomment.config import Config
from eyescomment.youtube import YoutubeVideo, YoutubeChannel
from eyescomment.youtube_api import YoutubeApi


logger = logging.getLogger(__name__)
CURRENT_PATH = path.dirname(path.abspath(__file__))


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--youtube-api-key',
                        default='AIzaSyBKWCDhu4PumaIgwie_hHw602uOHFWgR1o',
                        help='youtube api key')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show results only, do not plubic')
    return parser.parse_args()


def video_id_exist(video_id, video_id_db):
    if video_id in video_id_db:
        return True


def main():
    args = _parse_args()
    Config.set_dir(path.join(CURRENT_PATH, 'config.json'))
    youtube_api = YoutubeApi(args.youtube_api_key)
    channels_detail = YoutubeChannel(
        host=Config.instance().get('PORTAL_SERVER'),
        cache_path=Config.instance().get('CACHE_DIR'))
    videos = YoutubeVideo(
        host=Config.instance().get('PORTAL_SERVER'),
        cache_path=Config.instance().get('CACHE_DIR'),
        filter_params={"fields": {"videoId": True}})
    logger.info('proccess loading video id')
    video_id_series = [video['videoId'] for video in videos]
    for channel in channels_detail:
        logger.info('gen videos by channel id {}'.format(channel['channelId']))
        video_detail = youtube_api.gen_channel_video(
            channel['channelId'], max_result=50)
        if args.dry_run:
            return
        for key, detail in video_detail.items():
            if not video_id_exist(key, video_id_series):
                logger.info("push data: {}".format(detail))
                videos.push(detail)
            else:
                logger.info("Skip due to videoId '{}' exit".format(key))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
    )
    main()
