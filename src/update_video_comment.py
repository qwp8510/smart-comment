#!/usr/bin/env python
import logging
import argparse
from os import path
from eyescomment.config import Config
from eyescomment.rabbitmq_helper import RabbitMqFanout
from comment_handler import YoutubeComments, MqPublisher


logger = logging.getLogger(__name__)
CURRENT_PATH = path.dirname(path.abspath(__file__))


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--video-id', default='',
                        help='youtube video unique id')
    parser.add_argument('--channel-id', default='',
                        help='youtube channel')
    parser.add_argument('--youtube-api-key',
                        default='AIzaSyBhsPvi6a5lb7rFsnkqz93v5h65AIn7Nw4',
                        help='youtube api key')
    parser.add_argument('--rabbitmq-host',
                        default='localhost',
                        help='rabbitmq host default: localhost')
    parser.add_argument('--rabbitmq-queue',
                        default='online-comment-queue',
                        help='default: online-comment-queue')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show results only, do not plubish')
    return parser.parse_args()


def main():
    args = _parse_args()
    Config.set_dir(path.join(CURRENT_PATH, 'config.json'))
    yt_comments = YoutubeComments(args.youtube_api_key)
    video_comment_detail = dict(yt_comments.get_video_comment(args.video_id))
    if args.dry_run:
        exit()
    fanout_mq = RabbitMqFanout(args.rabbitmq_host, args.rabbitmq_queue)
    try:
        MqPublisher(
            mq_model=fanout_mq, channel_id=args.channel_id
            ).publish_video_comment(video_comment_detail)
    except Exception as err:
        logger.error('MqCommentsPublisher fail with: {}'.format(err))
    finally:
        fanout_mq.close()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s')
    main()
