#!/usr/bin/env python
import logging
import argparse
from os import path
from eyescomment.redis_helper import RedisHelper
from eyescomment.config import Config
from eyescomment.rabbitmq_helper import RabbitMqHelper


logger = logging.getLogger(__name__)
CURRENT_PATH = path.dirname(path.abspath(__file__))


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='localhost',
                        help='redis host')
    parser.add_argument('--port', default=6379,
                        help='Mongodb collection')
    parser.add_argument('--db', default=0,
                        help='Mongodb database')
    return parser.parse_args()


class RedisHandler(RedisHelper):
    def __init__(self, host='localhost', port=6379, db=0):
        super().__init__(host=host, port=port, db=db)

    def push_comment_detail(self, comment_detail):
        for video_id, video_comment_detail in comment_detail.items():
            if not video_comment_detail:
                continue
            for comment in video_comment_detail:
                try:
                    self.update_list(video_id, comment)
                    logger.info(
                        'pushing key: {} video comment detail to mongodb: {}'.format(
                            video_id, comment))
                except Exception as e:
                    logger.error('Fail push_comment_detail: {}'.format(e))

    def push(self, comments_detail):
        for channel_id, channel_comments_detail in comments_detail.items():
            self.push_comment_detail(channel_comments_detail)

    def callback(self, ch, method, properties, body):
        message = eval(body.decode())
        if message:
            self.push(message)


def main():
    args = _parse_args()
    Config.set_dir(path.join(CURRENT_PATH, 'config.json'))
    rabbitmq = RabbitMqHelper('localhost', exchange='comment-queue', exchange_type='fanout')
    redis_handler = RedisHandler(host=args.host, port=args.port, db=args.db)
    while True:
        try:
            rabbitmq.consume(redis_handler.callback)
        except KeyboardInterrupt:
            logger.warning('keyboard interrupt\n')
            break
        except Exception as e:
            logger.error('main exception: {}'.format(e))
            break


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s')
    main()
