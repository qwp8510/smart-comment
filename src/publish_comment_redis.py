#!/usr/bin/env python
import logging
import argparse
from os import path
from eyescomment.redis_helper import RedisHelper
from eyescomment.config import Config
from eyescomment.rabbitmq_helper import RabbitMqFanout


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
    parser.add_argument('--rabbitmq-host',
                        default='localhost',
                        help='rabbitmq host default: localhost')
    parser.add_argument('--rabbitmq-queue',
                        default='comment-queue',
                        help='default: online-comment-queue')
    return parser.parse_args()


class RedisHandler(RedisHelper):
    def __init__(self, host='localhost', port=6379, db=0):
        super().__init__(host=host, port=port, db=db)

    def push_comment_detail(self, comments_detail):
        for comment_detail in comments_detail:
            if not comment_detail:
                continue
            try:
                self.update_list(comment_detail.get('videoId', 'Miss_videoId'), comment_detail)
                logger.info(
                    'pushing key: {} video comment detail to redis: {}'.format(
                        comment_detail.get('videoId', 'Miss_videoId'), comment_detail))
            except Exception as e:
                logger.error('push_comment_detail fail with: {} to redis: {}'.format(
                    comment_detail.get('videoId', 'Miss_videoId'), e))

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
    mq_fanout = RabbitMqFanout(args.rabbitmq_host, args.rabbitmq_queue)
    redis_handler = RedisHandler(host=args.host, port=args.port, db=args.db)
    while True:
        try:
            mq_fanout.consume(redis_handler.callback)
        except KeyboardInterrupt:
            logger.warning('publish_comment_redis keyboard interrupt\n')
            break
        except Exception as e:
            logger.error('publish_comment_redis main exception: {}'.format(e))
            break
        finally:
            mq_fanout.close()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s')
    main()
