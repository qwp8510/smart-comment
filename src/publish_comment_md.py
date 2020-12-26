#!/usr/bin/env python
import logging
import argparse
from os import path
from eyescomment.md import Mongodb
from eyescomment.config import Config
from eyescomment.rabbitmq_helper import RabbitMqFanout


logger = logging.getLogger(__name__)
CURRENT_PATH = path.dirname(path.abspath(__file__))


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cluster', default='raw-comment-chinese',
                        help='Mongodb cluster')
    parser.add_argument('--db', default='comment-chinese',
                        help='Mongodb database')
    parser.add_argument('--collection', default=None,
                        help='Mongodb collection')
    parser.add_argument('--rabbitmq-host',
                        default='localhost',
                        help='rabbitmq host default: localhost')
    parser.add_argument('--rabbitmq-queue',
                        default='comment-queue',
                        help='default: comment-queue')
    return parser.parse_args()


class MdHandler(Mongodb):
    def __init__(
        self,
        cluster='raw-comment-chinese',
        database='comment-chinese',
        collection=None
    ):
        self.cluster = cluster
        self.database = database
        self.cl = collection

    def gen_collection(self, channel_id):
        return 'comment-{}'.format(channel_id)

    def push_comments_detail(self, comments_detail):
        for comment_detail in comments_detail:
            if not comment_detail:
                continue
            self.insert_one(comment_detail)
            logger.info(
                'pushing key: {} video comment detail to mongodb: {}'.format(
                    comment_detail.get('videoId', 'Miss_videoId'), comment_detail))

    def push_collection(self, comments_detail):
        for channel_id, channel_comments_detail in comments_detail.items():
            if self.cl:
                self.collection = self.cl
            else:
                self.collection = self.gen_collection(channel_id)
            super().__init__(
                cluster_name=self.cluster, db_name=self.database, collection_name=self.collection)
            self.push_comments_detail(channel_comments_detail)

    def callback(self, ch, method, properties, body):
        message = eval(body.decode())
        if message:
            self.push_collection(message)


def main():
    args = _parse_args()
    Config.set_dir(path.join(CURRENT_PATH, 'config.json'))
    mq_fanout = RabbitMqFanout(args.rabbitmq_host, args.rabbitmq_queue)
    md_handler = MdHandler(
        cluster=args.cluster,
        database=args.db,
        collection=args.collection)
    while True:
        try:
            mq_fanout.consume(md_handler.callback)
        except KeyboardInterrupt:
            logger.warning('keyboard interrupt\n')
            break
        except Exception as e:
            logger.error('main exception: {}'.format(e))
            break
        finally:
            mq_fanout.close()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s')
    main()
