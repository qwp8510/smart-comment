#!/usr/bin/env python
import logging
import subprocess
from os import path
import os
from eyescomment.rabbitmq_helper import RabbitMqTasks


logger = logging.getLogger(__name__)
CURRENT_PATH = path.dirname(path.abspath(__file__))


def run_publish_comment_md():
    cmd = 'python {}/publish_comment_md.py'.format(CURRENT_PATH)
    running_prc = os.popen('ps aux | grep "[p]ublish_comment_md.py"').read()
    if not running_prc:
        logger.info('publish_online_broker run : {}'.format(cmd))
        subprocess.Popen(cmd, shell=True)


def run_publish_comment_redis():
    cmd = 'python {}/publish_comment_redis.py'.format(CURRENT_PATH)
    running_prc = os.popen('ps aux | grep "[p]ublish_comment_redis.py"').read()
    if not running_prc:
        logger.info('publish_online_broker run : {}'.format(cmd))
        subprocess.Popen(cmd, shell=True)


def run_update_comment(channel_id, video_id):
    cmd = 'python {}/update_video_comment.py --channel-id {} --video-id {}'.format(
        CURRENT_PATH, channel_id, video_id)
    logger.info('publish_online_broker run : {}'.format(cmd))
    subprocess.Popen(cmd, shell=True)


def callback(ch, method, properties, body):
    message = eval(body.decode())
    logger.info('rabbitMq consume callback revceive message {}'.format(message))
    if not message:
        return
    channel_id, video_id = message.get('channelId'), message.get('videoId')
    run_update_comment(channel_id, video_id)


def main():
    rabbitmq = RabbitMqTasks(
        'localhost', exchange='', queue_name='online_update_comment', durable=True)
    run_publish_comment_md()
    run_publish_comment_redis()
    while True:
        try:
            rabbitmq.consume(callback)
        except KeyboardInterrupt:
            os.system('pkill -15 python ./publish_comment_md.py')
            os.system('pkill -15 python ./publish_comment_redis.py')
            logger.warning(
                'keyboard interrupt, then kill publish_comment_md.py & publish_comment_redis.py')
            break
        except Exception as e:
            os.system('pkill -15 python ./publish_comment_md.py')
            os.system('pkill -15 python ./publish_comment_redis.py')
            logger.warning(
                'keyboard interrupt, then kill publish_comment_md.py & publish_comment_redis.py')
            logger.error('main exception: {}'.format(e))
            break
    rabbitmq.close()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s')
    main()
