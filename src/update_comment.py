#!/usr/bin/env python
import logging
import argparse
import pandas as pd
import json
from time import gmtime, strftime
from os import path
from eyescomment.config import Config
from eyescomment.youtube import YoutubeVideo, YoutubeChannel
from eyescomment.youtube_api import YoutubeApi
from eyescomment.rabbitmq_helper import RabbitMqHelper


logger = logging.getLogger(__name__)
CURRENT_PATH = path.dirname(path.abspath(__file__))
TRAIN_DIR = path.join(
    CURRENT_PATH, 'smart_feautres/eyesComment/data/unlabel')
FEATURES_LABEL = 'feature.json'


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--video-id', default=None,
                        help='youtube video unique id')
    parser.add_argument('--channel-id', nargs='+', default=None,
                        help='youtube channel')
    parser.add_argument('--youtube-api-key',
                        default='AIzaSyBKWCDhu4PumaIgwie_hHw602uOHFWgR1o',
                        help='youtube api key')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show results only, do not plubic')
    parser.add_argument('--save', action='store_true',
                        help='save to csv')
    return parser.parse_args()


class CommentsUnlabelData():
    features_label_dir = path.join(
        CURRENT_PATH, 'features_Label', FEATURES_LABEL)
    front_keys = []
    filter_keys = ['publishedAt', 'updatedAt'] + front_keys

    @property
    def features_dict(self):
        with open(self.features_label_dir, 'r') as js:
            f = json.load(js)
        return f

    def get_column(self, features):
        return self.front_keys + list(pd.unique(list(features.values())))

    def filter_series(self, series):
        for key in self.filter_keys:
            if key in series.keys():
                del series[key]
        return series

    def trans_series_features(self, current_series, features_dict):
        """ transfer current series key to key of json file
        Args:
            current_series: the current series
            features_dict: json file with new feature
        Return:
            filter series
        """

        current_series = self.filter_series(current_series)
        for key in current_series.keys():
            if key in self.filter_keys:
                del current_series[key]
            else:
                transKey = features_dict[key]
                if transKey in current_series:
                    continue
                current_series[transKey] = current_series[key]
                del current_series[key]
        return current_series

    def gen_training_data(self, series):
        df = pd.DataFrame()
        features_dict = self.features_dict
        features_column = self.get_column(features_dict)
        for data in series:
            for data_values in data.values():
                for value in data_values:
                    # logging.info('saving {} policy_name dataframe to csv'.format(ser['video_id']))
                    transSer = self.trans_series_features(value, features_dict)
                    new_df = pd.DataFrame(transSer, columns=features_column, index=[0])
                    df = pd.concat([df, new_df], ignore_index=True)
        return df

    def save(self, file_dir, series):
        df = self.gen_training_data(series)
        file_path = gen_file_path(file_dir)
        df.fillna(0).to_csv(file_path, index=False, encoding='utf-8-sig')


def gen_file_path(file_dir):
    date = strftime("%Y-%m-%d-%H-%M", gmtime())
    file_name = "{}_.csv".format(date)
    return path.join(file_dir, file_name)


class YoutubeCommentHandler(YoutubeApi):
    def __init__(self, key, dry_run):
        self._dry_run = dry_run
        super().__init__(apiKey=key)

    def _publish(self, rabbitmq, channel_id, comments):
        print('rabbitmq:', channel_id)
        rabbitmq.publish({channel_id: comments})

    def _register_video_update(self, video_detail, video):
        new_video_update_times = video['updateTimes'] + 1
        video_detail.patch(
            id=video['id'], json_data={'updateTimes': new_video_update_times})

    def publish_gen_video_comments(self, channel_id, video_id):
        """ Publish video comments by gen Yt
        Args:
            video_id(str)

        Returns:
            comments(dict)

        """
        try:
            rabbitmq = RabbitMqHelper('localhost', 'comment-queue')
            comments = self.gen_comment(video_id, 50)
            if isinstance(channel_id, list):
                channel_id = channel_id[0]
            self._publish(rabbitmq, channel_id, comments)
            return comments
        except Exception as e:
            logger.error('publish_gen_video_comments fail: {}'.format(e))
            return {}
        finally:
            rabbitmq.close()

    def get_comment_detail(self, channel_id):
        video_detail = YoutubeVideo(
            host=Config.instance().get('PORTAL_SERVER'),
            cache_path=Config.instance().get('CACHE_DIR'),
            filter_params={"where": {"channelId": channel_id, "updateTimes": 0}})
        for video in video_detail:
            if video['updateTimes'] <= 0:
                video_id = video['videoId']
                logger.info('getting video id: {} comment'.format(video_id))
                if not self._dry_run:
                    self._register_video_update(video_detail, video)
                    yield self.publish_gen_video_comments(channel_id, video_id)

    def get_videos_comment(self, channels_id):
        for channel_id in channels_id:
            if isinstance(channel_id, dict):
                channel_id = channel_id['channelId']
            logger.info('processing channel id : {} videos'.format(channel_id))
            comment_detail = list(self.get_comment_detail(channel_id))
            if comment_detail:
                yield channel_id, comment_detail


def main():
    args = _parse_args()
    Config.set_dir(path.join(CURRENT_PATH, 'config.json'))
    yt_comment_handler = YoutubeCommentHandler(args.youtube_api_key, args.dry_run)
    if args.video_id and args.channel_id:
        return yt_comment_handler.publish_gen_video_comments(args.channel_id, args.video_id)
    elif not args.channel_id:
        args.channel_id = YoutubeChannel(
            host=Config.instance().get('PORTAL_SERVER'),
            cache_path=Config.instance().get('CACHE_DIR'),
            filter_params={"fields": {"channelId": True}})
    comments_detail = dict(yt_comment_handler.get_videos_comment(args.channel_id))
    if args.save:
        CommentsUnlabelData().save(TRAIN_DIR, comments_detail)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s')
    main()
