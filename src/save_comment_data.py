#!/usr/bin/env python
import logging
import argparse
import pandas as pd
from os import path
import json
from time import gmtime, strftime

from comment_handler import YoutubeComments
from eyescomment.config import Config


logger = logging.getLogger(__name__)
CURRENT_PATH = path.dirname(path.abspath(__file__))
TRAIN_DIR = path.join(
    CURRENT_PATH, 'smart_feautres/eyesComment/data/unlabel')
FEATURES_LABEL = 'feature.json'


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--video-id',
                        help='youtube channel')
    parser.add_argument('--youtube-api-key',
                        default='AIzaSyBhsPvi6a5lb7rFsnkqz93v5h65AIn7Nw4',
                        help='youtube api key')
    return parser.parse_args()


def gen_file_path(file_dir):
    date = strftime("%Y-%m-%d-%H-%M", gmtime())
    file_name = "{}_.csv".format(date)
    return path.join(file_dir, file_name)


class CommentsUnlabelData():
    features_label_dir = path.join(
        CURRENT_PATH, 'featuresLabel', FEATURES_LABEL)
    filter_keys = ['publishedAt', 'updatedAt']

    def __init__(self):
        self._features_dict = None

    @property
    def features_dict(self):
        if not self._features_dict:
            with open(self.features_label_dir, 'r') as js:
                self._features_dict = json.load(js)
        return self._features_dict

    def get_column(self, features):
        return list(pd.unique(list(features.values())))

    def _filter(self, data):
        for key in self.filter_keys:
            if key in data.keys():
                del data[key]
        return data

    def trans_data_features(self, video_data):
        """ transfer current series key to key of json file
        Args:
            data: the current series
        Return:
            filter series
        """

        data = self._filter(video_data)
        for key in data.keys():
            trans_key = self.features_dict.get(key, 'Unrecognize_key')
            if trans_key in data:
                continue
            data[trans_key] = data[key]
            del data[key]
        return data

    def gen_training_data(self, video_data):
        df = pd.DataFrame()
        features_column = self.get_column(self.features_dict)
        for video_id, data in video_data.items():
            for comment_detail in data:
                trans_data = self.trans_data_features(comment_detail)
                new_df = pd.DataFrame(trans_data, columns=features_column, index=[0])
                df = pd.concat([df, new_df], ignore_index=True)
        return df

    def save(self, file_dir, video_data):
        df = self.gen_training_data(video_data)
        file_path = gen_file_path(file_dir)
        df.fillna(0).to_csv(file_path, index=False, encoding='utf-8-sig')


def main():
    args = _parse_args()
    Config.set_dir(path.join(CURRENT_PATH, 'config.json'))
    yt_comments = YoutubeComments(args.youtube_api_key)
    if not args.video_id:
        logger.error('main fail with: video id ')
    video_comment_detail = yt_comments.get_video_comment(args.video_id)
    CommentsUnlabelData().save(TRAIN_DIR, video_comment_detail)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s')
    main()
