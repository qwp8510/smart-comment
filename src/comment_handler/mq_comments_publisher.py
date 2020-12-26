#!/usr/bin/env python
import logging

from eyescomment.md import Mongodb
from eyescomment.youtube import YoutubeVideo
from eyescomment.config import Config


logger = logging.getLogger(__name__)


class MqPublisher():
    PUBLISH_SLICE = 30

    def __init__(self, mq_model, channel_id=''):
        self._mq_model = mq_model
        self._channel_id = channel_id
        self._exist_comments_id = []

    def _set_exist_comments_id(self, video_id):
        comments_doc = list(Mongodb(
            cluster_name='raw-comment-chinese',
            db_name='comment-chinese',
            collection_name='comment-{}'.format(self._channel_id)).get({'videoId': video_id}))
        self._exist_comments_id = [comment_doc.get('commentId', '') for comment_doc in comments_doc]

    def _filter_exist_comment(self, comments_detail):
        for comment_detail in comments_detail:
            comment_id = comment_detail.get('commentId', None)
            if comment_id and comment_id not in self._exist_comments_id:
                yield comment_detail

    def _modify_video_update_times(self, video_id):
        yt_video = YoutubeVideo(
            host=Config.instance().get('PORTAL_SERVER'),
            cache_path=Config.instance().get('CACHE_DIR'),
            filter_params={"where": {"videoId": video_id}})
        for video in yt_video:
            if video_id == video.get('videoId', ''):
                yt_video.patch(
                    id=video['id'], json_data={'updateTimes': video.get('updateTimes', 0) + 1})

    def _publish(self, comments_detail_list):
        for idx in range(0, len(comments_detail_list), self.PUBLISH_SLICE):
            self._mq_model.publish({
                self._channel_id: list(
                    self._filter_exist_comment(comments_detail_list[idx : idx + self.PUBLISH_SLICE]))})

    def publish_video_comment(self, video_comments_detail):
        for video_id, comments_detail in video_comments_detail.items():
            logger.info('publish video id: {} to queue'.format(video_id))
            self._set_exist_comments_id(video_id)
            self._publish(comments_detail)
            self._modify_video_update_times(video_id)

    def publish_videos_comment(self, videos_comment_detail_list):
        for video_comments_detail in videos_comment_detail_list:
            self.publish_video_comment(video_comments_detail)

    def publish_channels_comment(self, channel_comments_detail):
        for channel_id, videos_comment_detail in channel_comments_detail.items():
            self._channel_id = channel_id
            self.publish_videos_comment(videos_comment_detail)
