#!/usr/bin/env python
import logging

from eyescomment.config import Config
from eyescomment.youtube import YoutubeVideo
from eyescomment.youtube_api import YoutubeApi
from sentiment.sentiment_score import SentimentScore


logger = logging.getLogger(__name__)


class YoutubeComments(YoutubeApi):
    def __init__(self, key):
        super().__init__(api_key=key)
        self.sentiment_score = SentimentScore()

    def _get_videos_detail(self, channel_id):
        return YoutubeVideo(
            host=Config.instance().get('PORTAL_SERVER'),
            cache_path=Config.instance().get('CACHE_DIR'),
            filter_params={"where": {"channelId": channel_id, "updateTimes": 0}})

    def _get_comments_with_sentiment_score(self, comments_dict):
        def load_sentiment_score_to_comment():
            for comment in comments:
                comment.update(self.sentiment_score.get(comment.get('text')))
                yield comment

        for video_id, comments in comments_dict.items():
            comments = filter(lambda x: x.get('text') is not None and x.get('text') != '', comments)
            yield video_id, list(load_sentiment_score_to_comment())

    def get_video_comment(self, video_id):
        try:
            return dict(self._get_comments_with_sentiment_score(self.gen_comment(video_id, 50)))
        except Exception as err:
            logger.error('get_video_comment fail with: {}'.format(err))
            return {}

    def get_videos_comment(self, channel_id):
        for videos_detail in self._get_videos_detail(channel_id):
            logger.info('getting video id: {} comment'.format(videos_detail.get('videoId'), ''))
            yield self.get_video_comment(videos_detail.get('videoId', ''))

    def get_channels_comment(self, channels_id):
        for channel_id in channels_id:
            logger.info('processing channel id : {} videos'.format(channel_id))
            videos_comment_detail = list(self.get_videos_comment(channel_id))
            if videos_comment_detail:
                yield channel_id, videos_comment_detail
