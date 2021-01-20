from os import path
import os
import sys
from collections import defaultdict

sys.path.insert(0, "{}/src/".format(os.getcwd()))
from eyescomment.config import Config
from comment_handler.youtube_comments import YoutubeComments


def _valid_comment(comments_details):
    assert len(comments_details) > 0
    assert type(comments_details[0]) == dict
    assert comments_details[0].get('author') is not None
    assert comments_details[0].get('authorChannelId') is not None
    assert comments_details[0].get('text') is not None
    assert comments_details[0].get('videoId') is not None


def test_get_video_comment():
    Config.set_dir(path.join(os.getcwd(), 'src/config.json'))
    yt_comments = YoutubeComments(Config.instance().get('YOUTUBE_API_KEYS')[0])
    video_comments_detail = yt_comments.get_video_comment('RnAXPLG_di8')
    assert type(video_comments_detail) == dict
    assert type(video_comments_detail.get('RnAXPLG_di8')) == list
    _valid_comment(video_comments_detail.get('RnAXPLG_di8'))


def test_get_comments_with_sentiment_score():
    video_id = 'RnAXPLG_di8'
    test_data = defaultdict(list)
    test_data[video_id].append({
        'commentId': 'comment_id',
        'videoId': video_id,
        'authorChannelId': 'author_channel_id',
        'author': 'author display name',
        'text': 'text_display',
        'likeCount': 0,
        'publishedAt': '2019-06-27T13:49:30Z',
        'updatedAt': '2019-06-27T13:49:30Z',
        'replyCount': 1
    })
    Config.set_dir(path.join(os.getcwd(), 'src/config.json'))
    yt_comments = YoutubeComments(
        Config.instance().get('YOUTUBE_API_KEYS')[0])
    result = dict(
        yt_comments._get_comments_with_sentiment_score(test_data))
    assert result.get(video_id) is not None
    assert result.get(video_id)[0].get('text') is not None
    assert result.get(video_id)[0].get('sentimentScore') is not None
