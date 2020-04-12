import os
import json
import logging
from collections import defaultdict
from urllib.parse import urlparse, urlencode, parse_qs
from urllib.request import  urlopen

from md import Mongodb

logger = logging.getLogger(__name__)
KEY = 'AIzaSyBKWCDhu4PumaIgwie_hHw602uOHFWgR1o'
videourl = 'https://www.youtube.com/watch?v=Azr2SA2Ers4'

class YoutubeApi(Mongodb):
    YOUTUBE_COMMENT_URL = 'https://www.googleapis.com/youtube/v3/commentThreads'
    YOUTUBE_SEARCH_URL = 'https://www.googleapis.com/youtube/v3/search'
    YOUTUBE_VIDEO_URL = 'https://www.youtube.com/watch?v='
    commentDetail = defaultdict(list)
    channelVidDetail = defaultdict(list)


    def __init__(self, apiKey, clusterName, dbName, collectionName):
        self.apiKey = apiKey
        super(YoutubeApi, self).__init__(
            clusterName=clusterName, dbName=dbName, collectionName=collectionName
        )

    def get_urlData(self, url, param):
        with urlopen(url + '?' + urlencode(param)) as f:
            data = f.read()
            f.close()
        content = data.decode("utf-8")
        return json.loads(content)

    def parse_videoId(self, url):
        try:
            video_id = urlparse(str(url))
            q = parse_qs(video_id.query)
            vid = q["v"][0]
            return vid
        except:
            logger.error("Invalid YouTube URL")

    def load_commentReplies(self, item):
        if 'replies' in item.keys():
            for reply in item['replies']['comments']:
                self.commentDetail.update({
                    'replyAuthor': reply['snippet']['authorDisplayName'],
                    'replyText': reply["snippet"]["textDisplay"]
                })

    def load_comment(self, data):
        for item in data["items"]:
            comment = item["snippet"]["topLevelComment"]
            detail = {
                'commentId': comment["id"],
                'videoId': item["snippet"]['videoId'],
                'author': comment["snippet"]["authorDisplayName"],
                'text': comment["snippet"]["textDisplay"],
                'likeCount': comment["snippet"]["likeCount"],
                'publishedAt': comment["snippet"]["publishedAt"],
                'updatedAt': comment["snippet"]["updatedAt"],
                'replyCount': item['snippet']['totalReplyCount']
            }
            logger.info('YoutubeApi.load_comment loading {} comment: {}'.format(item["snippet"]['videoId'], detail))
            self.commentDetail[item["snippet"]['videoId']].append(detail)
            # self.load_commentReplies(item)

    def gen_commentByPage(self, params, content):
        try:
            nextPageToken = content.get('nextPageToken')
            while nextPageToken:
                params.update({'pageToken': nextPageToken})
                content = self.get_urlData(self.YOUTUBE_COMMENT_URL, params)
                self.load_comment(content)
                nextPageToken = content.get('nextPageToken')
        except KeyboardInterrupt:
            logger.warning("User Aborted the Operation")
        except:
            logger.error("Cannot Open URL or Fetch comments at a moment")

    def gen_comment(self, url, maxResult=1):
        videoId = self.parse_videoId(url)
        params = {
            'part': "snippet,replies",
            'maxResults': maxResult,
            'videoId': videoId,
            'textFormat': 'plainText',
            'key': self.apiKey
        }
        content = self.get_urlData(self.YOUTUBE_COMMENT_URL, params)
        self.load_comment(content)
        self.gen_commentByPage(params, content)
        return self.commentDetail

    def load_channelVideo(self, data):
        for result in data['items']:
            if result["id"]["kind"] == "youtube#video":
                snippet = result['snippet']
                detail = {
                    'videoId': result['id']['videoId'],
                    'channelName': snippet['channelTitle'],
                    'channelId': snippet['channelId'],
                    'videoName': snippet['title'],
                    'description': snippet['description'],
                    'videoImage': snippet['thumbnails']['default']['url'],
                    'liveBroadcastContent': snippet['liveBroadcastContent'],
                    'publishedAt': snippet['publishedAt']
                }
                self.channelVidDetail[result['id']['videoId']].append(detail)

    def gen_videoByPage(self, params, content):
        try:
            nextPageToken = content.get('nextPageToken')
            while nextPageToken:
                params.update({'pageToken': nextPageToken})
                content = self.get_urlData(self.YOUTUBE_SEARCH_URL, params)
                self.load_channelVideo(content)
                nextPageToken = content.get('nextPageToken')
        except KeyboardInterrupt:
            logger.warning("User Aborted the Operation")
        except:
            logger.error("Cannot Open URL or Fetch comments at a moment")

    def gen_channelVideo(self, channelId, maxResult):
        params = {
            'part': 'id,snippet',
            'channelId': channelId,
            'maxResults': maxResult,
            'key': self.apiKey
        }
        content = self.get_urlData(self.YOUTUBE_SEARCH_URL, params)
        self.load_channelVideo(content)
        self.gen_videoByPage(params, content)
        logger.info(self.channelVidDetail)

class MongoYoutube(YoutubeApi):
    def __init__(self, key, cluster, db, collection):
        super(MongoYoutube, self).__init__(
            apiKey=key, clusterName=cluster, dbName=db, collectionName=collection
        )

    def push_comment(self, comment, dry_run=False):
        if not dry_run:
            self._insert_many(comment)


def main():
    youtubeApi = MongoYoutube(
        key=KEY,
        cluster='raw-comment-chinese',
        db='comment-chinese',
        collection='raw-comment'
    )
    youtubeApi.gen_channelVideo('UC6FcYHEm7SO1jpu5TKjNXEA', 10)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
    )
    main()