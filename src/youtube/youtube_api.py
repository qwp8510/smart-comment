import os
import json
import logging
from collections import defaultdict
from urllib.parse import urlparse, urlencode, parse_qs
from urllib.request import urlopen
from urllib.error import HTTPError


logger = logging.getLogger(__name__)
# KEY = 'AIzaSyBKWCDhu4PumaIgwie_hHw602uOHFWgR1o'
# videourl = 'https://www.youtube.com/watch?v=Azr2SA2Ers4'
API_KEY = [
    'AIzaSyCGokxpLFG-7M259tOp7-q7fsqYKqvmQNE',
    'AIzaSyD08pO1kEyZ1t7RXQuAyUFlOTyJO68FZYg',
    'AIzaSyBOWzgpes4ryDn0BHthJjj7vcGr1VlpndA',
    'AIzaSyBaFMdTVrz6pJhSosmWNMaailKVWElkjIw'
]

class YoutubeApi():
    YOUTUBE_COMMENT_URL = 'https://www.googleapis.com/youtube/v3/commentThreads'
    YOUTUBE_SEARCH_URL = 'https://www.googleapis.com/youtube/v3/search'
    YOUTUBE_VIDEO_URL = 'https://www.youtube.com/watch?v='

    def __init__(self, apiKey):
        self.apiKey = apiKey

    def update_param_api_key(self, url, param):
        logger.warning('Youtube.Api.update param api key')
        def check_http():
            youtube_url = url + '?' + urlencode(param)
            try:
                return urlopen(youtube_url)
            except HTTPError as e:
                logger.error('update_param_api_key fail {} HTTPEroor: {}'.format(youtube_url, e))    
            except Exception as e:
                logger.error('update_param_api_key fail Exception {}'.format(e))    

        for key in API_KEY:
            param.update({'key': key})
            if check_http():
                return param

    def get_urlData(self, url, param):
        content = '{}'
        try:
            youtube_url = url + '?' + urlencode(param)
            with urlopen(youtube_url) as f:
                data = f.read()
                f.close()
            content = data.decode("utf-8")
        except HTTPError as e:
            param = self.update_param_api_key(url, param)
            if param:
                return self.get_urlData(url, param)
            logger.error('Youtube.Api.get_urlData {} HTTPError fail: {}'.format(youtube_url, e))
        except Exception as e:
            logger.error('Youtube.Api.get_urlData fail Exception {}'.format(e))
        return json.loads(content)
                  

    def load_commentReplies(self, item):
        if 'replies' in item.keys():
            for reply in item['replies']['comments']:
                self.commentDetail.update({
                    'replyAuthor': reply['snippet']['authorDisplayName'],
                    'replyText': reply["snippet"]["textDisplay"]
                })

    def load_comment(self, data):
        for item in data.get("items", {}):
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

    def gen_comment(self, videoId=None, maxResult=1):
        self.commentDetail = defaultdict(list)
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
        for result in data.get('items', {}):
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

    def gen_channelVideo(self, channelId, maxResult=1):
        self.channelVidDetail = defaultdict(list)
        params = {
            'part': 'id,snippet',
            'channelId': channelId,
            'maxResults': maxResult,
            'key': self.apiKey
        }
        content = self.get_urlData(self.YOUTUBE_SEARCH_URL, params)
        self.load_channelVideo(content)
        self.gen_videoByPage(params, content)
        logger.info('gen_channelVideo: {}'.format(self.channelVidDetail))
        return self.channelVidDetail

# class MongoYoutube(YoutubeApi):
#     def __init__(self, key, cluster, db, collection):
#         super(MongoYoutube, self).__init__(
#             apiKey=key, clusterName=cluster, dbName=db, collectionName=collection
#         )

#     def push_comment(self, comment, dry_run=False):
#         if not dry_run:
#             self._insert_many(comment)
