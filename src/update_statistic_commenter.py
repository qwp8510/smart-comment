import logging
import argparse
from os import path, popen

from eyescomment.md import Mongodb
from eyescomment.config import Config
from eyescomment.youtube import YoutubeChannel


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
    parser.add_argument('--channels-id', nargs='+', default=[],
                        help='youtube channels id')
    return parser.parse_args()


def _get_commenter_statistic(code):
    hive_sql = '"select author, count(author) from  comment_{} group by author"'.format(code)
    result = popen(Config.instance().get('HIVE_CMD').format(hive_sql)).read()
    for line in result.splitlines():
        yield line.split('\t')


def _get_statistic_collection(code):
    return 'statistic-{}'.format(code)


class MdStatisticCommenter(Mongodb):
    def __init__(self, cluster, db, collection=None):
        self._commenter = None
        super().__init__(
            cluster_name=cluster, db_name=db, collection_name=collection
        )

    @property
    def exist_commenter(self):
        def get_commenter():
            for data in self.get():
                yield data.get('author')
        if not self._commenter:
            self._commenter = list(get_commenter())
            return self._commenter
        return self._commenter

    def _get_patch_message(self, comment_num):
        return {'commentNum': comment_num}

    def _get_create_message(self, commenter, comment_num):
        return {'author': commenter,
                'commentNum': comment_num}

    def _is_commenter_exist(self, commenter):
        return commenter in self.exist_commenter

    def _invalid_length(self, length):
        if length != 2:
            logger.error('raw_data {} got invalid format'.format(length))
            return True

    def _null_commenter(self, commenter):
        if commenter == 'NULL':
            return True

    def _is_invalid_raw_data(self, raw_data):
        return self._invalid_length(len(raw_data)) or \
               self._null_commenter(raw_data[0])

    def _patch(self, commenter, comment_num):
        self.update_one({'author': commenter}, self._get_patch_message(comment_num))
        logger.info('patch author:{}, commentNum: {}'.format(commenter, comment_num))

    def _create(self, commenter, comment_num):
        self.insert_one(self._get_create_message(commenter, comment_num))
        logger.info('create author:{}, commentNum: {}'.format(commenter, comment_num))

    def update_data(self, statistic_data):
        def update():
            if self._is_invalid_raw_data(raw_data):
                return
            try:
                commenter, comment_num = raw_data[0], int(raw_data[1])
                if self._is_commenter_exist(commenter):
                    self._patch(commenter, comment_num)
                else:
                    self._create(commenter, comment_num)
            except ValueError:
                logger.error('raw_data {} index 1 is not integer'.format(raw_data))
            except Exception as err:
                logger.error('update fail with {}'.format(err))

        for raw_data in statistic_data:
            update()


def _get_channels_id_code(channels_id):
    yt_channels = YoutubeChannel(
        host=Config.instance().get('PORTAL_SERVER'),
        cache_path=Config.instance().get('CACHE_DIR'))
    for channel in yt_channels:
        if channels_id != [] and channel.get('channelId') not in channels_id:
            continue

        yield channel.get('channelId'), channel.get('code')


def main():
    args = _parse_args()
    Config.set_dir(path.join(CURRENT_PATH, 'config.json'))
    for channel_id, code in list(_get_channels_id_code(args.channels_id)):
        statistic_data = list(_get_commenter_statistic(code))
        md_statistic_commenter = MdStatisticCommenter(
            cluster=args.cluster,
            db=args.db,
            collection=_get_statistic_collection(channel_id))
        md_statistic_commenter.update_data(statistic_data)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s')
    main()
