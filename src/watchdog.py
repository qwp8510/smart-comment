#!/usr/bin/env python
import logging
import subprocess
import os
from time import sleep


logger = logging.getLogger(__name__)


def monitor_process_exist():
    f = 'publish_comment.py'
    running_prc = os.popen('ps aux | grep "[p]ublish_comment.py"').read()
    if not running_prc:
        logger.info('Wachdog triger file: {}'.format(f))
        cmd = './{}'.format(f)
        subprocess.Popen(cmd, shell=True)


def main():
    while True:
        try:
            monitor_process_exist()
            sleep(10)
        except KeyboardInterrupt:
            os.system('pkill -15 python ./publish_comment_md.py')
            logger.warning('keyboard interrupt, then kill publish_comment_md.py')
            break
        except Exception as e:
            os.system('pkill -15 python ./publish_comment_md.py')
            logger.error('main exception: {}'.format(e))
            break


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s')
    main()
