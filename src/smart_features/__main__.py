import logging
import argparse
from preprocess import load_train_data, load_comments, load_smart_eyes_data
from eyesComment import train as eyescomment
from text_clustering import train as text_clustering


logger = logging.getLogger(__name__)


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--trainer',
                        help='select training model, ex: eyescomment')
    return parser.parse_args()


def run_trainer(trainer):
    logger.info('ready training feature: {}'.format(trainer))
    if trainer == 'eyescomment':
        eyescomment.train(load_train_data())
    elif trainer == 'text_clustering':
        text_clustering.train(load_comments())


def main():
    args = _parse_args()
    run_trainer(args.trainer)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
    )
    main()