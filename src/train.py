import logging
import argparse
from smart_features.preprocess import load_train_data, load_comments, load_smart_eyes_data
from smart_features.models import SmartFeatures


logger = logging.getLogger(__name__)
DATA = {'eyesComment': load_train_data, 'text_clustering': load_comments}


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model',
                        help='select model, ex: eyesComment')
    parser.add_argument('--model-behavior', default='train',
                        help='select model working type, default: train')
    return parser.parse_args()


def main():
    args = _parse_args()
    model = SmartFeatures.model(args.model, args.model_behavior)
    model.train(DATA.get(args.model)())


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s')
    main()
